%% -------- SKIPLIST ---------
%%
%% For storing small numbers of {K, V} pairs where reasonable insertion and
%% fetch times, but with fast support for flattening to a list or a sublist
%% within a certain key range
%%
%% Used instead of gb_trees to retain compatability of OTP16 (and Riak's
%% ongoing dependency on OTP16)
%%
%% Not a proper skip list.  Only supports a fixed depth.  Good enough for the
%% purposes of leveled.  Also uses peculiar enkey_passed function within
%% leveled.  Not tested beyond a depth of 2.

-module(leveled_skiplist).

-include("include/leveled.hrl").

-export([
        from_list/1,
        from_list/2,
        from_sortedlist/1,
        from_sortedlist/2,
        to_list/1,
        enter/3,
        enter/4,
        enter_nolookup/3,
        to_range/2,
        to_range/3,
        lookup/2,
        lookup/3,
        empty/0,
        empty/1,
        size/1
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(SKIP_WIDTH, 16).
-define(LIST_HEIGHT, 2).
-define(INFINITY_KEY, {null, null, null, null, null}).
-define(BITARRAY_SIZE, 2048).

%%%============================================================================
%%% SkipList API
%%%============================================================================

enter(Key, Value, SkipList) ->
    Hash = leveled_codec:magic_hash(Key),
    enter(Key, Hash, Value, SkipList).

enter(Key, Hash, Value, SkipList) ->
    Bloom0 =
        case element(1, SkipList) of
            list_only ->
                list_only;
            Bloom ->
                leveled_tinybloom:enter({hash, Hash}, Bloom)
        end,
    {Bloom0,
        enter(Key, Value, erlang:phash2(Key),
                element(2, SkipList),
                ?SKIP_WIDTH, ?LIST_HEIGHT)}.

%% Can iterate over a key entered this way, but never lookup the key
%% used for index terms
%% The key may still be a marker key - and the much cheaper native hash
%% is used to dtermine this, avoiding the more expensive magic hash
enter_nolookup(Key, Value, SkipList) ->
    {element(1, SkipList),
        enter(Key, Value, erlang:phash2(Key),
                element(2, SkipList),
                ?SKIP_WIDTH, ?LIST_HEIGHT)}.

from_list(UnsortedKVL) ->
    from_list(UnsortedKVL, false).

from_list(UnsortedKVL, BloomProtect) ->
    KVL = lists:ukeysort(1, UnsortedKVL),
    from_sortedlist(KVL, BloomProtect).

from_sortedlist(SortedKVL) ->
    from_sortedlist(SortedKVL, false).

from_sortedlist(SortedKVL, BloomProtect) ->
    Bloom0 =
        case BloomProtect of
            true ->
                lists:foldr(fun({K, _V}, Bloom) ->
                                        leveled_tinybloom:enter(K, Bloom) end,
                                    leveled_tinybloom:empty(?SKIP_WIDTH),
                                    SortedKVL);
            false ->
                list_only
    end,
    {Bloom0, from_list(SortedKVL, ?SKIP_WIDTH, ?LIST_HEIGHT)}.

lookup(Key, SkipList) ->
    case element(1, SkipList) of
        list_only ->
            list_lookup(Key, element(2, SkipList), ?LIST_HEIGHT);
        _ ->
            lookup(Key, leveled_codec:magic_hash(Key), SkipList)
    end.
    
lookup(Key, Hash, SkipList) ->
    case leveled_tinybloom:check({hash, Hash}, element(1, SkipList)) of
        false ->
            none;
        true ->
            list_lookup(Key, element(2, SkipList), ?LIST_HEIGHT)
    end.


%% Rather than support iterator_from like gb_trees, will just an output a key
%% sorted list for the desired range, which can the be iterated over as normal
to_range(SkipList, Start) ->
    to_range(element(2, SkipList), Start, ?INFINITY_KEY, ?LIST_HEIGHT).

to_range(SkipList, Start, End) ->
    to_range(element(2, SkipList), Start, End, ?LIST_HEIGHT).

to_list(SkipList) ->
    to_list(element(2, SkipList), ?LIST_HEIGHT).

empty() ->
    empty(false).

empty(BloomProtect) ->
    case BloomProtect of
        true ->
            {leveled_tinybloom:empty(?SKIP_WIDTH),
                empty([], ?LIST_HEIGHT)};
        false ->
            {list_only, empty([], ?LIST_HEIGHT)}
    end.

size(SkipList) ->
    size(element(2, SkipList), ?LIST_HEIGHT).


%%%============================================================================
%%% SkipList Base Functions
%%%============================================================================

enter(Key, Value, Hash, SkipList, Width, 1) ->
    {MarkerKey, SubList} = find_mark(Key, SkipList),
    case Hash rem Width of
        0 ->
            {LHS, RHS} = lists:splitwith(fun({K, _V}) ->
                                                K =< Key end,
                                            SubList),
            SkpL1 = lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, RHS}),
            SkpL2 = [{Key, lists:ukeysort(1, [{Key, Value}|LHS])}|SkpL1],
            lists:ukeysort(1, SkpL2);
        _ ->
            {LHS, RHS} = lists:splitwith(fun({K, _V}) -> K < Key end, SubList),
            UpdSubList =
                case RHS of
                    [] ->
                        LHS ++ [{Key, Value}];
                    [{FirstKey, _V}|RHSTail] ->
                        case FirstKey of
                            Key ->
                                LHS ++ [{Key, Value}] ++ RHSTail;
                            _ ->
                                LHS ++ [{Key, Value}] ++ RHS
                        end
                end,        
            lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, UpdSubList})
    end;
enter(Key, Value, Hash, SkipList, Width, Level) ->
    HashMatch = width(Level, Width),
    {MarkerKey, SubSkipList} = find_mark(Key, SkipList),
    UpdSubSkipList = enter(Key, Value, Hash, SubSkipList, Width, Level - 1),
    case Hash rem HashMatch of
        0 ->
            % 
            {LHS, RHS} = lists:splitwith(fun({K, _V}) ->
                                                K =< Key end,
                                            UpdSubSkipList),
            SkpL1 = lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, RHS}),
            lists:ukeysort(1, [{Key, LHS}|SkpL1]);
        _ ->
            % Need to replace Marker Key with sublist
            lists:keyreplace(MarkerKey,
                                1,
                                SkipList,
                                {MarkerKey, UpdSubSkipList})
    end.

from_list(SkipList, _SkipWidth, 0) ->
    SkipList;
from_list(KVList, SkipWidth, ListHeight) ->
    L0 = length(KVList),
    SL0 =
        case L0 > SkipWidth of
            true ->
                from_list(KVList, L0, [], SkipWidth);         
            false ->
                {LastK, _LastSL} = lists:last(KVList),
                [{LastK, KVList}]
        end,
    from_list(SL0, SkipWidth, ListHeight - 1).
    
from_list([], 0, SkipList, SkipWidth) ->
    SkipList;
from_list(KVList, L, SkipList, SkipWidth) ->
    SubLL = min(SkipWidth, L),
    {Head, Tail} = lists:split(SubLL, KVList),
    {LastK, _LastV} = lists:last(Head),
    from_list(Tail, L - SubLL, SkipList ++ [{LastK, Head}], SkipWidth).


list_lookup(Key, SkipList, 1) ->
    SubList = get_sublist(Key, SkipList),
    case lists:keyfind(Key, 1, SubList) of
        false ->
            none;
        {Key, V} ->
            {value, V}
    end;
list_lookup(Key, SkipList, Level) ->
    SubList = get_sublist(Key, SkipList),
    case SubList of
        null ->
            none;
        _ ->
            list_lookup(Key, SubList, Level - 1)
    end.


to_list(SkipList, 1) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> Acc ++ SL end, [], SkipList);
to_list(SkipList, Level) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> Acc ++ to_list(SL, Level - 1) end,
                [],
                SkipList).


to_range(SkipList, StartKey, EndKey, ListHeight) ->
    to_range(SkipList, StartKey, EndKey, ListHeight, [], true).

to_range(SkipList, StartKey, EndKey, ListHeight, Acc, StartIncl) ->
    SL = sublist_above(SkipList, StartKey, ListHeight, StartIncl),
    case SL of
        [] ->
            Acc;
        _ ->
            {LK, _LV} = lists:last(SL),
            case leveled_codec:endkey_passed(EndKey, LK) of
                false ->
                    to_range(SkipList,
                                LK,
                                EndKey,
                                ListHeight,
                                Acc ++ SL,
                                false);
                true ->
                    SplitFun =
                        fun({K, _V}) ->
                                not leveled_codec:endkey_passed(EndKey, K) end,
                    LHS = lists:takewhile(SplitFun, SL),
                    Acc ++ LHS
            end
    end.

sublist_above(SkipList, StartKey, 0, StartIncl) ->
    TestFun =
        fun({K, _V}) ->
            case StartIncl of
                true ->
                    K < StartKey;
                false ->
                    K =< StartKey
            end end,
    lists:dropwhile(TestFun, SkipList);
sublist_above(SkipList, StartKey, Level, StartIncl) ->
    TestFun =
        fun({K, _SL}) ->
            case StartIncl of
                true ->
                    K < StartKey;
                false ->
                    K =< StartKey
            end end,
    RHS = lists:dropwhile(TestFun, SkipList),
    case RHS of
        [] ->
            [];    
        [{_K, SL}|_Rest] ->
            sublist_above(SL, StartKey, Level - 1, StartIncl)
    end.

empty(SkipList, 1) ->
    [{?INFINITY_KEY, SkipList}];
empty(SkipList, Level) ->
    empty([{?INFINITY_KEY, SkipList}], Level - 1).

size(SkipList, 1) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> length(SL) + Acc end, 0, SkipList);
size(SkipList, Level) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> size(SL, Level - 1) + Acc end,
                    0,
                    SkipList).


%%%============================================================================
%%% Internal Functions
%%%============================================================================

width(1, Width) ->
    Width;
width(N, Width) ->
    width(N - 1, Width * Width).

find_mark(Key, SkipList) ->
    lists:foldl(fun({Marker, SL}, Acc) ->
                    case Acc of
                        false ->
                            case Marker >= Key of
                                true ->
                                   {Marker, SL};
                                false ->
                                    Acc
                            end;
                        _ ->
                            Acc
                    end end,
                false,
                SkipList).

get_sublist(Key, SkipList) ->
    lists:foldl(fun({SkipKey, SL}, Acc) ->
                        case {Acc, SkipKey} of
                            {null, SkipKey} when SkipKey >= Key ->
                                SL;
                            _ ->
                                Acc
                        end end,
                    null,
                    SkipList).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Seqn,
                        Count,
                        [],
                        BucketRangeLow,
                        BucketRangeHigh).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BNumber =
        case BRange of
            0 ->
                string:right(integer_to_list(BucketLow), 4, $0);
            _ ->
                BRand = random:uniform(BRange),
                string:right(integer_to_list(BucketLow + BRand), 4, $0)
        end,
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    {K, V} = {{o, "Bucket" ++ BNumber, "Key" ++ KNumber, null},
                {Seqn, {active, infinity}, null}},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{K, V}|Acc],
                        BucketLow,
                        BRange).

skiplist_small_test() ->
    % Check nothing bad happens with very small lists
    lists:foreach(fun(N) -> dotest_skiplist_small(N) end, lists:seq(1, 32)).


dotest_skiplist_small(N) ->
    KL = generate_randomkeys(1, N, 1, 2),
    SkipList1 = 
        lists:foldl(fun({K, V}, SL) ->
                            enter(K, V, SL)
                            end,
                        empty(),
                        KL),
    SkipList2 = from_list(lists:reverse(KL)),
    lists:foreach(fun({K, V}) -> ?assertMatch({value, V}, lookup(K, SkipList1))
                                    end,
                    lists:ukeysort(1, lists:reverse(KL))),
    lists:foreach(fun({K, V}) -> ?assertMatch({value, V}, lookup(K, SkipList2))
                                    end,
                    lists:ukeysort(1, lists:reverse(KL))).

skiplist_withbloom_test() ->
    io:format(user, "~n~nBloom protected skiplist test:~n~n", []),
    skiplist_tester(true).
    
skiplist_nobloom_test() ->
    io:format(user, "~n~nBloom free skiplist test:~n~n", []),
    skiplist_tester(false).
    
skiplist_tester(Bloom) ->
    N = 4000,
    KL = generate_randomkeys(1, N, 1, N div 5),
                
    SWaGSL = os:timestamp(),
    SkipList = from_list(lists:reverse(KL), Bloom),
    io:format(user, "Generating skip list with ~w keys in ~w microseconds~n" ++
                        "Top level key count of ~w~n",
                [N,
                    timer:now_diff(os:timestamp(), SWaGSL),
                    length(element(2, SkipList))]),
    io:format(user, "Second tier key counts of ~w~n",
                [lists:map(fun({_L, SL}) -> length(SL) end,
                    element(2, SkipList))]),
    KLSorted = lists:ukeysort(1, lists:reverse(KL)),
    
    SWaGSL2 = os:timestamp(),
    SkipList = from_sortedlist(KLSorted, Bloom),
    io:format(user, "Generating skip list with ~w sorted keys in ~w " ++
                        "microseconds~n",
                [N, timer:now_diff(os:timestamp(), SWaGSL2)]),

    SWaDSL = os:timestamp(),
    SkipList1 = 
        lists:foldl(fun({K, V}, SL) ->
                            enter(K, V, SL)
                            end,
                        empty(Bloom),
                        KL),
    io:format(user, "Dynamic load of skiplist with ~w keys took ~w " ++
                        "microseconds~n" ++
                        "Top level key count of ~w~n",
                [N,
                    timer:now_diff(os:timestamp(), SWaDSL),
                    length(element(2, SkipList1))]),
       io:format(user, "Second tier key counts of ~w~n",
                [lists:map(fun({_L, SL}) -> length(SL) end,
                    element(2, SkipList1))]),
    
    io:format(user, "~nRunning timing tests for generated skiplist:~n", []),
    skiplist_timingtest(KLSorted, SkipList, N, Bloom),

    io:format(user, "~nRunning timing tests for dynamic skiplist:~n", []),
    skiplist_timingtest(KLSorted, SkipList1, N, Bloom).

    
skiplist_timingtest(KL, SkipList, N, Bloom) ->
    io:format(user, "Timing tests on skiplist of size ~w~n",
                [leveled_skiplist:size(SkipList)]),
    CheckList1 = lists:sublist(KL, N div 4, 200),
    CheckList2 = lists:sublist(KL, N div 3, 200),
    CheckList3 = lists:sublist(KL, N div 2, 200),
    CheckList4 = lists:sublist(KL, N - 1000, 200),
    CheckList5 = lists:sublist(KL, N - 500, 200),
    CheckList6 = lists:sublist(KL, 1, 10),
    CheckList7 = lists:nthtail(N - 200, KL),
    CheckList8 = lists:sublist(KL, N div 2, 1),
    CheckAll = CheckList1 ++ CheckList2 ++ CheckList3 ++
                    CheckList4 ++ CheckList5 ++ CheckList6 ++ CheckList7,
    
    SWb = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({value, V}, lookup(K, SkipList))
                        end,
                    CheckAll),
    io:format(user, "Finding 1020 keys took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWb)]),
    
    RangeFun =
        fun(SkipListToQuery, CheckListForQ, Assert) ->
            KR =
                to_range(SkipListToQuery,
                            element(1, lists:nth(1, CheckListForQ)),
                            element(1, lists:last(CheckListForQ))),
            case Assert of
                true ->
                    CompareL = length(lists:usort(CheckListForQ)),
                    ?assertMatch(CompareL, length(KR));
                false ->
                    KR
            end
            end,
    
    SWc = os:timestamp(),
    RangeFun(SkipList, CheckList1, true),
    RangeFun(SkipList, CheckList2, true),
    RangeFun(SkipList, CheckList3, true),
    RangeFun(SkipList, CheckList4, true),
    RangeFun(SkipList, CheckList5, true),
    RangeFun(SkipList, CheckList6, true),
    RangeFun(SkipList, CheckList7, true),
    RangeFun(SkipList, CheckList8, true),
    
    KL_OOR1 = generate_randomkeys(1, 4, N div 5 + 1, N div 5 + 10),
    KR9 = RangeFun(SkipList, KL_OOR1, false),
    ?assertMatch([], KR9),
    
    KL_OOR2 = generate_randomkeys(1, 4, 0, 0),
    KR10 = RangeFun(SkipList, KL_OOR2, false),
    ?assertMatch([], KR10),
    
    io:format(user, "Finding 10 ranges took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWc)]),
            
    AltKL1 = generate_randomkeys(1, 2000, 1, 200),
    SWd = os:timestamp(),
    lists:foreach(fun({K, _V}) ->
                        lookup(K, SkipList)
                        end,
                    AltKL1),
    io:format(user, "Getting 2000 mainly missing keys took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWd)]),
    AltKL2 = generate_randomkeys(1, 1000, N div 5 + 1, N div 5 + 300),
    SWe = os:timestamp(),
    lists:foreach(fun({K, _V}) ->
                        none = lookup(K, SkipList)
                        end,
                    AltKL2),
    io:format(user, "Getting 1000 missing keys above range took ~w " ++
                        "microseconds~n",
                [timer:now_diff(os:timestamp(), SWe)]),
    AltKL3 = generate_randomkeys(1, 1000, 0, 0),
    SWf = os:timestamp(),
    lists:foreach(fun({K, _V}) ->
                        none = lookup(K, SkipList)
                        end,
                    AltKL3),
    io:format(user, "Getting 1000 missing keys below range took ~w " ++
                        "microseconds~n",
                [timer:now_diff(os:timestamp(), SWf)]),
    
    SWg = os:timestamp(),
    FlatList = to_list(SkipList),
    io:format(user, "Flattening skiplist took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWg)]),
    ?assertMatch(KL, FlatList),
    
    case Bloom of
        true ->
            HashList = lists:map(fun(_X) ->
                                        random:uniform(4294967295) end,
                                    lists:seq(1, 2000)),
            SWh = os:timestamp(),
            lists:foreach(fun(X) ->
                                lookup(X, X, SkipList) end,
                            HashList),
            io:format(user,
                        "Getting 2000 missing keys when hash was known " ++
                            "took ~w microseconds~n",
                        [timer:now_diff(os:timestamp(), SWh)]);
        false ->
            ok
    end.

define_kv(X) ->
    {{o, "Bucket", "Key" ++ string:right(integer_to_list(X), 6), null},
        {X, {active, infinity}, null}}.

skiplist_roundsize_test() ->
    KVL = lists:map(fun(X) -> define_kv(X) end, lists:seq(1, 4096)),
    SkipList = from_list(KVL),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({value, V}, lookup(K, SkipList)) end,
                    KVL),
    lists:foreach(fun(X) ->
                            {KS, _VS} = define_kv(X * 32 + 1),
                            {KE, _VE} = define_kv((X + 1) * 32),
                            R = to_range(SkipList, KS, KE),
                            L = lists:sublist(KVL,
                                                X * 32 + 1,
                                                32),
                            ?assertMatch(L, R) end,
                        lists:seq(0, 24)).

skiplist_nolookup_test() ->
    N = 4000,
    KL = generate_randomkeys(1, N, 1, N div 5),
    SkipList = lists:foldl(fun({K, V}, Acc) ->
                                enter_nolookup(K, V, Acc) end,
                            empty(true),
                            KL),
    KLSorted = lists:ukeysort(1, lists:reverse(KL)),
    lists:foreach(fun({K, _V}) ->
                        ?assertMatch(none, lookup(K, SkipList)) end,
                        KL),
    ?assertMatch(KLSorted, to_list(SkipList)).

skiplist_range_test() ->
    N = 150,
    KL = generate_randomkeys(1, N, 1, N div 5),
    
    KLSL1 = lists:sublist(lists:ukeysort(1, KL), 128),
    SkipList1 = from_list(KLSL1),
    {LastK1, V1} = lists:last(KLSL1),
    R1 = to_range(SkipList1, LastK1, LastK1),
    ?assertMatch([{LastK1, V1}], R1),
    
    KLSL2 = lists:sublist(lists:ukeysort(1, KL), 127),
    SkipList2 = from_list(KLSL2),
    {LastK2, V2} = lists:last(KLSL2),
    R2 = to_range(SkipList2, LastK2, LastK2),
    ?assertMatch([{LastK2, V2}], R2),
    
    KLSL3 = lists:sublist(lists:ukeysort(1, KL), 129),
    SkipList3 = from_list(KLSL3),
    {LastK3, V3} = lists:last(KLSL3),
    R3 = to_range(SkipList3, LastK3, LastK3),
    ?assertMatch([{LastK3, V3}], R3),
    
    {FirstK4, V4} = lists:nth(1, KLSL3),
    R4 = to_range(SkipList3, FirstK4, FirstK4),
    ?assertMatch([{FirstK4, V4}], R4).


empty_skiplist_size_test() ->
    ?assertMatch(0, leveled_skiplist:size(empty(false))),
    ?assertMatch(0, leveled_skiplist:size(empty(true))).

-endif.