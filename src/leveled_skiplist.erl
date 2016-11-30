%% -------- SKIPLIST ---------
%%
%% For storing small numbers of {K, V} pairs where reasonable insertion and
%% fetch times, but with fast support for flattening to a list or a sublist
%% within a certain key range
%%
%% Used instead of gb_trees to retain compatability of OTP16 (and Riak's
%% ongoing dependency on OTP16)
%%
%% Not a proper skip list.  Only supports a single depth.  Good enough for the
%% purposes of leveled.  Also uses peculiar enkey_passed function within
%% leveled

-module(leveled_skiplist).

-include("include/leveled.hrl").

-export([
        from_list/1,
        from_sortedlist/1,
        to_list/1,
        enter/3,
        to_range/2,
        to_range/3,
        lookup/2,
        empty/0,
        size/1
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(SKIP_WIDTH, 16).
-define(LIST_HEIGHT, 2).
-define(INFINITY_KEY, {null, null, null, null, null}).


%%%============================================================================
%%% SkipList API
%%%============================================================================

enter(Key, Value, SkipList) ->
    enter(Key, Value, SkipList, ?SKIP_WIDTH, ?LIST_HEIGHT).

from_list(UnsortedKVL) ->
    KVL = lists:ukeysort(1, UnsortedKVL),
    from_list(KVL, ?SKIP_WIDTH, ?LIST_HEIGHT).

from_sortedlist(SortedKVL) ->
    from_list(SortedKVL, ?SKIP_WIDTH, ?LIST_HEIGHT).

lookup(Key, SkipList) ->
    lookup(Key, SkipList, ?LIST_HEIGHT).


%% Rather than support iterator_from like gb_trees, will just an output a key
%% sorted list for the desired range, which can the be iterated over as normal
to_range(SkipList, Start) ->
    to_range(SkipList, Start, ?INFINITY_KEY, ?LIST_HEIGHT).

to_range(SkipList, Start, End) ->
    to_range(SkipList, Start, End, ?LIST_HEIGHT).

to_list(SkipList) ->
    to_list(SkipList, ?LIST_HEIGHT).

empty() ->
    empty([], ?LIST_HEIGHT).

size(SkipList) ->
    size(SkipList, ?LIST_HEIGHT).


%%%============================================================================
%%% SkipList Base Functions
%%%============================================================================

enter(Key, Value, SkipList, Width, 1) ->
    Hash = erlang:phash2(Key),
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
enter(Key, Value, SkipList, Width, Level) ->
    Hash = erlang:phash2(Key),
    HashMatch = width(Level, Width),
    {MarkerKey, SubSkipList} = find_mark(Key, SkipList),
    UpdSubSkipList = enter(Key, Value, SubSkipList, Width, Level - 1),
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


from_list(KVL, Width, 1) ->
    Slots = length(KVL) div Width,
    SkipList0 = lists:map(fun(X) ->
                                N = X * Width,
                                {K, _V} = lists:nth(N, KVL),
                                {K, lists:sublist(KVL,
                                                    N - Width + 1,
                                                    Width)}
                                end,
                            lists:seq(1, length(KVL) div Width)),
    case Slots * Width < length(KVL) of
        true ->
            {LastK, _V} = lists:last(KVL),
            SkipList0 ++ [{LastK, lists:nthtail(Slots * Width, KVL)}];
        false ->
            SkipList0
    end;
from_list(KVL, Width, Level) ->
    SkipWidth = width(Level, Width),
    LoftSlots = length(KVL) div SkipWidth,
    case LoftSlots of
        0 ->
            {K, _V} = lists:last(KVL),
            [{K, from_list(KVL, Width, Level - 1)}];
        _ ->
            SkipList0 =
                lists:map(fun(X) ->
                                N = X * SkipWidth,
                                {K, _V} = lists:nth(N, KVL),
                                SL = lists:sublist(KVL,
                                                    N - SkipWidth + 1,
                                                    SkipWidth),
                                {K, from_list(SL, Width, Level - 1)}
                                end,
                            lists:seq(1, LoftSlots)),
            case LoftSlots * SkipWidth < length(KVL) of
                true ->
                    {LastK, _V} = lists:last(KVL),
                    TailList = lists:nthtail(LoftSlots * SkipWidth, KVL),
                    SkipList0 ++ [{LastK, from_list(TailList,
                                                    Width,
                                                    Level - 1)}];
                false ->
                    SkipList0
            end
    end.


lookup(Key, SkipList, 1) ->
    SubList = get_sublist(Key, SkipList),
    case SubList of
        null ->
            none;
        SubList -> 
            case lists:keyfind(Key, 1, SubList) of
                false ->
                    none;
                {Key, V} ->
                    {value, V}
            end
    end;
lookup(Key, SkipList, Level) ->
    SubList = get_sublist(Key, SkipList),
    case SubList of
        null ->
            none;
        _ ->
            lookup(Key, SubList, Level - 1)
    end.


to_list(SkipList, 1) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> Acc ++ SL end, [], SkipList);
to_list(SkipList, Level) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> Acc ++ to_list(SL, Level - 1) end,
                [],
                SkipList).

to_range(SkipList, Start, End, 1) ->
    R = lists:foldl(fun({Mark, SL}, {PassedStart, PassedEnd, Acc, PrevList}) ->
                        
                case {PassedStart, PassedEnd} of
                    {true, true} ->
                        {true, true, Acc, null};
                    {false, false} ->
                        case Start > Mark of
                            true ->
                                {false, false, Acc, SL};
                            false ->
                                RHS = splitlist_start(Start, PrevList ++ SL),
                                case leveled_codec:endkey_passed(End, Mark) of
                                    true ->
                                        EL = splitlist_end(End, RHS),
                                        {true, true, EL, null};
                                    false ->
                                        {true, false, RHS, null}
                                end
                        end;
                    {true, false} ->
                        case leveled_codec:endkey_passed(End, Mark) of
                            true ->
                                EL = splitlist_end(End, SL),
                                {true, true, Acc ++ EL, null};
                            false ->
                                {true, false, Acc ++ SL, null}
                        end
                end end,
                    
                    {false, false, [], []},
                    SkipList),
    {_Bool1, _Bool2, SubList, _PrevList} = R,
    SubList;
to_range(SkipList, Start, End, Level) ->
    R = lists:foldl(fun({Mark, SL}, {PassedStart, PassedEnd, Acc, PrevList}) ->
                        
                case {PassedStart, PassedEnd} of
                    {true, true} ->
                        {true, true, Acc, null};
                    {false, false} ->
                        case Start > Mark of
                            true ->
                                {false, false, Acc, SL};
                            false ->
                                SkipLRange = to_range(PrevList,
                                                        Start, End,
                                                        Level - 1) ++
                                                to_range(SL,
                                                            Start, End,
                                                            Level - 1),
                                case leveled_codec:endkey_passed(End, Mark) of
                                    true ->
                                        {true, true, SkipLRange, null};
                                    false ->
                                        {true, false, SkipLRange, null}
                                end
                        end;
                    {true, false} ->
                        SkipLRange = to_range(SL, Start, End, Level - 1),
                        case leveled_codec:endkey_passed(End, Mark) of
                            true ->
                                {true, true, Acc ++ SkipLRange, null};
                            false ->
                                {true, false, Acc ++ SkipLRange, null}
                        end
                end end,
                    
                    {false, false, [], []},
                    SkipList),
    {_Bool1, _Bool2, SubList, _PrevList} = R,
    SubList.


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

splitlist_start(StartKey, SL) ->
    {_LHS, RHS} = lists:splitwith(fun({K, _V}) -> K < StartKey end, SL),
    RHS.

splitlist_end(EndKey, SL) ->
    {LHS, _RHS} = lists:splitwith(fun({K, _V}) ->
                                        not leveled_codec:endkey_passed(EndKey, K)
                                        end,
                                    SL),
    LHS.

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
    lists:foreach(fun({K, V}) -> ?assertMatch({value, V}, lookup(K, SkipList1))
                                    end,
                    lists:ukeysort(1, lists:reverse(KL))).

skiplist_test() ->
    N = 8000,
    KL = generate_randomkeys(1, N, 1, N div 5),
                
    SWaGSL = os:timestamp(),
    SkipList = from_list(lists:reverse(KL)),
    io:format(user, "Generating skip list with ~w keys in ~w microseconds~n" ++
                        "Top level key count of ~w~n",
                [N, timer:now_diff(os:timestamp(), SWaGSL), length(SkipList)]),
    io:format(user, "Second tier key counts of ~w~n",
                [lists:map(fun({_L, SL}) -> length(SL) end, SkipList)]),
    KLSorted = lists:ukeysort(1, lists:reverse(KL)),
    
    SWaGSL2 = os:timestamp(),
    SkipList = from_sortedlist(KLSorted),
    io:format(user, "Generating skip list with ~w sorted keys in ~w " ++
                        "microseconds~n",
                [N, timer:now_diff(os:timestamp(), SWaGSL2)]),

    SWaDSL = os:timestamp(),
    SkipList1 = 
        lists:foldl(fun({K, V}, SL) ->
                            enter(K, V, SL)
                            end,
                        empty(),
                        KL),
    io:format(user, "Dynamic load of skiplist with ~w keys took ~w " ++
                        "microseconds~n" ++
                        "Top level key count of ~w~n",
                [N, timer:now_diff(os:timestamp(), SWaDSL), length(SkipList1)]),
       io:format(user, "Second tier key counts of ~w~n",
                [lists:map(fun({_L, SL}) -> length(SL) end, SkipList1)]),
    
    io:format(user, "~nRunning timing tests for generated skiplist:~n", []),
    skiplist_timingtest(KLSorted, SkipList, N),

    io:format(user, "~nRunning timing tests for dynamic skiplist:~n", []),
    skiplist_timingtest(KLSorted, SkipList1, N).
    
    
skiplist_timingtest(KL, SkipList, N) ->
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
            
    AltKL1 = generate_randomkeys(1, 1000, 1, 200),
    SWd = os:timestamp(),
    lists:foreach(fun({K, _V}) ->
                        lookup(K, SkipList)
                        end,
                    AltKL1),
    io:format(user, "Getting 1000 mainly missing keys took ~w microseconds~n",
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
    ?assertMatch(KL, FlatList).

define_kv(X) ->
    {{o, "Bucket", "Key" ++ string:right(integer_to_list(X), 6), null},
        {X, {active, infinity}, null}}.

skiplist_roundsize_test() ->
    KVL = lists:map(fun(X) -> define_kv(X) end, lists:seq(1, 4000)),
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


-endif.