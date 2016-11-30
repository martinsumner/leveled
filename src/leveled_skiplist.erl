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
-define(INFINITY_KEY, {null, null, null, null, null}).
-define(EMPTY_SKIPLIST, [{?INFINITY_KEY, []}]).
-define(EMPTY_SKIPLIST_TWOLEVEL, [{?INFINITY_KEY, ?EMPTY_SKIPLIST}]).


%%%============================================================================
%%% SkipList API
%%%============================================================================


enter(Key, Value, SkipList) ->
    Hash = erlang:phash2(Key),
    {MarkerKey, SubSkipList} =
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
                        SkipList),
    UpdSubSkipList = enter_ground(Key, Value, SubSkipList),
    case Hash rem (?SKIP_WIDTH * ?SKIP_WIDTH) of
        0 ->
            % 
            {LHS, RHS} = lists:splitwith(fun({K, _V}) -> K =< Key end, UpdSubSkipList),
            SkpL1 = lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, RHS}),
            lists:ukeysort(1, [{Key, LHS}|SkpL1]);
        _ ->
            % Need to replace Marker Key with sublist
            lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, UpdSubSkipList})
    end.

enter_ground(Key, Value, SkipList) ->
    Hash = erlang:phash2(Key),
    {MarkerKey, SubList} = lists:foldl(fun({Marker, SL}, Acc) ->
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
                                    SkipList),
    case Hash rem ?SKIP_WIDTH of
        0 ->
            {LHS, RHS} = lists:splitwith(fun({K, _V}) -> K =< Key end, SubList),
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
    end.
            

from_list(UnsortedKVL) ->
    KVL = lists:ukeysort(1, UnsortedKVL),
    SkipWidth = ?SKIP_WIDTH * ?SKIP_WIDTH,
    LoftSlots = length(KVL) div SkipWidth,
    case LoftSlots of
        0 ->
            {K, _V} = lists:last(KVL),
            [{K, from_list_ground(KVL, true)}];
        _ ->
            SkipList0 =
                lists:map(fun(X) ->
                                N = X * SkipWidth,
                                {K, _V} = lists:nth(N, KVL),
                                SL = lists:sublist(KVL,
                                                    N - SkipWidth + 1,
                                                    SkipWidth),
                                {K, from_list_ground(SL, true)}
                                end,
                            lists:seq(1, LoftSlots)),
            case LoftSlots * SkipWidth < length(KVL) of
                true ->
                    {LastK, _V} = lists:last(KVL),
                    TailList = lists:nthtail(LoftSlots * SkipWidth, KVL),
                    SkipList0 ++ [{LastK, from_list_ground(TailList, true)}];
                false ->
                    SkipList0
            end
    end.
    
from_list_ground(KVL, true) ->
    Slots = length(KVL) div ?SKIP_WIDTH,
    SkipList0 = lists:map(fun(X) ->
                                N = X * ?SKIP_WIDTH,
                                {K, _V} = lists:nth(N, KVL),
                                {K, lists:sublist(KVL,
                                                    N - ?SKIP_WIDTH + 1,
                                                    ?SKIP_WIDTH)}
                                end,
                            lists:seq(1, length(KVL) div ?SKIP_WIDTH)),
    case Slots * ?SKIP_WIDTH < length(KVL) of
        true ->
            {LastK, _V} = lists:last(KVL),
            SkipList0 ++ [{LastK, lists:nthtail(Slots * ?SKIP_WIDTH, KVL)}];
        false ->
            SkipList0
    end.
    

lookup(Key, SkipList) ->
    SubList = lists:foldl(fun({SkipKey, SL}, Acc) ->
                        case {Acc, SkipKey} of
                            {null, SkipKey} when SkipKey >= Key ->
                                SL;
                            _ ->
                                Acc
                        end end,
                    null,
                    SkipList),
    case SubList of
        null ->
            none;
        _ ->
            lookup_ground(Key, SubList)
    end.

lookup_ground(Key, SkipList) ->
    SubList = lists:foldl(fun({SkipKey, SL}, Acc) ->
                                case {Acc, SkipKey} of
                                    {null, SkipKey} when SkipKey >= Key ->
                                        SL;
                                    _ ->
                                        Acc
                                end end,
                            null,
                            SkipList),
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
    end.


to_list(SkipList) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> Acc ++ to_list_ground(SL) end,
                [],
                SkipList).

to_list_ground(SkipList) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> Acc ++ SL end, [], SkipList).

%% Rather than support iterator_from like gb_trees, will just an output a key
%% sorted list for the desired range, which can the be iterated over as normal
to_range(SkipList, Start) ->
    to_range(SkipList, Start, ?INFINITY_KEY).

to_range(SkipList, Start, End) ->
    R = lists:foldl(fun({Mark, SL}, {PassedStart, PassedEnd, Acc, PrevList}) ->
                        
                case {PassedStart, PassedEnd} of
                    {true, true} ->
                        {true, true, Acc, null};
                    {false, false} ->
                        case Start > Mark of
                            true ->
                                {false, false, Acc, SL};
                            false ->
                                SkipLRange = to_range_ground(PrevList,
                                                                Start,
                                                                End) ++
                                                to_range_ground(SL,
                                                                Start,
                                                                End),
                                case leveled_codec:endkey_passed(End, Mark) of
                                    true ->
                                        {true, true, SkipLRange, null};
                                    false ->
                                        {true, false, SkipLRange, null}
                                end
                        end;
                    {true, false} ->
                        SkipLRange = to_range_ground(SL, Start, End),
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


to_range_ground(SkipList, Start, End) ->
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
    SubList.

empty() ->
    ?EMPTY_SKIPLIST_TWOLEVEL.

size(SkipList) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> size_ground(SL) + Acc end,
                    0,
                    SkipList).

size_ground(SkipList) ->
    lists:foldl(fun({_Mark, SL}, Acc) -> length(SL) + Acc end, 0, SkipList).



%%%============================================================================
%%% Internal Functions
%%%============================================================================


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
                        gb_trees:empty(),
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
                        gb_trees:enter(K, V, Acc),
                        BucketLow,
                        BRange).

skiplist_test() ->
    KL = gb_trees:to_list(generate_randomkeys(1, 4000, 1, 200)),
    SWaD = os:timestamp(),
    _D = lists:foldl(fun({K, V}, AccD) -> dict:store(K, V, AccD) end,
                        dict:new(),
                        KL),
    io:format(user, "Loading dict with 4000 keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaD)]),
                
    SWaGSL = os:timestamp(),
    SkipList = from_list(KL),
    io:format(user, "Generating skip list with 4000 keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaGSL)]),

    
    SWaDSL = os:timestamp(),
    SkipList1 = 
        lists:foldl(fun({K, V}, SL) ->
                            enter(K, V, SL)
                            end,
                        empty(),
                        KL),
    io:format(user, "Dynamic load of skiplist took ~w microseconds~n~n",
                [timer:now_diff(os:timestamp(), SWaDSL)]),

      
    io:format(user, "~nRunning timing tests for generated skiplist:~n", []),
    skiplist_timingtest(KL, SkipList),
    
    io:format(user, "~nRunning timing tests for dynamic skiplist:~n", []),
    skiplist_timingtest(KL, SkipList1).
    
    
skiplist_timingtest(KL, SkipList) ->
    io:format(user, "Timing tests on skiplist of size ~w~n",
                [leveled_skiplist:size(SkipList)]),
    CheckList1 = lists:sublist(KL, 1200, 200),
    CheckList2 = lists:sublist(KL, 1600, 200),
    CheckList3 = lists:sublist(KL, 2000, 200),
    CheckList4 = lists:sublist(KL, 2400, 200),
    CheckList5 = lists:sublist(KL, 2800, 200),
    CheckList6 = lists:sublist(KL, 1, 10),
    CheckList7 = lists:nthtail(3800, KL),
    CheckList8 = lists:sublist(KL, 2000, 1),
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
    
    KL_OOR1 = gb_trees:to_list(generate_randomkeys(1, 4, 201, 202)),
    KR9 = RangeFun(SkipList, KL_OOR1, false),
    ?assertMatch([], KR9),
    
    KL_OOR2 = gb_trees:to_list(generate_randomkeys(1, 4, 0, 0)),
    KR10 = RangeFun(SkipList, KL_OOR2, false),
    ?assertMatch([], KR10),
    
    io:format(user, "Finding 10 ranges took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWc)]),
            
    AltKL1 = gb_trees:to_list(generate_randomkeys(1, 1000, 1, 200)),
    SWd = os:timestamp(),
    lists:foreach(fun({K, _V}) ->
                        lookup(K, SkipList)
                        end,
                    AltKL1),
    io:format(user, "Getting 1000 mainly missing keys took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWd)]),
    AltKL2 = gb_trees:to_list(generate_randomkeys(1, 1000, 201, 300)),
    SWe = os:timestamp(),
    lists:foreach(fun({K, _V}) ->
                        none = lookup(K, SkipList)
                        end,
                    AltKL2),
    io:format(user, "Getting 1000 missing keys above range took ~w " ++
                        "microseconds~n",
                [timer:now_diff(os:timestamp(), SWe)]),
    AltKL3 = gb_trees:to_list(generate_randomkeys(1, 1000, 0, 0)),
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