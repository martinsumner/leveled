%% -------- PENCILLER MEMORY ---------
%%
%% Module that provides functions for maintaining the L0 memory of the
%% Penciller.
%%
%% It is desirable that the L0Mem can efficiently handle the push of new trees
%% whilst maintaining the capability to quickly snapshot the memory for clones
%% of the Penciller.
%%
%% ETS tables are not used due to complications with managing their mutability,
%% as the database is snapshotted.
%%
%% An attempt was made to merge all trees into a single tree on push (in a
%% spawned process), but this proved to have an expensive impact as the tree
%% got larger.
%%
%% This approach is to keep a list of trees which have been received in the
%% order which they were received.  There is then a fixed-size array of hashes
%% used to either point lookups at the right tree in the list, or inform the
%% requestor it is not present avoiding any lookups.
%%
%% Tests show this takes one third of the time at push (when compared to
%% merging to a single tree), and is an order of magnitude more efficient as
%% the tree reaches peak size.  It is also an order of magnitude more
%% efficient to use the hash index when compared to looking through all the
%% trees.
%%
%% Total time for single_tree 217000 microseconds
%% Total time for array_tree 209000 microseconds
%% Total time for array_list 142000 microseconds
%% Total time for array_filter 69000 microseconds
%% List of 2000 checked without array - success count of 90 in 36000 microsecs
%% List of 2000 checked with array - success count of 90 in 1000 microsecs
%%
%% The trade-off taken with the approach is that the size of the L0Cache is
%% uncertain.  The Size count is incremented if the hash is not already
%% present, so the size may be lower than the actual size due to hash
%% collisions

-module(leveled_pmem).

-include("include/leveled.hrl").

-export([
        add_to_index/5,
        to_list/2,
        new_index/0,
        check_levelzero/3,
        merge_trees/4
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(SLOT_WIDTH, {2048, 11}).
-define(SKIP_WIDTH, 32).
-define(INFINITE_KEY, {null, null, null, null, null}).
-define(EMPTY_SKIPLIST, [{?INFINITE_KEY, []}]).

%%%============================================================================
%%% API
%%%============================================================================






add_to_index(L0Index, L0Size, LevelMinus1, LedgerSQN, TreeList) ->
    SW = os:timestamp(),
    SlotInTreeList = length(TreeList) + 1,
    FoldFun = fun({K, V}, {AccMinSQN, AccMaxSQN, AccCount, HashIndex}) ->
                    SQN = leveled_codec:strip_to_seqonly({K, V}),
                    {Hash, Slot} = hash_to_slot(K),
                    L = array:get(Slot, HashIndex),
                    Count0 = case lists:keymember(Hash, 1, L) of
                                    true ->
                                        AccCount;
                                    false ->
                                        AccCount + 1
                                end,
                    {min(SQN, AccMinSQN),
                        max(SQN, AccMaxSQN),
                        Count0,
                        array:set(Slot, [{Hash, SlotInTreeList}|L], HashIndex)}
                    end,
    LM1List = gb_trees:to_list(LevelMinus1),
    StartingT = {infinity, 0, L0Size, L0Index},
    {MinSQN, MaxSQN, NewL0Size, UpdL0Index} = lists:foldl(FoldFun,
                                                            StartingT,
                                                            LM1List),
    leveled_log:log_timer("PM001", [NewL0Size], SW),
    if
        MinSQN > LedgerSQN ->
            {MaxSQN,
                NewL0Size,
                UpdL0Index,
                lists:append(TreeList, [LevelMinus1])}
    end.
    

to_list(Slots, FetchFun) ->
    SW = os:timestamp(),
    SlotList = lists:reverse(lists:seq(1, Slots)),
    FullList = lists:foldl(fun(Slot, Acc) ->
                                Tree = FetchFun(Slot),
                                L = gb_trees:to_list(Tree),
                                lists:ukeymerge(1, Acc, L)
                                end,
                            [],
                            SlotList),
    leveled_log:log_timer("PM002", [length(FullList)], SW),
    FullList.


new_index() ->
    array:new(element(1, ?SLOT_WIDTH), [{default, []}, fixed]).


check_levelzero(Key, L0Index, TreeList) ->
    {Hash, Slot} = hash_to_slot(Key),
    CheckList = array:get(Slot, L0Index),
    SlotList = lists:foldl(fun({H0, S0}, SL) ->
                                case H0 of
                                    Hash ->
                                        [S0|SL];
                                    _ ->
                                        SL
                                end
                                end,
                            [],
                            CheckList),
    lists:foldl(fun(SlotToCheck, {Found, KV}) ->
                        case Found of
                            true ->
                                {Found, KV};
                            false ->
                                CheckTree = lists:nth(SlotToCheck, TreeList),
                                case gb_trees:lookup(Key, CheckTree) of
                                    none ->
                                        {Found, KV};
                                    {value, Value} ->
                                        {true, {Key, Value}}
                                end
                        end
                        end,
                    {false, not_found},
                    lists:reverse(lists:usort(SlotList))).

    
merge_trees(StartKey, EndKey, TreeList, LevelMinus1) ->
    lists:foldl(fun(Tree, TreeAcc) ->
                        merge_nexttree(Tree, TreeAcc, StartKey, EndKey) end,
                    gb_trees:empty(),
                    lists:append(TreeList, [LevelMinus1])).

%%%============================================================================
%%% SkipList
%%%============================================================================


addkey_to_index(HashIndex, Key, Count) ->
    {Hash, Slot} = hash_to_slot(Key),
    L = array:get(Slot, HashIndex),
    case lists:member(Hash, L) of
        true ->
            {HashIndex, Count};
        false ->
            {array:set(Slot, [Hash|L], HashIndex), Count + 1}
    end.

merge_indexes(HashIndex, MergedIndex, Count, L0Slot) ->
    lists:foldl(fun(Slot, {MHI, AccCount}) ->
                    HashList = array:get(Slot, HashIndex),
                    case length(HashList) > 0 of
                        true ->
                            merge_indexes_singleslot(HashList,
                                                        Slot,
                                                        MHI,
                                                        L0Slot,
                                                        AccCount);
                        false ->
                            {MHI, AccCount}
                    end end,
                    {MergedIndex, Count},
                    lists:seq(0, element(1, ?SLOT_WIDTH) - 1)).

merge_indexes_singleslot(HashList, IndexSlot, MergedIndex, L0Slot, Count) ->
    L = array:get(IndexSlot, MergedIndex),
    {UpdHL, UpdCount} = lists:foldl(fun(H, {HL, C}) ->
                                            case lists:keymember(H, 1, L) of
                                                true ->
                                                    {[{H, L0Slot}|HL], C + 1};
                                                false ->
                                                    {[{H, L0Slot}|HL], C}
                                            end end,
                                        {L, Count},
                                        HashList),
    {array:set(IndexSlot, UpdHL, MergedIndex), UpdCount}.

load_dynamic_skiplist(SkipList, Key, Value, Hash) ->
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
            {LHS, RHS} = lists:splitwith(fun({K, _V}) -> K < Key end, SubList),
            SkpL1 = lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, RHS}),
            SkpL2 = [{Key, lists:ukeysort(1, [{Key, Value}|LHS])}|SkpL1],
            lists:ukeysort(1, SkpL2);
        _ ->
            UpdSubList = lists:ukeysort(1, [{Key, Value}|SubList]),
            lists:keyreplace(MarkerKey, 1, SkipList, {MarkerKey, UpdSubList})
    end.
            
generate_balanced_skiplist(UnsortedKVL) ->
    KVL = lists:ukeysort(1, UnsortedKVL),
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

fetchkey_from_skiplist(SkipList, Key) ->
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
            not_found;
        SubList -> 
            case lists:keyfind(Key, 1, SubList) of
                false ->
                    not_found;
                {Key, V} ->
                    {Key, V}
            end
    end.

fetchrange_from_skiplist(SkipList, StartKey, EndKey) ->
    R = lists:foldl(fun({SkipKey, SL}, {Continue, Acc}) ->
                            % io:format("SkipKey ~w StartKey ~w EndKey ~w~n", [SkipKey, StartKey, EndKey]),
                            case Continue of
                                true ->
                                    case StartKey > SkipKey of
                                        true ->
                                            % io:format("StartKey after SkipKey~n"),
                                            {true, Acc};
                                        false ->
                                            case leveled_codec:endkey_passed(EndKey, SkipKey) of
                                                true ->
                                                    % io:format("EndKey after SkipKey~n"),
                                                    {false, Acc ++ SL};
                                                false ->
                                                    % io:format("EndKey before SkipKey~n"),
                                                    {true, Acc ++ SL}
                                            end
                                    end;
                                false ->
                                    {false, Acc}
                            end end,
                        {true, []},
                        SkipList),
    {_Bool, SubList} = R,
    SubList.


%%%============================================================================
%%% Internal Functions
%%%============================================================================


hash_to_slot(Key) ->
    H = erlang:phash2(Key),
    {H bsr element(2, ?SLOT_WIDTH), H band (element(1, ?SLOT_WIDTH) - 1)}.

merge_nexttree(Tree, TreeAcc, StartKey, EndKey) ->
    Iter = gb_trees:iterator_from(StartKey, Tree),
    merge_nexttree(Iter, TreeAcc, EndKey).

merge_nexttree(Iter, TreeAcc, EndKey) ->
    case gb_trees:next(Iter) of
        none ->
            TreeAcc;
        {Key, Value, NewIter} ->
            case leveled_codec:endkey_passed(EndKey, Key) of
                true ->
                    TreeAcc;
                false ->
                    merge_nexttree(NewIter,
                                    gb_trees:enter(Key, Value, TreeAcc),
                                    EndKey)
            end
    end.

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
    BNumber = string:right(integer_to_list(BucketLow + random:uniform(BRange)),
                                            4, $0),
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    {K, V} = {{o, "Bucket" ++ BNumber, "Key" ++ KNumber, null},
                {Seqn, {active, infinity}, null}},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        gb_trees:enter(K, V, Acc),
                        BucketLow,
                        BRange).


compare_method_test() ->
    R = lists:foldl(fun(_X, {LedgerSQN, L0Size, L0Index, L0TreeList}) ->
                            LM1 = generate_randomkeys(LedgerSQN + 1,
                                                        2000, 1, 500),
                            add_to_index(L0Index, L0Size, LM1, LedgerSQN,
                                                            L0TreeList)
                            end,
                        {0, 0, new_index(), []},
                        lists:seq(1, 16)),
    
    {SQN, Size, Index, TreeList} = R,
    ?assertMatch(32000, SQN),
    ?assertMatch(true, Size =< 32000),
    
    TestList = gb_trees:to_list(generate_randomkeys(1, 2000, 1, 800)),

    S0 = lists:foldl(fun({Key, _V}, Acc) ->
            R0 = lists:foldr(fun(Tree, {Found, KV}) ->
                                    case Found of
                                        true ->
                                            {true, KV};
                                        false ->
                                            L0 = gb_trees:lookup(Key, Tree),
                                            case L0 of
                                                none ->
                                                    {false, not_found};
                                                {value, Value} ->
                                                    {true, {Key, Value}}
                                            end
                                    end
                                    end,
                                {false, not_found},
                                TreeList),
            [R0|Acc]
            end,
            [],
            TestList),
    
    S1 = lists:foldl(fun({Key, _V}, Acc) ->
                            R0 = check_levelzero(Key, Index, TreeList),
                            [R0|Acc]
                            end,
                        [],
                        TestList),
    
    ?assertMatch(S0, S1),
    
    StartKey = {o, "Bucket0100", null, null},
    EndKey = {o, "Bucket0200", null, null},
    SWa = os:timestamp(),
    FetchFun = fun(Slot) -> lists:nth(Slot, TreeList) end,
    DumpList = to_list(length(TreeList), FetchFun),
    Q0 = lists:foldl(fun({K, V}, Acc) ->
                            P = leveled_codec:endkey_passed(EndKey, K),
                            case {K, P} of
                                {K, false} when K >= StartKey ->
                                    gb_trees:enter(K, V, Acc);
                                _ ->
                                    Acc
                            end
                            end,
                        gb_trees:empty(),
                        DumpList),
    Sz0 = gb_trees:size(Q0),
    io:format(user, "Crude method took ~w microseconds resulting in tree of "
                        ++ "size ~w~n",
                [timer:now_diff(os:timestamp(), SWa), Sz0]),
    SWb = os:timestamp(),
    Q1 = merge_trees(StartKey, EndKey, TreeList, gb_trees:empty()),
    Sz1 = gb_trees:size(Q1),
    io:format(user, "Merge method took ~w microseconds resulting in tree of "
                        ++ "size ~w~n",
                [timer:now_diff(os:timestamp(), SWb), Sz1]),
    ?assertMatch(Sz0, Sz1).

skiplist_test() ->
    KL = gb_trees:to_list(generate_randomkeys(1, 4000, 1, 200)),
    SWaD = os:timestamp(),
    _D = lists:foldl(fun({K, V}, AccD) -> dict:store(K, V, AccD) end,
                        dict:new(),
                        KL),
    io:format(user, "Loading dict with 4000 keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaD)]),
                
    SWa = os:timestamp(),
    SkipList = generate_balanced_skiplist(KL),
    io:format(user, "Generating skip list with 4000 keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWa)]),
    
    CheckList1 = lists:sublist(KL, 1200, 100),
    CheckList2 = lists:sublist(KL, 1600, 100),
    CheckList3 = lists:sublist(KL, 2000, 100),
    CheckList4 = lists:sublist(KL, 2400, 100),
    CheckList5 = lists:sublist(KL, 2800, 100),
    CheckList6 = lists:sublist(KL, 1, 10),
    CheckList7 = lists:nthtail(3800, KL),
    CheckAll = CheckList1 ++ CheckList2 ++ CheckList3 ++
                    CheckList4 ++ CheckList5 ++ CheckList6 ++ CheckList7,
    
    SWb = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V},
                                        fetchkey_from_skiplist(SkipList, K))
                        end,
                    CheckAll),
    io:format(user, "Finding 520 keys took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWb)]),
    
    SWc = os:timestamp(),
    KR1 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList1)),
                                    element(1, lists:last(CheckList1))),
    io:format("Result length ~w ~n", [length(KR1)]),
    ?assertMatch(true, length(KR1) >= 100),
    ?assertMatch(true, length(KR1) < 400),
    KR2 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList2)),
                                    element(1, lists:last(CheckList2))),
    ?assertMatch(true, length(KR2) >= 100),
    ?assertMatch(true, length(KR2) < 400),
    KR3 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList3)),
                                    element(1, lists:last(CheckList3))),
    ?assertMatch(true, length(KR3) >= 100),
    ?assertMatch(true, length(KR3) < 400),
    KR4 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList4)),
                                    element(1, lists:last(CheckList4))),
    ?assertMatch(true, length(KR4) >= 100),
    ?assertMatch(true, length(KR4) < 400),
    KR5 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList5)),
                                    element(1, lists:last(CheckList5))),
    ?assertMatch(true, length(KR5) >= 100),
    ?assertMatch(true, length(KR5) < 400),
    KR6 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList6)),
                                    element(1, lists:last(CheckList6))),
    ?assertMatch(true, length(KR6) >= 10),
    ?assertMatch(true, length(KR6) < 200),
    KR7 = fetchrange_from_skiplist(SkipList,
                                    element(1, lists:nth(1, CheckList7)),
                                    element(1, lists:last(CheckList7))),
    ?assertMatch(true, length(KR7) >= 10),
    ?assertMatch(true, length(KR7) < 200),
    io:format(user, "Finding 7 ranges took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWc)]).

hash_index_test() ->
    KeyCount = 4000,
    SlotWidth  = element(1, ?SLOT_WIDTH),
    HI0 = new_index(),
    MHI0 = new_index(),
    KL0 = gb_trees:to_list(generate_randomkeys(1, KeyCount, 1, 200)),
    CheckList1 = lists:sublist(KL0, 1200, 100),
    CheckList2 = lists:sublist(KL0, 1600, 100),
    CheckList3 = lists:sublist(KL0, 2000, 100),
    CheckList4 = lists:sublist(KL0, 2400, 100),
    CheckList5 = lists:sublist(KL0, 2800, 100),
    CheckList6 = lists:sublist(KL0, 1, 10),
    CheckList7 = lists:nthtail(3800, KL0),
    CheckAll = CheckList1 ++ CheckList2 ++ CheckList3 ++
                    CheckList4 ++ CheckList5 ++ CheckList6 ++ CheckList7,
    
    SWa = os:timestamp(),
    SkipList1 = 
        lists:foldl(fun({K, V}, Acc) ->
                            {H, _Slot} = hash_to_slot(K),
                            load_dynamic_skiplist(Acc, K, V, H) end,
                        ?EMPTY_SKIPLIST,
                        KL0),
    io:format(user, "Dynamic load of skiplist took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWa)]),
    
    {LL, LN} = lists:foldl(fun({K, SL}, {Count, Number}) ->
                                {Count + length(SL), Number + 1} end,
                            {0, 0},
                            SkipList1),
    io:format(user,
                "Skip list has ~w markers with total members of ~w~n",
                [LN, LL]),
    ?assertMatch(true, LL / LN > ?SKIP_WIDTH / 2 ),
    ?assertMatch(true, LL / LN < ?SKIP_WIDTH * 2 ),
    
    SWb = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V},
                                        fetchkey_from_skiplist(SkipList1, K))
                        end,
                    CheckAll),
    io:format(user, "Fetching ~w keys from skiplist took ~w microseconds~n",
                [KeyCount, timer:now_diff(os:timestamp(), SWb)]),
    
    SWc = os:timestamp(),
    {HI1, _C1} = lists:foldl(fun({K, _V}, {HI, C}) ->
                                    addkey_to_index(HI, K, C) end,
                                {HI0, 0},
                                KL0),
    io:format(user, "Adding ~w keys to hashindex took ~w microseconds~n",
                [KeyCount, timer:now_diff(os:timestamp(), SWc)]),
    ?assertMatch(SlotWidth, array:size(HI1)),
    
    SWd = os:timestamp(),
    {MHI1, TC1} = merge_indexes(HI1, MHI0, 0, 0),
    io:format(user, "First merge to hashindex took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWd)]),
    ?assertMatch(SlotWidth, array:size(MHI1)),
    
    KL1 = gb_trees:to_list(generate_randomkeys(1, KeyCount, 1, 200)),
    
    SWe = os:timestamp(),
    HI2 = new_index(),
    {HI3, _C2} = lists:foldl(fun({K, _V}, {HI, C}) ->
                                    addkey_to_index(HI, K, C) end,
                                {HI2, 0},
                                KL1),
    io:format(user, "Adding ~w keys to hashindex took ~w microseconds~n",
                [KeyCount, timer:now_diff(os:timestamp(), SWe)]),
    
    SWf = os:timestamp(),
    {MHI2, TC2} = merge_indexes(HI3, MHI1, TC1, 1),
    io:format(user, "Second merge to hashindex took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWf)]),
    ?assertMatch(SlotWidth, array:size(MHI2)),
    
    SWg = os:timestamp(),
    HI4 = new_index(),
    {HI5, _C3} = lists:foldl(fun({K, _V}, {HI, C}) ->
                                    addkey_to_index(HI, K, C) end,
                                {HI4, 0},
                                KL1),
    io:format(user, "Adding ~w keys to hashindex took ~w microseconds~n",
                [KeyCount, timer:now_diff(os:timestamp(), SWg)]),
    
    SWh = os:timestamp(),
    {MHI3, _TC3} = merge_indexes(HI5, MHI2, TC2, 2),
    io:format(user, "Third merge to hashindex took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWh)]),
    ?assertMatch(SlotWidth, array:size(MHI2)).



-endif.