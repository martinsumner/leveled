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

-define(SLOT_WIDTH, {4096, 12}).
-define(SKIP_WIDTH, 128).


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


generate_skiplist(Dict) ->
    KVL = lists:ukeysort(1, dict:to_list(Dict)),
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
                            case Continue of
                                true ->
                                    case SkipKey of
                                        SkipKey when StartKey >= SkipKey ->
                                            {true, Acc};
                                        SkipKey when EndKey < SkipKey ->
                                            {false, Acc ++ SL}
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
    io:format("Crude method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWa), Sz0]),
    SWb = os:timestamp(),
    Q1 = merge_trees(StartKey, EndKey, TreeList, gb_trees:empty()),
    Sz1 = gb_trees:size(Q1),
    io:format("Merge method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWb), Sz1]),
    ?assertMatch(Sz0, Sz1).

skiplist_test() ->
    KL = gb_trees:to_list(generate_randomkeys(1, 4000, 1, 200)),
    D = lists:foldl(fun({K, V}, Acc) -> dict:store(K, V, Acc) end,
                        dict:new(),
                        KL),
    SWa = os:timestamp(),
    SkipList = generate_skiplist(D),
    io:format("Generating skip list with 4000 keys in ~w microseconds~n",
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
    io:format("Finding 520 keys took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWb)]),
    
    SWc = os:timestamp(),
    KR1 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList1),
                                    lists:last(CheckList1)),
    ?assertMatch(true, length(KR1) >= 100),
    ?assertMatch(true, length(KR1) < 400),
    KR2 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList2),
                                    lists:last(CheckList2)),
    ?assertMatch(true, length(KR2) >= 100),
    ?assertMatch(true, length(KR2) < 400),
    KR3 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList3),
                                    lists:last(CheckList3)),
    ?assertMatch(true, length(KR3) >= 100),
    ?assertMatch(true, length(KR3) < 400),
    KR4 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList4),
                                    lists:last(CheckList4)),
    ?assertMatch(true, length(KR4) >= 100),
    ?assertMatch(true, length(KR4) < 400),
    KR5 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList5),
                                    lists:last(CheckList5)),
    ?assertMatch(true, length(KR5) >= 100),
    ?assertMatch(true, length(KR5) < 400),
    KR6 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList6),
                                    lists:last(CheckList6)),
    ?assertMatch(true, length(KR6) >= 10),
    ?assertMatch(true, length(KR6) < 200),
    KR7 = fetchrange_from_skiplist(SkipList,
                                    lists:nth(1, CheckList7),
                                    lists:last(CheckList7)),
    ?assertMatch(true, length(KR7) >= 10),
    ?assertMatch(true, length(KR7) < 200),
    io:format("Finding 7 ranges took ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWc)]),
    
    ?assertMatch(true, false).

-endif.