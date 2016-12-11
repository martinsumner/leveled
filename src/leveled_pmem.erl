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
        add_to_cache/4,
        to_list/2,
        check_levelzero/3,
        merge_trees/4
        ]).      

-include_lib("eunit/include/eunit.hrl").


%%%============================================================================
%%% API
%%%============================================================================

add_to_cache(L0Size, {LevelMinus1, MinSQN, MaxSQN}, LedgerSQN, TreeList) ->
    LM1Size = leveled_skiplist:size(LevelMinus1),
    case LM1Size of
        0 ->
            {LedgerSQN, L0Size, TreeList};
        _ ->
            if
                MinSQN >= LedgerSQN ->
                    {MaxSQN,
                        L0Size + LM1Size,
                        lists:append(TreeList, [LevelMinus1])}
            end
    end.


to_list(Slots, FetchFun) ->
    SW = os:timestamp(),
    SlotList = lists:reverse(lists:seq(1, Slots)),
    FullList = lists:foldl(fun(Slot, Acc) ->
                                Tree = FetchFun(Slot),
                                L = leveled_skiplist:to_list(Tree),
                                lists:ukeymerge(1, Acc, L)
                                end,
                            [],
                            SlotList),
    leveled_log:log_timer("PM002", [length(FullList)], SW),
    FullList.


check_levelzero(Key, TreeList) ->
    check_levelzero(Key, leveled_codec:magic_hash(Key), TreeList).

check_levelzero(_Key, _Hash, []) ->
    {false, not_found};
check_levelzero(Key, Hash, TreeList) ->
    check_slotlist(Key, Hash, lists:seq(1, length(TreeList)), TreeList).


merge_trees(StartKey, EndKey, SkipListList, LevelMinus1) ->
    lists:foldl(fun(SkipList, Acc) ->
                        R = leveled_skiplist:to_range(SkipList,
                                                        StartKey,
                                                        EndKey),
                        lists:ukeymerge(1, Acc, R) end,
                    [],
                    [LevelMinus1|lists:reverse(SkipListList)]).

%%%============================================================================
%%% Internal Functions
%%%============================================================================

check_slotlist(Key, Hash, CheckList, TreeList) ->
    SlotCheckFun =
                fun(SlotToCheck, {Found, KV}) ->
                    case Found of
                        true ->
                            {Found, KV};
                        false ->
                            CheckTree = lists:nth(SlotToCheck, TreeList),
                            case leveled_skiplist:lookup(Key, Hash, CheckTree) of
                                none ->
                                    {Found, KV};
                                {value, Value} ->
                                    {true, {Key, Value}}
                            end
                    end
                    end,
    lists:foldl(SlotCheckFun,
                    {false, not_found},
                    lists:reverse(CheckList)).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Seqn,
                        Count,
                        leveled_skiplist:empty(true),
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
                        leveled_skiplist:enter(K, V, Acc),
                        BucketLow,
                        BRange).


compare_method_test() ->
    R = lists:foldl(fun(_X, {LedgerSQN, L0Size, L0TreeList}) ->
                            LM1 = generate_randomkeys(LedgerSQN + 1,
                                                        2000, 1, 500),
                            add_to_cache(L0Size,
                                            {LM1,
                                                LedgerSQN + 1,
                                                LedgerSQN + 2000},
                                            LedgerSQN,
                                            L0TreeList)
                            end,
                        {0, 0, []},
                        lists:seq(1, 16)),
    
    {SQN, Size, TreeList} = R,
    ?assertMatch(32000, SQN),
    ?assertMatch(true, Size =< 32000),
    
    TestList = leveled_skiplist:to_list(generate_randomkeys(1, 2000, 1, 800)),

    FindKeyFun =
        fun(Key) ->
                fun(Tree, {Found, KV}) ->
                    case Found of
                        true ->
                            {true, KV};
                        false ->
                            L0 = leveled_skiplist:lookup(Key, Tree),
                            case L0 of
                                none ->
                                    {false, not_found};
                                {value, Value} ->
                                    {true, {Key, Value}}
                            end
                    end
                end
            end,
    
    S0 = lists:foldl(fun({Key, _V}, Acc) ->
                            R0 = lists:foldr(FindKeyFun(Key),
                                                {false, not_found},
                                                TreeList),
                            [R0|Acc] end,
                        [],
                        TestList),
    
    S1 = lists:foldl(fun({Key, _V}, Acc) ->
                            R0 = check_levelzero(Key, TreeList),
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
                                    leveled_skiplist:enter(K, V, Acc);
                                _ ->
                                    Acc
                            end
                            end,
                        leveled_skiplist:empty(),
                        DumpList),
    Sz0 = leveled_skiplist:size(Q0),
    io:format("Crude method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWa), Sz0]),
    SWb = os:timestamp(),
    Q1 = merge_trees(StartKey, EndKey, TreeList, leveled_skiplist:empty()),
    Sz1 = length(Q1),
    io:format("Merge method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWb), Sz1]),
    ?assertMatch(Sz0, Sz1).



-endif.