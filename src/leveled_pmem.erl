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
%% The trade-off taken with the approach is that the size of the L0Cache is
%% uncertain.  The Size count is incremented based on the inbound size and so
%% does not necessarily reflect the size once the lists are merged (reflecting
%% rotating objects)

-module(leveled_pmem).

-include("leveled.hrl").

-export([
        prepare_for_index/2,
        add_to_cache/5,
        to_list/2,
        check_levelzero/3,
        check_levelzero/4,
        merge_trees/4,
        add_to_index/3,
        new_index/0,
        check_index/2,
        cache_full/1
        ]).

-define(MAX_CACHE_LINES, 31). % Must be less than 128

-type index_array() :: list(array:array())|[]|none.

-export_type([index_array/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec cache_full(list()) -> boolean().
%% @doc
%% If there are already 31 entries in the cache then the cache is full
cache_full(L0Cache) ->
    length(L0Cache) == ?MAX_CACHE_LINES.

-spec prepare_for_index(
    array:array(), leveled_codec:segment_hash()) -> array:array().
%% @doc
%% Add the hash of a key to the index.  This is 'prepared' in the sense that
%% this index is not use until it is loaded into the main index.
%%
%% prepare_for_index is called from the Bookie when been added to the ledger
%% cache, but the index is not used until that ledger cache is in the
%% penciller L0 memory
prepare_for_index(IndexArray, no_lookup) ->
    IndexArray;
prepare_for_index(IndexArray, Hash) ->
    {Slot, H0} = split_hash(Hash),
    Bin = array:get(Slot, IndexArray),
    array:set(Slot, <<Bin/binary, H0:24/integer>>, IndexArray).

-spec add_to_index(array:array(), index_array(), integer()) -> index_array().
%% @doc
%% Expand the penciller's current index array with the details from a new
%% ledger cache tree sent from the Bookie.  The tree will have a cache slot
%% which is the index of this ledger_cache in the list of the ledger_caches
add_to_index(LM1Array, L0Index, CacheSlot) when CacheSlot < 128 ->
    [LM1Array|L0Index].

-spec new_index() -> array:array().
%% @doc
%% Create a new index array
new_index() ->
    array:new([{size, 256}, {default, <<>>}]).

-spec check_index(leveled_codec:segment_hash(), index_array())
        -> list(non_neg_integer()).
%% @doc
%% return a list of positions in the list of cache arrays that may contain the
%% key associated with the hash being checked
check_index(Hash, L0Index) ->
    {Slot, H0} = split_hash(Hash),
    {_L, Positions} =
        lists:foldl(
            fun(A, {SlotC, PosList}) ->
                B = array:get(Slot, A),
                case find_pos(B, H0) of
                    true -> {SlotC + 1, [SlotC|PosList]};
                    false -> {SlotC + 1, PosList}
                end
            end,
            {1, []},
            L0Index),
    lists:reverse(Positions).    

-spec add_to_cache(
    integer(),
    {tuple(), integer(), integer()},
    integer(),
    list(),
    boolean()) -> {integer(), integer(), list()}|empty_push.
%% @doc
%% The penciller's cache is a list of leveled_trees, this adds a new tree to
%% that cache, providing an update to the approximate size of the cache and
%% the Ledger's SQN.
%% Updates to cache must set Writable to true if the update could generate a
%% Level 0 file - as this must guard against empty entries (which may lead to
%% an attempt to write an empty L0 file)
add_to_cache(L0Size, {LM1, MinSQN, MaxSQN}, LedgerSQN, TreeList, Writeable) ->
    case {Writeable, leveled_tree:tsize(LM1)} of
        {true, 0} ->
            empty_push;
        {_, LM1Size} ->
            if
                MinSQN >= LedgerSQN ->
                    {MaxSQN,
                        L0Size + LM1Size,
                        [LM1|TreeList]}
            end
    end.

-spec to_list(
    integer(), fun((pos_integer()) -> leveled_tree:leveled_tree())) -> list().
%% @doc
%% The cache is a list of leveled_trees of length Slots.  This will fetch
%% each tree in turn by slot ID and then produce a merged/sorted output of
%% Keys and Values (to load into a SST file).
%%
%% Each slot is requested in turn to avoid halting the penciller whilst it
%% does a large object copy of the whole cache.
to_list(Slots, FetchFun) ->
    SW = os:timestamp(),
    SlotList = lists:seq(1, Slots),
    FullList = lists:foldl(fun(Slot, Acc) ->
                                Tree = FetchFun(Slot),
                                L = leveled_tree:to_list(Tree),
                                lists:ukeymerge(1, Acc, L)
                                end,
                            [],
                            SlotList),
    leveled_log:log_timer(pm002, [length(FullList)], SW),
    FullList.

-spec check_levelzero(tuple(), list(integer()), list())
                                            -> {boolean(), tuple|not_found}.
%% @doc
%% Check for the presence of a given Key in the Level Zero cache, with the
%% index array having been checked first for a list of potential positions
%% in the list of ledger caches - and then each potential ledger_cache being
%% checked (with the most recently received cache being checked first) until a
%% match is found.
check_levelzero(Key, PosList, TreeList) ->
    check_levelzero(Key, leveled_codec:segment_hash(Key), PosList, TreeList).

-spec check_levelzero(tuple(), {integer(), integer()}, list(integer()), list())
                                            -> {boolean(), tuple|not_found}.
%% @doc
%% Check for the presence of a given Key in the Level Zero cache, with the
%% index array having been checked first for a list of potential positions
%% in the list of ledger caches - and then each potential ledger_cache being
%% checked (with the most recently received cache being checked first) until a
%% match is found.
check_levelzero(_Key, _Hash, _PosList, []) ->
    {false, not_found};
check_levelzero(_Key, _Hash, [], _TreeList) ->
    {false, not_found};
check_levelzero(Key, Hash, PosList, TreeList) ->
    check_slotlist(Key, Hash, PosList, TreeList).

-spec merge_trees(tuple(), tuple(), list(tuple()), tuple()) -> list().
%% @doc
%% Return a list of keys and values across the level zero cache (and the
%% currently unmerged bookie's ledger cache) that are between StartKey
%% and EndKey (inclusive).
merge_trees(StartKey, EndKey, TreeList, LevelMinus1) ->
    lists:foldl(
        fun(Tree, Acc) ->
            R = leveled_tree:match_range(StartKey, EndKey, Tree),
            lists:ukeymerge(1, Acc, R) end,
            [],
            [LevelMinus1|TreeList]).

%%%============================================================================
%%% Internal Functions
%%%============================================================================


find_pos(<<>>, _Hash) ->
    false;
find_pos(<<Hash:24/integer, _T/binary>>, Hash) ->
    true;
find_pos(<<_Miss:24/integer, T/binary>>, Hash) ->
    find_pos(T, Hash).


split_hash({SegmentID, ExtraHash}) ->
    Slot = SegmentID band 255,
    H0 = (SegmentID bsr 8) bor (ExtraHash bsl 8),
    {Slot, H0 band 16#FFFFFF}.

check_slotlist(Key, _Hash, CheckList, TreeList) ->
    SlotCheckFun =
        fun(SlotToCheck, {Found, KV}) ->
            case Found of
                true ->
                    {Found, KV};
                false ->
                    CheckTree = lists:nth(SlotToCheck, TreeList),
                    case leveled_tree:match(Key, CheckTree) of
                        none ->
                            {Found, KV};
                        {value, Value} ->
                            {true, {Key, Value}}
                    end
            end
            end,
    lists:foldl(SlotCheckFun, {false, not_found}, CheckList).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

generate_randomkeys_aslist(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    lists:ukeysort(1,
                    generate_randomkeys(Seqn,
                                            Count,
                                            [],
                                            BucketRangeLow,
                                            BucketRangeHigh)).
        
generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    KVL = generate_randomkeys(Seqn,
                                Count,
                                [],
                                BucketRangeLow,
                                BucketRangeHigh),
    leveled_tree:from_orderedlist(lists:ukeysort(1, KVL), ?CACHE_TYPE).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BNumber =
        lists:flatten(
            io_lib:format("~4..0B",
                            [BucketLow + leveled_rand:uniform(BRange)])),
    KNumber =
        lists:flatten(io_lib:format("~4..0B", [leveled_rand:uniform(1000)])),
    {K, V} = {{o, "Bucket" ++ BNumber, "Key" ++ KNumber, null},
                {Seqn, {active, infinity}, null}},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{K, V}|Acc],
                        BucketLow,
                        BRange).


compare_method_test() ->
    R = lists:foldl(fun(_X, {LedgerSQN, L0Size, L0TreeList}) ->
                            LM1 = generate_randomkeys(LedgerSQN + 1,
                                                        2000, 1, 500),
                            add_to_cache(
                                L0Size,
                                {LM1, LedgerSQN + 1, LedgerSQN + 2000},
                                LedgerSQN,
                                L0TreeList,
                                true)
                            end,
                        {0, 0, []},
                        lists:seq(1, 16)),
    
    {SQN, Size, TreeList} = R,
    ?assertMatch(32000, SQN),
    ?assertMatch(true, Size =< 32000),
    
    TestList = leveled_tree:to_list(generate_randomkeys(1, 2000, 1, 800)),

    FindKeyFun =
        fun(Key) ->
                fun(Tree, {Found, KV}) ->
                    case Found of
                        true ->
                            {true, KV};
                        false ->
                            L0 = leveled_tree:match(Key, Tree),
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
                            R0 = lists:foldl(FindKeyFun(Key),
                                                {false, not_found},
                                                TreeList),
                            [R0|Acc] end,
                        [],
                        TestList),
    
    PosList = lists:seq(1, length(TreeList)),
    S1 = lists:foldl(fun({Key, _V}, Acc) ->
                            R0 = check_levelzero(Key, PosList, TreeList),
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
                                    [{K, V}|Acc];
                                _ ->
                                    Acc
                            end
                            end,
                        [],
                        DumpList),
    Tree = leveled_tree:from_orderedlist(lists:ukeysort(1, Q0), ?CACHE_TYPE),
    Sz0 = leveled_tree:tsize(Tree),
    io:format("Crude method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWa), Sz0]),
    SWb = os:timestamp(),
    Q1 = merge_trees(StartKey, EndKey, TreeList, leveled_tree:empty(?CACHE_TYPE)),
    Sz1 = length(Q1),
    io:format("Merge method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWb), Sz1]),
    ?assertMatch(Sz0, Sz1).

with_index_test_() ->
    % Otherwise this test may timeout when run with coverage enabled
    {timeout, 60, fun with_index_test2/0}.

with_index_test2() ->
    IndexPrepareFun =
        fun({K, _V}, Acc) ->
            H = leveled_codec:segment_hash(K),
            prepare_for_index(Acc, H)
        end,
    LoadFun =
        fun(_X, {{LedgerSQN, L0Size, L0TreeList}, L0Idx, SrcList}) ->
            LM1 = generate_randomkeys_aslist(LedgerSQN + 1, 2000, 1, 500),
            LM1Array = lists:foldl(IndexPrepareFun, new_index(), LM1),
            LM1SL = leveled_tree:from_orderedlist(lists:ukeysort(1, LM1), ?CACHE_TYPE),
            UpdL0Index = add_to_index(LM1Array, L0Idx, length(L0TreeList) + 1),
            R = add_to_cache(
                L0Size, 
                {LM1SL, LedgerSQN + 1, LedgerSQN + 2000},
                LedgerSQN,
                L0TreeList,
                true),
            {R, UpdL0Index, lists:ukeymerge(1, LM1, SrcList)}
        end,
    
    R0 = lists:foldl(LoadFun, {{0, 0, []}, [], []}, lists:seq(1, 16)),
    
    {{SQN, Size, TreeList}, L0Index, SrcKVL} = R0,
    ?assertMatch(32000, SQN),
    ?assertMatch(true, Size =< 32000),

    CheckFun =
        fun({K, V}, {L0Idx, L0Cache}) ->
            H = leveled_codec:segment_hash(K),
            PosList = check_index(H, L0Idx),
            ?assertMatch({true, {K, V}},
                            check_slotlist(K, H, PosList, L0Cache)),
            {L0Idx, L0Cache}
        end,
    
    _R1 = lists:foldl(CheckFun, {L0Index, TreeList}, SrcKVL).
            

index_performance_test() ->
    LM1 = generate_randomkeys_aslist(1, 2000, 1, 500),
    LM2 = generate_randomkeys_aslist(2001, 2000, 1, 500),
    HL1 = lists:map(fun({K, _V}) -> leveled_codec:segment_hash(K) end, LM1),
    HL2 = lists:map(fun({K, _V}) -> leveled_codec:segment_hash(K) end, LM2),

    SWP = os:timestamp(),
    A1 =
        lists:foldl(
            fun(H, A) -> prepare_for_index(A, H) end,
            new_index(),
            HL1),
    io:format(
        user, 
        "~nPrepare single index takes ~w microsec~n",
        [timer:now_diff(os:timestamp(), SWP)]),
    
    SWL = os:timestamp(),
    PMI1 = 
        lists:foldl(
            fun(I, Idx) -> add_to_index(A1, Idx, I) end, [], lists:seq(1, 8)),
    io:format(
        user, 
        "Appending to array takes ~w microsec~n",
        [timer:now_diff(os:timestamp(), SWL)]),
    
    SWC1 = os:timestamp(),
    R0 = lists:seq(1, 8),
    lists:foreach(fun(H) -> ?assertMatch(R0, check_index(H, PMI1)) end, HL1),
    io:format(
        user, 
        "Checking 2000 matches in array at each level takes ~w microsec~n",
        [timer:now_diff(os:timestamp(), SWC1)]),
    
    SWC2 = os:timestamp(),
    FPT = 
        lists:foldl(
            fun(H, FPC) -> FPC + length(check_index(H, PMI1)) end,
            0,
            HL2),
    io:format(
        user, 
        "Checking 2000 misses in array at each level takes ~w microsec " ++
        "with ~w false positives~n",
        [timer:now_diff(os:timestamp(), SWC2), FPT]).



-endif.
