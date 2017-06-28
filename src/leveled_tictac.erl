%% -------- TIC-TAC ACTOR ---------
%%
%% The TicTac actor is responsible for tracking the state of the store and
%% signalling that state to other trusted actors
%%
%% https://en.wikipedia.org/wiki/Tic-tac
%%
%% This is achieved through the exchange of merkle trees, but *not* trees that
%% are secure to interference - there is no attempt to protect the tree from
%% byzantine faults or tampering.  The tree is only suited for use between
%% trusted actors across secure channels.
%%
%% In dropping the cryptographic security requirement, a simpler tree is
%% possible, and also one that allows for trees of a partitioned database to
%% be quickly merged to represent a global view of state for the database
%% across the partition boundaries.
%%
%% -------- PERSPECTIVES OF STATE ---------
%%
%% The insecure Merkle trees (Tic-Tac Trees) are intended to be used in two
%% ways:
%% - To support the building of a merkle tree across a coverage plan to
%% represent global state across many stores (or vnodes) i.e. scanning over
%% the real data by bucket, by key range or by index.
%% - To track changes with "recent" modification dates.
%%
%% -------- TIC-TAC TREES ---------
%%
%% The Tic-Tac tree takes is split into 256 * 4096 different segments.  Every
%% key is hashed to map it to one of those segment leaves using the
%% elrang:phash2 function.
%%
%% External to the leveled_tictac module, the value should also have been
%% hashed to a 4-byte integer (presumably based on a tag-specific hash
%% function).  The combination of the Object Key and the Hash is then
%% hashed together to get a segment-change hash.
%%
%% To change a segment-leaf hash, the segment-leaf hash is XORd with the
%% segment-change hash associated with the changing key.  This assumes that
%% only one version of the key is ever added to the segment-leaf hash if the
%% tree is to represent the state of store (or partition of the store.  If
%% not, the segment-leaf hash can only represent a history of changes under
%% that leaf, not the current state (unless the previous segment-change hash
%% for the key is removed by XORing it once more from the segment-leaf hash
%% that already contains it).
%%
%% A Level 1 hash is then created by XORing the 4096 Level 2 segment-hashes
%% in the level below it (or XORing both the previous version and the new
%% version of the segment-leaf hash from the previous level 1 hash).
%%


-module(leveled_tictac).

-include("include/leveled.hrl").

-export([
            new_tree/1,
            new_tree/2,
            add_kv/4,
            find_dirtyleaves/2,
            find_dirtysegments/2,
            fetch_root/1,
            fetch_leaves/2,
            merge_trees/2,
            get_segment/2
        ]).


-include_lib("eunit/include/eunit.hrl").

-define(HASH_SIZE, 4).
-define(SMALL, {8, 256, 256 * 256}).
-define(MEDIUM, {9, 512, 512 * 512}).
-define(LARGE, {10, 1024, 1024 * 1024}).
-define(XLARGE, {11, 2048, 2048 * 2048}).

-record(tictactree, {treeID :: any(),
                        size  :: small|medium|large|xlarge,
                        width :: integer(),
                        bitwidth :: integer(),
                        segment_count :: integer(),
                        level1 :: binary(),
                        level2 :: any() % an array - but OTP compatibility
                        }).

-type tictactree() :: #tictactree{}.

%%%============================================================================
%%% External functions
%%%============================================================================

-spec new_tree(any()) -> tictactree().
%% @doc
%% Create a new tree, zeroed out.
new_tree(TreeID) ->
    new_tree(TreeID, small).
    
new_tree(TreeID, Size) ->
    {BitWidth, Width, SegmentCount} =
        case Size of
            small ->
                ?SMALL;
            medium ->
                ?MEDIUM;
            large ->
                ?LARGE;
            xlarge ->
                ?XLARGE
        end,
    Lv1Width = Width * ?HASH_SIZE * 8,
    Lv1Init = <<0:Lv1Width/integer>>,
    Lv2SegBinSize = Width * ?HASH_SIZE * 8,
    Lv2SegBinInit = <<0:Lv2SegBinSize/integer>>,
    Lv2Init = array:new([{size, Width}, {default, Lv2SegBinInit}]),
    #tictactree{treeID = TreeID,
                    size = Size,
                    width = Width,
                    bitwidth = BitWidth,
                    segment_count = SegmentCount,
                    level1 = Lv1Init,
                    level2 = Lv2Init}.

-spec add_kv(tictactree(), tuple(), tuple(), fun()) -> tictactree().
%% @doc
%% Add a Key and value to a tictactree using the HashFun to calculate the Hash
%% based on that key and value
add_kv(TicTacTree, Key, Value, HashFun) ->
    HashV = HashFun(Key, Value),
    SegChangeHash = erlang:phash2(Key, HashV),
    Segment = get_segment(Key, TicTacTree#tictactree.segment_count),
    
    Level2Pos =
        Segment band (TicTacTree#tictactree.width - 1),
    Level1Pos =
        (Segment bsr TicTacTree#tictactree.bitwidth)
            band (TicTacTree#tictactree.width - 1),
    Level2BytePos = ?HASH_SIZE * Level2Pos,
    Level1BytePos = ?HASH_SIZE * Level1Pos,
    
    Level2 = array:get(Level1Pos, TicTacTree#tictactree.level2),
    
    HashIntLength = ?HASH_SIZE * 8,
    <<PreL2:Level2BytePos/binary,
        SegLeaf2:HashIntLength/integer,
        PostL2/binary>> = Level2,
    <<PreL1:Level1BytePos/binary,
        SegLeaf1:HashIntLength/integer,
        PostL1/binary>> = TicTacTree#tictactree.level1,
    
    SegLeaf2Upd = SegLeaf2 bxor SegChangeHash,
    SegLeaf1Upd = SegLeaf1 bxor SegChangeHash,
    
    Level1Upd = <<PreL1:Level1BytePos/binary,
                    SegLeaf1Upd:HashIntLength/integer,
                    PostL1/binary>>,
    Level2Upd = <<PreL2:Level2BytePos/binary,
                    SegLeaf2Upd:HashIntLength/integer,
                    PostL2/binary>>,
    TicTacTree#tictactree{level1 = Level1Upd,
                            level2 = array:set(Level1Pos,
                                                Level2Upd,
                                                TicTacTree#tictactree.level2)}.

-spec find_dirtyleaves(tictactree(), tictactree()) -> list(integer()).
%% @doc
%% Returns a list of segment IDs which hold differences between the state
%% represented by the two trees.
find_dirtyleaves(SrcTree, SnkTree) ->
    _Size = SrcTree#tictactree.size,
    _Size = SnkTree#tictactree.size,
    Width = SrcTree#tictactree.width,
    
    IdxList = find_dirtysegments(fetch_root(SrcTree), fetch_root(SnkTree)),
    SrcLeaves = fetch_leaves(SrcTree, IdxList),
    SnkLeaves = fetch_leaves(SnkTree, IdxList),
    
    FoldFun =
        fun(Idx, Acc) ->
            {Idx, SrcLeaf} = lists:keyfind(Idx, 1, SrcLeaves),
            {Idx, SnkLeaf} = lists:keyfind(Idx, 1, SnkLeaves),
            L2IdxList = segmentcompare(SrcLeaf, SnkLeaf),
            Acc ++ lists:map(fun(X) -> X + Idx * Width end, L2IdxList)
        end,
    lists:sort(lists:foldl(FoldFun, [], IdxList)).

-spec find_dirtysegments(binary(), binary()) -> list(integer()).
%% @doc
%% Returns a list of branch IDs that contain differences between the tress.
%% Pass in level 1 binaries to make the comparison.
find_dirtysegments(SrcBin, SinkBin) ->
    segmentcompare(SrcBin, SinkBin).

-spec fetch_root(tictactree()) -> binary().
%% @doc
%% Return the level1 binary for a tree.
fetch_root(TicTacTree) ->
    TicTacTree#tictactree.level1.

-spec fetch_leaves(tictactree(), list(integer())) -> list().
%% @doc
%% Return a keylist for the segment hashes for the leaves of the tree based on
%% the list of branch IDs provided
fetch_leaves(TicTacTree, BranchList) ->
    MapFun =
        fun(Idx) ->
            {Idx, array:get(Idx, TicTacTree#tictactree.level2)}
        end,
    lists:map(MapFun, BranchList).

-spec merge_trees(tictactree(), tictactree()) -> tictactree().
%% Merge two trees providing a result that represents the combined state,
%% assuming that the two trees were correctly partitioned pre-merge.  If a key
%% and value has been added to both trees, then the merge will not give the 
%% expected outcome.
merge_trees(TreeA, TreeB) ->
    Size = TreeA#tictactree.size,
    Size = TreeB#tictactree.size,
    
    MergedTree = new_tree(merge, Size),
    
    L1A = fetch_root(TreeA),
    L1B = fetch_root(TreeB),
    NewLevel1 = merge_binaries(L1A, L1B),
    
    MergeFun =
        fun(SQN, MergeL2) ->
            L2A = array:get(SQN, TreeA#tictactree.level2),
            L2B = array:get(SQN, TreeB#tictactree.level2),
            NewLevel2 = merge_binaries(L2A, L2B),
            array:set(SQN, NewLevel2, MergeL2)
        end,
    NewLevel2 = lists:foldl(MergeFun,
                                MergedTree#tictactree.level2,
                                lists:seq(0, MergedTree#tictactree.width - 1)),
    
    MergedTree#tictactree{level1 = NewLevel1, level2 = NewLevel2}.

get_segment(Key, SegmentCount) ->
    erlang:phash2(Key) band (SegmentCount - 1).


%%%============================================================================
%%% Internal functions
%%%============================================================================

segmentcompare(SrcBin, SinkBin) when byte_size(SrcBin)==byte_size(SinkBin) ->
    segmentcompare(SrcBin, SinkBin, [], 0).

segmentcompare(<<>>, <<>>, Acc, _Counter) ->
    Acc;
segmentcompare(SrcBin, SnkBin, Acc, Counter) ->
    <<SrcHash:?HASH_SIZE/binary, SrcTail/binary>> = SrcBin,
    <<SnkHash:?HASH_SIZE/binary, SnkTail/binary>> = SnkBin,
    case SrcHash of
        SnkHash ->
            segmentcompare(SrcTail, SnkTail, Acc, Counter + 1);
        _ ->
            segmentcompare(SrcTail, SnkTail, [Counter|Acc], Counter + 1)
    end.

checktree(TicTacTree) ->
    checktree(TicTacTree#tictactree.level1, TicTacTree, 0).

checktree(<<>>, TicTacTree, Counter) ->
    true = TicTacTree#tictactree.width == Counter;
checktree(Level1Bin, TicTacTree, Counter) ->
    BitSize = ?HASH_SIZE * 8,
    <<TopHash:BitSize/integer, Tail/binary>> = Level1Bin,
    L2Bin = array:get(Counter, TicTacTree#tictactree.level2),
    true = TopHash == segmentsummarise(L2Bin, 0),
    checktree(Tail, TicTacTree, Counter + 1).            

segmentsummarise(<<>>, L1Acc) ->
    L1Acc;
segmentsummarise(L2Bin, L1Acc) ->
    BitSize = ?HASH_SIZE * 8,
    <<TopHash:BitSize/integer, Tail/binary>> = L2Bin,
    segmentsummarise(Tail, L1Acc bxor TopHash).

merge_binaries(BinA, BinB) ->
    BitSize = bit_size(BinA),
    BitSize = bit_size(BinB),
    <<AInt:BitSize/integer>> = BinA,
    <<BInt:BitSize/integer>> = BinB,
    MergedInt = AInt bxor BInt,
    <<MergedInt:BitSize/integer>>.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


simple_bysize_test() ->
    simple_test_withsize(small),
    simple_test_withsize(medium),
    simple_test_withsize(large),
    simple_test_withsize(xlarge).

simple_test_withsize(Size) ->
    HashFun = fun(_K, V) -> erlang:phash2(V) end,
    
    Tree0 = new_tree(0, Size),
    Tree1 = add_kv(Tree0, {o, "B1", "K1", null}, {caine, 1}, HashFun),
    Tree2 = add_kv(Tree1, {o, "B1", "K2", null}, {caine, 2}, HashFun),
    Tree3 = add_kv(Tree2, {o, "B1", "K3", null}, {caine, 3}, HashFun),
    Tree3A = add_kv(Tree3, {o, "B1", "K3", null}, {caine, 4}, HashFun),
    ?assertMatch(true, Tree0#tictactree.level1 == Tree0#tictactree.level1),
    ?assertMatch(false, Tree0#tictactree.level1 == Tree1#tictactree.level1),
    ?assertMatch(false, Tree1#tictactree.level1 == Tree2#tictactree.level1),
    ?assertMatch(false, Tree2#tictactree.level1 == Tree3#tictactree.level1),
    ?assertMatch(false, Tree3#tictactree.level1 == Tree3A#tictactree.level1),
    
    Tree0X = new_tree(0, Size),
    Tree1X = add_kv(Tree0X, {o, "B1", "K3", null}, {caine, 3}, HashFun),
    Tree2X = add_kv(Tree1X, {o, "B1", "K1", null}, {caine, 1}, HashFun),
    Tree3X = add_kv(Tree2X, {o, "B1", "K2", null}, {caine, 2}, HashFun),
    Tree3XA = add_kv(Tree3X, {o, "B1", "K3", null}, {caine, 4}, HashFun),
    ?assertMatch(false, Tree1#tictactree.level1 == Tree1X#tictactree.level1),
    ?assertMatch(false, Tree2#tictactree.level1 == Tree2X#tictactree.level1),
    ?assertMatch(true, Tree3#tictactree.level1 == Tree3X#tictactree.level1),
    ?assertMatch(true, Tree3XA#tictactree.level1 == Tree3XA#tictactree.level1),
    
    SC = Tree0#tictactree.segment_count,
    
    DL0 = find_dirtyleaves(Tree1, Tree0),
    ?assertMatch(true, lists:member(get_segment({o, "B1", "K1", null}, SC), DL0)),
    DL1 = find_dirtyleaves(Tree3, Tree1),
    ?assertMatch(true, lists:member(get_segment({o, "B1", "K2", null}, SC), DL1)),
    ?assertMatch(true, lists:member(get_segment({o, "B1", "K3", null}, SC), DL1)),
    ?assertMatch(false, lists:member(get_segment({o, "B1", "K1", null}, SC), DL1)).

merge_bysize_small_test() ->
    merge_test_withsize(small).

merge_bysize_medium_test() ->
    merge_test_withsize(medium).

merge_bysize_large_test() ->
    merge_test_withsize(large).

% merge_bysize_xlarge_test() ->
%    merge_test_withsize(xlarge).
% timmeout on cover test - so commented

merge_test_withsize(Size) ->
    HashFun = fun(_K, V) -> erlang:phash2(V) end,
    
    TreeX0 = new_tree(0, Size),
    TreeX1 = add_kv(TreeX0, {o, "B1", "X1", null}, {caine, 1}, HashFun),
    TreeX2 = add_kv(TreeX1, {o, "B1", "X2", null}, {caine, 2}, HashFun),
    TreeX3 = add_kv(TreeX2, {o, "B1", "X3", null}, {caine, 3}, HashFun),
    TreeX4 = add_kv(TreeX3, {o, "B1", "X3", null}, {caine, 4}, HashFun),
    
    TreeY0 = new_tree(0, Size),
    TreeY1 = add_kv(TreeY0, {o, "B1", "Y1", null}, {caine, 101}, HashFun),
    TreeY2 = add_kv(TreeY1, {o, "B1", "Y2", null}, {caine, 102}, HashFun),
    TreeY3 = add_kv(TreeY2, {o, "B1", "Y3", null}, {caine, 103}, HashFun),
    TreeY4 = add_kv(TreeY3, {o, "B1", "Y3", null}, {caine, 104}, HashFun),
    
    TreeZ1 = add_kv(TreeX4, {o, "B1", "Y1", null}, {caine, 101}, HashFun),
    TreeZ2 = add_kv(TreeZ1, {o, "B1", "Y2", null}, {caine, 102}, HashFun),
    TreeZ3 = add_kv(TreeZ2, {o, "B1", "Y3", null}, {caine, 103}, HashFun),
    TreeZ4 = add_kv(TreeZ3, {o, "B1", "Y3", null}, {caine, 104}, HashFun),
    
    TreeM0 = merge_trees(TreeX4, TreeY4),
    checktree(TreeM0),
    ?assertMatch(true, TreeM0#tictactree.level1 == TreeZ4#tictactree.level1),
    
    TreeM1 = merge_trees(TreeX3, TreeY4),
    checktree(TreeM1),
    ?assertMatch(false, TreeM1#tictactree.level1 == TreeZ4#tictactree.level1).

-endif.

    
    
    
