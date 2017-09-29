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
            add_kv/5,
            find_dirtyleaves/2,
            find_dirtysegments/2,
            fetch_root/1,
            fetch_leaves/2,
            merge_trees/2,
            get_segment/2,
            tictac_hash/3,
            export_tree/1,
            import_tree/1
        ]).


-include_lib("eunit/include/eunit.hrl").

-define(HASH_SIZE, 4).
-define(XXSMALL, {6, 64, 64 * 64}).
-define(XSMALL, {7, 128, 128 * 128}).
-define(SMALL, {8, 256, 256 * 256}).
-define(MEDIUM, {9, 512, 512 * 512}).
-define(LARGE, {10, 1024, 1024 * 1024}).
-define(XLARGE, {11, 2048, 2048 * 2048}).
-define(EMPTY, <<0:8/integer>>).

-record(tictactree, {treeID :: any(),
                        size  :: xxsmall|xsmall|small|medium|large|xlarge,
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
    {BitWidth, Width, SegmentCount} = get_size(Size),
    Lv1Width = Width * ?HASH_SIZE * 8,
    Lv1Init = <<0:Lv1Width/integer>>,
    Lv2Init = array:new([{size, Width}, {default, ?EMPTY}]),
    #tictactree{treeID = TreeID,
                    size = Size,
                    width = Width,
                    bitwidth = BitWidth,
                    segment_count = SegmentCount,
                    level1 = Lv1Init,
                    level2 = Lv2Init}.

-spec export_tree(tictactree()) -> {struct, list()}.
%% @doc
%% Export the tree into a tuple list, with the level1 binary, and then for 
%% level2 {branchID, binary()}
export_tree(Tree) ->
    EncodeL2Fun = 
        fun(X, L2Acc) ->
            L2Element = zlib:compress(array:get(X, Tree#tictactree.level2)),
            [{integer_to_binary(X), base64:encode_to_string(L2Element)}|L2Acc]
        end,
    L2 = 
        lists:foldl(EncodeL2Fun, [], lists:seq(0, Tree#tictactree.width - 1)),
    {struct, 
        [{<<"level1">>, base64:encode_to_string(Tree#tictactree.level1)}, 
            {<<"level2">>, {struct, lists:reverse(L2)}}
            ]}.

-spec import_tree({struct, list()}) -> tictactree().
%% @doc
%% Reverse the export process
import_tree(ExportedTree) ->
    {struct, 
        [{<<"level1">>, L1Base64}, 
        {<<"level2">>, {struct, L2List}}]} = ExportedTree,
    L1Bin = base64:decode(L1Base64),
    Sizes = 
        lists:map(fun(SizeTag) -> {SizeTag, element(2, get_size(SizeTag))} end,
                    [xxsmall, xsmall, small, medium, large, xlarge]),
    Width = byte_size(L1Bin) div ?HASH_SIZE,
    {Size, Width} = lists:keyfind(Width, 2, Sizes),
    {BitWidth, Width, SegmentCount} = get_size(Size),
    Lv2Init = array:new([{size, Width}]),
    FoldFun = 
        fun({X, EncodedL2SegBin}, L2Array) ->
            L2SegBin = zlib:uncompress(base64:decode(EncodedL2SegBin)),
            array:set(binary_to_integer(X), L2SegBin, L2Array)
        end,
    Lv2 = lists:foldl(FoldFun, Lv2Init, L2List),
    #tictactree{treeID = import,
                    size = Size,
                    width = Width,
                    bitwidth = BitWidth,
                    segment_count = SegmentCount,
                    level1 = L1Bin,
                    level2 = Lv2}.

-spec add_kv(tictactree(), tuple(), tuple(), fun()) -> tictactree().
add_kv(TicTacTree, Key, Value, BinExtractFun) ->
    add_kv(TicTacTree, Key, Value, BinExtractFun, false).

-spec add_kv(tictactree(), tuple(), tuple(), fun(), boolean()) -> tictactree().
%% @doc
%% Add a Key and value to a tictactree using the BinExtractFun to extract a 
%% binary from the Key and value from which to generate the hash.  The 
%% BinExtractFun will also need to do any canonicalisation necessary to make
%% the hash consistent (such as whitespace removal, or sorting)
%%
%% For exportable trees the hash function will be based on the CJ Bernstein
%% magic hash.  For non-exportable trees erlang:phash2 will be used, and so 
%% non-binary Keys and Values can be returned from the BinExtractFun in this
%% case.
add_kv(TicTacTree, Key, Value, BinExtractFun, Exportable) ->
    {BinK, BinV} = BinExtractFun(Key, Value),
    {SegHash, SegChangeHash} = tictac_hash(BinK, BinV, Exportable),
    Segment = get_segment(SegHash, TicTacTree#tictactree.segment_count),
    
    Level2Pos =
        Segment band (TicTacTree#tictactree.width - 1),
    Level1Pos =
        (Segment bsr TicTacTree#tictactree.bitwidth)
            band (TicTacTree#tictactree.width - 1),
    Level2BytePos = ?HASH_SIZE * Level2Pos,
    Level1BytePos = ?HASH_SIZE * Level1Pos,
    
    Level2 = get_level2(TicTacTree, Level1Pos),
    
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
            {Idx, get_level2(TicTacTree, Idx)}
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
            L2A = get_level2(TreeA, SQN),
            L2B = get_level2(TreeB, SQN),
            NewLevel2 = merge_binaries(L2A, L2B),
            array:set(SQN, NewLevel2, MergeL2)
        end,
    NewLevel2 = lists:foldl(MergeFun,
                                MergedTree#tictactree.level2,
                                lists:seq(0, MergedTree#tictactree.width - 1)),
    
    MergedTree#tictactree{level1 = NewLevel1, level2 = NewLevel2}.

-spec get_segment(integer(), 
                    integer()|xxsmall|xsmall|small|medium|large|xlarge) -> 
                        integer().
%% @doc
%% Return the segment ID for a Key.  Can pass the tree size or the actual
%% segment count derived from the size
get_segment(Hash, SegmentCount) when is_integer(SegmentCount) ->
    Hash band (SegmentCount - 1);
get_segment(Hash, TreeSize) ->
    get_segment(Hash, element(3, get_size(TreeSize))).


-spec tictac_hash(any(), any(), boolean()) -> {integer(), integer()}.
%% @doc
%% Hash the key and term, to either something repetable in Erlang, or using 
%% the DJ Bernstein hash if it is the tree needs to be compared with one 
%% calculated with a non-Erlang store
%%
%% Boolean is Exportable.  does the hash need to be repetable by a non-Erlang
%% machine  
tictac_hash(BinKey, BinVal, true) 
                            when is_binary(BinKey) and is_binary(BinVal) ->
    HashKey = leveled_codec:magic_hash({binary, BinKey}),
    HashVal = leveled_codec:magic_hash({binary, BinVal}),
    {HashKey, HashKey bxor HashVal};
tictac_hash(BinKey, {is_hash, HashedVal}, false) ->
    {erlang:phash2(BinKey), erlang:phash2(BinKey) bxor HashedVal};
tictac_hash(BinKey, BinVal, false) ->
    {erlang:phash2(BinKey), erlang:phash2(BinKey) bxor erlang:phash2(BinVal)}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

get_level2(TicTacTree, L1Pos) ->
    case array:get(L1Pos, TicTacTree#tictactree.level2) of 
        ?EMPTY ->
            Lv2SegBinSize = TicTacTree#tictactree.width * ?HASH_SIZE * 8,
            <<0:Lv2SegBinSize/integer>>;
        SrcL2 ->
            SrcL2 
    end.

get_size(Size) ->
    case Size of
        xxsmall -> 
            ?XXSMALL;
        xsmall ->
            ?XSMALL;
        small ->
            ?SMALL;
        medium ->
            ?MEDIUM;
        large ->
            ?LARGE;
        xlarge ->
            ?XLARGE
    end.

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
    L2Bin = get_level2(TicTacTree, Counter),
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


simple_bysize_test_() ->
    {timeout, 60, fun simple_bysize_test_allsizes/0}.

simple_bysize_test_allsizes() ->
    simple_test_withsize(xxsmall),
    simple_test_withsize(xsmall),
    simple_test_withsize(small),
    simple_test_withsize(medium),
    simple_test_withsize(large),
    simple_test_withsize(xlarge).

simple_test_withsize(Size) ->
    BinFun = fun(K, V) -> {term_to_binary(K), term_to_binary(V)} end,
    
    K1 = {o, "B1", "K1", null},
    K2 = {o, "B1", "K2", null},
    K3 = {o, "B1", "K3", null},

    Tree0 = new_tree(0, Size),
    Tree1 = add_kv(Tree0, K1, {caine, 1}, BinFun),
    Tree2 = add_kv(Tree1, K2, {caine, 2}, BinFun),
    Tree3 = add_kv(Tree2, K3, {caine, 3}, BinFun),
    Tree3A = add_kv(Tree3, K3, {caine, 4}, BinFun),
    ?assertMatch(true, Tree0#tictactree.level1 == Tree0#tictactree.level1),
    ?assertMatch(false, Tree0#tictactree.level1 == Tree1#tictactree.level1),
    ?assertMatch(false, Tree1#tictactree.level1 == Tree2#tictactree.level1),
    ?assertMatch(false, Tree2#tictactree.level1 == Tree3#tictactree.level1),
    ?assertMatch(false, Tree3#tictactree.level1 == Tree3A#tictactree.level1),
    
    Tree0X = new_tree(0, Size),
    Tree1X = add_kv(Tree0X, K3, {caine, 3}, BinFun),
    Tree2X = add_kv(Tree1X, K1, {caine, 1}, BinFun),
    Tree3X = add_kv(Tree2X, K2, {caine, 2}, BinFun),
    Tree3XA = add_kv(Tree3X, K3, {caine, 4}, BinFun),
    ?assertMatch(false, Tree1#tictactree.level1 == Tree1X#tictactree.level1),
    ?assertMatch(false, Tree2#tictactree.level1 == Tree2X#tictactree.level1),
    ?assertMatch(true, Tree3#tictactree.level1 == Tree3X#tictactree.level1),
    ?assertMatch(true, Tree3XA#tictactree.level1 == Tree3XA#tictactree.level1),
    
    SC = Tree0#tictactree.segment_count,

    GetSegFun = 
        fun(TK) ->
            get_segment(erlang:phash2(term_to_binary(TK)), SC)
        end,
    
    DL0 = find_dirtyleaves(Tree1, Tree0),
    ?assertMatch(true, lists:member(GetSegFun(K1), DL0)),
    DL1 = find_dirtyleaves(Tree3, Tree1),
    ?assertMatch(true, lists:member(GetSegFun(K2), DL1)),
    ?assertMatch(true, lists:member(GetSegFun(K3), DL1)),
    ?assertMatch(false, lists:member(GetSegFun(K1), DL1)),
    
    % Export and import tree to confirm no difference
    ExpTree3 = export_tree(Tree3),
    ImpTree3 = import_tree(ExpTree3),
    ?assertMatch(DL1, find_dirtyleaves(ImpTree3, Tree1)).

merge_bysize_small_test() ->
    merge_test_withsize(small).

merge_bysize_medium_test() ->
    merge_test_withsize(medium).

merge_bysize_large_test() ->
    merge_test_withsize(large).

merge_bysize_xlarge_test_() ->
    {timeout, 60, fun merge_bysize_xlarge_test2/0}.

merge_bysize_xlarge_test2() ->
    merge_test_withsize(xlarge).

merge_test_withsize(Size) ->
    BinFun = fun(K, V) -> {term_to_binary(K), term_to_binary(V)} end,
    
    TreeX0 = new_tree(0, Size),
    TreeX1 = add_kv(TreeX0, {o, "B1", "X1", null}, {caine, 1}, BinFun),
    TreeX2 = add_kv(TreeX1, {o, "B1", "X2", null}, {caine, 2}, BinFun),
    TreeX3 = add_kv(TreeX2, {o, "B1", "X3", null}, {caine, 3}, BinFun),
    TreeX4 = add_kv(TreeX3, {o, "B1", "X3", null}, {caine, 4}, BinFun),
    
    TreeY0 = new_tree(0, Size),
    TreeY1 = add_kv(TreeY0, {o, "B1", "Y1", null}, {caine, 101}, BinFun),
    TreeY2 = add_kv(TreeY1, {o, "B1", "Y2", null}, {caine, 102}, BinFun),
    TreeY3 = add_kv(TreeY2, {o, "B1", "Y3", null}, {caine, 103}, BinFun),
    TreeY4 = add_kv(TreeY3, {o, "B1", "Y3", null}, {caine, 104}, BinFun),
    
    TreeZ1 = add_kv(TreeX4, {o, "B1", "Y1", null}, {caine, 101}, BinFun),
    TreeZ2 = add_kv(TreeZ1, {o, "B1", "Y2", null}, {caine, 102}, BinFun),
    TreeZ3 = add_kv(TreeZ2, {o, "B1", "Y3", null}, {caine, 103}, BinFun),
    TreeZ4 = add_kv(TreeZ3, {o, "B1", "Y3", null}, {caine, 104}, BinFun),
    
    TreeM0 = merge_trees(TreeX4, TreeY4),
    checktree(TreeM0),
    ?assertMatch(true, TreeM0#tictactree.level1 == TreeZ4#tictactree.level1),
    
    TreeM1 = merge_trees(TreeX3, TreeY4),
    checktree(TreeM1),
    ?assertMatch(false, TreeM1#tictactree.level1 == TreeZ4#tictactree.level1).

exportable_test() ->
    {Int1, Int2} = tictac_hash(<<"key">>, <<"value">>, true),
    ?assertMatch({true, true}, {Int1 >= 0, Int2 >=0}).

-endif.

    
    
    
