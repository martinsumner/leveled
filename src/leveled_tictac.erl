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

% -behaviour(gen_server).

-include("include/leveled.hrl").

-export([]).



-include_lib("eunit/include/eunit.hrl").

-define(LEVEL1_WIDTH, 256).
-define(LEVEL2_WIDTH, 4096).
-define(LEVEL2_BITWIDTH, 12).
-define(SEGMENT_COUNT, ?LEVEL1_WIDTH * ?LEVEL2_WIDTH).
-define(HASH_SIZE, 4).

-record(tictactree, {treeID ::integer(),
                        level1 :: binary(),
                        level2 :: array:array()}).

%%%============================================================================
%%% API
%%%============================================================================



%%%============================================================================
%%% External functions
%%%============================================================================


new_tree(TreeID) ->
    Lv1Width = ?LEVEL1_WIDTH * ?HASH_SIZE * 8,
    Lv1Init = <<0:Lv1Width/integer>>,
    Lv2SegBinSize = ?LEVEL2_WIDTH * ?HASH_SIZE * 8,
    Lv2SegBinInit = <<0:Lv2SegBinSize/integer>>,
    Lv2Init = array:new([{size, ?LEVEL1_WIDTH}, {default, Lv2SegBinInit}]),
    #tictactree{treeID = TreeID, level1 = Lv1Init, level2 = Lv2Init}.



add_kv(TicTacTree, Key, Value, HashFun) ->
    HashV = HashFun(Key, Value),
    SegChangeHash = erlang:phash2(Key, HashV),
    Segment = get_segment(Key),
    
    Level2Pos = Segment band (?LEVEL2_WIDTH - 1),
    Level1Pos = (Segment bsr ?LEVEL2_BITWIDTH) band (?LEVEL1_WIDTH - 1),
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
    SegLeaf1Upd = SegLeaf1 bxor SegLeaf2 bxor SegLeaf2Upd,
    
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
    

find_dirtyleaves(SrcTree, SinkTree) ->
    IdxList = segmentcompare(SrcTree#tictactree.level1,
                                SinkTree#tictactree.level1),
    
    FoldFun =
        fun(Idx, Acc) ->
            L2IdxList =
                segmentcompare(array:get(Idx, SrcTree#tictactree.level2),
                                array:get(Idx, SinkTree#tictactree.level2)),
            
            Acc ++ lists:map(fun(X) -> X + Idx * ?LEVEL2_WIDTH end, L2IdxList)
        end,
    
    lists:sort(lists:foldl(FoldFun, [], IdxList)).


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

get_segment(Key) ->
    erlang:phash2(Key) band (?SEGMENT_COUNT - 1).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


simple_test() ->
    HashFun = fun(_K, V) -> erlang:phash2(V) end,
    
    Tree0 = new_tree(0),
    Tree1 = add_kv(Tree0, "K1", 1, HashFun),
    Tree2 = add_kv(Tree1, "K2", 2, HashFun),
    Tree3 = add_kv(Tree2, "K3", 3, HashFun),
    Tree3A = add_kv(Tree3, "K3", 4, HashFun),
    ?assertMatch(true, Tree0#tictactree.level1 == Tree0#tictactree.level1),
    ?assertMatch(false, Tree0#tictactree.level1 == Tree1#tictactree.level1),
    ?assertMatch(false, Tree1#tictactree.level1 == Tree2#tictactree.level1),
    ?assertMatch(false, Tree2#tictactree.level1 == Tree3#tictactree.level1),
    ?assertMatch(false, Tree3#tictactree.level1 == Tree3A#tictactree.level1),
    
    Tree0X = new_tree(0),
    Tree1X = add_kv(Tree0X, "K3", 3, HashFun),
    Tree2X = add_kv(Tree1X, "K1", 1, HashFun),
    Tree3X = add_kv(Tree2X, "K2", 2, HashFun),
    Tree3XA = add_kv(Tree3X, "K3", 4, HashFun),
    ?assertMatch(false, Tree1#tictactree.level1 == Tree1X#tictactree.level1),
    ?assertMatch(false, Tree2#tictactree.level1 == Tree2X#tictactree.level1),
    ?assertMatch(true, Tree3#tictactree.level1 == Tree3X#tictactree.level1),
    ?assertMatch(true, Tree3XA#tictactree.level1 == Tree3XA#tictactree.level1),
    
    DL0 = find_dirtyleaves(Tree1, Tree0),
    ?assertMatch(true, lists:member(get_segment("K1"), DL0)),
    DL1 = find_dirtyleaves(Tree3, Tree1),
    ?assertMatch(true, lists:member(get_segment("K2"), DL1)),
    ?assertMatch(true, lists:member(get_segment("K3"), DL1)),
    ?assertMatch(false, lists:member(get_segment("K1"), DL1)).


-endif.

    
    
    
