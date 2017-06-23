-module(tictac_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            many_put_compare/1,
            index_compare/1
            ]).

all() -> [
            many_put_compare,
            index_compare
            ].


many_put_compare(_Config) ->
    TreeSize = small,
    SegmentCount = 256 * 256,
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),
    
    % Start the first database, load a test object, close it, start it again
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, riak_sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {B1, K1, V1, S1, MD} = {"Bucket",
                                "Key1.1.4567.4321",
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject, TestSpec} = testutil:generate_testobject(B1, K1, V1, S1, MD),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPathA},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 32000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forobject(Bookie2, TestObject),
    
    % Generate 200K objects to be sued within the test, and load them into
    % the first store (outputting the generated objects as a list of lists)
    % to be used elsewhere
    
    GenList = [2, 20002, 40002, 60002, 80002,
                100002, 120002, 140002, 160002, 180002],
    CLs = testutil:load_objects(20000,
                                GenList,
                                Bookie2,
                                TestObject,
                                fun testutil:generate_smallobjects/2,
                                20000),
    
    % Start a new store, and load the same objects (except fot the original
    % test object) into this store
    
    StartOpts3 = [{root_path, RootPathB},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie3, ObjL) end, CLs),
    
    % Now run a tictac query against both stores to see th extent to which
    % state between stores is consistent
    
    TicTacQ = {tictactree_obj,
                {o_rkv, "Bucket", null, null, false},
                TreeSize,
                fun(_B, _K) -> accumulate end},
    {async, TreeAFolder} = leveled_bookie:book_returnfolder(Bookie2, TicTacQ),
    {async, TreeBFolder} = leveled_bookie:book_returnfolder(Bookie3, TicTacQ),
    SWA0 = os:timestamp(),
    TreeA = TreeAFolder(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWA0)]),
    SWB0 = os:timestamp(),
    TreeB = TreeBFolder(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWB0)]),
    SWC0 = os:timestamp(),
    SegList0 = leveled_tictac:find_dirtyleaves(TreeA, TreeB),
    io:format("Compare tictac trees with 200K objects  in ~w~n",
                [timer:now_diff(os:timestamp(), SWC0)]),
    io:format("Tree comparison shows ~w different leaves~n",
                [length(SegList0)]),
    AltList = leveled_tictac:find_dirtyleaves(TreeA, 
                                                leveled_tictac:new_tree(0)),
    io:format("Tree comparison shows ~w altered leaves~n",
                [length(AltList)]),
    true = length(SegList0) == 1, 
    % only the test object should be different
    true = length(AltList) > 10000, 
    % check there are a significant number of differences from empty
    
    FoldKeysFun =
        fun(SegListToFind) ->
            fun(_B, K, Acc) ->
                Seg = leveled_tictac:get_segment(K, SegmentCount),
                case lists:member(Seg, SegListToFind) of
                    true ->
                        [K|Acc];
                    false ->
                        Acc
                end
            end
        end,
    SegQuery = {keylist, o_rkv, "Bucket", {FoldKeysFun(SegList0), []}},
    {async, SegKeyFinder} =
        leveled_bookie:book_returnfolder(Bookie2, SegQuery),
    SWSKL0 = os:timestamp(),
    SegKeyList = SegKeyFinder(),
    io:format("Finding ~w keys in ~w dirty segments in ~w~n",
                [length(SegKeyList),
                    length(SegList0),
                    timer:now_diff(os:timestamp(), SWSKL0)]),
    
    true = length(SegKeyList) >= 1,
    true = length(SegKeyList) < 10,
    true = lists:member("Key1.1.4567.4321", SegKeyList),
    
    % Now remove the object which represents the difference between these
    % stores and confirm that the tictac trees will now match
    
    testutil:book_riakdelete(Bookie2, B1, K1, []),
    {async, TreeAFolder0} = leveled_bookie:book_returnfolder(Bookie2, TicTacQ),
    SWA1 = os:timestamp(),
    TreeA0 = TreeAFolder0(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWA1)]),

    SegList1 = leveled_tictac:find_dirtyleaves(TreeA0, TreeB),
    io:format("Tree comparison following delete shows ~w different leaves~n",
                [length(SegList1)]),
    true = length(SegList1) == 0,
    % Removed test object so tictac trees should match
    
    ok = testutil:book_riakput(Bookie3, TestObject, TestSpec),
    {async, TreeBFolder0} = leveled_bookie:book_returnfolder(Bookie3, TicTacQ),
    SWB1 = os:timestamp(),
    TreeB0 = TreeBFolder0(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWB1)]),
    SegList2 = leveled_tictac:find_dirtyleaves(TreeA0, TreeB0),
    true = SegList2 == SegList0,
    % There is an identical difference now the difference is on Bookie3 not
    % Bookie 2 (compared to it being in Bookie2 not Bookie3)

    ok = leveled_bookie:book_close(Bookie3),
    
    % Replace Bookie 3 with two stores Bookie 4 and Bookie 5 where the ojects
    % have been randomly split between the stores
    
    StartOpts4 = [{root_path, RootPathC},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts4),
    StartOpts5 = [{root_path, RootPathD},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie5} = leveled_bookie:book_start(StartOpts5),
    
    SplitFun =
        fun(Obj) ->
            case erlang:phash2(Obj) rem 2 of
                0 ->
                    true;
                1 ->
                    false
            end
        end,
    lists:foreach(fun(ObjL) ->
                        {ObjLA, ObjLB} = lists:partition(SplitFun, ObjL),
                        testutil:riakload(Bookie4, ObjLA),
                        testutil:riakload(Bookie5, ObjLB)
                    end,
                    CLs),
    
    % query both the stores, then merge the trees - the result should be the
    % same as the result from the tree created aginst the store with both
    % partitions
    
    {async, TreeC0Folder} = leveled_bookie:book_returnfolder(Bookie4, TicTacQ),
    {async, TreeC1Folder} = leveled_bookie:book_returnfolder(Bookie5, TicTacQ),
    SWD0 = os:timestamp(),
    TreeC0 = TreeC0Folder(),
    io:format("Build tictac tree with 100K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWD0)]),
    SWD1 = os:timestamp(),
    TreeC1 = TreeC1Folder(),
    io:format("Build tictac tree with 100K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWD1)]),
    
    TreeC2 = leveled_tictac:merge_trees(TreeC0, TreeC1),
    SegList3 = leveled_tictac:find_dirtyleaves(TreeC2, TreeB),
    io:format("Tree comparison following delete shows ~w different leaves~n",
                [length(SegList3)]),
    true = length(SegList3) == 0,
    
    
    ok = leveled_bookie:book_close(Bookie2),
    ok = leveled_bookie:book_close(Bookie4),
    ok = leveled_bookie:book_close(Bookie5).
    

index_compare(_Config) ->
    TreeSize = small,
    LS = 2000,
    JS = 50000000,
    SS = testutil:sync_strategy(),
    % SegmentCount = 256 * 256,
    
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),
    % Book1A to get all objects
    {ok, Book1A} = leveled_bookie:book_start(RootPathA, LS, JS, SS),
    % Book1B/C/D will have objects partitioned across it
    {ok, Book1B} = leveled_bookie:book_start(RootPathB, LS, JS, SS),
    {ok, Book1C} = leveled_bookie:book_start(RootPathC, LS, JS, SS),
    {ok, Book1D} = leveled_bookie:book_start(RootPathD, LS, JS, SS),
    
    % Generate nine lists of objects
    BucketBin = list_to_binary("Bucket"),
    GenMapFun =
        fun(_X) ->
            V = testutil:get_compressiblevalue(),
            Indexes = testutil:get_randomindexes_generator(8),
            testutil:generate_objects(10000, binary_uuid, [], V, Indexes)
        end,
    
    ObjLists = lists:map(GenMapFun, lists:seq(1, 9)),
    
    % Load all nine lists into Book1A
    lists:foreach(fun(ObjL) -> testutil:riakload(Book1A, ObjL) end,
                    ObjLists), 
    
    % Split nine lists across Book1B to Book1D, three object lists in each
    lists:foreach(fun(ObjL) -> testutil:riakload(Book1B, ObjL) end,
                    lists:sublist(ObjLists, 1, 3)),
    lists:foreach(fun(ObjL) -> testutil:riakload(Book1C, ObjL) end,
                    lists:sublist(ObjLists, 4, 3)),
    lists:foreach(fun(ObjL) -> testutil:riakload(Book1D, ObjL) end,
                    lists:sublist(ObjLists, 7, 3)),
    
    GetTicTacTreeFun =
        fun(X, Bookie) ->
            SW = os:timestamp(),
            ST = "!",
            ET = "|",
            Q = {tictactree_idx,
                    {BucketBin, "idx" ++ integer_to_list(X) ++ "_bin", ST, ET},
                    TreeSize,
                    fun(_B, _K) -> accumulate end},
            {async, Folder} = leveled_bookie:book_returnfolder(Bookie, Q),
            R = Folder(),
            io:format("TicTac Tree for index ~w took " ++
                            "~w microseconds~n",
                        [X, timer:now_diff(os:timestamp(), SW)]),
            R
        end,
    
    % Get a TicTac tree representing one of the indexes in Bucket A
    TicTacTree1_Full = GetTicTacTreeFun(1, Book1A),
    TicTacTree1_P1 = GetTicTacTreeFun(1, Book1B),
    TicTacTree1_P2 = GetTicTacTreeFun(1, Book1C),
    TicTacTree1_P3 = GetTicTacTreeFun(1, Book1D),
    
    % Merge the tree across the partitions
    TicTacTree1_Joined = lists:foldl(fun leveled_tictac:merge_trees/2,
                                        TicTacTree1_P1,
                                        [TicTacTree1_P2, TicTacTree1_P3]),
    
    % Go compare! Also check we're not comparing empty trees
    DL1_0 = leveled_tictac:find_dirtyleaves(TicTacTree1_Full,
                                            TicTacTree1_Joined),
    EmptyTree = leveled_tictac:new_tree(empty, TreeSize),
    DL1_1 = leveled_tictac:find_dirtyleaves(TicTacTree1_Full, EmptyTree),
    true = DL1_0 == [],
    true = length(DL1_1) > 100,

    ok = leveled_bookie:book_close(Book1A),
    ok = leveled_bookie:book_close(Book1B),
    ok = leveled_bookie:book_close(Book1C),
    ok = leveled_bookie:book_close(Book1D),
    
    % Double chekc all is well still after a restart
    % Book1A to get all objects
    {ok, Book2A} = leveled_bookie:book_start(RootPathA, LS, JS, SS),
    % Book1B/C/D will have objects partitioned across it
    {ok, Book2B} = leveled_bookie:book_start(RootPathB, LS, JS, SS),
    {ok, Book2C} = leveled_bookie:book_start(RootPathC, LS, JS, SS),
    {ok, Book2D} = leveled_bookie:book_start(RootPathD, LS, JS, SS),
    % Get a TicTac tree representing one of the indexes in Bucket A
    TicTacTree2_Full = GetTicTacTreeFun(2, Book2A),
    TicTacTree2_P1 = GetTicTacTreeFun(2, Book2B),
    TicTacTree2_P2 = GetTicTacTreeFun(2, Book2C),
    TicTacTree2_P3 = GetTicTacTreeFun(2, Book2D),
    
    % Merge the tree across the partitions
    TicTacTree2_Joined = lists:foldl(fun leveled_tictac:merge_trees/2,
                                        TicTacTree2_P1,
                                        [TicTacTree2_P2, TicTacTree2_P3]),
    
    % Go compare! Also check we're not comparing empty trees
    DL2_0 = leveled_tictac:find_dirtyleaves(TicTacTree2_Full,
                                            TicTacTree2_Joined),
    EmptyTree = leveled_tictac:new_tree(empty, TreeSize),
    DL2_1 = leveled_tictac:find_dirtyleaves(TicTacTree2_Full, EmptyTree),
    true = DL2_0 == [],
    true = length(DL2_1) > 100,
    
    IdxSpc = {add, "idx2_bin", "zz999"},
    {TestObj, TestSpc} = testutil:generate_testobject(BucketBin,
                                                        term_to_binary("K9.Z"),
                                                        "Value1",
                                                        [IdxSpc],
                                                        [{"MDK1", "MDV1"}]),
    ok = testutil:book_riakput(Book2C, TestObj, TestSpc),
    testutil:check_forobject(Book2C, TestObj),
    
    TicTacTree3_Full = GetTicTacTreeFun(2, Book2A),
    TicTacTree3_P1 = GetTicTacTreeFun(2, Book2B),
    TicTacTree3_P2 = GetTicTacTreeFun(2, Book2C),
    TicTacTree3_P3 = GetTicTacTreeFun(2, Book2D),
    
    % Merge the tree across the partitions
    TicTacTree3_Joined = lists:foldl(fun leveled_tictac:merge_trees/2,
                                        TicTacTree3_P1,
                                        [TicTacTree3_P2, TicTacTree3_P3]),
                                        
    % Find all keys index, and then just the last key
    IdxQ1 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {"idx2_bin", "zz", "zz|"},
                {true, undefined}},
    {async, IdxFolder1} = leveled_bookie:book_returnfolder(Book2C, IdxQ1),
    true = IdxFolder1() >= 1,
    
    DL_3to2B = leveled_tictac:find_dirtyleaves(TicTacTree2_P1,
                                                TicTacTree3_P1),
    DL_3to2C = leveled_tictac:find_dirtyleaves(TicTacTree2_P2,
                                                TicTacTree3_P2),
    DL_3to2D = leveled_tictac:find_dirtyleaves(TicTacTree2_P3,
                                                TicTacTree3_P3),
    io:format("Individual tree comparison found dirty leaves of ~w ~w ~w~n",
                [DL_3to2B, DL_3to2C, DL_3to2D]),
    
    true = length(DL_3to2B) == 0,
    true = length(DL_3to2C) == 1,
    true = length(DL_3to2D) == 0,
    
    % Go compare! Should find a difference in one leaf
    DL3_0 = leveled_tictac:find_dirtyleaves(TicTacTree3_Full,
                                            TicTacTree3_Joined),
    io:format("Different leaves count ~w~n", [length(DL3_0)]),
    true = length(DL3_0) == 1.
