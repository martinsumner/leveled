-module(tictac_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            many_put_compare/1,
            index_compare/1,
            recent_aae_noaae/1,
            recent_aae_allaae/1,
            recent_aae_bucketaae/1,
            recent_aae_expiry/1
            ]).

all() -> [
            many_put_compare,
            index_compare,
            recent_aae_noaae,
            recent_aae_allaae,
            recent_aae_bucketaae,
            recent_aae_expiry
            ].

-define(LMD_FORMAT, "~4..0w~2..0w~2..0w~2..0w~2..0w").

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
    SegmentCount = 256 * 256,
    
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
    true = length(DL3_0) == 1,
    
    % Now we want to find for the {Term, Key} pairs that make up the segment
    % diferrence (there should only be one)
    %
    % We want the database to filter on segment - so this doesn't have the
    % overheads of key listing
    
    FoldKeysIndexQFun =
        fun(_Bucket, {Term, Key}, Acc) ->
            Seg = leveled_tictac:get_segment(Key, SegmentCount),
            case lists:member(Seg, DL3_0) of
                true ->
                    [{Term, Key}|Acc];
                false ->
                    Acc
            end
        end,
    
    MismatchQ = {index_query,
                    BucketBin,
                    {FoldKeysIndexQFun, []},
                    {"idx2_bin", "!", "|"},
                    {true, undefined}},
    {async, MMFldr_2A} = leveled_bookie:book_returnfolder(Book2A, MismatchQ),
    {async, MMFldr_2B} = leveled_bookie:book_returnfolder(Book2B, MismatchQ),
    {async, MMFldr_2C} = leveled_bookie:book_returnfolder(Book2C, MismatchQ),
    {async, MMFldr_2D} = leveled_bookie:book_returnfolder(Book2D, MismatchQ),
    
    SWSS = os:timestamp(),
    SL_Joined = MMFldr_2B() ++ MMFldr_2C() ++ MMFldr_2D(),
    SL_Full = MMFldr_2A(),
    io:format("Segment search across both clusters took ~w~n",
                [timer:now_diff(os:timestamp(), SWSS)]),
    
    io:format("Joined SegList ~w~n", [SL_Joined]),
    io:format("Full SegList ~w~n", [SL_Full]),
    
    Diffs = lists:subtract(SL_Full, SL_Joined)
                ++ lists:subtract(SL_Joined, SL_Full),
    
    io:format("Differences between lists ~w~n", [Diffs]),
    
    % The actual difference is discovered
    true = lists:member({"zz999", term_to_binary("K9.Z")}, Diffs),
    % Without discovering too many others
    true = length(Diffs) < 20,


    ok = leveled_bookie:book_close(Book2A),
    ok = leveled_bookie:book_close(Book2B),
    ok = leveled_bookie:book_close(Book2C),
    ok = leveled_bookie:book_close(Book2D).


recent_aae_noaae(_Config) ->
    % Starts databases with recent_aae tables, and attempt to query to fetch
    % recent aae trees returns empty trees as no index entries are found.
    
    TreeSize = small,
    % SegmentCount = 256 * 256,
    UnitMins = 2,
    
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),
    StartOptsA = aae_startopts(RootPathA, false),
    StartOptsB = aae_startopts(RootPathB, false),
    StartOptsC = aae_startopts(RootPathC, false),
    StartOptsD = aae_startopts(RootPathD, false),
    
    % Book1A to get all objects
    {ok, Book1A} = leveled_bookie:book_start(StartOptsA),
    % Book1B/C/D will have objects partitioned across it
    {ok, Book1B} = leveled_bookie:book_start(StartOptsB),
    {ok, Book1C} = leveled_bookie:book_start(StartOptsC),
    {ok, Book1D} = leveled_bookie:book_start(StartOptsD),
    
    {B1, K1, V1, S1, MD} = {"Bucket",
                                "Key1.1.4567.4321",
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject, TestSpec} = testutil:generate_testobject(B1, K1, V1, S1, MD),
    
    SW_StartLoad = os:timestamp(),
    
    ok = testutil:book_riakput(Book1A, TestObject, TestSpec),
    ok = testutil:book_riakput(Book1B, TestObject, TestSpec),
    testutil:check_forobject(Book1A, TestObject),
    testutil:check_forobject(Book1B, TestObject),
    
    {TicTacTreeJoined, TicTacTreeFull, EmptyTree, _LMDIndexes} =
        load_and_check_recentaae(Book1A, Book1B, Book1C, Book1D,
                                    SW_StartLoad, TreeSize, UnitMins,
                                    false),
    % Go compare! Also confirm we're not comparing empty trees
    DL1_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull,
                                            TicTacTreeJoined),
    
    DL1_1 = leveled_tictac:find_dirtyleaves(TicTacTreeFull, EmptyTree),
    true = DL1_0 == [],
    true = length(DL1_1) == 0,

    ok = leveled_bookie:book_close(Book1A),
    ok = leveled_bookie:book_close(Book1B),
    ok = leveled_bookie:book_close(Book1C),
    ok = leveled_bookie:book_close(Book1D).


recent_aae_allaae(_Config) ->
    % Leveled is started in blacklisted mode with no buckets blacklisted. 
    %
    % A number of changes are then loaded into a store, and also partitioned
    % across a separate set of three stores.  A merge tree is returned from
    % both the single store and the partitioned store, and proven to compare
    % the same.
    %
    % A single change is then made, but into one half of the system only.  The
    % aae index is then re-queried and it is verified that a signle segment
    % difference is found.
    %
    % The segment Id found is then used in a query to find the Keys that make
    % up that segment, and the delta discovered should be just that one key
    % which was known to have been changed
    
    TreeSize = small,
    % SegmentCount = 256 * 256,
    UnitMins = 2,
    AAE = {blacklist, [], 60, UnitMins},
    
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),
    StartOptsA = aae_startopts(RootPathA, AAE),
    StartOptsB = aae_startopts(RootPathB, AAE),
    StartOptsC = aae_startopts(RootPathC, AAE),
    StartOptsD = aae_startopts(RootPathD, AAE),
    
    % Book1A to get all objects
    {ok, Book1A} = leveled_bookie:book_start(StartOptsA),
    % Book1B/C/D will have objects partitioned across it
    {ok, Book1B} = leveled_bookie:book_start(StartOptsB),
    {ok, Book1C} = leveled_bookie:book_start(StartOptsC),
    {ok, Book1D} = leveled_bookie:book_start(StartOptsD),
    
    {B1, K1, V1, S1, MD} = {"Bucket",
                                "Key1.1.4567.4321",
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject, TestSpec} = testutil:generate_testobject(B1, K1, V1, S1, MD),
    
    SW_StartLoad = os:timestamp(),
    
    ok = testutil:book_riakput(Book1A, TestObject, TestSpec),
    ok = testutil:book_riakput(Book1B, TestObject, TestSpec),
    testutil:check_forobject(Book1A, TestObject),
    testutil:check_forobject(Book1B, TestObject),
    
    {TicTacTreeJoined, TicTacTreeFull, EmptyTree, LMDIndexes} =
        load_and_check_recentaae(Book1A, Book1B, Book1C, Book1D,
                                    SW_StartLoad, TreeSize, UnitMins,
                                    false),
    % Go compare! Also confirm we're not comparing empty trees
    DL1_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull,
                                            TicTacTreeJoined),
    
    DL1_1 = leveled_tictac:find_dirtyleaves(TicTacTreeFull, EmptyTree),
    true = DL1_0 == [],
    true = length(DL1_1) > 100,

    ok = leveled_bookie:book_close(Book1A),
    ok = leveled_bookie:book_close(Book1B),
    ok = leveled_bookie:book_close(Book1C),
    ok = leveled_bookie:book_close(Book1D),
    
    % Book2A to get all objects
    {ok, Book2A} = leveled_bookie:book_start(StartOptsA),
    % Book2B/C/D will have objects partitioned across it
    {ok, Book2B} = leveled_bookie:book_start(StartOptsB),
    {ok, Book2C} = leveled_bookie:book_start(StartOptsC),
    {ok, Book2D} = leveled_bookie:book_start(StartOptsD),
    
    {TicTacTreeJoined, TicTacTreeFull, EmptyTree, LMDIndexes} =
        load_and_check_recentaae(Book2A, Book2B, Book2C, Book2D,
                                    SW_StartLoad, TreeSize, UnitMins,
                                    LMDIndexes),
    % Go compare! Also confirm we're not comparing empty trees
    DL1_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull,
                                            TicTacTreeJoined),
    
    DL1_1 = leveled_tictac:find_dirtyleaves(TicTacTreeFull, EmptyTree),
    true = DL1_0 == [],
    true = length(DL1_1) > 100,
    
    V2 = "Value2",
    {TestObject2, TestSpec2} =
        testutil:generate_testobject(B1, K1, V2, S1, MD),
    
    New_startTS = os:timestamp(),
    
    ok = testutil:book_riakput(Book2B, TestObject2, TestSpec2),
    testutil:check_forobject(Book2B, TestObject2),
    testutil:check_forobject(Book2A, TestObject),
    
    New_endTS = os:timestamp(),
    NewLMDIndexes = determine_lmd_indexes(New_startTS, New_endTS, UnitMins),
    {TicTacTreeJoined2, TicTacTreeFull2, _EmptyTree, NewLMDIndexes} =
        load_and_check_recentaae(Book2A, Book2B, Book2C, Book2D,
                                    New_startTS, TreeSize, UnitMins,
                                    NewLMDIndexes),
    DL2_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull2,
                                            TicTacTreeJoined2),
    
    % DL2_1 = leveled_tictac:find_dirtyleaves(TicTacTreeFull, EmptyTree),
    true = length(DL2_0) == 1,
    
    [DirtySeg] = DL2_0,
    TermPrefix = string:right(integer_to_list(DirtySeg), 8, $0),

    LMDSegFolder =
        fun(LMD, {Acc, Bookie}) ->
            IdxLMD = list_to_binary("$aae." ++ LMD ++ "_bin"),
            IdxQ1 =
                {index_query,
                    <<"$all">>,
                    {fun testutil:foldkeysfun_returnbucket/3, []},
                    {IdxLMD,
                        list_to_binary(TermPrefix ++ "."),
                        list_to_binary(TermPrefix ++ "|")},
                    {true, undefined}},
            {async, IdxFolder} =
                leveled_bookie:book_returnfolder(Bookie, IdxQ1),
            {Acc ++ IdxFolder(), Bookie}
        end,
    {KeysTerms2A, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2A},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes)),
    true = length(KeysTerms2A) >= 1,

    {KeysTerms2B, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2B},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes)),
    {KeysTerms2C, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2C},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes)),
    {KeysTerms2D, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2D},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes)),
    
    KeysTerms2Joined = KeysTerms2B ++ KeysTerms2C ++ KeysTerms2D,
    DeltaX = lists:subtract(KeysTerms2A, KeysTerms2Joined),
    DeltaY = lists:subtract(KeysTerms2Joined, KeysTerms2A),
    
    io:format("DeltaX ~w~n", [DeltaX]),
    io:format("DeltaY ~w~n", [DeltaY]),
    
    true = length(DeltaX) == 0, % This hasn't seen any extra changes
    true = length(DeltaY) == 1, % This has seen an extra change
    [{_, {B1, K1}}] = DeltaY,
    
    ok = leveled_bookie:book_close(Book2A),
    ok = leveled_bookie:book_close(Book2B),
    ok = leveled_bookie:book_close(Book2C),
    ok = leveled_bookie:book_close(Book2D).



recent_aae_bucketaae(_Config) ->
    % Configure AAE to work only on a single whitelisted bucket
    % Confirm that we can spot a delta in this bucket, but not
    % in another bucket
    
    TreeSize = small,
    % SegmentCount = 256 * 256,
    UnitMins = 2,
    AAE = {whitelist, [<<"Bucket">>], 60, UnitMins},
    
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),
    StartOptsA = aae_startopts(RootPathA, AAE),
    StartOptsB = aae_startopts(RootPathB, AAE),
    StartOptsC = aae_startopts(RootPathC, AAE),
    StartOptsD = aae_startopts(RootPathD, AAE),
    
    % Book1A to get all objects
    {ok, Book1A} = leveled_bookie:book_start(StartOptsA),
    % Book1B/C/D will have objects partitioned across it
    {ok, Book1B} = leveled_bookie:book_start(StartOptsB),
    {ok, Book1C} = leveled_bookie:book_start(StartOptsC),
    {ok, Book1D} = leveled_bookie:book_start(StartOptsD),
    
    {B1, K1, V1, S1, MD} = {<<"Bucket">>,
                                "Key1.1.4567.4321",
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject, TestSpec} = testutil:generate_testobject(B1, K1, V1, S1, MD),
    
    SW_StartLoad = os:timestamp(),
    
    ok = testutil:book_riakput(Book1A, TestObject, TestSpec),
    ok = testutil:book_riakput(Book1B, TestObject, TestSpec),
    testutil:check_forobject(Book1A, TestObject),
    testutil:check_forobject(Book1B, TestObject),
    
    {TicTacTreeJoined, TicTacTreeFull, EmptyTree, LMDIndexes} =
        load_and_check_recentaae(Book1A, Book1B, Book1C, Book1D,
                                    SW_StartLoad, TreeSize, UnitMins,
                                    false, <<"Bucket">>),
    % Go compare! Also confirm we're not comparing empty trees
    DL1_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull,
                                            TicTacTreeJoined),
    
    DL1_1 = leveled_tictac:find_dirtyleaves(TicTacTreeFull, EmptyTree),
    true = DL1_0 == [],
    true = length(DL1_1) > 100,

    ok = leveled_bookie:book_close(Book1A),
    ok = leveled_bookie:book_close(Book1B),
    ok = leveled_bookie:book_close(Book1C),
    ok = leveled_bookie:book_close(Book1D),
    
    % Book2A to get all objects
    {ok, Book2A} = leveled_bookie:book_start(StartOptsA),
    % Book2B/C/D will have objects partitioned across it
    {ok, Book2B} = leveled_bookie:book_start(StartOptsB),
    {ok, Book2C} = leveled_bookie:book_start(StartOptsC),
    {ok, Book2D} = leveled_bookie:book_start(StartOptsD),
    
    % Change the value for a key in another bucket
    % If we get trees for this period, no difference should be found
    
    V2 = "Value2",
    {TestObject2, TestSpec2} =
        testutil:generate_testobject(<<"NotBucket">>, K1, V2, S1, MD),
    
    New_startTS2 = os:timestamp(),
    
    ok = testutil:book_riakput(Book2B, TestObject2, TestSpec2),
    testutil:check_forobject(Book2B, TestObject2),
    testutil:check_forobject(Book2A, TestObject),
    
    New_endTS2 = os:timestamp(),
    NewLMDIndexes2 = determine_lmd_indexes(New_startTS2, New_endTS2, UnitMins),
    {TicTacTreeJoined2, TicTacTreeFull2, _EmptyTree, NewLMDIndexes2} =
        load_and_check_recentaae(Book2A, Book2B, Book2C, Book2D,
                                    New_startTS2, TreeSize, UnitMins,
                                    NewLMDIndexes2, <<"Bucket">>),
    DL2_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull2,
                                            TicTacTreeJoined2),
    true = length(DL2_0) == 0,
    
    % Now create an object that is a change to an existing key in the
    % monitored bucket.  A differrence should be found
    
    {TestObject3, TestSpec3} =
        testutil:generate_testobject(B1, K1, V2, S1, MD),
    
    New_startTS3 = os:timestamp(),
    
    ok = testutil:book_riakput(Book2B, TestObject3, TestSpec3),
    testutil:check_forobject(Book2B, TestObject3),
    testutil:check_forobject(Book2A, TestObject),
    
    New_endTS3 = os:timestamp(),
    NewLMDIndexes3 = determine_lmd_indexes(New_startTS3, New_endTS3, UnitMins),
    {TicTacTreeJoined3, TicTacTreeFull3, _EmptyTree, NewLMDIndexes3} =
        load_and_check_recentaae(Book2A, Book2B, Book2C, Book2D,
                                    New_startTS3, TreeSize, UnitMins,
                                    NewLMDIndexes3, <<"Bucket">>),
    DL3_0 = leveled_tictac:find_dirtyleaves(TicTacTreeFull3,
                                            TicTacTreeJoined3),
    
    % DL2_1 = leveled_tictac:find_dirtyleaves(TicTacTreeFull, EmptyTree),
    true = length(DL3_0) == 1,
    
    % Find the dirty segment, and use that to find the dirty key
    %
    % Note that unlike when monitoring $all, fold_keys can be used as there
    % is no need to return the Bucket (as hte bucket is known)
    
    [DirtySeg] = DL3_0,
    TermPrefix = string:right(integer_to_list(DirtySeg), 8, $0),

    LMDSegFolder =
        fun(LMD, {Acc, Bookie}) ->
            IdxLMD = list_to_binary("$aae." ++ LMD ++ "_bin"),
            IdxQ1 =
                {index_query,
                    <<"Bucket">>,
                    {fun testutil:foldkeysfun/3, []},
                    {IdxLMD,
                        list_to_binary(TermPrefix ++ "."),
                        list_to_binary(TermPrefix ++ "|")},
                    {true, undefined}},
            {async, IdxFolder} =
                leveled_bookie:book_returnfolder(Bookie, IdxQ1),
            {Acc ++ IdxFolder(), Bookie}
        end,
    {KeysTerms2A, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2A},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes3)),
    true = length(KeysTerms2A) >= 1,

    {KeysTerms2B, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2B},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes3)),
    {KeysTerms2C, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2C},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes3)),
    {KeysTerms2D, _} = lists:foldl(LMDSegFolder,
                                    {[], Book2D},
                                    lists:usort(LMDIndexes ++ NewLMDIndexes3)),
    
    KeysTerms2Joined = KeysTerms2B ++ KeysTerms2C ++ KeysTerms2D,
    DeltaX = lists:subtract(KeysTerms2A, KeysTerms2Joined),
    DeltaY = lists:subtract(KeysTerms2Joined, KeysTerms2A),
    
    io:format("DeltaX ~w~n", [DeltaX]),
    io:format("DeltaY ~w~n", [DeltaY]),
    
    true = length(DeltaX) == 0, % This hasn't seen any extra changes
    true = length(DeltaY) == 1, % This has seen an extra change
    [{_, K1}] = DeltaY,
    
    ok = leveled_bookie:book_close(Book2A),
    ok = leveled_bookie:book_close(Book2B),
    ok = leveled_bookie:book_close(Book2C),
    ok = leveled_bookie:book_close(Book2D).


recent_aae_expiry(_Config) ->
    % Proof that the index entries are indeed expired
    
    TreeSize = small,
    % SegmentCount = 256 * 256,
    UnitMins = 1,
    TotalMins = 2,
    AAE = {blacklist, [], TotalMins, UnitMins},
    
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    StartOptsA = aae_startopts(RootPathA, AAE),
    
    % Book1A to get all objects
    {ok, Book1A} = leveled_bookie:book_start(StartOptsA),
    
    GenMapFun =
        fun(_X) ->
            V = testutil:get_compressiblevalue(),
            Indexes = testutil:get_randomindexes_generator(8),
            testutil:generate_objects(5000,
                                        binary_uuid,
                                        [],
                                        V,
                                        Indexes)
        end,
                
    ObjLists = lists:map(GenMapFun, lists:seq(1, 3)),
    
    SW0 = os:timestamp(),
    % Load all nine lists into Book1A
    lists:foreach(fun(ObjL) -> testutil:riakload(Book1A, ObjL) end,
                    ObjLists), 
    SW1 = os:timestamp(),
    % sleep for two minutes, so all index entries will have expired
    GetTicTacTreeFun =
        fun(Bookie) ->
            get_tictactree_fun(Bookie, <<"$all">>, TreeSize)
        end,
    EmptyTree = leveled_tictac:new_tree(empty, TreeSize),
    LMDIndexes = determine_lmd_indexes(SW0, SW1, UnitMins),
    
    % Should get a non-empty answer to the query
    TicTacTree1_Full =
        lists:foldl(GetTicTacTreeFun(Book1A), EmptyTree, LMDIndexes),    
    DL3_0 = leveled_tictac:find_dirtyleaves(TicTacTree1_Full, EmptyTree),
    io:format("Dirty leaves found before expiry ~w~n", [length(DL3_0)]),

    true = length(DL3_0) > 0,
    
    SecondsSinceLMD = timer:now_diff(os:timestamp(), SW0) div 1000000,
    SecondsToExpiry = (TotalMins + UnitMins) * 60,
    
    io:format("SecondsToExpiry ~w SecondsSinceLMD ~w~n", 
                [SecondsToExpiry, SecondsSinceLMD]),
    io:format("LMDIndexes ~w~n", [LMDIndexes]),

    case SecondsToExpiry > SecondsSinceLMD of
        true ->
            timer:sleep((1 + SecondsToExpiry - SecondsSinceLMD) * 1000);
        false ->
            timer:sleep(1000)
    end,
    
    % Should now get an empty answer - all entries have expired
    TicTacTree2_Full =
        lists:foldl(GetTicTacTreeFun(Book1A), EmptyTree, LMDIndexes),    
    DL4_0 = leveled_tictac:find_dirtyleaves(TicTacTree2_Full, EmptyTree),
    io:format("Dirty leaves found after expiry ~w~n", [length(DL4_0)]),

    timer:sleep(10000),

    TicTacTree3_Full =
        lists:foldl(GetTicTacTreeFun(Book1A), EmptyTree, LMDIndexes),    
    DL5_0 = leveled_tictac:find_dirtyleaves(TicTacTree3_Full, EmptyTree),
    io:format("Dirty leaves found after expiry plus 10s ~w~n", [length(DL5_0)]),


    ok = leveled_bookie:book_close(Book1A),

    true = length(DL4_0) == 0.



load_and_check_recentaae(Book1A, Book1B, Book1C, Book1D,
                            SW_StartLoad, TreeSize, UnitMins,
                            LMDIndexes_Loaded) ->
    load_and_check_recentaae(Book1A, Book1B, Book1C, Book1D,
                                SW_StartLoad, TreeSize, UnitMins,
                                LMDIndexes_Loaded, <<"$all">>).

load_and_check_recentaae(Book1A, Book1B, Book1C, Book1D,
                            SW_StartLoad, TreeSize, UnitMins,
                            LMDIndexes_Loaded, Bucket) ->
    LMDIndexes = 
        case LMDIndexes_Loaded of
            false ->
                % Generate nine lists of objects
                % BucketBin = list_to_binary("Bucket"),
                GenMapFun =
                    fun(_X) ->
                        V = testutil:get_compressiblevalue(),
                        Indexes = testutil:get_randomindexes_generator(8),
                        testutil:generate_objects(5000,
                                                    binary_uuid,
                                                    [],
                                                    V,
                                                    Indexes)
                    end,
                
                ObjLists = lists:map(GenMapFun, lists:seq(1, 9)),
                
                % Load all nine lists into Book1A
                lists:foreach(fun(ObjL) -> testutil:riakload(Book1A, ObjL) end,
                                ObjLists), 
                
                % Split nine lists across Book1B to Book1D, three object lists
                % in each
                lists:foreach(fun(ObjL) -> testutil:riakload(Book1B, ObjL) end,
                                lists:sublist(ObjLists, 1, 3)),
                lists:foreach(fun(ObjL) -> testutil:riakload(Book1C, ObjL) end,
                                lists:sublist(ObjLists, 4, 3)),
                lists:foreach(fun(ObjL) -> testutil:riakload(Book1D, ObjL) end,
                                lists:sublist(ObjLists, 7, 3)),
                
                SW_EndLoad = os:timestamp(),
                determine_lmd_indexes(SW_StartLoad, SW_EndLoad, UnitMins);
            _ ->
                LMDIndexes_Loaded
        end,
    
    EmptyTree = leveled_tictac:new_tree(empty, TreeSize),
    
    GetTicTacTreeFun =
        fun(Bookie) ->
            get_tictactree_fun(Bookie, Bucket, TreeSize)
        end,
    
    % Get a TicTac tree representing one of the indexes in Bucket A
    TicTacTree1_Full =
        lists:foldl(GetTicTacTreeFun(Book1A), EmptyTree, LMDIndexes),
    
    TicTacTree1_P1 =
        lists:foldl(GetTicTacTreeFun(Book1B), EmptyTree, LMDIndexes),
    TicTacTree1_P2 =
        lists:foldl(GetTicTacTreeFun(Book1C), EmptyTree, LMDIndexes),
    TicTacTree1_P3 =
        lists:foldl(GetTicTacTreeFun(Book1D), EmptyTree, LMDIndexes),
    
    % Merge the tree across the partitions
    TicTacTree1_Joined = lists:foldl(fun leveled_tictac:merge_trees/2,
                                        TicTacTree1_P1,
                                        [TicTacTree1_P2, TicTacTree1_P3]),
    
    {TicTacTree1_Full, TicTacTree1_Joined, EmptyTree, LMDIndexes}.
    

aae_startopts(RootPath, AAE) ->
    LS = 2000,
    JS = 50000000,
    SS = testutil:sync_strategy(),
    [{root_path, RootPath},
        {sync_strategy, SS},
        {cache_size, LS},
        {max_journalsize, JS},
        {recent_aae, AAE}].


determine_lmd_indexes(StartTS, EndTS, UnitMins) ->
    StartDT = calendar:now_to_datetime(StartTS),
    EndDT = calendar:now_to_datetime(EndTS),
    StartTimeStr = get_strtime(StartDT, UnitMins),
    EndTimeStr = get_strtime(EndDT, UnitMins),
    
    AddTimeFun =
        fun(X, Acc) ->
            case lists:member(EndTimeStr, Acc) of
                true ->
                    Acc;
                false ->
                    NextTime =
                        UnitMins * 60 * X +
                            calendar:datetime_to_gregorian_seconds(StartDT),
                    NextDT =
                        calendar:gregorian_seconds_to_datetime(NextTime),
                    Acc ++ [get_strtime(NextDT, UnitMins)]
            end
        end,
    
    lists:foldl(AddTimeFun, [StartTimeStr], lists:seq(1, 10)).        
    
    
get_strtime(DateTime, UnitMins) ->
    {{Y, M, D}, {Hour, Minute, _Second}} = DateTime,
    RoundMins =
        UnitMins * (Minute div UnitMins),
    StrTime =
        lists:flatten(io_lib:format(?LMD_FORMAT,
                                        [Y, M, D, Hour, RoundMins])),
    StrTime.


get_tictactree_fun(Bookie, Bucket, TreeSize) ->
    fun(LMD, Acc) ->
        SW = os:timestamp(),
        ST = <<"0">>,
        ET = <<"A">>,
        Q = {tictactree_idx,
                {Bucket,
                    list_to_binary("$aae." ++ LMD ++ "_bin"),
                    ST,
                    ET},
                TreeSize,
                fun(_B, _K) -> accumulate end},
        {async, Folder} = leveled_bookie:book_returnfolder(Bookie, Q),
        R = Folder(),
        io:format("TicTac Tree for index ~s took " ++
                        "~w microseconds~n",
                    [LMD, timer:now_diff(os:timestamp(), SW)]),
        leveled_tictac:merge_trees(R, Acc)
    end.
