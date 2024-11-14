-module(tictac_SUITE).
-include("leveled.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
            multiput_subkeys/1,
            many_put_compare/1,
            index_compare/1,
            basic_headonly/1,
            tuplebuckets_headonly/1
            ]).

all() -> [
            multiput_subkeys,
            many_put_compare,
            index_compare,
            basic_headonly,
            tuplebuckets_headonly
            ].

-define(V1_VERS, 1).
-define(MAGIC, 53). % riak_kv -> riak_object

init_per_suite(Config) ->
    testutil:init_per_suite([{suite, "tictac"}|Config]),
    Config.

end_per_suite(Config) ->
    testutil:end_per_suite(Config).


multiput_subkeys(_Config) ->
    multiput_subkeys_byvalue({null, 0}),
    multiput_subkeys_byvalue(null),
    multiput_subkeys_byvalue(<<"binaryValue">>).

multiput_subkeys_byvalue(V) ->
    RootPath = testutil:reset_filestructure("subkeyTest"),
    StartOpts = [{root_path, RootPath},
                    {max_journalsize, 10000000},
                    {max_pencillercachesize, 12000},
                    {head_only, no_lookup},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie} = leveled_bookie:book_start(StartOpts),
    SubKeyCount = 200000,

    B = {<<"MultiBucketType">>, <<"MultiBucket">>},
    ObjSpecLGen =
        fun(K) ->
            lists:map(
                fun(I) ->
                    {add, v1, B, K, <<I:32/integer>>, [os:timestamp()], V}
                end,
                lists:seq(1, SubKeyCount)
            )
        end,
    
    SpecL1 = ObjSpecLGen(<<1:32/integer>>),
    load_objectspecs(SpecL1, 32, Bookie),
    SpecL2 = ObjSpecLGen(<<2:32/integer>>),
    load_objectspecs(SpecL2, 32, Bookie),
    SpecL3 = ObjSpecLGen(<<3:32/integer>>),
    load_objectspecs(SpecL3, 32, Bookie),
    SpecL4 = ObjSpecLGen(<<4:32/integer>>),
    load_objectspecs(SpecL4, 32, Bookie),
    SpecL5 = ObjSpecLGen(<<5:32/integer>>),
    load_objectspecs(SpecL5, 32, Bookie),

    FoldFun =
        fun(Bucket, {Key, SubKey}, _Value, Acc) ->
            case Bucket of
                Bucket when Bucket == B ->
                    [{Key, SubKey}|Acc]
            end
        end,
    QueryFun =
        fun(KeyRange) ->
            Range = {range, B, KeyRange},
            {async, R} =
                leveled_bookie:book_headfold(
                    Bookie, ?HEAD_TAG, Range, {FoldFun, []}, false, true, false
                ),
            L = length(R()),
            io:format("query result for range ~p is ~w~n", [Range, L]),
            L
        end,
    
    KR1 = {{<<1:32/integer>>, <<>>}, {<<2:32/integer>>, <<>>}},
    KR2 = {{<<3:32/integer>>, <<>>}, {<<5:32/integer>>, <<>>}},
    KR3 =
        {
            {<<1:32/integer>>, <<10:32/integer>>},
            {<<2:32/integer>>, <<19:32/integer>>}
        },
    true = SubKeyCount == QueryFun(KR1),
    true = (SubKeyCount * 2) == QueryFun(KR2),
    true = (SubKeyCount + 10) == QueryFun(KR3),
    leveled_bookie:book_destroy(Bookie).

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
    {B1, K1, V1, S1, MD} =
        {
            <<"Bucket">>,
            <<"Key1.1.4567.4321">>,
            <<"Value1">>,
            [],
            [{<<"MDK1">>, <<"MDV1">>}]
        },
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
    CLs =
        testutil:load_objects(
            20000,
            GenList,
            Bookie2,
            TestObject,
            fun testutil:generate_smallobjects/2,
            20000
        ),

    % Start a new store, and load the same objects (except fot the original
    % test object) into this store

    StartOpts3 = [{root_path, RootPathB},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie3, ObjL) end, CLs),

    % Now run a tictac query against both stores to see the extent to which
    % state between stores is consistent

    TicTacQ = {tictactree_obj,
                {o_rkv, <<"Bucket">>, null, null, true},
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
    AltList =
      leveled_tictac:find_dirtyleaves(TreeA,
                                      leveled_tictac:new_tree(0, TreeSize)),
    io:format("Tree comparison shows ~w altered leaves~n",
                [length(AltList)]),
    true = length(SegList0) == 1,
    % only the test object should be different
    true = length(AltList) > 10000,
    % check there are a significant number of differences from empty

    WrongPartitionTicTacQ =
        {
            tictactree_obj,
            {o_rkv, <<"Bucket">>, null, null, false},
            TreeSize,
            fun(_B, _K) -> pass end
        },
    {async, TreeAFolder_WP} = 
        leveled_bookie:book_returnfolder(Bookie2, WrongPartitionTicTacQ),
    TreeAWP = TreeAFolder_WP(),
    DoubleEmpty =
        leveled_tictac:find_dirtyleaves(TreeAWP,
                                        leveled_tictac:new_tree(0, TreeSize)),
    true = length(DoubleEmpty) == 0,

    % Now run the same query by putting the tree-building responsibility onto
    % the fold_objects_fun

    ExtractClockFun =
        fun(Key, Value) ->
            {proxy_object, HeadBin, _Size, _FetchFun} = binary_to_term(Value),
            <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
                VclockBin:VclockLen/binary, _Rest/binary>> = HeadBin,
            case is_binary(Key) of 
                true -> 
                    {Key, 
                        lists:sort(binary_to_term(VclockBin))};
                false ->
                    {term_to_binary(Key), 
                        lists:sort(binary_to_term(VclockBin))}
            end
        end,
    FoldObjectsFun =
      fun(_Bucket, Key, Value, Acc) ->
          leveled_tictac:add_kv(Acc, Key, Value, ExtractClockFun)
      end,

    FoldAccT = {FoldObjectsFun, leveled_tictac:new_tree(0, TreeSize)},
    {async, TreeAObjFolder0} =
        leveled_bookie:book_headfold(Bookie2,
                                     o_rkv,
                                     {range, <<"Bucket">>, all},
                                     FoldAccT,
                                     false,
                                     true,
                                     false),
    
    SWB0Obj = os:timestamp(),
    TreeAObj0 = TreeAObjFolder0(),
    io:format("Build tictac tree via object fold with no "++
                    "presence check and 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWB0Obj)]),
    true = length(leveled_tictac:find_dirtyleaves(TreeA, TreeAObj0)) == 0,

    InitAccTree = leveled_tictac:new_tree(0, TreeSize, true),
    
    {async, TreeAObjFolder1} =
        leveled_bookie:book_headfold(
            Bookie2, 
            ?RIAK_TAG,
            {range, <<"Bucket">>, all},
            {FoldObjectsFun, InitAccTree},
            true,
            true,
            false
        ),
    SWB1Obj = os:timestamp(),
    TreeAObj1 = TreeAObjFolder1(),
    io:format(
        "Build tictac tree via object fold with map level 1 "
        "presence check and 200K objects in ~w~n",
        [timer:now_diff(os:timestamp(), SWB1Obj)]
    ),
    true = length(leveled_tictac:find_dirtyleaves(TreeA, TreeAObj1)) == 0,
    {async, TreeAObjFolder1Alt} =
        leveled_bookie:book_headfold(
            Bookie2, 
            ?RIAK_TAG,
            {range, <<"Bucket">>, all},
            {FoldObjectsFun, leveled_tictac:new_tree(0, TreeSize, false)},
            true,
            true,
            false
        ),
    SWB1ObjAlt = os:timestamp(),
    TreeAObj1Alt = TreeAObjFolder1Alt(),
    io:format(
        "Build tictac tree via object fold with binary level 1 "
        "presence check and 200K objects in ~w~n",
        [timer:now_diff(os:timestamp(), SWB1ObjAlt)]
    ),
    true = length(leveled_tictac:find_dirtyleaves(TreeA, TreeAObj1Alt)) == 0,

    % For an exportable comparison, want hash to be based on something not 
    % coupled to erlang language - so use exportable query
    AltExtractFun =
        fun(K, V) ->
            {proxy_object, HeadBin, _Size, _FetchFun} = binary_to_term(V),
            <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
                VclockBin:VclockLen/binary, _Rest/binary>> = HeadBin,
            {term_to_binary(K), VclockBin}
        end,
    AltFoldObjectsFun =
        fun(_Bucket, Key, Value, Acc) ->
            leveled_tictac:add_kv(Acc, Key, Value, AltExtractFun)
        end,
    
    {async, TreeAAltObjFolder0} =
        leveled_bookie:book_headfold(
            Bookie2, 
            ?RIAK_TAG,
            {range, <<"Bucket">>, all},
            {AltFoldObjectsFun,  InitAccTree},
            false,
            true,
            false
        ),
    SWB2Obj = os:timestamp(),
    TreeAAltObj = TreeAAltObjFolder0(),
    io:format(
        "Build tictac tree via object fold with no "
        "presence check and 200K objects  and alt hash in ~w~n",
        [timer:now_diff(os:timestamp(), SWB2Obj)]
    ),
    {async, TreeBAltObjFolder0} =
        leveled_bookie:book_headfold(
            Bookie3, 
            ?RIAK_TAG,
            {range, <<"Bucket">>, all},
            {AltFoldObjectsFun, InitAccTree},
            false,
            true,
            false
        ),
    SWB3Obj = os:timestamp(),
    TreeBAltObj = TreeBAltObjFolder0(),
    io:format(
        "Build tictac tree via object fold with no "
        "presence check and 200K objects  and alt hash in ~w~n",
        [timer:now_diff(os:timestamp(), SWB3Obj)]),
    DL_ExportFold = 
        length(leveled_tictac:find_dirtyleaves(TreeBAltObj, TreeAAltObj)),
    io:format("Found dirty leaves with exportable comparison of ~w~n",
                [DL_ExportFold]),
    true = DL_ExportFold == 1,


    %% Finding differing keys
    FoldKeysFun =
        fun(SegListToFind) ->
            fun(_B, K, Acc) ->
                Seg = get_segment(K, SegmentCount),
                case lists:member(Seg, SegListToFind) of
                    true ->
                        [K|Acc];
                    false ->
                        Acc
                end
            end
        end,
    SegQuery = {keylist, o_rkv, <<"Bucket">>, {FoldKeysFun(SegList0), []}},
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
    true = lists:member(<<"Key1.1.4567.4321">>, SegKeyList),

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
    TreeSize = xxsmall,
    LS = 2000,
    JS = 50000000,
    SS = testutil:sync_strategy(),
    SegmentCount = 64 * 64,

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
            ST = <<"!">>,
            ET = <<"|">>,
            Q = {tictactree_idx,
                    {BucketBin,
                        list_to_binary("idx" ++ integer_to_list(X) ++ "_bin"),
                        ST,
                        ET},
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

    IdxSpc = {add, <<"idx2_bin">>, <<"zz999">>},
    {TestObj, TestSpc} =
        testutil:generate_testobject(
            BucketBin,
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
    TicTacTree3_Joined =
        lists:foldl(
            fun leveled_tictac:merge_trees/2,
            TicTacTree3_P1,
            [TicTacTree3_P2, TicTacTree3_P3]),

    % Find all keys index, and then just the last key
    IdxQ1 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {<<"idx2_bin">>, <<"zz">>, <<"zz|">>},
                {true, undefined}},
    {async, IdxFolder1} = leveled_bookie:book_returnfolder(Book2C, IdxQ1),
    true = IdxFolder1() >= 1,

    DL_3to2B =
        leveled_tictac:find_dirtyleaves(
            TicTacTree2_P1, TicTacTree3_P1),
    DL_3to2C =
        leveled_tictac:find_dirtyleaves(
            TicTacTree2_P2, TicTacTree3_P2),
    DL_3to2D =
        leveled_tictac:find_dirtyleaves(
            TicTacTree2_P3, TicTacTree3_P3),
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
            Seg = get_segment(Key, SegmentCount),
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
                    {<<"idx2_bin">>, <<"!">>, <<"|">>},
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
    true = lists:member({<<"zz999">>, term_to_binary("K9.Z")}, Diffs),
    % Without discovering too many others
    true = length(Diffs) < 20,


    ok = leveled_bookie:book_close(Book2A),
    ok = leveled_bookie:book_close(Book2B),
    ok = leveled_bookie:book_close(Book2C),
    ok = leveled_bookie:book_close(Book2D).


tuplebuckets_headonly(_Config) ->
    ObjectCount = 60000,

    RootPathHO = testutil:reset_filestructure("testTBHO"),
    StartOpts1 = [{root_path, RootPathHO},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, none},
                    {head_only, with_lookup},
                    {max_journalsize, 500000}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    ObjectSpecFun =
        fun(Op) -> 
            fun(N) ->
                Bucket = {<<"BucketType">>, <<"B", 0:4/integer, N:4/integer>>},
                Key = <<"K", N:32/integer>>,
                <<Hash:32/integer, _RestBN/bitstring>> =
                    crypto:hash(md5, <<N:32/integer>>),
                {Op, Bucket, Key, null, Hash}
            end 
        end,
    
    ObjectSpecL = lists:map(ObjectSpecFun(add), lists:seq(1, ObjectCount)),

    SW0 = os:timestamp(),
    ok = load_objectspecs(ObjectSpecL, 32, Bookie1),
    io:format("Loaded an object count of ~w in ~w ms~n", 
                [ObjectCount, timer:now_diff(os:timestamp(), SW0)/1000]),
    
    CheckHeadFun = 
        fun({add, B, K, null, H}) ->
            {ok, H} = 
                leveled_bookie:book_headonly(Bookie1, B, K, null)
        end,
    lists:foreach(CheckHeadFun, ObjectSpecL),

    BucketList = 
        lists:map(fun(I) ->
                        {<<"BucketType">>, <<"B", 0:4/integer, I:4/integer>>}
                    end,
                    lists:seq(0, 15)),

    FoldHeadFun =
        fun(B, {K, null}, V, Acc) ->
            [{add, B, K, null, V}|Acc]
        end,
    SW1 = os:timestamp(),

    {async, HeadRunner1} =
        leveled_bookie:book_headfold(
            Bookie1,
            ?HEAD_TAG,
            {bucket_list, BucketList},
            {FoldHeadFun, []},
            false, false,
            false
        ),
    ReturnedObjSpecL1 = lists:reverse(HeadRunner1()),
    [FirstItem|_Rest] = ReturnedObjSpecL1,
    LastItem = lists:last(ReturnedObjSpecL1),

    io:format(
        "Returned ~w objects with first ~w and last ~w in ~w ms~n",
        [length(ReturnedObjSpecL1),
                FirstItem, LastItem,
                timer:now_diff(os:timestamp(), SW1)/1000]),

    true = ReturnedObjSpecL1 == lists:sort(ObjectSpecL),

    {add, {TB, B1}, K1, null, _H1} = FirstItem,
    {add, {TB, BL}, KL, null, _HL} = LastItem,
    SegList = [testutil:get_aae_segment({TB, B1}, K1),
                testutil:get_aae_segment({TB, BL}, KL)],
    
    SW2 = os:timestamp(),
    {async, HeadRunner2} =
        leveled_bookie:book_headfold(
            Bookie1,
            ?HEAD_TAG,
            {bucket_list, BucketList},
            {FoldHeadFun, []},
            false, false,
            SegList
        ),
    ReturnedObjSpecL2 = lists:reverse(HeadRunner2()),

    io:format("Returned ~w objects using seglist in ~w ms~n",
                [length(ReturnedObjSpecL2),
                    timer:now_diff(os:timestamp(), SW2)/1000]),
    
    true = length(ReturnedObjSpecL2) < (ObjectCount/1000 + 2),
        % Not too many false positives
    true = lists:member(FirstItem, ReturnedObjSpecL2),
    true = lists:member(LastItem, ReturnedObjSpecL2),

    leveled_bookie:book_destroy(Bookie1).


basic_headonly(_Config) ->
    ObjectCount = 200000,
    RemoveCount = 100,
    basic_headonly_test(ObjectCount, RemoveCount, with_lookup),
    basic_headonly_test(ObjectCount, RemoveCount, no_lookup).


basic_headonly_test(ObjectCount, RemoveCount, HeadOnly) ->
    % Load some AAE type objects into Leveled using the read_only mode.  This
    % should allow for the items to be added in batches.  Confirm that the 
    % journal is garbage collected as expected, and that it is possible to 
    % perform a fold_heads style query 
    RootPathHO = testutil:reset_filestructure("testHO"),
    StartOpts1 = [{root_path, RootPathHO},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, sync},
                    {head_only, HeadOnly},
                    {max_journalsize, 500000}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {B1, K1, V1, S1, MD} =
        {
            <<"Bucket">>,
            <<"Key1.1.4567.4321">>,
            <<"Value1">>,
            [],
            [{<<"MDK1">>, <<"MDV1">>}]
        },
    {TestObject, TestSpec} = testutil:generate_testobject(B1, K1, V1, S1, MD),
    {unsupported_message, put} = 
        testutil:book_riakput(Bookie1, TestObject, TestSpec),
    
    ObjectSpecFun =
        fun(Op) -> 
            fun(N) ->
                Bucket = <<"B", N:8/integer>>,
                Key = <<"K", N:32/integer>>,
                <<SegmentID:20/integer, _RestBS/bitstring>> = 
                    crypto:hash(md5, term_to_binary({Bucket, Key})),
                <<Hash:32/integer, _RestBN/bitstring>> =
                    crypto:hash(md5, <<N:32/integer>>),
                {Op, <<SegmentID:32/integer>>, Bucket, Key, Hash}
            end 
        end,
    
    ObjectSpecL = lists:map(ObjectSpecFun(add), lists:seq(1, ObjectCount)),

    SW0 = os:timestamp(),
    ok = load_objectspecs(ObjectSpecL, 32, Bookie1),
    io:format("Loaded an object count of ~w in ~w microseconds with ~w~n", 
                [ObjectCount, timer:now_diff(os:timestamp(), SW0), HeadOnly]),

    FoldFun = 
        fun(_B, _K, V, {HashAcc, CountAcc}) ->
            {HashAcc bxor V, CountAcc + 1}
        end,
    InitAcc = {0, 0},

    RunnerDefinition = 
        {foldheads_allkeys, h, {FoldFun, InitAcc}, 
            false, false, false, false, false},
    {async, Runner1} = 
        leveled_bookie:book_returnfolder(Bookie1, RunnerDefinition),

    SW1 = os:timestamp(),
    {AccH1, AccC1} = Runner1(),
    io:format("AccH and AccC of ~w ~w in ~w microseconds~n", 
                [AccH1, AccC1, timer:now_diff(os:timestamp(), SW1)]),

    true = AccC1 == ObjectCount, 

    JFP = RootPathHO ++ "/journal/journal_files",
    {ok, FNs} = file:list_dir(JFP),
    
    ok = leveled_bookie:book_trimjournal(Bookie1),

    WaitForTrimFun =
        fun(N, _Acc) ->
            {ok, PollFNs} = file:list_dir(JFP),
            case length(PollFNs) < length(FNs) of 
                true ->
                    true;
                false ->
                    timer:sleep(N * 1000),
                    false
            end
        end,
    
    true = lists:foldl(WaitForTrimFun, false, [1, 2, 3, 5, 8, 13]),
    
    {ok, FinalFNs} = file:list_dir(JFP),

    ok = leveled_bookie:book_trimjournal(Bookie1),
    % CCheck a second trim is still OK

    [{add, SegmentID0, Bucket0, Key0, Hash0}|_Rest] = ObjectSpecL,
    case HeadOnly of 
        with_lookup ->
            % If we allow HEAD_TAG to be suubject to a lookup, then test this 
            % here
            {ok, Hash0} = 
                leveled_bookie:book_headonly(Bookie1, 
                                                SegmentID0, 
                                                Bucket0, 
                                                Key0),
            CheckHeadFun = 
                fun(DB) ->
                    fun({add, SegID, B, K, H}) ->
                        {ok, H} = 
                            leveled_bookie:book_headonly(DB, SegID, B, K)
                    end
                end,
            lists:foreach(CheckHeadFun(Bookie1), ObjectSpecL),
            {ok, Snapshot} =
                leveled_bookie:book_start([{snapshot_bookie, Bookie1}]),
            ok = leveled_bookie:book_loglevel(Snapshot, warn),
            ok =
                leveled_bookie:book_addlogs(
                    Snapshot, [b0001, b0002, b0003, i0027, p0007]
                ),
            ok =
                leveled_bookie:book_removelogs(
                    Snapshot, [b0019]
                ),
            io:format(
                "Checking for ~w objects against Snapshot ~w~n",
                [length(ObjectSpecL), Snapshot]),
            lists:foreach(CheckHeadFun(Snapshot), ObjectSpecL),
            io:format("Closing snapshot ~w~n", [Snapshot]),
            ok = leveled_bookie:book_close(Snapshot),
            {ok, AltSnapshot} =
                leveled_bookie:book_start([{snapshot_bookie, Bookie1}]),
            ok =
                leveled_bookie:book_addlogs(
                    AltSnapshot, [b0001, b0002, b0003, b0004, i0027, p0007]
                ),
            true = is_process_alive(AltSnapshot),
            io:format(
                "Closing actual store ~w with snapshot ~w open~n",
                [Bookie1, AltSnapshot]
            ),
            ok = leveled_bookie:book_close(Bookie1),
            % Sleep a beat so as not to race with the 'DOWN' message
            timer:sleep(10),
            false = is_process_alive(AltSnapshot);
        no_lookup ->
            {unsupported_message, head} = 
                leveled_bookie:book_head(
                    Bookie1,  SegmentID0,   {Bucket0, Key0},  h),
            {unsupported_message, head} = 
                leveled_bookie:book_headonly(
                    Bookie1, SegmentID0, Bucket0, Key0),
            io:format("Closing actual store ~w~n", [Bookie1]),
            ok = leveled_bookie:book_close(Bookie1)
    end,
    
    {ok, FinalJournals} = file:list_dir(JFP),
    io:format(
        "Trim has reduced journal count from "
        "~w to ~w and ~w after restart~n", 
        [length(FNs), length(FinalFNs), length(FinalJournals)]
    ),

    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),

    {async, Runner2} = 
        leveled_bookie:book_returnfolder(Bookie2, RunnerDefinition),

    {AccH2, AccC2} = Runner2(),
    true = AccC2 == ObjectCount,

    case HeadOnly of 
        with_lookup ->
            % If we allow HEAD_TAG to be suubject to a lookup, then test this 
            % here
            {ok, Hash0} = 
                leveled_bookie:book_head(
                    Bookie2, SegmentID0, {Bucket0, Key0}, h);
        no_lookup ->
            {unsupported_message, head} = 
                leveled_bookie:book_head(
                    Bookie2, SegmentID0, {Bucket0, Key0}, h)
    end,

    RemoveSpecL0 = lists:sublist(ObjectSpecL, RemoveCount),
    RemoveSpecL1 = 
        lists:map(fun(Spec) -> setelement(1, Spec, remove) end, RemoveSpecL0),
    ok = load_objectspecs(RemoveSpecL1, 32, Bookie2),

    {async, Runner3} = 
        leveled_bookie:book_returnfolder(Bookie2, RunnerDefinition),

    {AccH3, AccC3} = Runner3(),
    true = AccC3 == (ObjectCount - RemoveCount),
    false = AccH3 == AccH2,

    ok = leveled_bookie:book_close(Bookie2).


load_objectspecs([], _SliceSize, _Bookie) ->
    ok;
load_objectspecs(ObjectSpecL, SliceSize, Bookie) 
                                    when length(ObjectSpecL) < SliceSize ->
    load_objectspecs(ObjectSpecL, length(ObjectSpecL), Bookie);
load_objectspecs(ObjectSpecL, SliceSize, Bookie) ->
    {Head, Tail} = lists:split(SliceSize, ObjectSpecL),
    case leveled_bookie:book_mput(Bookie, Head) of
        ok ->
            load_objectspecs(Tail, SliceSize, Bookie);
        pause ->
            timer:sleep(10),
            load_objectspecs(Tail, SliceSize, Bookie)
    end.


get_segment(K, SegmentCount) ->
    BinKey = 
        case is_binary(K) of
            true ->
                K;
            false ->
                term_to_binary(K)
        end,
    {SegmentID, ExtraHash} = leveled_codec:segment_hash(BinKey),
    SegHash = (ExtraHash band 65535) bsl 16 + SegmentID,
    leveled_tictac:get_segment(SegHash, SegmentCount).
