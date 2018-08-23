-module(riak_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            crossbucket_aae/1,
            handoff/1,
         dollar_bucket_index/1,
         dollar_key_index/1
            ]).

all() -> [
            crossbucket_aae,
            handoff,
            dollar_bucket_index,
            dollar_key_index
            ].

-define(MAGIC, 53). % riak_kv -> riak_object

crossbucket_aae(_Config) ->
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),

    % Start the first database, load a test object, close it, start it again
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, riak_sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {B1, K1, V1, S1, MD} = {<<"Bucket">>,
                                <<"Key1.1.4567.4321">>,
                                <<"Value1">>,
                                [],
                                [{<<"MDK1">>, <<"MDV1">>}]},
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

    % Generate 200K objects to be used within the test, and load them into
    % the first store (outputting the generated objects as a list of lists)
    % to be used elsewhere

    GenList = 
        [{binary, 2}, {binary, 40002}, {binary, 80002}, {binary, 120002}],
    CLs = testutil:load_objects(40000,
                                GenList,
                                Bookie2,
                                TestObject,
                                fun testutil:generate_smallobjects/2,
                                40000),

    %% Check all the objects are found - used to trigger HEAD performance log
    ok = testutil:checkhead_forlist(Bookie2, lists:nth(1, CLs)),

    test_segfilter_query(Bookie2, CLs),

    % Start a new store, and load the same objects (except fot the original
    % test object) into this store
    %
    % This is now the comparison part of the test

    StartOpts3 = [{root_path, RootPathB},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie3, ObjL) end, CLs),
    test_singledelta_stores(Bookie2, Bookie3, small, {B1, K1}),
    test_singledelta_stores(Bookie2, Bookie3, medium, {B1, K1}),
    test_singledelta_stores(Bookie2, Bookie3, xsmall, {B1, K1}),
    test_singledelta_stores(Bookie2, Bookie3, xxsmall, {B1, K1}),

    % Test with a newly opened book (i.e with no block indexes cached)
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie2A} = leveled_bookie:book_start(StartOpts2),

    test_segfilter_query(Bookie2A, CLs),
    test_segfilter_query(Bookie2A, CLs),

    test_singledelta_stores(Bookie2A, Bookie3, small, {B1, K1}),

    ok = leveled_bookie:book_close(Bookie2A),
    ok = leveled_bookie:book_close(Bookie3).


test_segfilter_query(Bookie, CLs) ->
    % This part of the test tests an issue with accelerating folds by segment
    % list, when there is more than one key with a matching segment in the 
    % slot.  Previously this was not handled correctly - and this test part
    % of the test detects this, by finding slices of keys which are probably
    % in the same slot 
    SW0 = os:timestamp(),
    SliceSize = 20,

    CL1 = lists:sublist(lists:nth(1, CLs), 100, SliceSize),
    CL2 = lists:sublist(lists:nth(2, CLs), 100, SliceSize),
    CL3 = lists:sublist(lists:nth(3, CLs), 100, SliceSize),
    CL4 = lists:sublist(lists:nth(4, CLs), 100, SliceSize),

    SegMapFun = 
        fun({_RN, RiakObject, _Spc}) ->
            B = RiakObject#r_object.bucket,
            K = RiakObject#r_object.key,
            leveled_tictac:keyto_segment32(<<B/binary, K/binary>>)
        end,
    BKMapFun = 
        fun({_RN, RiakObject, _Spc}) ->
            B = RiakObject#r_object.bucket,
            K = RiakObject#r_object.key,
            {B, K}
        end,

    SL1 = lists:map(SegMapFun, CL1),
    SL2 = lists:map(SegMapFun, CL2),
    SL3 = lists:map(SegMapFun, CL3),
    SL4 = lists:map(SegMapFun, CL4),

    BK1 = lists:map(BKMapFun, CL1),
    BK2 = lists:map(BKMapFun, CL2),
    BK3 = lists:map(BKMapFun, CL3),
    BK4 = lists:map(BKMapFun, CL4),

    HeadSegmentFolderGen =
        fun(SegL, BKL) ->
            {foldheads_allkeys,
                ?RIAK_TAG,
                {fun(B, K, _PO, Acc) -> 
                        case lists:member({B, K}, BKL) of 
                            true ->
                                Acc + 1;
                            false ->
                                Acc
                        end
                        end,  0},
                false, true, SegL}
        end,

    {async, SL1Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL1, BK1)),
    {async, SL2Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL2, BK2)),
    {async, SL3Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL3, BK3)),
    {async, SL4Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL4, BK4)),

    Results = [SL1Folder(), SL2Folder(), SL3Folder(), SL4Folder()],
    io:format("SegList folders returned results of ~w " ++ 
                "for SliceSize ~w in ~w ms~n",
                [Results, SliceSize,
                    timer:now_diff(os:timestamp(), SW0)/1000]),
    lists:foreach(fun(R) -> true = R == SliceSize end, Results).


test_singledelta_stores(BookA, BookB, TreeSize, DeltaKey) ->
    io:format("Test for single delta with tree size ~w~n", [TreeSize]),
    % Now run a tictac query against both stores to see the extent to which
    % state between stores is consistent
    TicTacFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {fun head_tictac_foldfun/4, 
                {0, leveled_tictac:new_tree(test, TreeSize)}},
            false, true, false},
    % tictac query by bucket (should be same result as all stores)
    TicTacByBucketFolder = 
        {foldheads_bybucket, 
                ?RIAK_TAG, <<"Bucket">>, 
                all, 
                {fun head_tictac_foldfun/4, 
                    {0, leveled_tictac:new_tree(test, TreeSize)}},
                false, false, false},

    DLs = check_tictacfold(BookA, BookB, 
                            TicTacFolder, 
                            DeltaKey, 
                            TreeSize),
    DLs = check_tictacfold(BookA, BookB, 
                            TicTacByBucketFolder, 
                            DeltaKey, 
                            TreeSize),
    
    HeadSegmentFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true, false},
    
    SW_SL0 = os:timestamp(),
    {async, BookASegFolder} =
        leveled_bookie:book_returnfolder(BookA, HeadSegmentFolder),
    {async, BookBSegFolder} =
        leveled_bookie:book_returnfolder(BookB, HeadSegmentFolder),
    BookASegList = BookASegFolder(),
    BookBSegList = BookBSegFolder(),
    Time_SL0 = timer:now_diff(os:timestamp(), SW_SL0)/1000,
    io:format("Two unfiltered segment list folds took ~w milliseconds ~n", 
                [Time_SL0]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList), length(BookBSegList)]),

    Delta = lists:subtract(BookASegList, BookBSegList),
    true = length(Delta) == 1,

    SegFilterList = leveled_tictac:generate_segmentfilter_list(DLs, TreeSize),
    
    SuperHeadSegmentFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true, SegFilterList},
    
    SW_SL1 = os:timestamp(),
    {async, BookASegFolder1} =
        leveled_bookie:book_returnfolder(BookA, SuperHeadSegmentFolder),
    {async, BookBSegFolder1} =
        leveled_bookie:book_returnfolder(BookB, SuperHeadSegmentFolder),
    BookASegList1 = BookASegFolder1(),
    BookBSegList1 = BookBSegFolder1(),
    Time_SL1 = timer:now_diff(os:timestamp(), SW_SL1)/1000,
    io:format("Two filtered segment list folds took ~w milliseconds ~n", 
                [Time_SL1]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList1), length(BookBSegList1)]),
    
    SuperHeadSegmentFolderCP = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            true, true, SegFilterList},
    
    SW_SL1CP = os:timestamp(),
    {async, BookASegFolder1CP} =
        leveled_bookie:book_returnfolder(BookA, SuperHeadSegmentFolderCP),
    {async, BookBSegFolder1CP} =
        leveled_bookie:book_returnfolder(BookB, SuperHeadSegmentFolderCP),
    BookASegList1CP = BookASegFolder1CP(),
    BookBSegList1CP = BookBSegFolder1CP(),
    Time_SL1CP = timer:now_diff(os:timestamp(), SW_SL1CP)/1000,
    io:format("Two filtered segment list folds " ++ 
                "with presence check took ~w milliseconds ~n", 
                [Time_SL1CP]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList1CP), length(BookBSegList1CP)]),
    

    FalseMatchFilter = DLs ++ [1, 100, 101, 1000, 1001],
    SegFilterListF = 
        leveled_tictac:generate_segmentfilter_list(FalseMatchFilter, TreeSize),
    SuperHeadSegmentFolderF = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true, SegFilterListF},
    
    SW_SL1F = os:timestamp(),
    {async, BookASegFolder1F} =
        leveled_bookie:book_returnfolder(BookA, SuperHeadSegmentFolderF),
    {async, BookBSegFolder1F} =
        leveled_bookie:book_returnfolder(BookB, SuperHeadSegmentFolderF),
    BookASegList1F = BookASegFolder1F(),
    BookBSegList1F = BookBSegFolder1F(),
    Time_SL1F = timer:now_diff(os:timestamp(), SW_SL1F)/1000,
    io:format("Two filtered segment list folds " ++ 
                " with false positives took ~w milliseconds ~n", 
                [Time_SL1F]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList1F), length(BookBSegList1F)]),

    Delta1F = lists:subtract(BookASegList1F, BookBSegList1F),
    io:format("Delta found of ~w~n", [Delta1F]),
    true = length(Delta1F) == 1.


get_segment_folder(SegmentList, TreeSize) ->
    fun(B, K, PO, KeysAndClocksAcc) ->
        SegmentH = leveled_tictac:keyto_segment32(<<B/binary, K/binary>>),
        Segment = leveled_tictac:get_segment(SegmentH, TreeSize),
        case lists:member(Segment, SegmentList) of
            true ->
                {VC, _Sz, _SC} = summary_from_binary(PO),
                [{B, K, VC}|KeysAndClocksAcc];
            false ->
                KeysAndClocksAcc
        end     
    end.

head_tictac_foldfun(B, K, PO, {Count, TreeAcc}) ->
    ExtractFun = 
        fun({BBin, KBin}, Obj) ->
            {VC, _Sz, _SC} = summary_from_binary(Obj),
            {<<BBin/binary, KBin/binary>>, lists:sort(VC)}
        end,
    {Count + 1, 
        leveled_tictac:add_kv(TreeAcc, {B, K}, PO, ExtractFun)}.


check_tictacfold(BookA, BookB, HeadTicTacFolder, DeltaKey, TreeSize) ->
    SW_TT0 = os:timestamp(),
    {async, BookATreeFolder} =
        leveled_bookie:book_returnfolder(BookA, HeadTicTacFolder),
    {async, BookBTreeFolder} =
        leveled_bookie:book_returnfolder(BookB, HeadTicTacFolder),
    {CountA, BookATree} = BookATreeFolder(),
    {CountB, BookBTree} = BookBTreeFolder(),
    Time_TT0 = timer:now_diff(os:timestamp(), SW_TT0)/1000,
    io:format("Two tree folds took ~w milliseconds ~n", [Time_TT0]),

    io:format("Fold over keys revealed counts of ~w and ~w~n", 
                [CountA, CountB]),

    DLs = leveled_tictac:find_dirtyleaves(BookATree, BookBTree),
    io:format("Found dirty leaves with Riak fold_heads of ~w~n",
                [length(DLs)]),
    case DeltaKey of
        {B1, K1} ->
            % There should be a single delta between the stores
            1 = CountA - CountB,
            true = length(DLs) == 1,
            ExpSeg = leveled_tictac:keyto_segment32(<<B1/binary, K1/binary>>),
            TreeSeg = leveled_tictac:get_segment(ExpSeg, TreeSize),
            [ActualSeg] = DLs,
            true = TreeSeg == ActualSeg;
        none ->
            0 = CountA - CountB,
            true = length(DLs) == 0
    end,
    DLs.


summary_from_binary(<<131, _Rest/binary>>=ObjBin) ->
    {proxy_object, HeadBin, ObjSize, _Fetcher} = binary_to_term(ObjBin),
    summary_from_binary(HeadBin, ObjSize);
summary_from_binary(ObjBin) when is_binary(ObjBin) ->
    summary_from_binary(ObjBin, byte_size(ObjBin)).

summary_from_binary(ObjBin, ObjSize) ->
    <<?MAGIC:8/integer, 
        1:8/integer, 
        VclockLen:32/integer, VclockBin:VclockLen/binary, 
        SibCount:32/integer, 
        _Rest/binary>> = ObjBin,
    {lists:usort(binary_to_term(VclockBin)), ObjSize, SibCount}.



handoff(_Config) ->
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),

    % Start the first database, load a test object, close it, start it again
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    % Add some noe Riak objects in - which should be ignored in folds.
    Hashes = testutil:stdload(Bookie1, 1000),
    % Generate 200K objects to be used within the test, and load them into
    % the first store (outputting the generated objects as a list of lists)
    % to be used elsewhere

    GenList = 
        [binary_uuid, binary_uuid, binary_uuid, binary_uuid],
    [CL0, CL1, CL2, CL3] = 
        testutil:load_objects(40000,
                                GenList,
                                Bookie1,
                                no_check,
                                fun testutil:generate_smallobjects/2,
                                40000),
    
    % Update an delete some objects
    testutil:update_some_objects(Bookie1, CL0, 1000),
    testutil:update_some_objects(Bookie1, CL1, 20000),
    testutil:delete_some_objects(Bookie1, CL2, 10000),
    testutil:delete_some_objects(Bookie1, CL3, 4000),

    % Compact the journal
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    testutil:wait_for_compaction(Bookie1),

    % Start two new empty stores
    StartOpts2 = [{root_path, RootPathB},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, none}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    StartOpts3 = [{root_path, RootPathC},
                    {max_pencillercachesize, 30000},
                    {sync_strategy, none}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    StartOpts4 = [{root_path, RootPathD},
                    {max_pencillercachesize, 30000},
                    {sync_strategy, none}],
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts4),

    FoldStObjectsFun = 
        fun(B, K, V, Acc) ->
            [{B, K, erlang:phash2(V)}|Acc]
        end,

    FoldObjectsFun = 
        fun(Book) ->
            fun(B, K, Obj, ok) ->
                leveled_bookie:book_put(Book, B, K, Obj, [], ?RIAK_TAG),
                ok
            end
        end,
    
    % Handoff the data from the first store to the other three stores
    {async, Handoff2} =
        leveled_bookie:book_objectfold(Bookie1,
                                       ?RIAK_TAG,
                                       {FoldObjectsFun(Bookie2), ok},
                                       false,
                                       key_order),
    SW2 = os:timestamp(),
    ok = Handoff2(),
    Time_HO2 = timer:now_diff(os:timestamp(), SW2)/1000,
    io:format("Handoff to Book2 in key_order took ~w milliseconds ~n", 
                [Time_HO2]),
    SW3 = os:timestamp(),
    {async, Handoff3} =
        leveled_bookie:book_objectfold(Bookie1,
                                       ?RIAK_TAG,
                                       {FoldObjectsFun(Bookie3), ok},
                                       true,
                                       sqn_order),
    ok = Handoff3(),
    Time_HO3 = timer:now_diff(os:timestamp(), SW3)/1000,
    io:format("Handoff to Book3 in sqn_order took ~w milliseconds ~n", 
                [Time_HO3]),
    SW4 = os:timestamp(),
    {async, Handoff4} =
        leveled_bookie:book_objectfold(Bookie1,
                                       ?RIAK_TAG,
                                       {FoldObjectsFun(Bookie4), ok},
                                       true,
                                       sqn_order),

    ok = Handoff4(),
    Time_HO4 = timer:now_diff(os:timestamp(), SW4)/1000,
    io:format("Handoff to Book4 in sqn_order took ~w milliseconds ~n", 
                [Time_HO4]),

    % Run tictac folds to confirm all stores consistent after handoff
    TreeSize = xxsmall,

    TicTacFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {fun head_tictac_foldfun/4, 
                {0, leveled_tictac:new_tree(test, TreeSize)}},
            false, true, false},
    check_tictacfold(Bookie1, Bookie2, TicTacFolder, none, TreeSize),
    check_tictacfold(Bookie2, Bookie3, TicTacFolder, none, TreeSize),
    check_tictacfold(Bookie3, Bookie4, TicTacFolder, none, TreeSize),

    StdFolder = 
        {foldobjects_allkeys,
            ?STD_TAG,
            FoldStObjectsFun,
            true, 
            sqn_order},
    
    {async, StdFold1} = leveled_bookie:book_returnfolder(Bookie1, StdFolder),
    {async, StdFold2} = leveled_bookie:book_returnfolder(Bookie2, StdFolder),
    {async, StdFold3} = leveled_bookie:book_returnfolder(Bookie3, StdFolder),
    {async, StdFold4} = leveled_bookie:book_returnfolder(Bookie4, StdFolder),
    StdFoldOut1 = lists:sort(StdFold1()),
    StdFoldOut2 = lists:sort(StdFold2()),
    StdFoldOut3 = lists:sort(StdFold3()),
    StdFoldOut4 = lists:sort(StdFold4()),
    true = StdFoldOut1 == lists:sort(Hashes),
    true = StdFoldOut2 == [],
    true = StdFoldOut3 == [],
    true = StdFoldOut4 == [],

    % Shutdown
    ok = leveled_bookie:book_close(Bookie1),
    ok = leveled_bookie:book_close(Bookie2),
    ok = leveled_bookie:book_close(Bookie3),
    ok = leveled_bookie:book_close(Bookie4).

%% @doc test that the riak specific $key index can be iterated using
%% leveled's existing folders
dollar_key_index(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} = leveled_bookie:book_start(RootPath,
                                              2000,
                                              50000000,
                                              testutil:sync_strategy()),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 = testutil:generate_objects(1300,
                                      {fixed_binary, 1},
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket1">>),
    testutil:riakload(Bookie1, ObjL1),

    FoldKeysFun = fun(_B, K, Acc) ->
                          [ K |Acc]
                  end,

    StartKey = testutil:fixed_bin_key(123),
    EndKey = testutil:fixed_bin_key(779),


    {async, Folder} = leveled_bookie:book_keylist(Bookie1,
                                                  ?RIAK_TAG,
                                                  <<"Bucket1">>,
                                                  {StartKey, EndKey}, {FoldKeysFun, []}
                                                 ),
    ResLen = length(Folder()),
    io:format("Length of Result of folder ~w~n", [ResLen]),
    true = 657 == ResLen,

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().

%% @doc test that the riak specific $bucket indexes can be iterated
%% using leveled's existing folders
dollar_bucket_index(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} = leveled_bookie:book_start(RootPath,
                                              2000,
                                              50000000,
                                              testutil:sync_strategy()),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 = testutil:generate_objects(1300,
                                      uuid,
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket1">>),
    testutil:riakload(Bookie1, ObjL1),
    ObjL2 = testutil:generate_objects(1700,
                                      uuid,
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket2">>),
    testutil:riakload(Bookie1, ObjL2),
    ObjL3 = testutil:generate_objects(7000,
                                      uuid,
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket3">>),

    testutil:riakload(Bookie1, ObjL3),

    FoldKeysFun = fun(B, K, Acc) ->
                          [{B, K}|Acc]
                  end,
    FoldAccT = {FoldKeysFun, []},

    {async, Folder} = leveled_bookie:book_keylist(Bookie1, ?RIAK_TAG, <<"Bucket2">>, FoldAccT),
    ResLen = length(Folder()),

    io:format("Length of Result of folder ~w~n", [ResLen]),

    true = 1700 == ResLen,

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().

