-module(riak_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            perbucket_aae/1
            ]).

all() -> [
            perbucket_aae
            ].

-define(MAGIC, 53). % riak_kv -> riak_object

perbucket_aae(_Config) ->
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

    % Generate 200K objects to be sued within the test, and load them into
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

    % Start a new store, and load the same objects (except fot the original
    % test object) into this store

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
    
    % Test with a newly opend book (i.e with no blovk indexes cached)
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie2A} = leveled_bookie:book_start(StartOpts2),
    test_singledelta_stores(Bookie2A, Bookie3, small, {B1, K1}),
    
    ok = leveled_bookie:book_close(Bookie2A),
    ok = leveled_bookie:book_close(Bookie3).



test_singledelta_stores(BookA, BookB, TreeSize, DeltaKey) ->
    io:format("Test for single delta with tree size ~w~n", [TreeSize]),
    {B1, K1} = DeltaKey,
    % Now run a tictac query against both stores to see the extent to which
    % state between stores is consistent
    HeadTicTacFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {fun head_tictac_foldfun/4, 
                {0, leveled_tictac:new_tree(test, TreeSize)}},
            false,
            true},

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

    % There should be a single delta between the stores
    1 = CountA - CountB,

    DLs = leveled_tictac:find_dirtyleaves(BookATree, BookBTree),
    io:format("Found dirty leaves with Riak fold_heads of ~w~n",
                [length(DLs)]),
    true = length(DLs) == 1,
    ExpSeg = leveled_tictac:keyto_segment32(<<B1/binary, K1/binary>>),
    TreeSeg = leveled_tictac:get_segment(ExpSeg, TreeSize),
    [ActualSeg] = DLs,
    true = TreeSeg == ActualSeg,
    
    HeadSegmentFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true},
    
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

