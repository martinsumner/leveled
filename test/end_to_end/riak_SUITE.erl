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
    TreeSize = small,
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
    {async, Book2TreeFolder} =
        leveled_bookie:book_returnfolder(Bookie2, HeadTicTacFolder),
    {async, Book3TreeFolder} =
        leveled_bookie:book_returnfolder(Bookie3, HeadTicTacFolder),
    {Count2, Book2Tree} = Book2TreeFolder(),
    {Count3, Book3Tree} = Book3TreeFolder(),
    Time_TT0 = timer:now_diff(os:timestamp(), SW_TT0)/1000,
    io:format("Two tree folds took ~w milliseconds ~n", [Time_TT0]),

    io:format("Fold over keys revealed counts of ~w and ~w~n", 
                [Count2, Count3]),

    1 = Count2 - Count3,

    DLs = leveled_tictac:find_dirtyleaves(Book2Tree, Book3Tree),
    io:format("Found dirty leaves with Riak fold_heads of ~w~n",
                [length(DLs)]),
    true = length(DLs) == 1,
    {ExpSeg, _ExpExtra} = leveled_codec:segment_hash(<<B1/binary, K1/binary>>),
    [ActualSeg] = DLs,
    true = ExpSeg == ActualSeg band 65535,
    
    HeadSegmentFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false,
            true},
    
    SW_SL0 = os:timestamp(),
    {async, Book2SegFolder} =
        leveled_bookie:book_returnfolder(Bookie2, HeadSegmentFolder),
    {async, Book3SegFolder} =
        leveled_bookie:book_returnfolder(Bookie3, HeadSegmentFolder),
    Book2SegList = Book2SegFolder(),
    Book3SegList = Book3SegFolder(),
    Time_SL0 = timer:now_diff(os:timestamp(), SW_SL0)/1000,
    io:format("Two segment list folds took ~w milliseconds ~n", [Time_SL0]),
    io:format("Segment lists found ~w ~w~n", [Book2SegList, Book3SegList]),

    Delta = lists:subtract(Book2SegList, Book3SegList),
    true = length(Delta) == 1.


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

