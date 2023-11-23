-module(perf_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0, suite/0]).
-export([bigpcl_bucketlist/1, riak_load/1]).


all() -> [bigpcl_bucketlist, riak_load].
suite() -> [{timetrap, {hours, 1}}].
    
bigpcl_bucketlist(_Config) ->
    %% https://github.com/martinsumner/leveled/issues/326
    %% In OTP 22+ there appear to be issues with anonymous functions which
    %% have a reference to loop state, requiring a copy of all the loop state
    %% to be made when returning the function.
    %% This test creates  alarge loop state on the leveled_penciller to prove
    %% this.
    %% The problem can be resolved simply by renaming the element of the loop
    %% state using within the anonymous function.
    RootPath = testutil:reset_filestructure(),
    BucketCount = 500,
    ObjectCount = 100,
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 50000000},
                    {cache_size, 4000},
                    {max_pencillercachesize, 128000},
                    {max_sstslots, 256},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_point, on_compact}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    BucketList =
        lists:map(fun(I) -> list_to_binary(integer_to_list(I)) end,
                    lists:seq(1, BucketCount)),

    MapFun =
        fun(B) ->
            testutil:generate_objects(ObjectCount, 1, [], 
                                        leveled_rand:rand_bytes(100), 
                                        fun() -> [] end, 
                                        B)
        end,
    ObjLofL = lists:map(MapFun, BucketList),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie1, ObjL) end, ObjLofL),
    BucketFold =
        fun(B, _K, _V, Acc) ->
            case sets:is_element(B, Acc) of
                true ->
                    Acc;
                false ->
                    sets:add_element(B, Acc)
            end
        end,
    FBAccT = {BucketFold, sets:new()},

    {async, BucketFolder1} = 
        leveled_bookie:book_headfold(Bookie1,
                                        ?RIAK_TAG,
                                        {bucket_list, BucketList},
                                        FBAccT,
                                        false, false, false),

    {FoldTime1, BucketList1} = timer:tc(BucketFolder1, []),
    true = BucketCount == sets:size(BucketList1),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    
    {async, BucketFolder2} = 
        leveled_bookie:book_headfold(Bookie2,
                                        ?RIAK_TAG,
                                        {bucket_list, BucketList},
                                        FBAccT,
                                        false, false, false),
    {FoldTime2, BucketList2} = timer:tc(BucketFolder2, []),
    true = BucketCount == sets:size(BucketList2),

    io:format("Fold pre-close ~w ms post-close ~w ms~n",
                [FoldTime1 div 1000, FoldTime2 div 1000]),

    true = FoldTime1 < 10 * FoldTime2,
    %% The fold in-memory should be the same order of magnitude of response
    %% time as the fold post-persistence

    ok = leveled_bookie:book_destroy(Bookie2).

riak_load(_Config) ->
    riak_load_tester(<<"B0">>, 800000, 64).

riak_load_tester(Bucket, KeyCount, ObjSize) ->
    io:format("Basic riak test with KeyCount ~w~n", [KeyCount]),
    IndexCount = 1000,

    RootPath = testutil:reset_filestructure("riakLoad"),
    StartOpts1 = [{root_path, RootPath},
        {max_journalsize, 500000000},
        {max_pencillercachesize, 24000},
        {sync_strategy, testutil:sync_strategy()},
        {log_level, warn},
        {forced_logs, [b0015, b0016, b0017, b0018, p0032, sst12]}
    ],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    IndexGenFun =
        fun(ListID) ->
            fun() ->
                RandInt = leveled_rand:uniform(IndexCount - 1),
                IntIndex = "integer" ++ integer_to_list(ListID) ++ "_int",
                BinIndex = "binary" ++ integer_to_list(ListID) ++ "_bin",
                [{add, list_to_binary(IntIndex), RandInt},
                {add, list_to_binary(IntIndex), RandInt + 1},
                {add, list_to_binary(BinIndex), <<RandInt:32/integer>>},
                {add, list_to_binary(BinIndex), <<(RandInt + 1):32/integer>>}]
            end
        end,

    CountPerList = KeyCount div 10,

    TC4 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 4),
    TC1 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 1),
    TC9 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 9),
    TC8 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 8),
    TC5 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 5),
    TC2 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 2),
    TC6 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 6),
    TC3 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 3),
    TC7 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 7),
    TC10 = load_chunk(Bookie1, CountPerList, ObjSize, IndexGenFun, Bucket, 10),
    
    size_estimate_summary(Bookie1),
    io:format(
        "Load time per group ~w ~w ~w ~w ~w ~w ~w ~w ~w ~wms~n",
        lists:map(
            fun(T) -> T div 1000 end,
            [TC4, TC1, TC9, TC8, TC5, TC2, TC6, TC3, TC7, TC10])
    ),
    io:format(
        "Total load time ~w ms~n",
        [(TC1 + TC2 + TC3 + TC4 + TC5 + TC6 + TC7 + TC8 + TC9 + TC10) div 1000]
    ),

    leveled_bookie:book_destroy(Bookie1).

size_estimate_summary(Bookie) ->
    Loops = 10,
    {{TotalGuessTime, TotalEstimateTime, TotalCountTime}, 
            {TotalEstimateVariance, TotalGuessVariance}} =
        lists:foldl(
            fun(_I, {{GT, ET, CT}, {AET, AGT}}) ->
                timer:sleep(1000),
                {{GT0, ET0, CT0}, {AE0, AG0}} = size_estimate_tester(Bookie),
                {{GT + GT0, ET + ET0, CT + CT0}, {AET + AE0, AGT + AG0}}
            end,
            {{0, 0, 0}, {0, 0}},
            lists:seq(1, Loops)
        ),
    io:format(
        "SET: MeanGuess ~w ms MeanEstimate ~w ms MeanCount ~w ms~n",
        [TotalGuessTime div 10000,
            TotalEstimateTime div 10000,
            TotalCountTime div 10000]
    ),
    io:format(
        "Mean variance in Estimate ~w Guess ~w~n",
        [TotalEstimateVariance div Loops, TotalGuessVariance div Loops]
    ).


load_chunk(Bookie, CountPerList, ObjSize, IndexGenFun, Bucket, Chunk) ->
    io:format("Generating and loading ObjList ~w~n", [Chunk]),
    ObjList =
        testutil:generate_objects(
            CountPerList, 
            {fixed_binary, (Chunk - 1) * CountPerList + 1}, [],
            leveled_rand:rand_bytes(ObjSize),
            IndexGenFun(Chunk),
            Bucket
        ),
    {TC, ok} = timer:tc(fun() -> testutil:riakload(Bookie, ObjList) end),
    garbage_collect(),
    TC.

size_estimate_tester(Bookie) ->
    %% Data size test - calculate data size, then estimate data size
    io:format("SET: estimating data size~n"),
    {async, DataSizeCounter} =
        leveled_bookie:book_headfold(
            Bookie,
            ?RIAK_TAG,
            {fun(_B, _K, _V, AccC) ->  AccC + 1 end, 0},
            false,
            true,
            false
        ),
    TictacTreeSize = 1024 * 1024,
    RandomSegment = rand:uniform(TictacTreeSize - 128) - 1,
    {async, DataSizeEstimater} =
        leveled_bookie:book_headfold(
            Bookie,
            ?RIAK_TAG,
            {fun(_B, _K, _V, AccC) ->  AccC + 256 end, 0},
            false,
            true,
            lists:seq(RandomSegment, RandomSegment + 127)
        ),
    {async, DataSizeGuesser} =
        leveled_bookie:book_headfold(
            Bookie,
            ?RIAK_TAG,
            {fun(_B, _K, _V, AccC) ->  AccC + 1024 end, 0},
            false,
            true,
            lists:seq(RandomSegment, RandomSegment + 31)
        ),
    {CountTS, Count} = timer:tc(DataSizeCounter),
    {CountTSEstimate, CountEstimate} = timer:tc(DataSizeEstimater),
    {CountTSGuess, CountGuess} = timer:tc(DataSizeGuesser),
    io:format(
        "SET: estimate ~w of size ~w with estimate taking ~w ms vs ~w ms~n",
        [CountEstimate, Count, CountTSEstimate div 1000, CountTS div 1000]
    ),
    io:format(
        "SET: guess ~w of size ~w with guess taking ~w ms vs ~w ms~n",
        [CountGuess, Count, CountTSGuess div 1000, CountTS div 1000]
    ),
    true =
        ((CountGuess / Count) > 0.9) and ((CountGuess / Count) < 1.1),
    true =
        ((CountEstimate / Count) > 0.92) and ((CountEstimate / Count) < 1.08),
    {{CountTSGuess, CountTSEstimate, CountTS},
        {abs(CountEstimate - Count), abs(CountGuess - Count)}}.