-module(perf_SUITE).
-include("../include/leveled.hrl").
-define(INFO, info).
-export([all/0, suite/0]).
-export([
    riak_ctperf/1, riak_fullperf/1, riak_profileperf/1, riak_miniperf/1
]).

-define(PEOPLE_INDEX, <<"people_bin">>).
-define(MINI_QUERY_DIVISOR, 8).
-define(RGEX_QUERY_DIVISOR, 32).

-ifdef(perf_full).
    all() -> [riak_fullperf].
-else.
    -ifdef(perf_mini).
        all() -> [riak_miniperf].
    -else.
        -ifdef(perf_prof).
            all() -> [riak_profileperf].
        -else.
            all() -> [riak_ctperf].
        -endif.
    -endif.
-endif.
suite() -> [{timetrap, {hours, 16}}].

riak_fullperf(_Config) ->
    riak_fullperf(2048, zstd, as_store).

riak_fullperf(ObjSize, PM, LC) ->
    Bucket = {<<"SensibleBucketTypeName">>, <<"SensibleBucketName0">>},
    R2A = riak_load_tester(Bucket, 2000000, ObjSize, [], PM, LC),
    output_result(R2A),
    R2B = riak_load_tester(Bucket, 2000000, ObjSize, [], PM, LC),
    output_result(R2B),
    R2C = riak_load_tester(Bucket, 2000000, ObjSize, [], PM, LC),
    output_result(R2C),
    R5A = riak_load_tester(Bucket, 4000000, ObjSize, [], PM, LC),
    output_result(R5A),
    R5B = riak_load_tester(Bucket, 4000000, ObjSize, [], PM, LC),
    output_result(R5B),
    R10 = riak_load_tester(Bucket, 6000000, ObjSize, [], PM, LC),
    output_result(R10)
    .

riak_profileperf(_Config) ->
    riak_load_tester(
        {<<"SensibleBucketTypeName">>, <<"SensibleBucketName0">>},
        1200000,
        2048,
        [load, head, get, query, mini_query, regex_query, full, guess, estimate, update],
        zstd,
        as_store
    ).

% For standard ct test runs
riak_ctperf(_Config) ->
    riak_load_tester(<<"B0">>, 400000, 1024, [], native, as_store).

riak_miniperf(_Config) ->
    Bucket = {<<"SensibleBucketTypeName">>, <<"SensibleBucketName0">>},
    R2A = riak_load_tester(Bucket, 2000000, 2048, [], zstd, as_store),
    output_result(R2A).

riak_load_tester(Bucket, KeyCount, ObjSize, ProfileList, PM, LC) ->
    ct:log(
        ?INFO,
        "Basic riak test with KeyCount ~w ObjSize ~w PressMethod ~w Ledger ~w",
        [KeyCount, ObjSize, PM, LC]
    ),

    IndexCount = 100000,

    GetFetches = KeyCount div 4,
    HeadFetches = KeyCount div 2,
    IndexesReturned = KeyCount * 2,

    RootPath = testutil:reset_filestructure("riakLoad"),
    StartOpts1 =
        [{root_path, RootPath},
            {sync_strategy, testutil:sync_strategy()},
            {log_level, warn},
            {compression_method, PM},
            {ledger_compression, LC},
            {forced_logs,
                [b0015, b0016, b0017, b0018, p0032, sst12]}
        ],

    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    IndexGenFun =
        fun(ListID) ->
            fun() ->
                RandInt = leveled_rand:uniform(IndexCount - 1),
                IntIndex = "integer" ++ integer_to_list(ListID) ++ "_int",
                BinIndex = "binary" ++ integer_to_list(ListID) ++ "_bin",
                [{add, list_to_binary(IntIndex), RandInt},
                {add, ?PEOPLE_INDEX, list_to_binary(random_people_index())},
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

    ct:log(
        ?INFO,
        "Load time per group ~w ~w ~w ~w ~w ~w ~w ~w ~w ~w ms",
        lists:map(
            fun(T) -> T div 1000 end,
            [TC4, TC1, TC9, TC8, TC5, TC2, TC6, TC3, TC7, TC10])
    ),
    TotalLoadTime =
        (TC1 + TC2 + TC3 + TC4 + TC5 + TC6 + TC7 + TC8 + TC9 + TC10) div 1000,
    ct:log(?INFO, "Total load time ~w ms", [TotalLoadTime]),

    {MT0, MP0, MB0} = memory_usage(),

    TotalHeadTime = 
        random_fetches(head, Bookie1, Bucket, KeyCount, HeadFetches),
    
    {MT1, MP1, MB1} = memory_usage(),

    TotalGetTime = 
        random_fetches(get, Bookie1, Bucket, KeyCount, GetFetches),

    {MT2, MP2, MB2} = memory_usage(),

    QuerySize = max(10, IndexCount div 1000),
    MiniQuerySize = max(1, IndexCount div 50000),
    TotalQueryTime =
        random_queries(
            Bookie1,
            Bucket,
            10,
            IndexCount,
            QuerySize,
            IndexesReturned),
    TotalMiniQueryTime =
        random_queries(
            Bookie1,
            Bucket,
            10,
            IndexCount,
            MiniQuerySize,
            IndexesReturned div ?MINI_QUERY_DIVISOR),

    {MT3, MP3, MB3} = memory_usage(),

    RegexQueryTime =
        random_people_queries(
            Bookie1,
            Bucket,
            IndexesReturned div ?RGEX_QUERY_DIVISOR),
    
    {MT4, MP4, MB4} = memory_usage(),

    {FullFoldTime, SegFoldTime} = size_estimate_summary(Bookie1),

    {MT5, MP5, MB5} = memory_usage(),

    TotalUpdateTime =
        rotate_chunk(Bookie1, <<"UpdBucket">>, KeyCount div 50, ObjSize),

    {MT6, MP6, MB6} = memory_usage(),

    DiskSpace = lists:nth(1, string:tokens(os:cmd("du -sh riakLoad"), "\t")),
    ct:log(?INFO, "Disk space taken by test ~s", [DiskSpace]),

    MemoryUsage = erlang:memory(),
    ct:log(?INFO, "Memory in use at end of test ~p", [MemoryUsage]),

    ProfileData =
        {Bookie1, Bucket, KeyCount, ObjSize, IndexCount, IndexesReturned},
    lists:foreach(
        fun(P) ->
            ct:log(?INFO, "Profile of ~w", [P]),
            P0 =
                case P of
                    mini_query ->
                        {mini_query, MiniQuerySize};
                    query ->
                        {query, QuerySize};
                    head ->
                        {head, HeadFetches};
                    get ->
                        {get, GetFetches};
                    load ->
                        {load, IndexGenFun};
                    P ->
                        P
                end,
            io:format(user, "~nProfile ~p:~n", [P]),
            ProFun = profile_fun(P0, ProfileData),
            profile_test(Bookie1, ProFun, P)
        end,
        ProfileList),

    {_Inker, _Pcl, SSTPids, _PClerk, CDBPids, _IClerk} = get_pids(Bookie1),
    leveled_bookie:book_destroy(Bookie1),
    
    {KeyCount, ObjSize, {PM, LC},
        TotalLoadTime,
        TotalHeadTime, TotalGetTime,
        TotalQueryTime, TotalMiniQueryTime, RegexQueryTime,
        FullFoldTime, SegFoldTime,
        TotalUpdateTime,
        DiskSpace,
        {(MT0 + MT1 + MT2 + MT3 + MT4 + MT5 + MT6) div 6000000,
            (MP0 + MP1 + MP2 + MP3 + MP4 + MP5 + MP6) div 6000000,
            (MB0 + MB1 + MB2 + MB3 + MB4 + MB5 + MB6) div 6000000},
        SSTPids, CDBPids}.

profile_test(Bookie, ProfileFun, P) ->
    {Inker, Pcl, SSTPids, PClerk, CDBPids, IClerk} = get_pids(Bookie),
    TestPid = self(),
    profile_app(
        [TestPid, Bookie, Inker, IClerk, Pcl, PClerk] ++ SSTPids ++ CDBPids,
        ProfileFun,
        P
    ).

get_pids(Bookie) ->
    {ok, Inker, Pcl} = leveled_bookie:book_returnactors(Bookie),
    SSTPids = leveled_penciller:pcl_getsstpids(Pcl),
    PClerk = leveled_penciller:pcl_getclerkpid(Pcl),
    CDBPids = leveled_inker:ink_getcdbpids(Inker),
    IClerk = leveled_inker:ink_getclerkpid(Inker),
    {Inker, Pcl, SSTPids, PClerk, CDBPids, IClerk}.

output_result(
    {KeyCount, ObjSize, PressMethod,
        TotalLoadTime,
        TotalHeadTime, TotalGetTime,
        TotalQueryTime, TotalMiniQueryTime, RegexQueryTime,
        TotalFullFoldTime, TotalSegFoldTime,
        TotalUpdateTime,
        DiskSpace,
        {TotalMemoryMB, ProcessMemoryMB, BinaryMemoryMB},
        SSTPids, CDBPids}
) ->
    %% TODO ct:pal not working?  even with rebar3 ct --verbose?
    io:format(
        user,
        "~n"
        "Outputs from profiling with KeyCount ~w ObjSize ~w Compression ~w:~n"
        "TotalLoadTime - ~w ms~n"
        "TotalHeadTime - ~w ms~n"
        "TotalGetTime - ~w ms~n"
        "TotalQueryTime - ~w ms~n"
        "TotalMiniQueryTime - ~w ms~n"
        "TotalRegexQueryTime - ~w ms~n"
        "TotalFullFoldTime - ~w ms~n"
        "TotalAAEFoldTime - ~w ms~n"
        "TotalUpdateTime - ~w ms~n"
        "Disk space required for test - ~s~n"
        "Average Memory usage for test - Total ~p Proc ~p Bin ~p MB~n"
        "Closing count of SST Files - ~w~n"
        "Closing count of CDB Files - ~w~n",
        [KeyCount, ObjSize, PressMethod,
            TotalLoadTime, TotalHeadTime, TotalGetTime,
            TotalQueryTime, TotalMiniQueryTime, RegexQueryTime,
            TotalFullFoldTime, TotalSegFoldTime,
            TotalUpdateTime,
            DiskSpace,
            TotalMemoryMB, ProcessMemoryMB, BinaryMemoryMB,
            length(SSTPids), length(CDBPids)]
    ).

memory_usage() ->
    garbage_collect(), % GC the test process
    MemoryUsage = erlang:memory(),
    {element(2, lists:keyfind(total, 1, MemoryUsage)),
        element(2, lists:keyfind(processes, 1, MemoryUsage)),
        element(2, lists:keyfind(binary, 1, MemoryUsage))}.

profile_app(Pids, ProfiledFun, P) ->

    eprof:start(),
    eprof:start_profiling(Pids),

    ProfiledFun(),

    eprof:stop_profiling(),
    eprof:log(atom_to_list(P) ++ ".log"),
    eprof:analyze(total, [{filter, [{time, 150000}]}]),
    eprof:stop(),
    {ok, Analysis} = file:read_file(atom_to_list(P) ++ ".log"),
    io:format(user, "~n~s~n", [Analysis])
    .

size_estimate_summary(Bookie) ->
    Loops = 10,
    ct:log(
        ?INFO,
        "Size Estimate Tester (SET) started with Loops ~w",
        [Loops]
    ),
    {{TotalGuessTime, TotalEstimateTime, TotalCountTime}, 
            {TotalEstimateVariance, TotalGuessVariance}} =
        lists:foldl(
            fun(_I, {{GT, ET, CT}, {AET, AGT}}) ->
                {{GT0, ET0, CT0}, {AE0, AG0}} = size_estimate_tester(Bookie),
                {{GT + GT0, ET + ET0, CT + CT0}, {AET + AE0, AGT + AG0}}
            end,
            {{0, 0, 0}, {0, 0}},
            lists:seq(1, Loops)
        ),
    ct:log(
        ?INFO,
        "SET: MeanGuess ~w ms MeanEstimate ~w ms MeanCount ~w ms",
        [TotalGuessTime div 10000,
            TotalEstimateTime div 10000,
            TotalCountTime div 10000]
    ),
    ct:log(
        ?INFO,
        "Mean variance in Estimate ~w Guess ~w",
        [TotalEstimateVariance div Loops, TotalGuessVariance div Loops]
    ),
    %% Assume that segment-list folds are 10 * as common as all folds
    {TotalCountTime div 1000, (TotalGuessTime + TotalEstimateTime) div 1000}.


rotate_chunk(Bookie, Bucket, KeyCount, ObjSize) ->
    ct:log(
        ?INFO,
        "Rotating an ObjList ~w - "
        "time includes object generation",
        [KeyCount]),
    V1 = base64:encode(leveled_rand:rand_bytes(ObjSize)),
    V2 = base64:encode(leveled_rand:rand_bytes(ObjSize)),
    V3 = base64:encode(leveled_rand:rand_bytes(ObjSize)),
    {TC, ok} = 
        timer:tc(
            fun() ->
                testutil:rotation_withnocheck(
                    Bookie, Bucket, KeyCount, V1, V2, V3)
            end),
    TC div 1000.

load_chunk(Bookie, CountPerList, ObjSize, IndexGenFun, Bucket, Chunk) ->
    ct:log(?INFO, "Generating and loading ObjList ~w", [Chunk]),
    ObjList =
        generate_chunk(CountPerList, ObjSize, IndexGenFun, Bucket, Chunk),
    {TC, ok} = timer:tc(fun() -> testutil:riakload(Bookie, ObjList) end),
    garbage_collect(),
    timer:sleep(2000),
    TC.

generate_chunk(CountPerList, ObjSize, IndexGenFun, Bucket, Chunk) ->
    testutil:generate_objects(
        CountPerList, 
        {fixed_binary, (Chunk - 1) * CountPerList + 1}, [],
        base64:encode(leveled_rand:rand_bytes(ObjSize)),
        IndexGenFun(Chunk),
        Bucket
    ).

size_estimate_tester(Bookie) ->
    %% Data size test - calculate data size, then estimate data size
    {CountTS, Count} = counter(Bookie, full),
    {CountTSEstimate, CountEstimate} = counter(Bookie, estimate),
    {CountTSGuess, CountGuess} = counter(Bookie, guess),
    {GuessTolerance, EstimateTolerance} =
        case Count of
            C when C < 500000 ->
                {0.20, 0.15};
            C when C < 1000000 ->
                {0.12, 0.1};
            C when C < 2000000 ->
                {0.1, 0.08};
            _C ->
                {0.08, 0.05}
        end,

    true =
        ((CountGuess / Count) > (1.0 - GuessTolerance))
            and ((CountGuess / Count) < (1.0 + GuessTolerance)),
    true =
        ((CountEstimate / Count) > (1.0 - EstimateTolerance))
            and ((CountEstimate / Count) < (1.0 + EstimateTolerance)),
    {{CountTSGuess, CountTSEstimate, CountTS},
        {abs(CountEstimate - Count), abs(CountGuess - Count)}}.

counter(Bookie, full) ->
    {async, DataSizeCounter} =
        leveled_bookie:book_headfold(
            Bookie,
            ?RIAK_TAG,
            {fun(_B, _K, _V, AccC) ->  AccC + 1 end, 0},
            false,
            true,
            false
        ),
    timer:tc(DataSizeCounter);
counter(Bookie, guess) ->
    TictacTreeSize = 1024 * 1024,
    RandomSegment = rand:uniform(TictacTreeSize - 32) - 1,
    {async, DataSizeGuesser} =
        leveled_bookie:book_headfold(
            Bookie,
            ?RIAK_TAG,
            {fun(_B, _K, _V, AccC) ->  AccC + 1024 end, 0},
            false,
            true,
            lists:seq(RandomSegment, RandomSegment + 31)
        ),
    timer:tc(DataSizeGuesser);
counter(Bookie, estimate) ->
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
    timer:tc(DataSizeEstimater).
    

random_fetches(FetchType, Bookie, Bucket, ObjCount, Fetches) ->
    KeysToFetch =
        lists:map(
            fun(I) ->
                Twenty = ObjCount div 5,
                case I rem 5 of
                    1 ->
                        testutil:fixed_bin_key(
                            Twenty + leveled_rand:uniform(ObjCount - Twenty));
                    _ ->
                        testutil:fixed_bin_key(leveled_rand:uniform(Twenty))
                end
            end,
            lists:seq(1, Fetches)
        ),
    {TC, ok} =
        timer:tc(
            fun() ->
                lists:foreach(
                    fun(K) ->
                        {ok, _} =
                            case FetchType of
                                get ->
                                    testutil:book_riakget(Bookie, Bucket, K);
                                head ->
                                    testutil:book_riakhead(Bookie, Bucket, K)
                            end
                    end,
                    KeysToFetch
                )
            end
        ),
    ct:log(
        ?INFO,
        "Fetch of type ~w ~w keys in ~w ms",
        [FetchType, Fetches, TC div 1000]
    ),
    TC div 1000.
    
random_queries(Bookie, Bucket, IDs, IdxCnt, MaxRange, IndexesReturned) ->
    QueryFun =
        fun() ->
            ID = leveled_rand:uniform(IDs), 
            BinIndex =
                list_to_binary("binary" ++ integer_to_list(ID) ++ "_bin"),
            Twenty = IdxCnt div 5,
            RI = leveled_rand:uniform(MaxRange),
            [Start, End] =
                case RI of
                    RI when RI < (MaxRange div 5) ->
                        R0 = leveled_rand:uniform(IdxCnt - (Twenty + RI)),
                        [R0 + Twenty, R0 + Twenty + RI];
                    _ ->
                        R0 = leveled_rand:uniform(Twenty - RI),
                        [R0, R0 + RI]
                end,
            FoldKeysFun =  fun(_B, _K, Cnt) -> Cnt + 1 end,
            {async, R} =
                leveled_bookie:book_indexfold(
                    Bookie,
                    {Bucket, <<>>}, 
                    {FoldKeysFun, 0},
                    {BinIndex, <<Start:32/integer>>, <<End:32/integer>>},
                    {true, undefined}),
            R()
        end,
    
    {TC, {QC, EF}} =
        timer:tc(fun() -> run_queries(QueryFun, 0, 0, IndexesReturned) end),
    ct:log(
        ?INFO,
        "Fetch of ~w index entries in ~w queries took ~w ms",
        [EF, QC, TC div 1000]
    ),
    TC div 1000.

random_people_queries(Bookie, Bucket, IndexesReturned) ->
    QueryFun =
        fun() ->
            Surname = get_random_surname(),
            Range =
                {?PEOPLE_INDEX,
                    Surname,
                    <<Surname/binary, 126:8/integer>>
            },
            %% born in the 70s with Willow as a given name
            {ok, TermRegex} =
                re:compile(
                    "[^\\|]*\\|197[0-9]{5}\\|[^\\|]*\\|[^\\|]*#Willow[^\\|]*\\|[^\\|]*#LS[^\\|]*"
                ),
            FoldKeysFun =  fun(_B, _K, Cnt) -> Cnt + 1 end,
            {async, R} =
                leveled_bookie:book_indexfold(
                    Bookie,
                    {Bucket, <<>>}, 
                    {FoldKeysFun, 0},
                    Range,
                    {true, TermRegex}),
            R()
        end,
    
    {TC, {QC, EF}} =
        timer:tc(fun() -> run_queries(QueryFun, 0, 0, IndexesReturned) end),
    ct:log(
        ?INFO,
        "Fetch of ~w index entries by regex in ~w queries took ~w ms",
        [EF, QC, TC div 1000]
    ),
    TC div 1000.


run_queries(_QueryFun, QueryCount, EntriesFound, TargetEntries)
        when EntriesFound >= TargetEntries ->
    {QueryCount, EntriesFound};
run_queries(QueryFun, QueryCount, EntriesFound, TargetEntries) ->
    Matches = QueryFun(),
    run_queries(
        QueryFun, QueryCount + 1, EntriesFound + Matches, TargetEntries).

profile_fun(false, _ProfileData) ->
    fun() -> ok end;
profile_fun(
        {mini_query, QuerySize},
        {Bookie, Bucket, _KeyCount, _ObjSize, IndexCount, IndexesReturned}) ->
    fun() ->
        random_queries(
            Bookie, Bucket, 10, IndexCount, QuerySize,
            IndexesReturned div ?MINI_QUERY_DIVISOR)
    end;
profile_fun(
        {query, QuerySize},
        {Bookie, Bucket, _KeyCount, _ObjSize, IndexCount, IndexesReturned}) ->
    fun() ->
        random_queries(
            Bookie, Bucket, 10, IndexCount, QuerySize, IndexesReturned)
    end;
profile_fun(
        regex_query,
        {Bookie, Bucket, _KeyCount, _ObjSize, _IndexCount, IndexesReturned}) ->
    fun() ->
        random_people_queries(
            Bookie, Bucket, IndexesReturned div ?RGEX_QUERY_DIVISOR)
    end;
profile_fun(
        {head, HeadFetches},
        {Bookie, Bucket, KeyCount, _ObjSize, _IndexCount, _IndexesReturned}) ->
    fun() ->
        random_fetches(head, Bookie, Bucket, KeyCount, HeadFetches)
    end;
profile_fun(
        {get, GetFetches},
        {Bookie, Bucket, KeyCount, _ObjSize, _IndexCount, _IndexesReturned}) ->
    fun() ->
        random_fetches(get, Bookie, Bucket, KeyCount, GetFetches)
    end;
profile_fun(
        {load, IndexGenFun},
        {Bookie, Bucket, KeyCount, ObjSize, _IndexCount, _IndexesReturned}) ->
    ObjList11 =
        generate_chunk(KeyCount div 10, ObjSize, IndexGenFun, Bucket, 11),
    fun() ->
        testutil:riakload(Bookie, ObjList11)
    end;
profile_fun(
        update,
        {Bookie, _Bucket, KeyCount, ObjSize, _IndexCount, _IndexesReturned}) ->
    fun() ->
        rotate_chunk(Bookie, <<"ProfileB">>, KeyCount div 50, ObjSize)
    end;
profile_fun(
        CounterFold,
        {Bookie, _Bucket, _KeyCount, _ObjSize, _IndexCount, _IndexesReturned}) ->
    Runs =
        case CounterFold of
            full ->
                20;
            estimate ->
                40;
            guess ->
                100
        end,
    fun() ->
        lists:foreach(
            fun(_I) ->
                _ = counter(Bookie, CounterFold)
            end,
            lists:seq(1, Runs)
        )
    end.

random_people_index() ->
    io_lib:format(
        "~s|~s|~s|#~s#~s#~s|#~s#~s#~s",
        [get_random_surname(),
            get_random_dob(),
            get_random_dod(),
            get_random_givenname(), get_random_givenname(), get_random_givenname(),
            get_random_postcode(), get_random_postcode(), get_random_postcode()
        ]
    ).

get_random_surname() ->
    lists:nth(
        rand:uniform(100),
        [<<"Smith">>, <<"Jones">>, <<"Taylor">>, <<"Brown">>, <<"Williams">>,
            <<"Wilson">>, <<"Johnson">>, <<"Davies">>, <<"Patel">>, <<"Robinson">>,
            <<"Wright">>, <<"Thompson">>, <<"Evans">>, <<"Walker">>, <<"White">>,
            <<"Roberts">>, <<"Green">>, <<"Hall">>, <<"Thomas">>, <<"Clarke">>,
            <<"Jackson">>, <<"Wood">>, <<"Harris">>, <<"Edwards">>, <<"Turner">>,
            <<"Martin">>, <<"Cooper">>, <<"Hill">>, <<"Ward">>, <<"Hughes">>,
            <<"Moore">>, <<"Clark">>, <<"King">>, <<"Harrison">>, <<"Lewis">>,
            <<"Baker">>, <<"Lee">>, <<"Allen">>, <<"Morris">>, <<"Khan">>,
            <<"Scott">>, <<"Watson">>, <<"Davis">>, <<"Parker">>, <<"James">>,
            <<"Bennett">>, <<"Young">>, <<"Phillips">>, <<"Richardson">>, <<"Mitchell">>,
            <<"Bailey">>, <<"Carter">>, <<"Cook">>, <<"Singh">>, <<"Shaw">>,
            <<"Bell">>, <<"Collins">>, <<"Morgan">>, <<"Kelly">>, <<"Begum">>,
            <<"Miller">>, <<"Cox">>, <<"Hussain">>, <<"Marshall">>, <<"Simpson">>,
            <<"Price">>, <<"Anderson">>, <<"Adams">>, <<"Wilkinson">>, <<"Ali">>,
            <<"Ahmed">>, <<"Foster">>, <<"Ellis">>, <<"Murphy">>, <<"Chapman">>,
            <<"Mason">>, <<"Gray">>, <<"Richards">>, <<"Webb">>, <<"Griffiths">>,
            <<"Hunt">>, <<"Palmer">>, <<"Campbell">>, <<"Holmes">>, <<"Mills">>,
            <<"Rogers">>, <<"Barnes">>, <<"Knight">>, <<"Matthews">>, <<"Barker">>,
            <<"Powell">>, <<"Stevens">>, <<"Kaur">>, <<"Fisher">>, <<"Butler">>,
            <<"Dixon">>, <<"Russell">>, <<"Harvey">>, <<"Pearson">>, <<"Graham">>]
    ).

get_random_givenname() ->
    lists:nth(
        rand:uniform(20),
        [<<"Noah">>, <<"Oliver">>, <<"George">>, <<"Arthur">>, <<"Muhammad">>,
            <<"Leo">>, <<"Harry">>, <<"Oscar">> , <<"Archie">>, <<"Henry">>,
            <<"Olivia">>, <<"Amelia">>, <<"Isla">>, <<"Ava">>, <<"Ivy">>,
            <<"Freya">>, <<"Lily">>, <<"Florence">>, <<"Mia">>, <<"Willow">>
    ]).

get_random_dob() ->
    io_lib:format(
        "~4..0B~2..0B~2..0B",
        [1900 + rand:uniform(99), rand:uniform(12), rand:uniform(28)]
    ).

get_random_dod() ->
    io_lib:format(
        "~4..0B~2..0B~2..0B",
        [2000 + rand:uniform(20), rand:uniform(12), rand:uniform(28)]
    ).

get_random_postcode() ->
    io_lib:format(
        "LS~w ~wXX", [rand:uniform(26), rand:uniform(9)]
    ).
