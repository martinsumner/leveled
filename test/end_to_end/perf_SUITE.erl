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

-ifndef(performance).
  -define(performance, riak_ctperf).
-endif.
all() -> [?performance].

-if(?performance == riak_profileperf andalso ?OTP_RELEASE >= 24).
   % Requires map functions from OTP 24
   -define(ACCOUNTING, true).
-else.
   -define(ACCOUNTING, false).
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
    R5A = riak_load_tester(Bucket, 5000000, ObjSize, [], PM, LC),
    output_result(R5A),
    R5B = riak_load_tester(Bucket, 5000000, ObjSize, [], PM, LC),
    output_result(R5B),
    R10 = riak_load_tester(Bucket, 8000000, ObjSize, [], PM, LC),
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

    LoadMemoryTracker = memory_tracking(load, 1000),
    LoadAccountant = accounting(load, 10000, ProfileList),
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
    ok = stop_accounting(LoadAccountant),
    {MT0, MP0, MB0} = stop_tracker(LoadMemoryTracker),

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

    HeadMemoryTracker = memory_tracking(head, 1000),
    HeadAccountant = accounting(head, 2000, ProfileList),
    TotalHeadTime = 
        random_fetches(head, Bookie1, Bucket, KeyCount, HeadFetches),
    ok = stop_accounting(HeadAccountant),
    {MT1, MP1, MB1} = stop_tracker(HeadMemoryTracker),

    GetMemoryTracker = memory_tracking(get, 1000),
    GetAccountant = accounting(get, 3000, ProfileList),
    TotalGetTime = 
        random_fetches(get, Bookie1, Bucket, KeyCount, GetFetches),
    ok = stop_accounting(GetAccountant),
    {MT2, MP2, MB2} = stop_tracker(GetMemoryTracker),

    QueryMemoryTracker = memory_tracking(query, 1000),
    QueryAccountant = accounting(query, 1000, ProfileList),
    QuerySize = max(10, IndexCount div 1000),
    TotalQueryTime =
        random_queries(
            Bookie1,
            Bucket,
            10,
            IndexCount,
            QuerySize,
            IndexesReturned),
    ok = stop_accounting(QueryAccountant),
    {MT3a, MP3a, MB3a} = stop_tracker(QueryMemoryTracker),

    MiniQueryMemoryTracker = memory_tracking(mini_query, 1000),
    MiniQueryAccountant = accounting(mini_query, 1000, ProfileList),
    MiniQuerySize = max(1, IndexCount div 50000),
    TotalMiniQueryTime =
        random_queries(
            Bookie1,
            Bucket,
            10,
            IndexCount,
            MiniQuerySize,
            IndexesReturned div ?MINI_QUERY_DIVISOR),
    ok = stop_accounting(MiniQueryAccountant),
    {MT3b, MP3b, MB3b} = stop_tracker(MiniQueryMemoryTracker),

    RegexQueryMemoryTracker = memory_tracking(regex_query, 1000),
    RegexQueryAccountant = accounting(regex_query, 2000, ProfileList),
    RegexQueryTime =
        random_people_queries(
            Bookie1,
            Bucket,
            IndexesReturned div ?RGEX_QUERY_DIVISOR),
    ok = stop_accounting(RegexQueryAccountant),
    {MT3c, MP3c, MB3c} = stop_tracker(RegexQueryMemoryTracker),

    GuessMemoryTracker = memory_tracking(guess, 1000),
    GuessAccountant = accounting(guess, 1000, ProfileList),
    {GuessTime, GuessCount} =
        lists:foldl(
            fun(_I, {TSAcc, CountAcc}) ->
                {TS, Count} = counter(Bookie1, guess),
                {TSAcc + TS, CountAcc + Count}
            end,
            {0, 0},
            lists:seq(1, 60)
        ),
    ok = stop_accounting(GuessAccountant),
    {MT4a, MP4a, MB4a} = stop_tracker(GuessMemoryTracker),

    EstimateMemoryTracker = memory_tracking(estimate, 1000),
    EstimateAccountant = accounting(estimate, 1000, ProfileList),
    {EstimateTime, EstimateCount} =
        lists:foldl(
            fun(_I, {TSAcc, CountAcc}) ->
                {TS, Count} = counter(Bookie1, estimate),
                {TSAcc + TS, CountAcc + Count}
            end,
            {0, 0},
            lists:seq(1, 40)
        ),
    ok = stop_accounting(EstimateAccountant),
    {MT4b, MP4b, MB4b} = stop_tracker(EstimateMemoryTracker),

    SegFoldTime = (GuessTime + EstimateTime) div 1000,
    
    FullFoldMemoryTracker = memory_tracking(full, 1000),
    FullFoldAccountant = accounting(full, 2000, ProfileList),
    {FullFoldTime, FullFoldCount} =
        lists:foldl(
            fun(_I, {TSAcc, CountAcc}) ->
                {TS, Count} = counter(Bookie1, full),
                {TSAcc + TS, CountAcc + Count}
            end,
            {0, 0},
            lists:seq(1, 5)
        ),
    ok = stop_accounting(FullFoldAccountant),
    {MT5, MP5, MB5} = stop_tracker(FullFoldMemoryTracker),

    ct:log(
        info,
        "Guess size ~w Estimate size ~w Actual size ~w",
        [GuessCount div 60, EstimateCount div 40, FullFoldCount div 10]
    ),

    UpdateMemoryTracker = memory_tracking(update, 1000),
    UpdateAccountant = accounting(update, 1000, ProfileList),
    TotalUpdateTime =
        rotate_chunk(Bookie1, <<"UpdBucket">>, KeyCount div 50, ObjSize),
    ok = stop_accounting(UpdateAccountant),
    {MT6, MP6, MB6} = stop_tracker(UpdateMemoryTracker),

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
        FullFoldTime div 1000, SegFoldTime,
        TotalUpdateTime,
        DiskSpace,
        {(MT0 + MT1 + MT2 + MT3a + MT3b + MT3c + MT4a + MT4b + MT5 + MT6)
                div 9,
            (MP0 + MP1 + MP2 + MP3a + MP3b + MP3c + MP4a + MP4b + MP5 + MP6)
                div 9,
            (MB0 + MB1 + MB2 + MB3a + MB3b + MB3c + MB4a + MB4b + MB5 + MB6)
                div 9},
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

generate_chunk(CountPerList, ObjSize, IndexGenFun, Bucket, Chunk) ->
    testutil:generate_objects(
        CountPerList, 
        {fixed_binary, (Chunk - 1) * CountPerList + 1}, [],
        base64:encode(leveled_rand:rand_bytes(ObjSize)),
        IndexGenFun(Chunk),
        Bucket
    ).

load_chunk(Bookie, CountPerList, ObjSize, IndexGenFun, Bucket, Chunk) ->
    ct:log(?INFO, "Generating and loading ObjList ~w", [Chunk]),
    time_load_chunk(
        Bookie,
        {Bucket, base64:encode(leveled_rand:rand_bytes(ObjSize)), IndexGenFun(Chunk)},
        (Chunk - 1) * CountPerList + 1,
        Chunk * CountPerList,
        0,
        0
    ).

time_load_chunk(
        _Bookie, _ObjDetails, KeyNumber, TopKey, TotalTime, PC)
        when KeyNumber > TopKey ->
    garbage_collect(),
    timer:sleep(2000),
    ct:log(
        ?INFO,
        "Count of ~w pauses during chunk load",
        [PC]
    ),
    TotalTime;
time_load_chunk(
        Bookie, {Bucket, Value, IndexGen}, KeyNumber, TopKey, TotalTime, PC) ->
    ThisProcess = self(),
    spawn(
        fun() ->
            {RiakObj, IndexSpecs} =
                testutil:set_object(
                    Bucket, testutil:fixed_bin_key(KeyNumber), Value, IndexGen, []),
            {TC, R} =
                timer:tc(
                    testutil, book_riakput, [Bookie, RiakObj, IndexSpecs]
                ),
            case R of
                ok ->
                    ThisProcess! {TC, 0};
                pause ->
                    timer:sleep(40),
                    ThisProcess ! {TC + 40000, 1}
            end
        end
    ),
    receive
        {PutTime, Pause} ->
            time_load_chunk(
                Bookie, 
                {Bucket, Value, IndexGen},
                KeyNumber + 1,
                TopKey,
                TotalTime + PutTime,
                PC + Pause
            )
    end.

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
    SeventiesWillowRegex =
        "[^\\|]*\\|197[0-9]{5}\\|[^\\|]*\\|"
        "[^\\|]*#Willow[^\\|]*\\|[^\\|]*#LS[^\\|]*",
        %% born in the 70s with Willow as a given name
    QueryFun =
        fun() ->
            Surname = get_random_surname(),
            Range =
                {?PEOPLE_INDEX,
                    Surname,
                    <<Surname/binary, 126:8/integer>>
            },
            {ok, TermRegex} =
                re:compile(SeventiesWillowRegex),
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


memory_tracking(Phase, Timeout) ->
    spawn(
        fun() ->
            memory_tracking(Phase, Timeout, {0, 0, 0}, 0)
        end
    ).

memory_tracking(Phase, Timeout, {TAcc, PAcc, BAcc}, Loops) ->
    receive
        {stop, Caller} ->
            {T, P, B} = memory_usage(),
            TAvg = (T + TAcc) div ((Loops + 1) * 1000000),
            PAvg = (P + PAcc) div ((Loops + 1) * 1000000),
            BAvg = (B + BAcc) div ((Loops + 1) * 1000000),
            print_memory_stats(Phase, TAvg, PAvg, BAvg),
            Caller ! {TAvg, PAvg, BAvg}
    after Timeout ->
        {T, P, B} = memory_usage(),
        memory_tracking(
            Phase, Timeout, {TAcc + T, PAcc + P, BAcc + B}, Loops + 1)
    end.


-if(?performance == riak_ctperf).
print_memory_stats(_Phase, _TAvg, _PAvg, _BAvg) ->
    ok.
-else.
print_memory_stats(Phase, TAvg, PAvg, BAvg) ->
    io:format(
        user,
        "~nFor ~w memory stats: total ~wMB process ~wMB binary ~wMB~n",
        [Phase, TAvg, PAvg, BAvg]
    ).
-endif.

dummy_accountant() ->
    spawn(fun() -> receive {stop, Caller} -> Caller ! ok end end).
    
stop_accounting(Accountant) ->
    Accountant ! {stop, self()},
    receive ok -> ok end.

stop_tracker(Tracker) ->
    garbage_collect(),
        % Garbage collect the test process, before getting the memory stats
    Tracker ! {stop, self()},
    receive MemStats -> MemStats end.

-if(?ACCOUNTING).

-define(ACCT_TYPES, [scheduler, dirty_io_scheduler, dirty_cpu_scheduler, aux]).

accounting(Phase, Timeout, ProfileList) ->
    case lists:member(Phase, ProfileList) of
        true ->
            ZeroCounters =
                #{
                    emulator => 0,
                    aux => 0,
                    check_io => 0,
                    gc => 0,
                    other => 0
                },
            InitCounters =
                lists:map(fun(T) -> {T, ZeroCounters} end, ?ACCT_TYPES),
            spawn(
                fun() ->
                    accounting(Phase, Timeout, maps:from_list(InitCounters), 0)
                end
            );
        false ->
            dummy_accountant()
    end.

accounting(Phase, Timeout, Counters, Loops) ->
    receive
        {stop, Caller} ->
            io:format(
                user,
                "~n~nStats for Phase ~p after loops ~p:~n",
                [Phase, Loops]
            ),
            lists:foreach(
                fun(S) ->
                    scheduler_output(S, maps:get(S, Counters))
                end,
                ?ACCT_TYPES
            ),
            Caller ! ok
    after Timeout ->
        msacc:start(Timeout div 5),
        UpdCounters =
            lists:foldl(
                fun(StatMap, CountersAcc) ->
                    Type = maps:get(type, StatMap),
                    case lists:member(Type, ?ACCT_TYPES) of
                        true ->
                            TypeAcc =
                                maps:intersect_with(
                                    fun(_K, V1, V2) -> V1 + V2 end,
                                    maps:get(counters, StatMap),
                                    maps:get(Type, CountersAcc)
                                ),
                            maps:update(Type, TypeAcc, CountersAcc);
                        false ->
                            CountersAcc
                    end
                end,
                Counters,
                msacc:stats()
            ),
        accounting(Phase, Timeout, UpdCounters, Loops + 1)
    end.

scheduler_output(Scheduler, CounterMap) ->
    Total =
        maps:get(emulator, CounterMap) +
        maps:get(aux, CounterMap) +
        maps:get(check_io, CounterMap) +
        maps:get(gc, CounterMap) +
        maps:get(other, CounterMap),
    GC = maps:get(gc, CounterMap),
    GCperc = case Total > 0 of true -> GC/Total; false -> 0.0 end,
    io:format(
        user,
        "~nFor ~w:~n"
        "emulator=~w, aux=~w, check_io=~w, gc=~w, other=~w~n"
        "total ~w~n"
        "percentage_gc ~.2f %~n",
        [Scheduler,
            maps:get(emulator, CounterMap),
            maps:get(aux, CounterMap),
            maps:get(check_io, CounterMap),
            GC,
            maps:get(other, CounterMap),
            Total,
            GCperc
        ]
    ).

-else.

accounting(_Phase, _Timeout, _ProfileList) ->
    dummy_accountant().

-endif.