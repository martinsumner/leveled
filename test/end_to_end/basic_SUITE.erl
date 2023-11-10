-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([simple_put_fetch_head_delete/1,
            many_put_fetch_head/1,
            journal_compaction/1,
            fetchput_snapshot/1,
            load_and_count/1,
            load_and_count_withdelete/1,
            space_clear_ondelete/1,
            is_empty_test/1,
            many_put_fetch_switchcompression/1,
            bigjournal_littlejournal/1,
            bigsst_littlesst/1,
            safereaderror_startup/1,
            remove_journal_test/1
            ]).

all() -> [
            simple_put_fetch_head_delete,
            many_put_fetch_head,
            journal_compaction,
            fetchput_snapshot,
            load_and_count,
            load_and_count_withdelete,
            space_clear_ondelete,
            is_empty_test,
            many_put_fetch_switchcompression,
            bigjournal_littlejournal,
            bigsst_littlesst,
            safereaderror_startup,
            remove_journal_test
            ].


simple_put_fetch_head_delete(_Config) ->
    io:format("simple test with info and no forced logs~n"),
    simple_test_withlog(info, []),
    io:format("simple test with error and no forced logs~n"),
    simple_test_withlog(error, []),
    io:format("simple test with error and stats logs~n"),
    simple_test_withlog(
        error,
        [b0015, b0016, b0017, b0018, p0032, sst12, cdb19, sst13, i0019]).


simple_test_withlog(LogLevel, ForcedLogs) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {sync_strategy, testutil:sync_strategy()},
                    {log_level, LogLevel},
                    {forced_logs, ForcedLogs}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_formissingobject(Bookie1, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPath},
                    {max_journalsize, 3000000},
                    {sync_strategy, testutil:sync_strategy()},
                    {log_level, LogLevel},
                    {forced_logs, ForcedLogs}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    
    testutil:check_forobject(Bookie2, TestObject),
    ObjList1 = testutil:generate_objects(5000, 2),
    testutil:riakload(Bookie2, ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    testutil:check_forlist(Bookie2, ChkList1),
    testutil:check_forobject(Bookie2, TestObject),
    testutil:check_formissingobject(Bookie2, "Bucket1", "Key2"),
    ok = leveled_bookie:book_put(Bookie2, "Bucket1", "Key2", "Value2",
                                    [{add, "Index1", "Term1"}]),
    {ok, "Value2"} = leveled_bookie:book_get(Bookie2, "Bucket1", "Key2"),
    {ok, {62888926, S, undefined}} =
        leveled_bookie:book_head(Bookie2, "Bucket1", "Key2"),
    true = (S == 58) or (S == 60),
        % After OTP 26 the object is 58 bytes not 60
    testutil:check_formissingobject(Bookie2, "Bucket1", "Key2"),
    ok = leveled_bookie:book_put(Bookie2, "Bucket1", "Key2", <<"Value2">>,
                                    [{remove, "Index1", "Term1"},
                                    {add, "Index1", <<"Term2">>}]),
    {ok, <<"Value2">>} = leveled_bookie:book_get(Bookie2, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts2),
    {ok, <<"Value2">>} = leveled_bookie:book_get(Bookie3, "Bucket1", "Key2"),
    ok = leveled_bookie:book_delete(Bookie3, "Bucket1", "Key2",
                                    [{remove, "Index1", "Term1"}]),
    not_found = leveled_bookie:book_get(Bookie3, "Bucket1", "Key2"),
    not_found = leveled_bookie:book_head(Bookie3, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie3),
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts2),
    not_found = leveled_bookie:book_get(Bookie4, "Bucket1", "Key2"),
    ok = leveled_bookie:book_destroy(Bookie4).

many_put_fetch_head(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 =
        [{root_path, RootPath},
            {max_pencillercachesize, 16000},
            {sync_strategy, riak_sync},
            {compression_point, on_compact}
        ],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    {ok, 1} = leveled_bookie:book_sqn(Bookie1,
                                        testutil:get_bucket(TestObject),
                                        testutil:get_key(TestObject),
                                        ?RIAK_TAG),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPath},
                    {max_journalsize, 50000000},
                    {max_pencillercachesize, 32000},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_point, on_receipt}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    ok = leveled_bookie:book_loglevel(Bookie2, error),
    ok = leveled_bookie:book_addlogs(Bookie2, ["B0015"]),
    testutil:check_forobject(Bookie2, TestObject),
    {ok, 1} = leveled_bookie:book_sqn(Bookie2,
                                        testutil:get_bucket(TestObject),
                                        testutil:get_key(TestObject),
                                        ?RIAK_TAG),
    GenList = [2, 20002, 40002, 60002, 80002,
                100002, 120002, 140002, 160002, 180002],
    CLs = testutil:load_objects(20000, GenList, Bookie2, TestObject,
                                fun testutil:generate_smallobjects/2),
    {error, ["B0015"]} = leveled_bookie:book_logsettings(Bookie2),
    ok = leveled_bookie:book_removelogs(Bookie2, ["B0015"]),
    CL1A = lists:nth(1, CLs),
    ChkListFixed = lists:nth(length(CLs), CLs),
    testutil:check_forlist(Bookie2, CL1A),
    {error, []} = leveled_bookie:book_logsettings(Bookie2),
    ok = leveled_bookie:book_loglevel(Bookie2, info),
    ObjList2A = testutil:generate_objects(5000, 2),
    testutil:riakload(Bookie2, ObjList2A),
    ChkList2A = lists:sublist(lists:sort(ObjList2A), 1000),
    testutil:check_forlist(Bookie2, ChkList2A),
    testutil:check_forlist(Bookie2, ChkListFixed),
    testutil:check_forobject(Bookie2, TestObject),
    testutil:check_forlist(Bookie2, ChkList2A),
    testutil:check_forlist(Bookie2, ChkListFixed),
    testutil:check_forobject(Bookie2, TestObject),
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forlist(Bookie3, ChkList2A),
    testutil:check_forobject(Bookie3, TestObject),
    {ok, 1} = leveled_bookie:book_sqn(Bookie3,
                                        testutil:get_bucket(TestObject),
                                        testutil:get_key(TestObject),
                                        ?RIAK_TAG),
    not_found = leveled_bookie:book_sqn(Bookie3,
                                        testutil:get_bucket(TestObject),
                                        testutil:get_key(TestObject),
                                        ?STD_TAG),
    not_found = leveled_bookie:book_sqn(Bookie3,
                                        testutil:get_bucket(TestObject),
                                        testutil:get_key(TestObject)),
    testutil:check_formissingobject(Bookie3, "Bookie1", "MissingKey0123"),
    ok = leveled_bookie:book_destroy(Bookie3).

bigjournal_littlejournal(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 50000000},
                    {max_pencillercachesize, 32000},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_point, on_compact}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    ObjL1 = 
        testutil:generate_objects(100, 1, [], 
                                    leveled_rand:rand_bytes(10000), 
                                    fun() -> [] end, <<"B">>),
    testutil:riakload(Bookie1, ObjL1),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = lists:ukeysort(1, [{max_journalsize, 5000}|StartOpts1]),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    ObjL2 = 
        testutil:generate_objects(10, 1000, [], 
                                    leveled_rand:rand_bytes(10000), 
                                    fun() -> [] end, <<"B">>),
    testutil:riakload(Bookie2, ObjL2),
    testutil:check_forlist(Bookie2, ObjL1),
    testutil:check_forlist(Bookie2, ObjL2),
    ok = leveled_bookie:book_destroy(Bookie2).
    

bigsst_littlesst(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 50000000},
                    {cache_size, 500},
                    {max_pencillercachesize, 16000},
                    {max_sstslots, 256},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_point, on_compact}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    ObjL1 = 
        lists:keysort(
            1,
            testutil:generate_objects(
                100000,
                1,
                [],
                leveled_rand:rand_bytes(100),
                fun() -> [] end,
                <<"B">>)
        ),
    testutil:riakload(Bookie1, ObjL1),
    testutil:check_forlist(Bookie1, ObjL1),
    timer:sleep(10000), % Wait for delete timeout
    JFP = RootPath ++ "/ledger/ledger_files/",
    {ok, FNS1} = file:list_dir(JFP),
    ok = leveled_bookie:book_destroy(Bookie1),

    StartOpts2 = lists:ukeysort(1, [{max_sstslots, 24}|StartOpts1]),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:riakload(Bookie2, ObjL1),
    testutil:check_forlist(Bookie2, ObjL1),
    timer:sleep(10000), % Wait for delete timeout
    {ok, FNS2} = file:list_dir(JFP),
    ok = leveled_bookie:book_destroy(Bookie2),
    io:format("Big SST ~w files Little SST ~w files~n",
                [length(FNS1), length(FNS2)]),
    true = length(FNS2) >=  (2 * length(FNS1)).
    


journal_compaction(_Config) ->
    journal_compaction_tester(false, 3600),
    journal_compaction_tester(false, undefined),
    journal_compaction_tester(true, 3600).

journal_compaction_tester(Restart, WRP) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 10000000},
                    {max_run_length, 1},
                    {sync_strategy, testutil:sync_strategy()},
                    {waste_retention_period, WRP}],
    {ok, Bookie0} = leveled_bookie:book_start(StartOpts1),
    ok = leveled_bookie:book_compactjournal(Bookie0, 30000),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie0, TestObject, TestSpec),
    testutil:check_forobject(Bookie0, TestObject),
    ObjList1 = testutil:generate_objects(20000, 2),
    testutil:riakload(Bookie0, ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 10000),
    testutil:check_forlist(Bookie0, ChkList1),
    testutil:check_forobject(Bookie0, TestObject),
    {B2, K2, V2, Spec2, MD} = {"Bucket2",
                                "Key2",
                                "Value2",
                                [],
                                [{"MDK2", "MDV2"}]},
    {TestObject2, TestSpec2} = testutil:generate_testobject(B2, K2,
                                                            V2, Spec2, MD),
    ok = testutil:book_riakput(Bookie0, TestObject2, TestSpec2),
    ok = leveled_bookie:book_compactjournal(Bookie0, 30000),
    testutil:check_forlist(Bookie0, ChkList1),
    testutil:check_forobject(Bookie0, TestObject),
    testutil:check_forobject(Bookie0, TestObject2),
    testutil:check_forlist(Bookie0, ChkList1),
    testutil:check_forobject(Bookie0, TestObject),
    testutil:check_forobject(Bookie0, TestObject2),
    %% Delete some of the objects
    ObjListD = testutil:generate_objects(10000, 2),
    lists:foreach(fun({_R, O, _S}) ->
                        testutil:book_riakdelete(Bookie0,
                                                    testutil:get_bucket(O),
                                                    testutil:get_key(O),
                                                    [])
                        end,
                    ObjListD),
    
    %% Now replace all the other objects
    ObjList2 = testutil:generate_objects(40000, 10002),
    testutil:riakload(Bookie0, ObjList2),

    Bookie1 =
        case Restart of 
            true ->
                ok = leveled_bookie:book_close(Bookie0),
                {ok, RestartedB} = leveled_bookie:book_start(StartOpts1),
                RestartedB;
            false ->
                Bookie0 
        end,
    
    
    WasteFP = RootPath ++ "/journal/journal_files/waste",
    % Start snapshot - should stop deletions
    {ok, PclClone, InkClone} = 
        leveled_bookie:book_snapshot(Bookie1, store, undefined, false),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    testutil:wait_for_compaction(Bookie1),
    % Wait to cause delete_pending to be blocked by snapshot
    % timeout on switch to delete_pending is 10s
    timer:sleep(10100),
    case WRP of 
        undefined -> 
            ok;
        _ ->
            % Check nothing got deleted
            {ok, CJs} = file:list_dir(WasteFP),
            true = length(CJs) == 0
    end,
    ok = leveled_penciller:pcl_close(PclClone),
    ok = leveled_inker:ink_close(InkClone),
    % Snapshot released so deletes should occur at next timeout
    case WRP of 
        undefined ->
            timer:sleep(10100); % wait for delete_pending timeout
    % Wait 2 seconds for files to be deleted
        _ ->
            FindDeletedFilesFun =
                fun(X, Found) ->
                    case Found of
                        true ->
                            Found;
                        false ->
                            {ok, Files} = file:list_dir(WasteFP),
                            if
                                length(Files) > 0 ->
                                    io:format("Deleted files found~n"),
                                    true;
                                length(Files) == 0 ->
                                    timer:sleep(X),
                                    false
                            end
                    end
                end,
            lists:foldl(FindDeletedFilesFun,
                            false,
                            [2000,2000,2000,2000,2000,2000])
    end,
    {ok, ClearedJournals} = file:list_dir(WasteFP),
    io:format("~w ClearedJournals found~n", [length(ClearedJournals)]),
    case is_integer(WRP) of 
        true ->
            true = length(ClearedJournals) > 0;
        false ->
            true = length(ClearedJournals) == 0
    end,
    
    ChkList3 = lists:sublist(lists:sort(ObjList2), 500),
    testutil:check_forlist(Bookie1, ChkList3),
    
    ok = leveled_bookie:book_close(Bookie1),
    
    % Restart
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    testutil:check_forobject(Bookie2, TestObject),
    testutil:check_forlist(Bookie2, ChkList3),
    
    ok = leveled_bookie:book_close(Bookie2),
    
    StartOpts2 = [{root_path, RootPath},
                    {max_journalsize, 10000000},
                    {max_run_length, 1},
                    {waste_retention_period, 1},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts2),
    ok = leveled_bookie:book_compactjournal(Bookie3, 30000),
    busy = leveled_bookie:book_compactjournal(Bookie3, 30000),
    testutil:wait_for_compaction(Bookie3),
    ok = leveled_bookie:book_close(Bookie3),
    
    {ok, ClearedJournalsPC} = file:list_dir(WasteFP),
    io:format("~w ClearedJournals found~n", [length(ClearedJournalsPC)]),
    case is_integer(WRP) of 
        true ->
            true = length(ClearedJournals) > 0;
        false ->
            true = length(ClearedJournals) == 0
    end,
    
    testutil:reset_filestructure(10000).


fetchput_snapshot(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 30000000},
                    {sync_strategy, none}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    
    % Load up 5000 objects

    ObjList1 = testutil:generate_objects(5000, 2),
    testutil:riakload(Bookie1, ObjList1),
    
    % Now take a snapshot - check it has the same objects

    SnapOpts1 = [{snapshot_bookie, Bookie1}],
    {ok, SnapBookie1} = leveled_bookie:book_start(SnapOpts1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forlist(SnapBookie1, ChkList1),

    compare_foldwithsnap(Bookie1, SnapBookie1, ChkList1),

    % Close the snapshot, check the original store still has the objects

    ok = leveled_bookie:book_close(SnapBookie1),
    testutil:check_forlist(Bookie1, ChkList1),
    ok = leveled_bookie:book_close(Bookie1),
    io:format("Closed initial bookies~n"),

    % all now closed
    
    % Open a new store (to start with the previously loaded data)

    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    SnapOpts2 = [{snapshot_bookie, Bookie2}],

    % And take a snapshot of that store

    {ok, SnapBookie2} = leveled_bookie:book_start(SnapOpts2),
    io:format("Bookies restarted~n"),
    

    % Check both the newly opened store and its snapshot have the data

    testutil:check_forlist(Bookie2, ChkList1),
    io:format("Check active bookie still contains original data~n"),
    testutil:check_forlist(SnapBookie2, ChkList1),
    io:format("Check snapshot still contains original data~n"),
    
    % Generate some replacement objects, load them up - check the master 
    % store has the replacement objects, but the snapshot still has the old
    % objects
    
    ObjList2 = testutil:generate_objects(5000, 2),
    testutil:riakload(Bookie2, ObjList2),
    io:format("Replacement objects put~n"),
    
    ChkList2 = lists:sublist(lists:sort(ObjList2), 100),
    testutil:check_forlist(Bookie2, ChkList2),
    testutil:check_forlist(SnapBookie2, ChkList1),
    io:format("Checked for replacement objects in active bookie" ++
                    ", old objects in snapshot~n"),
    
    % Check out how many ledger files we now have (should just be 1)

    ok = filelib:ensure_dir(RootPath ++ "/ledger/ledger_files"),
    {ok, FNsA} = file:list_dir(RootPath ++ "/ledger/ledger_files"),

    % generate some new objects and load them up.  Check that the master store
    % has the new objects, and the snapshot doesn't

    ObjList3 = testutil:generate_objects(15000, 5002),
    testutil:riakload(Bookie2, ObjList3),
    ChkList3 = lists:sublist(lists:sort(ObjList3), 100),
    testutil:check_forlist(Bookie2, ChkList3),
    testutil:check_formissinglist(SnapBookie2, ChkList3),

    % Now loads lots of new objects

    GenList = [20002, 40002, 60002, 80002, 100002, 120002, 140002, 160002],
    CLs2 = testutil:load_objects(20000, GenList, Bookie2, TestObject,
                                    fun testutil:generate_smallobjects/2),
    io:format("Loaded significant numbers of new objects~n"),
    
    testutil:check_forlist(Bookie2, lists:nth(length(CLs2), CLs2)),
    io:format("Checked active bookie has new objects~n"),
    
    % Start a second snapshot, which should have the new objects, whilst the
    % previous snapshot still doesn't

    {ok, SnapBookie3} = leveled_bookie:book_start(SnapOpts2),
    testutil:check_forlist(SnapBookie3, lists:nth(length(CLs2), CLs2)),
    testutil:check_formissinglist(SnapBookie2, ChkList3),
    testutil:check_formissinglist(SnapBookie2, lists:nth(length(CLs2), CLs2)),
    testutil:check_forlist(Bookie2, ChkList2),
    testutil:check_forlist(SnapBookie3, ChkList2),
    testutil:check_forlist(SnapBookie2, ChkList1),
    io:format("Started new snapshot and check for new objects~n"),

    compare_foldwithsnap(Bookie2, SnapBookie3, ChkList3),
    
    % Load yet more objects, these are replacement objects for the last load

    CLs3 = testutil:load_objects(20000, GenList, Bookie2, TestObject,
                                    fun testutil:generate_smallobjects/2),
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    testutil:check_forlist(Bookie2, lists:nth(1, CLs3)),
    
    io:format("Starting 15s sleep in which snap2 should block deletion~n"),
    timer:sleep(15000),

    % There should be lots of ledger files, as we have replaced the objects 
    % which has created new files, but the old files are still in demand from 
    % the snapshot

    {ok, FNsB} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    ok = leveled_bookie:book_close(SnapBookie2),
    io:format("Starting 15s sleep as snap2 close should unblock deletion~n"),
    timer:sleep(15000),
    io:format("Pause for deletion has ended~n"),
    
    % So the pause here is to allow for delete pendings to take effect after the 
    % closing of the snapshot

    % Now check that any deletions haven't impacted the availability of data
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),

    % Close the other snapshot, and pause - after the pause there should be a 
    % reduction in the number of ledger files due to the deletes

    ok = leveled_bookie:book_close(SnapBookie3),
    io:format("Starting 15s sleep as snap3 close should unblock deletion~n"),
    timer:sleep(15000),
    io:format("Pause for deletion has ended~n"),
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    testutil:check_forlist(Bookie2, lists:nth(1, CLs3)),

    {ok, FNsC} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    io:format("FNsA ~w FNsB ~w FNsC ~w~n", 
                [length(FNsA), length(FNsB), length(FNsC)]),
    true = length(FNsB) > length(FNsA),
    true = length(FNsB) > length(FNsC), 
        % smaller due to replacements and files deleting
        % This is dependent on the sleep though (yuk)
    
    {B1Size, B1Count} = testutil:check_bucket_stats(Bookie2, "Bucket1"),
    true = B1Size > 0,
    true = B1Count == 1,
    {B1Size, B1Count} = testutil:check_bucket_stats(Bookie2, "Bucket1"),
    {BSize, BCount} = testutil:check_bucket_stats(Bookie2, "Bucket"),
    true = BSize > 0,
    true = BCount == 180000,

    io:format("Shutdown with overhanging snapshot~n"),
    
    {ok, SnpPCL1, SnpJrnl1} =
        leveled_bookie:book_snapshot(Bookie2, store, undefined, true),
    {ok, SnpPCL2, SnpJrnl2} =
        leveled_bookie:book_snapshot(Bookie2, store, undefined, true),

    TestPid = self(),
    spawn(
        fun() ->
            ok = leveled_bookie:book_close(Bookie2),
            TestPid ! ok
        end),

    timer:sleep(5000),
    ok = leveled_penciller:pcl_close(SnpPCL1),
    ok = leveled_inker:ink_close(SnpJrnl1),
    true = is_process_alive(SnpPCL2),
    true = is_process_alive(SnpJrnl2),

    io:format("Time for close to complete is 2 * 10s~n"),
    io:format("Both Inker and Penciller will have snapshot delay~n"),

    receive ok -> ok end,

    false = is_process_alive(SnpPCL2),
    false = is_process_alive(SnpJrnl2),
    testutil:reset_filestructure().


compare_foldwithsnap(Bookie, SnapBookie, ChkList) ->
    HeadFoldFun = fun(B, K, _Hd, Acc) -> [{B, K}|Acc] end,
    KeyFoldFun = fun(B, K, Acc) -> [{B, K}|Acc] end,
    {async, HeadFoldDB} =
        leveled_bookie:book_headfold(
            Bookie, ?RIAK_TAG, {HeadFoldFun, []}, true, false, false
        ),
    {async, HeadFoldSnap} =
        leveled_bookie:book_headfold(
            SnapBookie, ?RIAK_TAG, {HeadFoldFun, []}, true, false, false
        ),
    true = HeadFoldDB() == HeadFoldSnap(),

    testutil:check_forlist(SnapBookie, ChkList),

    {async, KeyFoldSnap} =
        leveled_bookie:book_keylist(
            SnapBookie, ?RIAK_TAG, {KeyFoldFun, []}
        ),
    true = HeadFoldSnap() == KeyFoldSnap().


load_and_count(_Config) ->
    % Use artificially small files, and the load keys, counting they're all
    % present
    load_and_count(50000000, 2500, 28000),
    load_and_count(200000000, 50, 200000),
    load_and_count(50000000, 1000, 5000).


load_and_count(JournalSize, BookiesMemSize, PencillerMemSize) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, JournalSize},
                    {cache_size, BookiesMemSize},
                    {max_pencillercachesize, PencillerMemSize},
                    {sync_strategy, testutil:sync_strategy()},
                    {stats_logfrequency, 5},
                    {stats_probability, 80}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Loading initial small objects~n"),
    G1 = fun testutil:generate_smallobjects/2,
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G1),
                        {_S, Count} = 
                            testutil:check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        0,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Loading larger compressible objects~n"),
    G2 = fun testutil:generate_compressibleobjects/2,
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G2),
                        {_S, Count} = 
                            testutil:check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        100000,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Replacing small objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G1),
                        {_S, Count} = 
                            testutil:check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Count == 200000 ->
                                ok
                        end,
                        Acc + 5000 end,
                        0,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Loading more small objects~n"),
    io:format("Now with unused snapshot so deletions are blocked~n"),
    {ok, PclClone, null} = 
        leveled_bookie:book_snapshot(Bookie1, ledger, undefined, true),
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G2),
                        {_S, Count} = 
                            testutil:check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        200000,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_penciller:pcl_close(PclClone),
    {_S, 300000} = testutil:check_bucket_stats(Bookie1, "Bucket"),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    {_, 300000} = testutil:check_bucket_stats(Bookie2, "Bucket"),
    
    ok = leveled_bookie:book_close(Bookie2),

    ManifestFP =
        leveled_pmanifest:filepath(filename:join(RootPath, ?LEDGER_FP),
                                    manifest),
    IsManifest = fun(FN) -> filename:extension(FN) == ".man" end,
    {ok, RawManList} = file:list_dir(ManifestFP),
    ManList = lists:filter(IsManifest, RawManList),
    io:format("Length of manifest file list ~w~n", [length(ManList)]),
    true = length(ManList) =< 5,
    testutil:reset_filestructure().

load_and_count_withdelete(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 50000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Loading initial small objects~n"),
    G1 = fun testutil:generate_smallobjects/2,
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G1),
                        {_S, Count} = testutil:check_bucket_stats(Bookie1,
                                                                    "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        0,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    {BucketD, KeyD} =
        {testutil:get_bucket(TestObject), testutil:get_key(TestObject)},
    {_, 1} = testutil:check_bucket_stats(Bookie1, BucketD),
    ok = testutil:book_riakdelete(Bookie1, BucketD, KeyD, []),
    not_found = testutil:book_riakget(Bookie1, BucketD, KeyD),
    {_, 0} = testutil:check_bucket_stats(Bookie1, BucketD),
    io:format("Loading larger compressible objects~n"),
    G2 = fun testutil:generate_compressibleobjects/2,
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                no_check,
                                                G2),
                        {_S, Count} = testutil:check_bucket_stats(Bookie1,
                                                                    "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        100000,
                        lists:seq(1, 20)),
    not_found = testutil:book_riakget(Bookie1, BucketD, KeyD),
    ok = leveled_bookie:book_close(Bookie1),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    testutil:check_formissingobject(Bookie2, BucketD, KeyD),
    testutil:check_formissingobject(Bookie2, "Bookie1", "MissingKey0123"),
    {_BSize, 0} = testutil:check_bucket_stats(Bookie2, BucketD),
    ok = leveled_bookie:book_close(Bookie2),

    testutil:reset_filestructure().


space_clear_ondelete(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 10000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Book1} = leveled_bookie:book_start(StartOpts1),
    G2 = fun testutil:generate_compressibleobjects/2,
    testutil:load_objects(20000,
                            [uuid, uuid, uuid, uuid],
                            Book1,
                            no_check,
                            G2),
    
    FoldKeysFun = fun(B, K, Acc) -> [{B, K}|Acc] end,

    {async, F1} = leveled_bookie:book_keylist(Book1, o_rkv, {FoldKeysFun, []}),

    SW1 = os:timestamp(),
    KL1 = F1(),
    ok = case length(KL1) of
                80000 ->
                    io:format("Key list took ~w microseconds for 80K keys~n",
                                [timer:now_diff(os:timestamp(), SW1)]),
                    ok
            end,
    timer:sleep(10000), % Allow for any L0 file to be rolled
    {ok, FNsA_L} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    {ok, FNsA_J} = file:list_dir(RootPath ++ "/journal/journal_files"),
    io:format("FNsA - Bookie created ~w journal files and ~w ledger files~n",
                    [length(FNsA_J), length(FNsA_L)]),

    % Get an iterator to lock the inker during compaction
    FoldObjectsFun = fun(B, K, ObjBin, Acc) ->
                            [{B, K, erlang:phash2(ObjBin)}|Acc] end,

    {async, HTreeF1} = leveled_bookie:book_objectfold(Book1,
                                                      ?RIAK_TAG,
                                                      {FoldObjectsFun, []},
                                                      false),

        % This query does not Snap PreFold - and so will not prevent
        % pending deletes from prompting actual deletes

    {async, KF1} = leveled_bookie:book_keylist(Book1, o_rkv, {FoldKeysFun, []}),
        % This query does Snap PreFold, and so will prevent deletes from
        % the ledger

    % Delete the keys
    SW2 = os:timestamp(),
    lists:foreach(fun({Bucket, Key}) ->
                        testutil:book_riakdelete(Book1,
                                                        Bucket,
                                                        Key,
                                                        [])
                        end,
                    KL1),
    io:format("Deletion took ~w microseconds for 80K keys~n",
                                [timer:now_diff(os:timestamp(), SW2)]),
    
    
    
    ok = leveled_bookie:book_compactjournal(Book1, 30000),
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(20000),
                                F(Book1)
                        end end,
                    true,
                    lists:seq(1, 15)),
    io:format("Waiting for journal deletes - blocked~n"),
    timer:sleep(20000),
    
    io:format("Sleep over - Fold Objects query ~n"),
    % for this query snapshot is made at fold time, and so the results are 
    % empty
    true = length(HTreeF1()) == 0,
    
    % This query uses a genuine async fold on a snasphot made at request time
    % and so the results should be non-empty
    io:format("Now Query 2 - Fold Keys query~n"),
    true = length(KF1()) == 80000, 
    
    io:format("Waiting for journal deletes - unblocked~n"),
    timer:sleep(20000),
    {ok, FNsB_L} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    {ok, FNsB_J} = file:list_dir(RootPath ++ "/journal/journal_files"),
    {ok, FNsB_PC} = file:list_dir(RootPath
                                    ++ "/journal/journal_files/post_compact"),
    PointB_Journals = length(FNsB_J) + length(FNsB_PC),
    io:format("FNsB - Bookie has ~w journal files and ~w ledger files " ++
                    "after deletes~n",
                [PointB_Journals, length(FNsB_L)]),
    
    {async, F2} = leveled_bookie:book_keylist(Book1, o_rkv, {FoldKeysFun, []}),
    SW3 = os:timestamp(),
    KL2 = F2(),
    ok = case length(KL2) of
                0 ->
                    io:format("Key list took ~w microseconds for no keys~n",
                                [timer:now_diff(os:timestamp(), SW3)]),
                    ok
            end,
    ok = leveled_bookie:book_close(Book1),
    
    {ok, Book2} = leveled_bookie:book_start(StartOpts1),
    {async, F3} = leveled_bookie:book_keylist(Book2, o_rkv, {FoldKeysFun, []}),
    SW4 = os:timestamp(),
    KL3 = F3(),
    ok = case length(KL3) of
                0 ->
                    io:format("Key list took ~w microseconds for no keys~n",
                                [timer:now_diff(os:timestamp(), SW4)]),
                    ok
            end,
    ok = leveled_bookie:book_close(Book2),
    {ok, FNsC_L} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    io:format("FNsC - Bookie has ~w ledger files " ++
                    "after close~n", [length(FNsC_L)]),
    
    {ok, Book3} = leveled_bookie:book_start(StartOpts1),
    io:format("This should cause a final ledger merge event~n"),
    io:format("Will require the penciller to resolve the issue of creating" ++
                " an empty file as all keys compact on merge~n"),
    
    CheckFun = 
        fun(X, FileCount) ->
            case FileCount of
                0 ->
                    0;
                _ ->
                    timer:sleep(X),
                    {ok, NewFC} = 
                        file:list_dir(RootPath ++ "/ledger/ledger_files"),
                    io:format("Looping with ledger file count ~w~n", 
                                [length(NewFC)]),
                    length(strip_nonsst(NewFC))
            end
        end,
    
    FC = lists:foldl(CheckFun, infinity, [2000, 3000, 5000, 8000]),
    ok = leveled_bookie:book_close(Book3),
    case FC of 
        0 ->
            ok;
        _ ->
            {ok, Book4} = leveled_bookie:book_start(StartOpts1),
            lists:foldl(CheckFun, infinity, [2000, 3000, 5000, 8000]),
            leveled_bookie:book_close(Book4)
    end,

    {ok, FNsD_L} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    io:format("FNsD - Bookie has ~w ledger files " ++
                    "after second close~n", [length(strip_nonsst(FNsD_L))]),
    lists:foreach(fun(FN) -> 
                        io:format("FNsD - Ledger file is ~s~n", [FN]) 
                    end, 
                    FNsD_L),
    true = PointB_Journals < length(FNsA_J),
    true = length(strip_nonsst(FNsD_L)) < length(strip_nonsst(FNsA_L)),
    true = length(strip_nonsst(FNsD_L)) < length(strip_nonsst(FNsB_L)),
    true = length(strip_nonsst(FNsD_L)) =< length(strip_nonsst(FNsC_L)),
    true = length(strip_nonsst(FNsD_L)) == 0.


strip_nonsst(FileList) ->
    SSTOnlyFun = 
        fun(FN, Acc) ->
            case filename:extension(FN) of 
                ".sst" -> 
                    [FN|Acc];
                _ ->
                    Acc 
            end
        end,
    lists:foldl(SSTOnlyFun, [], FileList).


is_empty_test(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    
    {B1, K1, V1, Spec, MD} = {term_to_binary("Bucket1"),
                                term_to_binary("Key1"),
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject1, TestSpec1} =
        testutil:generate_testobject(B1, K1, V1, Spec, MD),
    {B1, K2, V2, Spec, MD} = {term_to_binary("Bucket1"),
                                term_to_binary("Key2"),
                                "Value2",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject2, TestSpec2} =
        testutil:generate_testobject(B1, K2, V2, Spec, MD),
    {B2, K3, V3, Spec, MD} = {term_to_binary("Bucket2"),
                                term_to_binary("Key3"),
                                "Value3",
                                [],
                                [{"MDK1", "MDV1"}]},
    {TestObject3, TestSpec3} =
        testutil:generate_testobject(B2, K3, V3, Spec, MD),
    ok = testutil:book_riakput(Bookie1, TestObject1, TestSpec1),
    ok = testutil:book_riakput(Bookie1, TestObject2, TestSpec2),
    ok = testutil:book_riakput(Bookie1, TestObject3, TestSpec3),
    
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,
    BucketListQuery = {bucket_list,
                        ?RIAK_TAG,
                        {FoldBucketsFun, sets:new()}},
    {async, BL} = leveled_bookie:book_returnfolder(Bookie1, BucketListQuery),
    true = sets:size(BL()) == 2,
    
    ok = leveled_bookie:book_put(Bookie1, B2, K3, delete, [], ?RIAK_TAG),
    {async, BLpd1} = leveled_bookie:book_returnfolder(Bookie1, BucketListQuery),
    true = sets:size(BLpd1()) == 1,
    
    ok = leveled_bookie:book_put(Bookie1, B1, K2, delete, [], ?RIAK_TAG),
    {async, BLpd2} = leveled_bookie:book_returnfolder(Bookie1, BucketListQuery),
    true = sets:size(BLpd2()) == 1,
    
    ok = leveled_bookie:book_put(Bookie1, B1, K1, delete, [], ?RIAK_TAG),
    {async, BLpd3} = leveled_bookie:book_returnfolder(Bookie1, BucketListQuery),
    true = sets:size(BLpd3()) == 0,
    
    ok = leveled_bookie:book_close(Bookie1).


remove_journal_test(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_point, on_compact}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    GenList = [1, 20001, 40001, 60001],
    CLs = testutil:load_objects(20000, GenList, Bookie1, no_check,
                                fun testutil:generate_smallobjects/2),
    CheckList1 = lists:sublist(lists:nth(1, CLs), 100, 1000),
    CheckList2 = lists:sublist(lists:nth(2, CLs), 100, 1000),
    CheckList3 = lists:sublist(lists:nth(3, CLs), 100, 1000),
    CheckList4 = lists:sublist(lists:nth(4, CLs), 100, 1000),
    testutil:check_forlist(Bookie1, CheckList1),
    testutil:check_forlist(Bookie1, CheckList2),
    testutil:check_forlist(Bookie1, CheckList3),
    testutil:check_forlist(Bookie1, CheckList4),
    
    ok = leveled_bookie:book_close(Bookie1),
    leveled_inker:clean_testdir(RootPath ++ "/journal"),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),

    % If we're not careful here new data will be added, and we 
    % won't be able to read it
    [NewCheckList] = 
        testutil:load_objects(1000, [80001], Bookie2, no_check,
                                fun testutil:generate_smallobjects/2),
    
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts1),
    testutil:check_forlist(Bookie3, NewCheckList),
    ok = leveled_bookie:book_destroy(Bookie3).



many_put_fetch_switchcompression(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_pencillercachesize, 16000},
                    {max_journalobjectcount, 30000},
                    {compression_level, 3},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_method, native}],
    StartOpts2 = [{root_path, RootPath},
                    {max_pencillercachesize, 24000},
                    {max_journalobjectcount, 30000},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_method, lz4}],
    StartOpts3 = [{root_path, RootPath},
                    {max_pencillercachesize, 16000},
                    {max_journalobjectcount, 30000},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_method, none}],
    
    
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    CL1s =
        testutil:load_objects(
            40000,
            [2, 40002],
            Bookie1,
            TestObject,
            fun testutil:generate_smallobjects/2),
    
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie1, CL) end, CL1s),
    ok = leveled_bookie:book_close(Bookie1),
    
    %% Change compression method -> lz4
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forobject(Bookie2, TestObject),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie2, CL) end, CL1s),

    CL2s =
        testutil:load_objects(
            40000,
            [80002, 120002],
            Bookie2,
            TestObject,
            fun testutil:generate_smallobjects/2),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie2, CL) end, CL2s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie2, CL) end, CL1s),
    ok = leveled_bookie:book_close(Bookie2),

    %% Change method back again
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts1),
    testutil:check_formissingobject(Bookie3, "Bookie1", "MissingKey0123"),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie3, CL) end, CL2s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie3, CL) end, CL1s),
    
    CL3s =
        testutil:load_objects(
            40000,
            [160002, 200002],
            Bookie3,
            TestObject,
            fun testutil:generate_smallobjects/2,
            30000
        ),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie3, CL) end, CL3s),
    ok = leveled_bookie:book_close(Bookie3),

    % Change method to no compression
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL2s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL1s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL3s),
    
    CL4s =
        testutil:load_objects(
            40000,
            [240002, 280002],
            Bookie4,
            TestObject,
            fun testutil:generate_smallobjects/2
        ),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL3s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL4s),
    testutil:delete_some_objects(Bookie4, lists:flatten(CL3s), 60000),
    CL5s =
        testutil:load_objects(
            40000,
            [320002, 360002],
            Bookie4,
            TestObject,
            fun testutil:generate_smallobjects/2
        ),
    ok = leveled_bookie:book_compactjournal(Bookie4, 30000),
    testutil:wait_for_compaction(Bookie4),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL4s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie4, CL) end, CL5s),
    
    ok = leveled_bookie:book_close(Bookie4),

    %% Change compression method -> lz4
    {ok, Bookie5} = leveled_bookie:book_start(StartOpts2),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie5, CL) end, CL1s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie5, CL) end, CL4s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie5, CL) end, CL5s),
    ok = leveled_bookie:book_close(Bookie5),

    %% Change compression method -> native
    {ok, Bookie6} = leveled_bookie:book_start(StartOpts1),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie6, CL) end, CL1s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie6, CL) end, CL4s),
    lists:foreach(
        fun(CL) -> ok = testutil:check_forlist(Bookie6, CL) end, CL5s),

    ok = leveled_bookie:book_destroy(Bookie6).


safereaderror_startup(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath}, 
                    {compression_point, on_compact},
                    {max_journalsize, 1000}, {cache_size, 2060}],
    {ok, Bookie1} = leveled_bookie:book_plainstart(StartOpts1),
    B1 = <<98, 117, 99, 107, 101, 116, 51>>,
    K1 = 
        <<38, 50, 201, 47, 167, 125, 57, 232, 84, 38, 14, 114, 24, 62,
            12, 74>>,
    Obj1 = 
        <<87, 150, 217, 230, 4, 81, 170, 68, 181, 224, 60, 232, 4, 74,
            159, 12, 156, 56, 194, 181, 18, 158, 195, 207, 106, 191, 80,
            111, 100, 81, 252, 248>>,
    Obj2 = 
        <<86, 201, 253, 149, 213, 10, 32, 166, 33, 136, 42, 79, 103, 250,
            139, 95, 42, 143, 161, 3, 185, 74, 149, 226, 232, 214, 183, 64,
            69, 56, 167, 78>>,
    ok = leveled_bookie:book_put(Bookie1, B1, K1, Obj1, []),
    ok = leveled_bookie:book_put(Bookie1, B1, K1, Obj2, []),
    exit(Bookie1, kill),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    {ok, ReadBack} = leveled_bookie:book_get(Bookie2, B1, K1),
    io:format("Read back ~w", [ReadBack]),
    true = ReadBack == Obj2,
    ok = leveled_bookie:book_close(Bookie2).