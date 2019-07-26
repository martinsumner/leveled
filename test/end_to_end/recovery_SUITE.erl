-module(recovery_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            recovery_with_samekeyupdates/1,
            hot_backup_simple/1,
            hot_backup_changes/1,
            retain_strategy/1,
            recovr_strategy/1,
            aae_missingjournal/1,
            aae_bustedjournal/1,
            journal_compaction_bustedjournal/1,
            close_duringcompaction/1,
            allkeydelta_journal_multicompact/1,
            recompact_keydeltas/1
            ]).

all() -> [
            recovery_with_samekeyupdates,
            hot_backup_simple,
            hot_backup_changes,
            retain_strategy,
            recovr_strategy,
            aae_missingjournal,
            aae_bustedjournal,
            journal_compaction_bustedjournal,
            close_duringcompaction,
            allkeydelta_journal_multicompact,
            recompact_keydeltas
            ].


close_duringcompaction(_Config) ->
    % Prompt a compaction, and close immedately - confirm that the close 
    % happens without error.
    % This should trigger the iclerk to receive a close during the file
    % scoring stage
    RootPath = testutil:reset_filestructure(),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 2000},
                    {max_journalsize, 2000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Spcl1, LastV1} = rotating_object_check(BookOpts, "Bucket1", 6400),
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    ok = leveled_bookie:book_compactjournal(Book1, 30000),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    ok = testutil:check_indexed_objects(Book2, "Bucket1", Spcl1, LastV1),
    ok = leveled_bookie:book_close(Book2).

recovery_with_samekeyupdates(_Config) ->
    % Setup to prove https://github.com/martinsumner/leveled/issues/229
    % run a test that involves many updates to the same key, and check that
    % this doesn't cause performance to flatline in either the normal "PUT"
    % case, or in the case of the recovery from a lost keystore
    AcceptableDuration = 180, % 3 minutes
    E2E_SW = os:timestamp(), % Used to track time for overall job
    
    RootPath = testutil:reset_filestructure(),
    BackupPath = testutil:reset_filestructure("backupSKU"),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 2000},
                    {max_journalsize, 20000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Book1} = leveled_bookie:book_start(BookOpts),

    % Load in 5K different keys
    % Load 5 keys in 5K times each
    % Load in 5K different keys
    io:format("Commence object generation and load~n"),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 = testutil:generate_objects(5000,
                                      {fixed_binary, 1},
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket1">>),
    testutil:riakload(Book1, ObjL1),
    RepeatedLoadFun =
        fun(_I, _Acc) ->
            ObjRL =
                testutil:generate_objects(5,
                                            {fixed_binary, 5001},
                                            [],
                                            ObjectGen,
                                            IndexGen,
                                            <<"Bucket1">>),
            testutil:riakload(Book1, ObjRL),
            ObjRL
        end,
    FinalObjRL = lists:foldl(RepeatedLoadFun, [], lists:seq(1, 5000)),
    ObjL2 = testutil:generate_objects(5000,
                                      {fixed_binary, 6001},
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket1">>),
    testutil:riakload(Book1, ObjL2),

    % Fetch all of ObjL1
    io:format("Check for presence of unique objects~n"),
    ok = testutil:checkhead_forlist(Book1, ObjL1),
    % Fetch all of ObjL2
    ok = testutil:checkhead_forlist(Book1, ObjL2),
    io:format("Check for presence of repeated objects~n"),
    % Fetch repeated objects 200 times each
    CheckFun1 = 
        fun(_I) -> ok = testutil:checkhead_forlist(Book1, FinalObjRL) end,
    lists:foreach(CheckFun1, lists:seq(1, 200)),
    io:format("Checks complete~n"),

    io:format("Backup - get journal with no ledger~n"),
    {async, BackupFun} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun(BackupPath),

    io:format("Restarting without key store~n"),
    ok = leveled_bookie:book_close(Book1),

    BookOptsBackup = [{root_path, BackupPath},
                        {cache_size, 2000},
                        {max_journalsize, 20000000},
                        {sync_strategy, testutil:sync_strategy()}],
    {ok, Book2} = leveled_bookie:book_start(BookOptsBackup),

    % Fetch all of ObjL1
    io:format("Check for presence of unique objects~n"),
    ok = testutil:checkhead_forlist(Book2, ObjL1),
    % Fetch all of ObjL2
    ok = testutil:checkhead_forlist(Book2, ObjL2),
    io:format("Check for presence of repeated objects~n"),
    % Fetch repeated objects 200 times each
    CheckFun2 = 
        fun(_I) -> ok = testutil:checkhead_forlist(Book2, FinalObjRL) end,
    lists:foreach(CheckFun2, lists:seq(1, 200)),
    io:format("Checks complete from backup~n"),
    
    DurationOfTest = timer:now_diff(os:timestamp(), E2E_SW)/(1000 * 1000),
    io:format("Duration of test was ~w s~n", [DurationOfTest]),
    true = DurationOfTest < AcceptableDuration,

    ok = leveled_bookie:book_close(Book2),
    testutil:reset_filestructure(BackupPath),
    testutil:reset_filestructure().




hot_backup_simple(_Config) ->
    % The journal may have a hot backup.  This allows for an online Bookie
    % to be sent a message to prepare a backup function, which an asynchronous
    % worker can then call to generate a backup taken at the point in time
    % the original message was processsed.
    %
    % The basic test is to:
    % 1 - load a Bookie, take a backup, delete the original path, restore from
    % that path
    RootPath = testutil:reset_filestructure(),
    BackupPath = testutil:reset_filestructure("backup0"),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalsize, 10000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Spcl1, LastV1} = rotating_object_check(BookOpts, "Bucket1", 3200),
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    {async, BackupFun} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun(BackupPath),
    ok = leveled_bookie:book_close(Book1),
    RootPath = testutil:reset_filestructure(),
    BookOptsBackup = [{root_path, BackupPath},
                        {cache_size, 2000},
                        {max_journalsize, 20000000},
                        {sync_strategy, testutil:sync_strategy()}],
    {ok, BookBackup} = leveled_bookie:book_start(BookOptsBackup),
    ok = testutil:check_indexed_objects(BookBackup, "Bucket1", Spcl1, LastV1),
    ok = leveled_bookie:book_close(BookBackup),
    BackupPath = testutil:reset_filestructure("backup0").

hot_backup_changes(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BackupPath = testutil:reset_filestructure("backup0"),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalsize, 10000000},
                    {sync_strategy, testutil:sync_strategy()}],
    B = "Bucket0",    

    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    {KSpcL1, _V1} = testutil:put_indexed_objects(Book1, B, 20000),
    
    {async, BackupFun1} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun1(BackupPath),
    {ok, FileList1} = 
        file:list_dir(filename:join(BackupPath, "journal/journal_files/")),
    
    {KSpcL2, V2} = testutil:put_altered_indexed_objects(Book1, B, KSpcL1),

    {async, BackupFun2} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun2(BackupPath),
    {ok, FileList2} = 
        file:list_dir(filename:join(BackupPath, "journal/journal_files/")),

    ok = testutil:check_indexed_objects(Book1, B, KSpcL2, V2),
    compact_and_wait(Book1),

    {async, BackupFun3} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun3(BackupPath),
    {ok, FileList3} = 
        file:list_dir(filename:join(BackupPath, "journal/journal_files/")),
    % Confirm null impact of backing up twice in a row
    {async, BackupFun4} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun4(BackupPath),
    {ok, FileList4} = 
        file:list_dir(filename:join(BackupPath, "journal/journal_files/")),

    true = length(FileList2) > length(FileList1),
    true = length(FileList2) > length(FileList3),
    true = length(FileList3) == length(FileList4),

    ok = leveled_bookie:book_close(Book1),

    RootPath = testutil:reset_filestructure(),
    BookOptsBackup = [{root_path, BackupPath},
                        {cache_size, 2000},
                        {max_journalsize, 20000000},
                        {sync_strategy, testutil:sync_strategy()}],
    {ok, BookBackup} = leveled_bookie:book_start(BookOptsBackup),

    ok = testutil:check_indexed_objects(BookBackup, B, KSpcL2, V2),

    testutil:reset_filestructure("backup0"),
    testutil:reset_filestructure().



retain_strategy(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalsize, 5000000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?RIAK_TAG, retain}]}],
    BookOptsAlt = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalsize, 100000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?RIAK_TAG, retain}]},
                    {max_run_length, 8}],
    {ok, Spcl3, LastV3} = rotating_object_check(BookOpts, "Bucket3", 800),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3}]),
    {ok, Spcl4, LastV4} = rotating_object_check(BookOpts, "Bucket4", 1600),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4}]),
    {ok, Spcl5, LastV5} = rotating_object_check(BookOpts, "Bucket5", 3200),
    ok = restart_from_blankledger(BookOptsAlt, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket5", Spcl5, LastV5}]),
    {ok, Spcl6, LastV6} = rotating_object_check(BookOpts, "Bucket6", 6400),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4},
                                                {"Bucket5", Spcl5, LastV5},
                                                {"Bucket6", Spcl6, LastV6}]),
    testutil:reset_filestructure().


recovr_strategy(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalsize, 5000000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?RIAK_TAG, recovr}]}],
    
    R6 = rotating_object_check(BookOpts, "Bucket6", 6400),
    {ok, AllSpcL, V4} = R6,
    leveled_penciller:clean_testdir(proplists:get_value(root_path, BookOpts) ++
                                    "/ledger"),
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Book1, TestObject, TestSpec),
    ok = testutil:book_riakdelete(Book1,
                                    testutil:get_bucket(TestObject),
                                    testutil:get_key(TestObject),
                                    []),
    
    lists:foreach(fun({K, _SpcL}) -> 
                        {ok, OH} = testutil:book_riakhead(Book1, "Bucket6", K),
                        VCH = testutil:get_vclock(OH),
                        {ok, OG} = testutil:book_riakget(Book1, "Bucket6", K),
                        V = testutil:get_value(OG),
                        VCG = testutil:get_vclock(OG),
                        true = V == V4,
                        true = VCH == VCG
                        end,
                    lists:nthtail(6400, AllSpcL)),
    Q = fun(RT) -> {index_query,
                        "Bucket6",
                        {fun testutil:foldkeysfun/3, []},
                        {"idx1_bin", "#", "|"},
                        {RT, undefined}}
                    end,
    {async, TFolder} = leveled_bookie:book_returnfolder(Book1, Q(true)),
    KeyTermList = TFolder(),
    {async, KFolder} = leveled_bookie:book_returnfolder(Book1, Q(false)),
    KeyList = lists:usort(KFolder()),
    io:format("KeyList ~w KeyTermList ~w~n",
                [length(KeyList), length(KeyTermList)]),
    true = length(KeyList) == 6400,
    true = length(KeyList) < length(KeyTermList),
    true = length(KeyTermList) < 25600,
    ok = leveled_bookie:book_close(Book1),
    testutil:reset_filestructure().


aae_missingjournal(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts = [{root_path, RootPath},
                    {max_journalsize, 20000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    GenList = [2],
    _CLs = testutil:load_objects(20000, GenList, Bookie1, TestObject,
                                fun testutil:generate_objects/2),
    
    FoldHeadsFun =
        fun(B, K, _V, Acc) -> [{B, K}|Acc] end,
    
    {async, AllHeadF1} =
        leveled_bookie:book_headfold(Bookie1,
                                     ?RIAK_TAG,
                                     {FoldHeadsFun, []},
                                     true,
                                     true,
                                     false),
    HeadL1 = length(AllHeadF1()),
    io:format("Fold head returned ~w objects~n", [HeadL1]),
    
    ok = leveled_bookie:book_close(Bookie1),
    CDBFiles = testutil:find_journals(RootPath),
    [HeadF|_Rest] = CDBFiles,
    io:format("Selected Journal for removal of ~s~n", [HeadF]),
    ok = file:delete(RootPath ++ "/journal/journal_files/" ++ HeadF),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts),
    % Check that fold heads picks up on the missing file
    {async, AllHeadF2} =
        leveled_bookie:book_returnfolder(Bookie2,
                                            {foldheads_allkeys,
                                                ?RIAK_TAG,
                                                FoldHeadsFun,
                                                true, true, false,
                                                false, false}),
    HeadL2 = length(AllHeadF2()),
    io:format("Fold head returned ~w objects~n", [HeadL2]),
    true = HeadL2 < HeadL1,
    true = HeadL2 > 0,
    
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().

aae_bustedjournal(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts = [{root_path, RootPath},
                    {max_journalsize, 20000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    GenList = [2],
    _CLs = testutil:load_objects(16000, GenList, Bookie1, TestObject,
                                fun testutil:generate_objects/2),
    ok = leveled_bookie:book_close(Bookie1),

    CDBFiles = testutil:find_journals(RootPath),
    [HeadF|_Rest] = CDBFiles,
    % Select the file to corrupt before completing the load - so as 
    % not to corrupt the journal required on startup 
    {ok, TempB} = leveled_bookie:book_start(StartOpts),
    % Load the remaining objects which may be reloaded on startup due to 
    % non-writing of L0
    _CLsAdd = testutil:load_objects(4000, 
                                        [16002], 
                                        TempB, 
                                        TestObject,
                                        fun testutil:generate_objects/2),
    ok = leveled_bookie:book_close(TempB),

    io:format("Selected Journal for corruption of ~s~n", [HeadF]),
    testutil:corrupt_journal(RootPath, HeadF, 1000, 2048, 1000),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts),
    
    FoldKeysFun = fun(B, K, Acc) -> [{B, K}|Acc] end,
    AllKeyQuery = {keylist, o_rkv, {FoldKeysFun, []}},
    {async, KeyF} = leveled_bookie:book_returnfolder(Bookie2, AllKeyQuery),
    KeyList = KeyF(),
    20001 = length(KeyList),
    HeadCount = lists:foldl(fun({B, K}, Acc) ->
                                    case testutil:book_riakhead(Bookie2,
                                                                        B,
                                                                        K) of
                                        {ok, _} -> Acc + 1;
                                        not_found -> Acc
                                    end
                                    end,
                                0,
                                KeyList),
    20001 = HeadCount,
    GetCount = lists:foldl(fun({B, K}, Acc) ->
                                    case testutil:book_riakget(Bookie2,
                                                                        B,
                                                                        K) of
                                        {ok, _} -> Acc + 1;
                                        not_found -> Acc
                                    end
                                    end,
                                0,
                                KeyList),
    true = GetCount > 19000,
    true = GetCount < HeadCount,
    
    {async, HashTreeF1} = leveled_bookie:book_returnfolder(Bookie2,
                                                            {hashlist_query,
                                                                ?RIAK_TAG,
                                                                false}),
    KeyHashList1 = HashTreeF1(),
    20001 = length(KeyHashList1),
    {async, HashTreeF2} = leveled_bookie:book_returnfolder(Bookie2,
                                                            {hashlist_query,
                                                                ?RIAK_TAG,
                                                                true}),
    KeyHashList2 = HashTreeF2(),
    % The file is still there, and the hashtree is not corrupted
    KeyHashList2 = KeyHashList1,
    % Will need to remove the file or corrupt the hashtree to get presence to
    % fail
    
    FoldObjectsFun = 
        fun(B, K, V, Acc) -> 
            VC = testutil:get_vclock(V),
            H = erlang:phash2(lists:sort(VC)),
            [{B, K, H}|Acc]
        end,
    SW = os:timestamp(),
    {async, HashTreeF3} = leveled_bookie:book_returnfolder(Bookie2,
                                                            {foldobjects_allkeys,
                                                                ?RIAK_TAG,
                                                                FoldObjectsFun,
                                                                false}),
    KeyHashList3 = HashTreeF3(),
    
    true = length(KeyHashList3) > 19000,
    true = length(KeyHashList3) < HeadCount,
    Delta = length(lists:subtract(KeyHashList1, KeyHashList3)),
    true = Delta < 1001,
    io:format("Fetch of hashtree using fold objects took ~w microseconds" ++
                " and found a Delta of ~w and an objects count of ~w~n",
                [timer:now_diff(os:timestamp(), SW),
                    Delta,
                    length(KeyHashList3)]),
    
    ok = leveled_bookie:book_close(Bookie2),
    {ok, BytesCopied} = testutil:restore_file(RootPath, HeadF),
    io:format("File restored is of size ~w~n", [BytesCopied]),
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts),
    
    SW4 = os:timestamp(),
    {async, HashTreeF4} = leveled_bookie:book_returnfolder(Bookie3,
                                                            {foldobjects_allkeys,
                                                                ?RIAK_TAG,
                                                                FoldObjectsFun,
                                                                false}),
    KeyHashList4 = HashTreeF4(),
    
    true = length(KeyHashList4) == 20001,
    io:format("Fetch of hashtree using fold objects took ~w microseconds" ++
                " and found an object count of ~w~n",
                [timer:now_diff(os:timestamp(), SW4), length(KeyHashList4)]),
    
    ok = leveled_bookie:book_close(Bookie3),
    testutil:corrupt_journal(RootPath, HeadF, 500, BytesCopied - 8000, 14),
    
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts),
    
    SW5 = os:timestamp(),
    {async, HashTreeF5} = leveled_bookie:book_returnfolder(Bookie4,
                                                            {foldobjects_allkeys,
                                                                ?RIAK_TAG,
                                                                FoldObjectsFun,
                                                                false}),
    KeyHashList5 = HashTreeF5(),
    
    true = length(KeyHashList5) > 19000,
    true = length(KeyHashList5) < HeadCount,
    Delta5 = length(lists:subtract(KeyHashList1, KeyHashList5)),
    true = Delta5 < 1001,
    io:format("Fetch of hashtree using fold objects took ~w microseconds" ++
                " and found a Delta of ~w and an objects count of ~w~n",
                [timer:now_diff(os:timestamp(), SW5),
                    Delta5,
                    length(KeyHashList5)]),
    
    {async, HashTreeF6} = leveled_bookie:book_returnfolder(Bookie4,
                                                            {hashlist_query,
                                                                ?RIAK_TAG,
                                                                true}),
    KeyHashList6 = HashTreeF6(),
    true = length(KeyHashList6) > 19000,
    true = length(KeyHashList6) < HeadCount,
     
    ok = leveled_bookie:book_close(Bookie4),
    
    testutil:restore_topending(RootPath, HeadF),
    
    {ok, Bookie5} = leveled_bookie:book_start(StartOpts),
    
    SW6 = os:timestamp(),
    {async, HashTreeF7} = leveled_bookie:book_returnfolder(Bookie5,
                                                            {foldobjects_allkeys,
                                                                ?RIAK_TAG,
                                                                FoldObjectsFun,
                                                                false}),
    KeyHashList7 = HashTreeF7(),
    
    true = length(KeyHashList7) == 20001,
    io:format("Fetch of hashtree using fold objects took ~w microseconds" ++
                " and found an object count of ~w~n",
                [timer:now_diff(os:timestamp(), SW6), length(KeyHashList7)]),
    
    ok = leveled_bookie:book_close(Bookie5),
    testutil:reset_filestructure().


journal_compaction_bustedjournal(_Config) ->
    % Different circumstances will be created in different runs
    busted_journal_test(10000000, native, on_receipt, true),
    busted_journal_test(7777777, lz4, on_compact, true),
    busted_journal_test(8888888, lz4, on_receipt, true),
    busted_journal_test(7777777, lz4, on_compact, false).
    

busted_journal_test(MaxJournalSize, PressMethod, PressPoint, Bust) ->
    % Simply confirms that none of this causes a crash
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, MaxJournalSize},
                    {max_run_length, 10},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_method, PressMethod},
                    {compression_point, PressPoint}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ObjList1 = testutil:generate_objects(50000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        testutil:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    %% Now replace all the objects
    ObjList2 = testutil:generate_objects(50000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        testutil:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList2),
    ok = leveled_bookie:book_close(Bookie1),
    
    case Bust of
        true ->
            CDBFiles = testutil:find_journals(RootPath),
            lists:foreach(fun(FN) ->
                                testutil:corrupt_journal(RootPath, 
                                                            FN, 
                                                            100, 2048, 1000)
                            end,
                            CDBFiles);
        false ->
            ok 
    end,
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    
    ok = leveled_bookie:book_compactjournal(Bookie2, 30000),
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(20000),
                                F(Bookie2)
                        end end,
                    true,
                    lists:seq(1, 15)),
    
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure(10000).


allkeydelta_journal_multicompact(_Config) ->
    RootPath = testutil:reset_filestructure(),
    B = <<"test_bucket">>,
    StartOptsFun = 
        fun(JOC) ->
            [{root_path, RootPath},
                {max_journalobjectcount, JOC},
                {max_run_length, 4},
                {singlefile_compactionpercentage, 70.0},
                {maxrunlength_compactionpercentage, 85.0},
                {sync_strategy, testutil:sync_strategy()}]
        end,
    {ok, Bookie1} = leveled_bookie:book_start(StartOptsFun(14000)),
    {KSpcL1, _V1} = testutil:put_indexed_objects(Bookie1, B, 24000),
    {KSpcL2, V2} = testutil:put_altered_indexed_objects(Bookie1,
                                                        B,
                                                        KSpcL1,
                                                        false),
    compact_and_wait(Bookie1, 0),
    compact_and_wait(Bookie1, 0),
    {ok, FileList1} = 
        file:list_dir(
            filename:join(RootPath, "journal/journal_files/post_compact")),
    io:format("Number of files after compaction ~w~n", [length(FileList1)]),
    compact_and_wait(Bookie1, 0),
    {ok, FileList2} = 
        file:list_dir(
            filename:join(RootPath, "journal/journal_files/post_compact")),
    io:format("Number of files after compaction ~w~n", [length(FileList2)]),
    true = FileList1 == FileList2,

    ok = testutil:check_indexed_objects(Bookie1,
                                        B,
                                        KSpcL1 ++ KSpcL2,
                                        V2),

    ok = leveled_bookie:book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    io:format("Restart without ledger~n"),
    {ok, Bookie2} = leveled_bookie:book_start(StartOptsFun(13000)),

    ok = testutil:check_indexed_objects(Bookie2,
                                        B,
                                        KSpcL1 ++ KSpcL2,
                                        V2),
    
    {KSpcL3, _V3} = testutil:put_altered_indexed_objects(Bookie2,
                                                        B,
                                                        KSpcL2,
                                                        false),
    compact_and_wait(Bookie2, 0),
    {ok, FileList3} = 
        file:list_dir(
            filename:join(RootPath, "journal/journal_files/post_compact")),
    io:format("Number of files after compaction ~w~n", [length(FileList3)]),

    ok = leveled_bookie:book_close(Bookie2),

    io:format("Restart with smaller journal object count~n"),
    {ok, Bookie3} = leveled_bookie:book_start(StartOptsFun(7000)),

    {KSpcL4, V4} = testutil:put_altered_indexed_objects(Bookie3,
                                                        B,
                                                        KSpcL3,
                                                        false),
    
    compact_and_wait(Bookie3, 0),
    
    ok = testutil:check_indexed_objects(Bookie3,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4,
                                        V4),
    {ok, FileList4} = 
        file:list_dir(
            filename:join(RootPath, "journal/journal_files/post_compact")),
    io:format("Number of files after compaction ~w~n", [length(FileList4)]),
    true = length(FileList4) >= length(FileList3) + 3,

    ok = leveled_bookie:book_close(Bookie3),
    testutil:reset_filestructure(10000).

recompact_keydeltas(_Config) ->
    RootPath = testutil:reset_filestructure(),
    B = <<"test_bucket">>,
    StartOptsFun = 
        fun(JOC) ->
            [{root_path, RootPath},
                {max_journalobjectcount, JOC},
                {max_run_length, 4},
                {singlefile_compactionpercentage, 70.0},
                {maxrunlength_compactionpercentage, 85.0},
                {sync_strategy, testutil:sync_strategy()}]
        end,
    {ok, Bookie1} = leveled_bookie:book_start(StartOptsFun(45000)),
    {KSpcL1, _V1} = testutil:put_indexed_objects(Bookie1, B, 24000),
    {KSpcL2, _V2} = testutil:put_altered_indexed_objects(Bookie1,
                                                        B,
                                                        KSpcL1,
                                                        false),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOptsFun(45000)),                 
    compact_and_wait(Bookie2, 0),
    {KSpcL3, V3} = testutil:put_altered_indexed_objects(Bookie2,
                                                        B,
                                                        KSpcL2,
                                                        false),
    compact_and_wait(Bookie2, 0),
    ok = testutil:check_indexed_objects(Bookie2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3,
                                        V3),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure(10000).



rotating_object_check(BookOpts, B, NumberOfObjects) ->
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    {KSpcL1, V1} = testutil:put_indexed_objects(Book1, B, NumberOfObjects),
    ok = testutil:check_indexed_objects(Book1,
                                        B,
                                        KSpcL1,
                                        V1),
    {KSpcL2, V2} = testutil:put_altered_indexed_objects(Book1,
                                                        B,
                                                        KSpcL1,
                                                        false),
    ok = testutil:check_indexed_objects(Book1,
                                        B,
                                        KSpcL1 ++ KSpcL2,
                                        V2),
    {KSpcL3, V3} = testutil:put_altered_indexed_objects(Book1,
                                                        B,
                                                        KSpcL2,
                                                        false),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3,
                                        V3),
    {KSpcL4, V4} = testutil:put_altered_indexed_objects(Book2,
                                                        B,
                                                        KSpcL3,
                                                        false),
    io:format("Bucket complete - checking index before compaction~n"),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4,
                                        V4),
    
    compact_and_wait(Book2),
    
    io:format("Checking index following compaction~n"),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4,
                                        V4),
    
    ok = leveled_bookie:book_close(Book2),
    {ok, KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4, V4}.

compact_and_wait(Book) ->
    compact_and_wait(Book, 20000).

compact_and_wait(Book, WaitForDelete) ->
    ok = leveled_bookie:book_compactjournal(Book, 30000),
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(20000),
                                F(Book)
                        end end,
                    true,
                    lists:seq(1, 15)),
    io:format("Waiting for journal deletes~n"),
    timer:sleep(WaitForDelete).

restart_from_blankledger(BookOpts, B_SpcL) ->
    leveled_penciller:clean_testdir(proplists:get_value(root_path, BookOpts) ++
                                    "/ledger"),
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    io:format("Checking index following restart~n"),
    lists:foreach(fun({B, SpcL, V}) ->
                        ok = testutil:check_indexed_objects(Book1, B, SpcL, V)
                        end,
                    B_SpcL),
    ok = leveled_bookie:book_close(Book1),
    ok.
