-module(recovery_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            recovery_with_samekeyupdates/1,
            same_key_rotation_withindexes/1,
            hot_backup_changes/1,
            retain_strategy/1,
            recalc_strategy/1,
            recalc_transition_strategy/1,
            recovr_strategy/1,
            stdtag_recalc/1,
            aae_missingjournal/1,
            aae_bustedjournal/1,
            journal_compaction_bustedjournal/1,
            close_duringcompaction/1,
            recompact_keydeltas/1,
            simple_cachescoring/1,
            replace_everything/1
            ]).

all() -> [
            recovery_with_samekeyupdates,
            same_key_rotation_withindexes,
            hot_backup_changes,
            retain_strategy,
            recalc_strategy,
            recalc_transition_strategy,
            recovr_strategy,
            aae_missingjournal,
            aae_bustedjournal,
            journal_compaction_bustedjournal,
            close_duringcompaction,
            recompact_keydeltas,
            stdtag_recalc,
            simple_cachescoring,
            replace_everything
            ].


replace_everything(_Config) ->
    % See https://github.com/martinsumner/leveled/issues/389
    % Also replaces previous test which was checking the comapction process
    % respects the journal object count passed at startup
    RootPath = testutil:reset_filestructure(),
    BackupPath = testutil:reset_filestructure("backupRE"),
    CompPath = filename:join(RootPath, "journal/journal_files/post_compact"),
    SmallJournalCount = 7000,
    StdJournalCount = 20000,
    BookOpts = 
        fun(JournalObjectCount) ->
            [{root_path, RootPath},
            {cache_size, 2000},
            {max_journalobjectcount, JournalObjectCount},
            {sync_strategy, testutil:sync_strategy()},
            {reload_strategy, [{?RIAK_TAG, recalc}]}]
        end,
    {ok, Book1} = leveled_bookie:book_start(BookOpts(StdJournalCount)),
    BKT = "ReplaceAll",
    BKT1 = "ReplaceAll1",
    BKT2 = "ReplaceAll2",
    BKT3 = "ReplaceAll3",
    {KSpcL1, V1} =
        testutil:put_indexed_objects(Book1, BKT, 50000),
    ok = testutil:check_indexed_objects(Book1, BKT, KSpcL1, V1),
    {KSpcL2, V2} = 
        testutil:put_altered_indexed_objects(Book1, BKT, KSpcL1),
    ok = testutil:check_indexed_objects(Book1, BKT, KSpcL2, V2),
    compact_and_wait(Book1, 1000),
    compact_and_wait(Book1, 1000),
    {ok, FileList1} =  file:list_dir(CompPath),
    io:format("Number of files after compaction ~w~n", [length(FileList1)]),
    compact_and_wait(Book1, 1000),
    {ok, FileList2} =  file:list_dir(CompPath),
    io:format("Number of files after compaction ~w~n", [length(FileList2)]),
    true = FileList1 =< FileList2,
        %% There will normally be 5 journal files after 50K write then alter
        %% That may be two files with entirely altered objects - which will be
        %% compacted, and will be compacted to nothing.
        %% The "middle" file - which will be 50% compactable may be scored to
        %% be part of the first run, or may end up in the second run.  If in
        %% the first run, the second run will not compact and FL1 == FL2.
        %% Otherwise FL1 could be 0 and FL2 1.  Hard to control this as there
        %% is randomisation in both the scoring and the journal size (due to
        %% jittering of parameters).
    compact_and_wait(Book1, 1000),
    {ok, FileList3} =  file:list_dir(CompPath),
    io:format("Number of files after compaction ~w~n", [length(FileList3)]),
        %% By the third compaction there should be no further changes
    true = FileList2 == FileList3,
    {async, BackupFun} = leveled_bookie:book_hotbackup(Book1),
    ok = BackupFun(BackupPath),

    io:format("Restarting without key store~n"),
    ok = leveled_bookie:book_close(Book1),

    BookOptsBackup = [{root_path, BackupPath},
                        {cache_size, 2000},
                        {sync_strategy, testutil:sync_strategy()}],
    SW1 = os:timestamp(),
    {ok, Book2} = leveled_bookie:book_start(BookOptsBackup),
    
    io:format(
        "Opened backup with no ledger in ~w ms~n",
        [timer:now_diff(os:timestamp(), SW1) div 1000]),
    ok = testutil:check_indexed_objects(Book2, BKT, KSpcL2, V2),
    ok = leveled_bookie:book_close(Book2),
    
    SW2 = os:timestamp(),
    {ok, Book3} = leveled_bookie:book_start(BookOptsBackup),
    io:format(
        "Opened backup with ledger in ~w ms~n",
        [timer:now_diff(os:timestamp(), SW2) div 1000]),
    ok = testutil:check_indexed_objects(Book3, BKT, KSpcL2, V2),
    ok = leveled_bookie:book_destroy(Book3),

    {ok, Book4} = leveled_bookie:book_start(BookOpts(StdJournalCount)),
    {KSpcL3, V3} = testutil:put_indexed_objects(Book4, BKT1, 1000),
    {KSpcL4, _V4} = testutil:put_indexed_objects(Book4, BKT2, 50000),
    {KSpcL5, V5} = 
        testutil:put_altered_indexed_objects(Book4, BKT2, KSpcL4),
    compact_and_wait(Book4),
    {async, BackupFun4} = leveled_bookie:book_hotbackup(Book4),
    ok = BackupFun4(BackupPath),
    ok = leveled_bookie:book_close(Book4),

    io:format("Restarting without key store~n"),
    SW5 = os:timestamp(),
    {ok, Book5} = leveled_bookie:book_start(BookOptsBackup),    
    io:format(
        "Opened backup with no ledger in ~w ms~n",
        [timer:now_diff(os:timestamp(), SW5) div 1000]),
    ok = testutil:check_indexed_objects(Book5, BKT, KSpcL2, V2),
    ok = testutil:check_indexed_objects(Book5, BKT1, KSpcL3, V3),
    ok = testutil:check_indexed_objects(Book5, BKT2, KSpcL5, V5),
    ok = leveled_bookie:book_destroy(Book5),

    io:format("Testing with sparse distribution after update~n"),
    io:format(
        "Also use smaller Journal files and confirm value used "
        "in compaction~n"),
    {ok, Book6} = leveled_bookie:book_start(BookOpts(SmallJournalCount)),
    {KSpcL6, _V6} = testutil:put_indexed_objects(Book6, BKT3, 60000),
    {OSpcL6, RSpcL6} = lists:split(200, lists:ukeysort(1, KSpcL6)),
    {KSpcL7, V7} = 
        testutil:put_altered_indexed_objects(Book6, BKT3, RSpcL6),
    {ok, FileList4} =  file:list_dir(CompPath),
    compact_and_wait(Book6),
    {ok, FileList5} =  file:list_dir(CompPath),
    {OSpcL6A, V7} = 
        testutil:put_altered_indexed_objects(Book6, BKT3, OSpcL6, true, V7),
    {async, BackupFun6} = leveled_bookie:book_hotbackup(Book6),
    ok = BackupFun6(BackupPath),
    ok = leveled_bookie:book_close(Book6),

    io:format("Checking object count in newly compacted journal files~n"),
    NewlyCompactedFiles = lists:subtract(FileList5, FileList4),
    true = length(NewlyCompactedFiles) >= 1,
    CDBFilterFun = fun(_K, _V, _P, Acc, _EF) -> {loop, Acc + 1} end,
    CheckLengthFun =
        fun(FN) ->
            {ok, CF} =
                leveled_cdb:cdb_open_reader(filename:join(CompPath, FN)),
            {_LP, TK} =
                leveled_cdb:cdb_scan(CF, CDBFilterFun, 0, undefined),
            io:format("File ~s has ~w keys~n", [FN, TK]),
            true = TK =< SmallJournalCount
        end,
    lists:foreach(CheckLengthFun, NewlyCompactedFiles),

    io:format("Restarting without key store~n"),
    SW7 = os:timestamp(),
    {ok, Book7} = leveled_bookie:book_start(BookOptsBackup),    
    io:format(
        "Opened backup with no ledger in ~w ms~n",
        [timer:now_diff(os:timestamp(), SW7) div 1000]),
    ok = testutil:check_indexed_objects(Book7, BKT3, KSpcL7 ++ OSpcL6A, V7),
    ok = leveled_bookie:book_destroy(Book7),

    testutil:reset_filestructure(BackupPath),
    testutil:reset_filestructure().


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

same_key_rotation_withindexes(_Config) ->
    % If we have the same key - but the indexes change.  Do we consistently
    % recalc the indexes correctly, even when the key exists multiple times
    % in the loader's mock ledger cache
    RootPath = testutil:reset_filestructure(),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 2000},
                    {max_journalsize, 20000000},
                    {reload_strategy, [{?RIAK_TAG, recalc}]},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    IndexGenFun =
        fun(ID) ->
            fun() ->
                [{add, list_to_binary("binary_bin"), <<ID:32/integer>>}]
            end
        end,
    
    Bucket = <<"TestBucket">>,

    ObjectGenFun =
        fun(KeyID, IndexID) ->
            Key = list_to_binary("Key" ++ integer_to_list(KeyID)),
            Value = <<IndexID:32/integer>>,
            GenRemoveFun = IndexGenFun(IndexID - 1),
            testutil:set_object(Bucket,
                                Key,
                                Value,
                                IndexGenFun(IndexID),
                                GenRemoveFun())
        end,
    
    IdxCnt = 8,
    KeyCnt = 50,

    Sequence =
        lists:map(fun(K) -> lists:map(fun(I) -> {K, I} end, lists:seq(1, IdxCnt)) end,
                    lists:seq(1, KeyCnt)),
    ObjList =
        lists:map(fun({K, I}) -> ObjectGenFun(K, I) end, lists:flatten(Sequence)),

    lists:foreach(
            fun({Obj, SpcL}) -> testutil:book_riakput(Book1, Obj, SpcL) end,
            ObjList),

    FoldKeysFun = fun(_B, K, Acc) -> [K|Acc] end,
    CheckFun =
        fun(Bookie) ->
            {async, R} =
                leveled_bookie:book_indexfold(
                    Bookie,
                    {Bucket, <<>>},
                    {FoldKeysFun, []},
                    {list_to_binary("binary_bin"),
                        <<0:32/integer>>,
                        <<255:32/integer>>},
                    {true, undefined}),
            QR = R(),
            BadAnswers =
                lists:filter(fun({I, _K}) -> I =/= <<IdxCnt:32/integer>> end, QR),
            io:format("Results ~w BadAnswers ~w~n",
                        [length(QR), length(BadAnswers)]),
            true = length(QR) == KeyCnt,
            true = [] == BadAnswers
        end,

    CheckFun(Book1),
    ok = leveled_bookie:book_close(Book1),

    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    CheckFun(Book2),
    ok = leveled_bookie:book_close(Book2),

    testutil:reset_filestructure().


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

    ok = leveled_bookie:book_close(BookBackup),

    testutil:reset_filestructure("backup0"),
    testutil:reset_filestructure().


retain_strategy(_Config) ->
    rotate_wipe_compact(retain, retain).

recalc_strategy(_Config) ->
    rotate_wipe_compact(recalc, recalc).

recalc_transition_strategy(_Config) ->
    rotate_wipe_compact(retain, recalc).


rotate_wipe_compact(Strategy1, Strategy2) ->
    RootPath = testutil:reset_filestructure(),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalobjectcount, 5000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?RIAK_TAG, Strategy1}]}],
    BookOptsAlt = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalobjectcount, 2000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?RIAK_TAG, Strategy2}]},
                    {max_run_length, 8}],
    {ok, Spcl3, LastV3} = rotating_object_check(BookOpts, "Bucket3", 400),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3}]),
    {ok, Spcl4, LastV4} = rotating_object_check(BookOpts, "Bucket4", 800),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4}]),
    {ok, Spcl5, LastV5} = rotating_object_check(BookOpts, "Bucket5", 1600),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket5", Spcl5, LastV5}]),
    {ok, Spcl6, LastV6} = rotating_object_check(BookOpts, "Bucket6", 3200),

    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    compact_and_wait(Book1),
    ok = leveled_bookie:book_close(Book1),

    ok = restart_from_blankledger(BookOptsAlt, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4},
                                                {"Bucket5", Spcl5, LastV5},
                                                {"Bucket6", Spcl6, LastV6}]),

    {ok, Book2} = leveled_bookie:book_start(BookOptsAlt),
    compact_and_wait(Book2),
    ok = leveled_bookie:book_close(Book2),

    ok = restart_from_blankledger(BookOptsAlt, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4},
                                                {"Bucket5", Spcl5, LastV5},
                                                {"Bucket6", Spcl6, LastV6}]),

    {ok, Book3} = leveled_bookie:book_start(BookOptsAlt),

    {KSpcL2, _V2} = testutil:put_indexed_objects(Book3, "AltBucket6", 3000),
    Q2 =
        fun(RT) -> 
            {index_query,
                "AltBucket6",
                {fun testutil:foldkeysfun/3, []},
                {<<"idx1_bin">>, <<"#">>, <<"|">>},
                {RT, undefined}}
        end,
    {async, KFolder2A} = leveled_bookie:book_returnfolder(Book3, Q2(false)),
    KeyList2A = lists:usort(KFolder2A()),
    true = length(KeyList2A) == 3000,

    DeleteFun =
        fun({DK, [{add, DIdx, DTerm}]}) ->
            ok = testutil:book_riakdelete(Book3,
                                            "AltBucket6",
                                            DK,
                                            [{remove, DIdx, DTerm}])
        end,
    lists:foreach(DeleteFun, KSpcL2),

    {async, KFolder3AD} = leveled_bookie:book_returnfolder(Book3, Q2(false)),
    KeyList3AD = lists:usort(KFolder3AD()),
    true = length(KeyList3AD) == 0,

    ok = leveled_bookie:book_close(Book3),

    {ok, Book4} = leveled_bookie:book_start(BookOptsAlt),

    io:format("Compact after deletions~n"),

    compact_and_wait(Book4),
    
    {async, KFolder4AD} = leveled_bookie:book_returnfolder(Book4, Q2(false)),
    KeyList4AD = lists:usort(KFolder4AD()),
    true = length(KeyList4AD) == 0,

    ok = leveled_bookie:book_close(Book4),

    testutil:reset_filestructure().


stdtag_recalc(_Config) ->
    %% Setting the ?STD_TAG to do recalc, should result in the ?STD_TAG
    %% behaving like recovr - as no recalc is done for ?STD_TAG

    %% NOTE -This is a test to confirm bad things happen!

    RootPath = testutil:reset_filestructure(),
    B0 = <<"B0">>,
    KeyCount = 7000,
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalobjectcount, 5000},
                    {max_pencillercachesize, 10000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?STD_TAG, recalc}]}],
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    LoadFun =
        fun(Book) ->
            fun(I) ->
                testutil:stdload_object(Book,
                                        B0, erlang:phash2(I rem KeyCount),
                                        I, erlang:phash2({value, I}),
                                        infinity, ?STD_TAG, false, false)
            end
        end,
    lists:foreach(LoadFun(Book1), lists:seq(1, KeyCount)),
    lists:foreach(LoadFun(Book1), lists:seq(KeyCount + 1, KeyCount * 2)),

    CountFold =
        fun(Book, CurrentCount) ->
            leveled_bookie:book_indexfold(Book,
                                            B0,
                                            {fun(_BF, _KT, Acc) -> Acc + 1 end,
                                                0},
                                            {<<"temp_int">>, 0, CurrentCount},
                                            {true, undefined})
        end,

    {async, FolderA} = CountFold(Book1, 2 * KeyCount),
    CountA = FolderA(),
    io:format("Counted double index entries ~w - everything loaded OK~n",
                [CountA]),
    true = 2 * KeyCount == CountA,

    ok = leveled_bookie:book_close(Book1),

    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    lists:foreach(LoadFun(Book2), lists:seq(KeyCount * 2 + 1, KeyCount * 3)),

    {async, FolderB} = CountFold(Book2, 3 * KeyCount),
    CountB = FolderB(),
    io:format("Maybe counted less index entries ~w - everything not loaded~n",
                [CountB]),
    true = 3 * KeyCount >= CountB,

    compact_and_wait(Book2),
    ok = leveled_bookie:book_close(Book2),

    io:format("Restart from blank ledger"),

    leveled_penciller:clean_testdir(proplists:get_value(root_path, BookOpts) ++
                                    "/ledger"),
    {ok, Book3} = leveled_bookie:book_start(BookOpts),

    {async, FolderC} = CountFold(Book3, 3 * KeyCount),
    CountC = FolderC(),
    io:format("Missing index entries ~w - recalc not supported on ?STD_TAG~n",
                [CountC]),
    true = 3 * KeyCount > CountC,

    ok = leveled_bookie:book_close(Book3),
    
    testutil:reset_filestructure().


recovr_strategy(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalobjectcount, 8000},
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
    Q =
        fun(RT) ->
            {index_query,
                "Bucket6",
                {fun testutil:foldkeysfun/3, []},
                {<<"idx1_bin">>, <<"#">>, <<"|">>},
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

    RevisedOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalobjectcount, 2000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{?RIAK_TAG, recovr}]}],

    {ok, Book2} = leveled_bookie:book_start(RevisedOpts),

    {KSpcL2, _V2} = testutil:put_indexed_objects(Book2, "AltBucket6", 3000),
    {async, KFolder2} = leveled_bookie:book_returnfolder(Book2, Q(false)),
    KeyList2 = lists:usort(KFolder2()),
    true = length(KeyList2) == 6400,

    Q2 =
        fun(RT) ->
            {index_query,
                "AltBucket6",
                {fun testutil:foldkeysfun/3, []},
                {<<"idx1_bin">>, <<"#">>, <<"|">>},
                {RT, undefined}}
        end,
    {async, KFolder2A} = leveled_bookie:book_returnfolder(Book2, Q2(false)),
    KeyList2A = lists:usort(KFolder2A()),
    true = length(KeyList2A) == 3000,

    DeleteFun =
        fun({DK, [{add, DIdx, DTerm}]}) ->
            ok = testutil:book_riakdelete(Book2,
                                            "AltBucket6",
                                            DK,
                                            [{remove, DIdx, DTerm}])
        end,
    lists:foreach(DeleteFun, KSpcL2),

    {async, KFolder2AD} = leveled_bookie:book_returnfolder(Book2, Q2(false)),
    KeyList2AD = lists:usort(KFolder2AD()),
    true = length(KeyList2AD) == 0,

    compact_and_wait(Book2),
    compact_and_wait(Book2),

    ok = leveled_bookie:book_close(Book2),

    {ok, Book3} = leveled_bookie:book_start(RevisedOpts),
    {async, KFolder3AD} = leveled_bookie:book_returnfolder(Book3, Q2(false)),
    KeyList3AD = lists:usort(KFolder3AD()),
    true = length(KeyList3AD) == 0,

    ok = leveled_bookie:book_close(Book3),

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

simple_cachescoring(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts = [{root_path, RootPath},
                    {max_journalobjectcount, 2000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} =
        leveled_bookie:book_start(StartOpts ++
                                    [{journalcompaction_scoreonein, 8}]),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    GenList = [2, 32002, 64002, 96002],
    _CLs = testutil:load_objects(32000, GenList, Bookie1, TestObject,
                                fun testutil:generate_objects/2),
    
    F = fun leveled_bookie:book_islastcompactionpending/1,
    WaitForCompaction =
        fun(B) -> 
            fun(X, Pending) ->
                case X of 
                    1 ->
                        leveled_bookie:book_compactjournal(B, 30000);
                    _ ->
                        ok
                end,
                case Pending of
                    false ->
                        false;
                    true ->
                        io:format("Loop ~w waiting for journal "
                            ++ "compaction to complete~n", [X]),
                        timer:sleep(100),
                        F(B)
                end
            end
        end,
    io:format("Scoring for first time - every file should need scoring~n"),
    Args1 = [WaitForCompaction(Bookie1), true, lists:seq(1, 300)],
    {TC0, false} = timer:tc(lists, foldl, Args1),
    io:format("Score four more times with cached scoring~n"),
    {TC1, false} = timer:tc(lists, foldl, Args1),
    {TC2, false} = timer:tc(lists, foldl, Args1),
    {TC3, false} = timer:tc(lists, foldl, Args1),
    {TC4, false} = timer:tc(lists, foldl, Args1),
    
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} =
        leveled_bookie:book_start(StartOpts),
    io:format("Re-opened bookie withour caching - re-compare compaction time~n"),
    io:format("Scoring for first time - every file should need scoring~n"),
    Args2 = [WaitForCompaction(Bookie2), true, lists:seq(1, 300)],
    {TN0, false} = timer:tc(lists, foldl, Args2),
    io:format("Score four more times with cached scoring~n"),
    {TN1, false} = timer:tc(lists, foldl, Args2),
    {TN2, false} = timer:tc(lists, foldl, Args2),
    {TN3, false} = timer:tc(lists, foldl, Args2),
    {TN4, false} = timer:tc(lists, foldl, Args2),
    
    AvgSecondRunCache = (TC1 + TC2 +TC3 + TC4) div 4000,
    AvgSecondRunNoCache = (TN1 + TN2 +TN3 + TN4) div 4000,

    io:format("With caching ~w first run ~w average other runs~n",
                [TC0 div 1000, AvgSecondRunCache]),
    io:format("Without caching ~w first run ~w average other runs~n",
                [TN0 div 1000, AvgSecondRunNoCache]),
    true = (TC0 > AvgSecondRunCache),
    true = (TC0/AvgSecondRunCache) > (TN0/AvgSecondRunNoCache),
    ok = leveled_bookie:book_close(Bookie2),

    io:format("Exit having proven simply that caching score is faster~n"),
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
    {KSpcL2, _V2} =
        testutil:put_altered_indexed_objects(Bookie1, B, KSpcL1, false),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOptsFun(45000)),                 
    compact_and_wait(Bookie2, 0),
    {KSpcL3, V3} =
        testutil:put_altered_indexed_objects(Bookie2, B, KSpcL2, false),
    compact_and_wait(Bookie2, 0),
    ok =
        testutil:check_indexed_objects(
            Bookie2, B, KSpcL1 ++ KSpcL2 ++ KSpcL3, V3),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure(10000).

rotating_object_check(BookOpts, B, NumberOfObjects) ->
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    {KSpcL1, V1} = testutil:put_indexed_objects(Book1, B, NumberOfObjects),
    ok = testutil:check_indexed_objects(Book1, B, KSpcL1, V1),
    {KSpcL2, V2} =
        testutil:put_altered_indexed_objects(Book1, B, KSpcL1, false),
    ok = 
        testutil:check_indexed_objects(
            Book1, B, KSpcL1 ++ KSpcL2, V2),
    {KSpcL3, V3} =
        testutil:put_altered_indexed_objects(Book1, B, KSpcL2, false),
    ok = 
        testutil:check_indexed_objects(
            Book1, B, KSpcL1 ++ KSpcL2 ++ KSpcL3, V3),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    ok = 
        testutil:check_indexed_objects(
            Book2, B, KSpcL1 ++ KSpcL2 ++ KSpcL3, V3),
    {KSpcL4, V4} =
        testutil:put_altered_indexed_objects(Book2, B, KSpcL3, false),
    io:format("Bucket complete - checking index before compaction~n"),
    ok = 
        testutil:check_indexed_objects(
            Book2, B, KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4, V4),
    
    compact_and_wait(Book2),
    
    io:format("Checking index following compaction~n"),
    ok =
        testutil:check_indexed_objects(
            Book2, B, KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4, V4),
    
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
