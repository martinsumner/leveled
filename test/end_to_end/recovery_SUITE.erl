-module(recovery_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([retain_strategy/1,
            recovr_strategy/1,
            aae_missingjournal/1,
            aae_bustedjournal/1,
            journal_compaction_bustedjournal/1
            ]).

all() -> [
            retain_strategy,
            recovr_strategy,
            aae_missingjournal,
            aae_bustedjournal,
            journal_compaction_bustedjournal
            ].

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
                                    TestObject#r_object.bucket,
                                    TestObject#r_object.key,
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
        leveled_bookie:book_returnfolder(Bookie1,
                                            {foldheads_allkeys,
                                                ?RIAK_TAG,
                                                FoldHeadsFun}),
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
                                                FoldHeadsFun}),
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
    _CLs = testutil:load_objects(20000, GenList, Bookie1, TestObject,
                                fun testutil:generate_objects/2),
    ok = leveled_bookie:book_close(Bookie1),
    CDBFiles = testutil:find_journals(RootPath),
    [HeadF|_Rest] = CDBFiles,
    io:format("Selected Journal for corruption of ~s~n", [HeadF]),
    testutil:corrupt_journal(RootPath, HeadF, 1000, 2048, 1000),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts),
    
    FoldKeysFun = fun(B, K, Acc) -> Acc ++ [{B, K}] end,
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
                                                                check_presence}),
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
                                                                FoldObjectsFun}),
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
                                                                FoldObjectsFun}),
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
                                                                FoldObjectsFun}),
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
                                                                check_presence}),
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
                                                                FoldObjectsFun}),
    KeyHashList7 = HashTreeF7(),
    
    true = length(KeyHashList7) == 20001,
    io:format("Fetch of hashtree using fold objects took ~w microseconds" ++
                " and found an object count of ~w~n",
                [timer:now_diff(os:timestamp(), SW6), length(KeyHashList7)]),
    
    ok = leveled_bookie:book_close(Bookie5),
    testutil:reset_filestructure().


journal_compaction_bustedjournal(_Config) ->
    % Different circumstances will be created in different runs
    busted_journal_test(10000000),
    busted_journal_test(7777777).
    

busted_journal_test(MaxJournalSize) ->
    % Simply confirms that none of this causes a crash
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, MaxJournalSize},
                    {max_run_length, 10},
                    {sync_strategy, testutil:sync_strategy()}],
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
    
    CDBFiles = testutil:find_journals(RootPath),
    lists:foreach(fun(FN) ->
                        testutil:corrupt_journal(RootPath, FN, 100, 2048, 1000)
                        end,
                    CDBFiles),
    
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
    
    ok = leveled_bookie:book_compactjournal(Book2, 30000),
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(20000),
                                F(Book2)
                        end end,
                    true,
                    lists:seq(1, 15)),
    io:format("Waiting for journal deletes~n"),
    timer:sleep(20000),
    
    io:format("Checking index following compaction~n"),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4,
                                        V4),
    
    ok = leveled_bookie:book_close(Book2),
    {ok, KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4, V4}.
    
    
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
