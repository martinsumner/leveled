-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([simple_put_fetch_head_delete/1,
            many_put_fetch_head/1,
            journal_compaction/1,
            fetchput_snapshot/1,
            load_and_count/1,
            load_and_count_withdelete/1
            ]).

all() -> [simple_put_fetch_head_delete,
            many_put_fetch_head,
            journal_compaction,
            fetchput_snapshot,
            load_and_count,
            load_and_count_withdelete
            ].


simple_put_fetch_head_delete(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_formissingobject(Bookie1, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = #bookie_options{root_path=RootPath,
                                 max_journalsize=3000000},
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forobject(Bookie2, TestObject),
    ObjList1 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    testutil:check_forlist(Bookie2, ChkList1),
    testutil:check_forobject(Bookie2, TestObject),
    testutil:check_formissingobject(Bookie2, "Bucket1", "Key2"),
    ok = leveled_bookie:book_put(Bookie2, "Bucket1", "Key2", "Value2",
                                    [{add, "Index1", "Term1"}]),
    {ok, "Value2"} = leveled_bookie:book_get(Bookie2, "Bucket1", "Key2"),
    {ok, {62888926, 43}} = leveled_bookie:book_head(Bookie2,
                                                    "Bucket1",
                                                    "Key2"),
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
    ok = leveled_bookie:book_close(Bookie3),
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts2),
    not_found = leveled_bookie:book_get(Bookie4, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie4),
    testutil:reset_filestructure().

many_put_fetch_head(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = #bookie_options{root_path=RootPath,
                                 max_journalsize=1000000000},
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forobject(Bookie2, TestObject),
    GenList = [2, 20002, 40002, 60002, 80002,
                100002, 120002, 140002, 160002, 180002],
    CLs = testutil:load_objects(20000, GenList, Bookie2, TestObject,
                                fun testutil:generate_smallobjects/2),
    CL1A = lists:nth(1, CLs),
    ChkListFixed = lists:nth(length(CLs), CLs),
    testutil:check_forlist(Bookie2, CL1A),
    ObjList2A = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList2A),
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
    ok = leveled_bookie:book_close(Bookie3),
    testutil:reset_filestructure().

journal_compaction(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath,
                                 max_journalsize=4000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ObjList1 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 1000),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    {B2, K2, V2, Spec2, MD} = {"Bucket1",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    {TestObject2, TestSpec2} = testutil:generate_testobject(B2, K2,
                                                            V2, Spec2, MD),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject2, TestSpec2),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_forobject(Bookie1, TestObject2),
    timer:sleep(5000), % Allow for compaction to complete
    io:format("Has journal completed?~n"),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_forobject(Bookie1, TestObject2),
    %% Now replace all the objects
    ObjList2 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList2),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    ChkList3 = lists:sublist(lists:sort(ObjList2), 500),
    testutil:check_forlist(Bookie1, ChkList3),
    ok = leveled_bookie:book_close(Bookie1),
    % Restart
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    testutil:check_forobject(Bookie2, TestObject),
    testutil:check_forlist(Bookie2, ChkList3),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().


fetchput_snapshot(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath, max_journalsize=3000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    ObjList1 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    SnapOpts1 = #bookie_options{snapshot_bookie=Bookie1},
    {ok, SnapBookie1} = leveled_bookie:book_start(SnapOpts1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forlist(SnapBookie1, ChkList1),
    ok = leveled_bookie:book_close(SnapBookie1),
    testutil:check_forlist(Bookie1, ChkList1),
    ok = leveled_bookie:book_close(Bookie1),
    io:format("Closed initial bookies~n"),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    SnapOpts2 = #bookie_options{snapshot_bookie=Bookie2},
    {ok, SnapBookie2} = leveled_bookie:book_start(SnapOpts2),
    io:format("Bookies restarted~n"),
    
    testutil:check_forlist(Bookie2, ChkList1),
    io:format("Check active bookie still contains original data~n"),
    testutil:check_forlist(SnapBookie2, ChkList1),
    io:format("Check snapshot still contains original data~n"),
    
    
    ObjList2 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList2),
    io:format("Replacement objects put~n"),
    
    ChkList2 = lists:sublist(lists:sort(ObjList2), 100),
    testutil:check_forlist(Bookie2, ChkList2),
    testutil:check_forlist(SnapBookie2, ChkList1),
    io:format("Checked for replacement objects in active bookie" ++
                    ", old objects in snapshot~n"),
    
    ok = filelib:ensure_dir(RootPath ++ "/ledger/ledger_files"),
    {ok, FNsA} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    ObjList3 = testutil:generate_objects(15000, 5002),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList3),
    ChkList3 = lists:sublist(lists:sort(ObjList3), 100),
    testutil:check_forlist(Bookie2, ChkList3),
    testutil:check_formissinglist(SnapBookie2, ChkList3),
    GenList = [20002, 40002, 60002, 80002, 100002, 120002],
    CLs2 = testutil:load_objects(20000, GenList, Bookie2, TestObject,
                                    fun testutil:generate_smallobjects/2),
    io:format("Loaded significant numbers of new objects~n"),
    
    testutil:check_forlist(Bookie2, lists:nth(length(CLs2), CLs2)),
    io:format("Checked active bookie has new objects~n"),
    
    {ok, SnapBookie3} = leveled_bookie:book_start(SnapOpts2),
    testutil:check_forlist(SnapBookie3, lists:nth(length(CLs2), CLs2)),
    testutil:check_formissinglist(SnapBookie2, ChkList3),
    testutil:check_formissinglist(SnapBookie2, lists:nth(length(CLs2), CLs2)),
    testutil:check_forlist(Bookie2, ChkList2),
    testutil:check_forlist(SnapBookie3, ChkList2),
    testutil:check_forlist(SnapBookie2, ChkList1),
    io:format("Started new snapshot and check for new objects~n"),
    
    CLs3 = testutil:load_objects(20000, GenList, Bookie2, TestObject,
                                    fun testutil:generate_smallobjects/2),
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    testutil:check_forlist(Bookie2, lists:nth(1, CLs3)),
    {ok, FNsB} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    ok = leveled_bookie:book_close(SnapBookie2),
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    ok = leveled_bookie:book_close(SnapBookie3),
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    testutil:check_forlist(Bookie2, lists:nth(1, CLs3)),
    timer:sleep(90000),
    {ok, FNsC} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    true = length(FNsB) > length(FNsA),
    true = length(FNsB) > length(FNsC),
    
    {B1Size, B1Count} = testutil:check_bucket_stats(Bookie2, "Bucket1"),
    true = B1Size > 0,
    true = B1Count == 1,
    {B1Size, B1Count} = testutil:check_bucket_stats(Bookie2, "Bucket1"),
    {BSize, BCount} = testutil:check_bucket_stats(Bookie2, "Bucket"),
    true = BSize > 0,
    true = BCount == 140000,
    
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().


load_and_count(_Config) ->
    % Use artificially small files, and the load keys, counting they're all
    % present
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath, max_journalsize=50000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
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
    io:format("Loading larger compressible objects~n"),
    G2 = fun testutil:generate_compressibleobjects/2,
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
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
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Replacing small objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G1),
                        {_S, Count} = testutil:check_bucket_stats(Bookie1,
                                                                    "Bucket"),
                        if
                            Count == 200000 ->
                                ok
                        end,
                        Acc + 5000 end,
                        0,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    io:format("Loading more small objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        testutil:load_objects(5000,
                                                [Acc + 2],
                                                Bookie1,
                                                TestObject,
                                                G2),
                        {_S, Count} = testutil:check_bucket_stats(Bookie1,
                                                                    "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        200000,
                        lists:seq(1, 20)),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    {_, 300000} = testutil:check_bucket_stats(Bookie2, "Bucket"),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().

load_and_count_withdelete(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath, max_journalsize=50000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
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
    {BucketD, KeyD} = leveled_codec:riakto_keydetails(TestObject),
    {_, 1} = testutil:check_bucket_stats(Bookie1, BucketD),
    ok = leveled_bookie:book_riakdelete(Bookie1, BucketD, KeyD, []),
    not_found = leveled_bookie:book_riakget(Bookie1, BucketD, KeyD),
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
    not_found = leveled_bookie:book_riakget(Bookie1, BucketD, KeyD),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    testutil:check_formissingobject(Bookie2, BucketD, KeyD),
    {_BSize, 0} = testutil:check_bucket_stats(Bookie2, BucketD),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().
