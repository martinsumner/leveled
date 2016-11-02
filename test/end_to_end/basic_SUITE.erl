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
            space_clear_ondelete/1
            ]).

all() -> [
            simple_put_fetch_head_delete,
            many_put_fetch_head,
            journal_compaction,
            fetchput_snapshot,
            load_and_count,
            load_and_count_withdelete,
            space_clear_ondelete
            ].


simple_put_fetch_head_delete(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_formissingobject(Bookie1, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPath},
                    {max_journalsize, 3000000}],
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
    {ok, {62888926, 56}} = leveled_bookie:book_head(Bookie2,
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
    not_found = leveled_bookie:book_head(Bookie3, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie3),
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts2),
    not_found = leveled_bookie:book_get(Bookie4, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie4),
    testutil:reset_filestructure().

many_put_fetch_head(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath}, {max_pencillercachesize, 16000}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPath},
                    {max_journalsize, 1000000000},
                    {max_pencillercachesize, 32000}],
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
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 10000000},
                    {max_run_length, 1}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ObjList1 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 10000),
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
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_forobject(Bookie1, TestObject2),
    %% Now replace all the objects
    ObjList2 = testutil:generate_objects(50000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList2),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(20000),
                                F(Bookie1)
                        end end,
                    true,
                    lists:seq(1, 15)),
    
    ChkList3 = lists:sublist(lists:sort(ObjList2), 500),
    testutil:check_forlist(Bookie1, ChkList3),
    ok = leveled_bookie:book_close(Bookie1),
    % Restart
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    testutil:check_forobject(Bookie2, TestObject),
    testutil:check_forlist(Bookie2, ChkList3),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure(10000).


fetchput_snapshot(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath}, {max_journalsize, 30000000}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    ObjList1 = testutil:generate_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    SnapOpts1 = [{snapshot_bookie, Bookie1}],
    {ok, SnapBookie1} = leveled_bookie:book_start(SnapOpts1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forlist(SnapBookie1, ChkList1),
    ok = leveled_bookie:book_close(SnapBookie1),
    testutil:check_forlist(Bookie1, ChkList1),
    ok = leveled_bookie:book_close(Bookie1),
    io:format("Closed initial bookies~n"),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    SnapOpts2 = [{snapshot_bookie, Bookie2}],
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
    GenList = [20002, 40002, 60002, 80002],
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
    
    io:format("Starting 15s sleep in which snap2 should block deletion~n"),
    timer:sleep(15000),
    {ok, FNsB} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    ok = leveled_bookie:book_close(SnapBookie2),
    io:format("Starting 15s sleep as snap2 close should unblock deletion~n"),
    timer:sleep(15000),
    io:format("Pause for deletion has ended~n"),
    
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    ok = leveled_bookie:book_close(SnapBookie3),
    io:format("Starting 15s sleep as snap3 close should unblock deletion~n"),
    timer:sleep(15000),
    io:format("Pause for deletion has ended~n"),
    testutil:check_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    testutil:check_forlist(Bookie2, lists:nth(1, CLs3)),
    {ok, FNsC} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    true = length(FNsB) > length(FNsA),
    true = length(FNsB) > length(FNsC),
    
    {B1Size, B1Count} = testutil:check_bucket_stats(Bookie2, "Bucket1"),
    true = B1Size > 0,
    true = B1Count == 1,
    {B1Size, B1Count} = testutil:check_bucket_stats(Bookie2, "Bucket1"),
    {BSize, BCount} = testutil:check_bucket_stats(Bookie2, "Bucket"),
    true = BSize > 0,
    true = BCount == 100000,
    
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().


load_and_count(_Config) ->
    % Use artificially small files, and the load keys, counting they're all
    % present
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath}, {max_journalsize, 50000000}],
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
    StartOpts1 = [{root_path, RootPath}, {max_journalsize, 50000000}],
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


space_clear_ondelete(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath}, {max_journalsize, 20000000}],
    {ok, Book1} = leveled_bookie:book_start(StartOpts1),
    G2 = fun testutil:generate_compressibleobjects/2,
    testutil:load_objects(20000,
                            [uuid, uuid, uuid, uuid],
                            Book1,
                            no_check,
                            G2),
    
    {async, F1} = leveled_bookie:book_returnfolder(Book1, {keylist, o_rkv}),
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
    io:format("Bookie created ~w journal files and ~w ledger files~n",
                    [length(FNsA_J), length(FNsA_L)]),
    SW2 = os:timestamp(),
    lists:foreach(fun({Bucket, Key}) ->
                        ok = leveled_bookie:book_riakdelete(Book1,
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
    io:format("Waiting for journal deletes~n"),
    timer:sleep(20000), 
    {ok, FNsB_L} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    {ok, FNsB_J} = file:list_dir(RootPath ++ "/journal/journal_files"),
    {ok, FNsB_PC} = file:list_dir(RootPath ++ "/journal/journal_files/post_compact"),
    PointB_Journals = length(FNsB_J) + length(FNsB_PC),
    io:format("Bookie has ~w journal files and ~w ledger files " ++
                    "after deletes~n",
                [PointB_Journals, length(FNsB_L)]),
    
    {async, F2} = leveled_bookie:book_returnfolder(Book1, {keylist, o_rkv}),
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
    {async, F3} = leveled_bookie:book_returnfolder(Book2, {keylist, o_rkv}),
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
    io:format("Bookie has ~w ledger files " ++
                    "after close~n", [length(FNsC_L)]),
    
    {ok, Book3} = leveled_bookie:book_start(StartOpts1),
    io:format("This should cause a final ledger merge event~n"),
    io:format("Will require the penciller to resolve the issue of creating" ++
                " an empty file as all keys compact on merge~n"),
    timer:sleep(5000),
    ok = leveled_bookie:book_close(Book3),
    {ok, FNsD_L} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    io:format("Bookie has ~w ledger files " ++
                    "after second close~n", [length(FNsD_L)]),
    true = PointB_Journals < length(FNsA_J),
    true = length(FNsD_L) < length(FNsA_L),
    true = length(FNsD_L) < length(FNsB_L),
    true = length(FNsD_L) < length(FNsC_L),
    true = length(FNsD_L) == 0.