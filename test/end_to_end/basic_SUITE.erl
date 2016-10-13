-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("../include/leveled.hrl").
-export([all/0]).
-export([simple_put_fetch_head/1,
            many_put_fetch_head/1,
            journal_compaction/1,
            fetchput_snapshot/1,
            load_and_count/1]).

all() -> [simple_put_fetch_head,
            many_put_fetch_head,
            journal_compaction,
            fetchput_snapshot,
            load_and_count].


simple_put_fetch_head(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    check_bookie_forobject(Bookie1, TestObject),
    check_bookie_formissingobject(Bookie1, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = #bookie_options{root_path=RootPath,
                                 max_journalsize=3000000},
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    check_bookie_forobject(Bookie2, TestObject),
    ObjList1 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    check_bookie_forlist(Bookie2, ChkList1),
    check_bookie_forobject(Bookie2, TestObject),
    check_bookie_formissingobject(Bookie2, "Bucket1", "Key2"),
    ok = leveled_bookie:book_close(Bookie2),
    reset_filestructure().

many_put_fetch_head(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    check_bookie_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = #bookie_options{root_path=RootPath,
                                 max_journalsize=1000000000},
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    check_bookie_forobject(Bookie2, TestObject),
    GenList = [2, 20002, 40002, 60002, 80002,
                100002, 120002, 140002, 160002, 180002],
    CLs = load_objects(20000, GenList, Bookie2, TestObject,
                        fun generate_multiple_smallobjects/2),
    CL1A = lists:nth(1, CLs),
    ChkListFixed = lists:nth(length(CLs), CLs),
    check_bookie_forlist(Bookie2, CL1A),
    ObjList2A = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList2A),
    ChkList2A = lists:sublist(lists:sort(ObjList2A), 1000),
    check_bookie_forlist(Bookie2, ChkList2A),
    check_bookie_forlist(Bookie2, ChkListFixed),
    check_bookie_forobject(Bookie2, TestObject),
    check_bookie_forlist(Bookie2, ChkList2A),
    check_bookie_forlist(Bookie2, ChkListFixed),
    check_bookie_forobject(Bookie2, TestObject),
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts2),
    check_bookie_forlist(Bookie3, ChkList2A),
    check_bookie_forobject(Bookie3, TestObject),
    ok = leveled_bookie:book_close(Bookie3),
    reset_filestructure().

journal_compaction(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath,
                                 max_journalsize=4000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    check_bookie_forobject(Bookie1, TestObject),
    ObjList1 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 1000),
    check_bookie_forlist(Bookie1, ChkList1),
    check_bookie_forobject(Bookie1, TestObject),
    {B2, K2, V2, Spec2, MD} = {"Bucket1",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    {TestObject2, TestSpec2} = generate_testobject(B2, K2, V2, Spec2, MD),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject2, TestSpec2),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    check_bookie_forlist(Bookie1, ChkList1),
    check_bookie_forobject(Bookie1, TestObject),
    check_bookie_forobject(Bookie1, TestObject2),
    timer:sleep(5000), % Allow for compaction to complete
    io:format("Has journal completed?~n"),
    check_bookie_forlist(Bookie1, ChkList1),
    check_bookie_forobject(Bookie1, TestObject),
    check_bookie_forobject(Bookie1, TestObject2),
    %% Now replace all the objects
    ObjList2 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList2),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    ChkList3 = lists:sublist(lists:sort(ObjList2), 500),
    check_bookie_forlist(Bookie1, ChkList3),
    ok = leveled_bookie:book_close(Bookie1),
    % Restart
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    check_bookie_forobject(Bookie2, TestObject),
    check_bookie_forlist(Bookie2, ChkList3),
    ok = leveled_bookie:book_close(Bookie2),
    reset_filestructure().


fetchput_snapshot(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath, max_journalsize=3000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    ObjList1 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    SnapOpts1 = #bookie_options{snapshot_bookie=Bookie1},
    {ok, SnapBookie1} = leveled_bookie:book_start(SnapOpts1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    check_bookie_forlist(Bookie1, ChkList1),
    check_bookie_forlist(SnapBookie1, ChkList1),
    ok = leveled_bookie:book_close(SnapBookie1),
    check_bookie_forlist(Bookie1, ChkList1),
    ok = leveled_bookie:book_close(Bookie1),
    io:format("Closed initial bookies~n"),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    SnapOpts2 = #bookie_options{snapshot_bookie=Bookie2},
    {ok, SnapBookie2} = leveled_bookie:book_start(SnapOpts2),
    io:format("Bookies restarted~n"),
    
    check_bookie_forlist(Bookie2, ChkList1),
    io:format("Check active bookie still contains original data~n"),
    check_bookie_forlist(SnapBookie2, ChkList1),
    io:format("Check snapshot still contains original data~n"),
    
    
    ObjList2 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList2),
    io:format("Replacement objects put~n"),
    
    ChkList2 = lists:sublist(lists:sort(ObjList2), 100),
    check_bookie_forlist(Bookie2, ChkList2),
    check_bookie_forlist(SnapBookie2, ChkList1),
    io:format("Checked for replacement objects in active bookie" ++
                    ", old objects in snapshot~n"),
    
    {ok, FNsA} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    ObjList3 = generate_multiple_objects(15000, 5002),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList3),
    ChkList3 = lists:sublist(lists:sort(ObjList3), 100),
    check_bookie_forlist(Bookie2, ChkList3),
    check_bookie_formissinglist(SnapBookie2, ChkList3),
    GenList = [20002, 40002, 60002, 80002, 100002, 120002],
    CLs2 = load_objects(20000, GenList, Bookie2, TestObject,
                        fun generate_multiple_smallobjects/2),
    io:format("Loaded significant numbers of new objects~n"),
    
    check_bookie_forlist(Bookie2, lists:nth(length(CLs2), CLs2)),
    io:format("Checked active bookie has new objects~n"),
    
    {ok, SnapBookie3} = leveled_bookie:book_start(SnapOpts2),
    check_bookie_forlist(SnapBookie3, lists:nth(length(CLs2), CLs2)),
    check_bookie_formissinglist(SnapBookie2, ChkList3),
    check_bookie_formissinglist(SnapBookie2, lists:nth(length(CLs2), CLs2)),
    check_bookie_forlist(SnapBookie3, ChkList2),
    check_bookie_forlist(SnapBookie2, ChkList1),
    io:format("Started new snapshot and check for new objects~n"),
    
    CLs3 = load_objects(20000, GenList, Bookie2, TestObject,
                        fun generate_multiple_smallobjects/2),
    check_bookie_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    check_bookie_forlist(Bookie2, lists:nth(1, CLs3)),
    {ok, FNsB} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    ok = leveled_bookie:book_close(SnapBookie2),
    check_bookie_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    ok = leveled_bookie:book_close(SnapBookie3),
    check_bookie_forlist(Bookie2, lists:nth(length(CLs3), CLs3)),
    check_bookie_forlist(Bookie2, lists:nth(1, CLs3)),
    timer:sleep(90000),
    {ok, FNsC} = file:list_dir(RootPath ++ "/ledger/ledger_files"),
    true = length(FNsB) > length(FNsA),
    true = length(FNsB) > length(FNsC),
    
    {B1Size, B1Count} = check_bucket_stats(Bookie2, "Bucket1"),
    true = B1Size > 0,
    true = B1Count == 1,
    {B1Size, B1Count} = check_bucket_stats(Bookie2, "Bucket1"),
    {BSize, BCount} = check_bucket_stats(Bookie2, "Bucket"),
    true = BSize > 0,
    true = BCount == 140000,
    
    ok = leveled_bookie:book_close(Bookie2),
    reset_filestructure().


load_and_count(_Config) ->
    % Use artificially small files, and the load keys, counting they're all
    % present
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath, max_journalsize=50000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    check_bookie_forobject(Bookie1, TestObject),
    io:format("Loading initial small objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        load_objects(5000, [Acc + 2], Bookie1, TestObject,
                            fun generate_multiple_smallobjects/2),
                        {_Size, Count} = check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        0,
                        lists:seq(1, 20)),
    check_bookie_forobject(Bookie1, TestObject),
    io:format("Loading larger compressible objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        load_objects(5000, [Acc + 2], Bookie1, TestObject,
                            fun generate_multiple_compressibleobjects/2),
                        {_Size, Count} = check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        100000,
                        lists:seq(1, 20)),
    check_bookie_forobject(Bookie1, TestObject),
    io:format("Replacing small objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        load_objects(5000, [Acc + 2], Bookie1, TestObject,
                            fun generate_multiple_smallobjects/2),
                        {_Size, Count} = check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Count == 200000 ->
                                ok
                        end,
                        Acc + 5000 end,
                        0,
                        lists:seq(1, 20)),
    check_bookie_forobject(Bookie1, TestObject),
    io:format("Loading more small objects~n"),
    lists:foldl(fun(_X, Acc) ->
                        load_objects(5000, [Acc + 2], Bookie1, TestObject,
                            fun generate_multiple_compressibleobjects/2),
                        {_Size, Count} = check_bucket_stats(Bookie1, "Bucket"),
                        if
                            Acc + 5000 == Count ->
                                ok
                        end,
                        Acc + 5000 end,
                        200000,
                        lists:seq(1, 20)),
    check_bookie_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    {_BSize, 300000} = check_bucket_stats(Bookie2, "Bucket"),
    ok = leveled_bookie:book_close(Bookie2),
    reset_filestructure().


reset_filestructure() ->
    RootPath  = "test",
    filelib:ensure_dir(RootPath ++ "/journal/"),
    filelib:ensure_dir(RootPath ++ "/ledger/"),
    leveled_inker:clean_testdir(RootPath ++ "/journal"),
    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    RootPath.



check_bucket_stats(Bookie, Bucket) ->
    FoldSW1 = os:timestamp(),
    io:format("Checking bucket size~n"),
    {async, Folder1} = leveled_bookie:book_returnfolder(Bookie,
                                                        {bucket_stats,
                                                            Bucket}),
    {B1Size, B1Count} = Folder1(),
    io:format("Bucket fold completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), FoldSW1)]),
    io:format("Bucket ~s has size ~w and count ~w~n",
                [Bucket, B1Size, B1Count]),
    {B1Size, B1Count}.


check_bookie_forlist(Bookie, ChkList) ->
    check_bookie_forlist(Bookie, ChkList, false).

check_bookie_forlist(Bookie, ChkList, Log) ->
    SW = os:timestamp(),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    if
                        Log == true ->
                            io:format("Fetching Key ~w~n", [Obj#r_object.key]);
                        true ->
                            ok
                    end,
                    R = leveled_bookie:book_riakget(Bookie,
                                                    Obj#r_object.bucket,
                                                    Obj#r_object.key),
                    R = {ok, Obj} end,
                ChkList),
    io:format("Fetch check took ~w microseconds checking list of length ~w~n",
                    [timer:now_diff(os:timestamp(), SW), length(ChkList)]).

check_bookie_formissinglist(Bookie, ChkList) ->
    SW = os:timestamp(),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    R = leveled_bookie:book_riakget(Bookie,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                    R = not_found end,
                ChkList),
    io:format("Miss check took ~w microseconds checking list of length ~w~n",
                    [timer:now_diff(os:timestamp(), SW), length(ChkList)]).

check_bookie_forobject(Bookie, TestObject) ->
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    {ok, HeadObject} = leveled_bookie:book_riakhead(Bookie,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    ok = case {HeadObject#r_object.bucket,
                    HeadObject#r_object.key,
                    HeadObject#r_object.vclock} of
                {B1, K1, VC1} when B1 == TestObject#r_object.bucket,
                                    K1 == TestObject#r_object.key,
                                    VC1 == TestObject#r_object.vclock ->
                        ok
            end.

check_bookie_formissingobject(Bookie, Bucket, Key) ->
    not_found = leveled_bookie:book_riakget(Bookie, Bucket, Key),
    not_found = leveled_bookie:book_riakhead(Bookie, Bucket, Key).


generate_testobject() ->
    {B1, K1, V1, Spec1, MD} = {"Bucket1",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    generate_testobject(B1, K1, V1, Spec1, MD).

generate_testobject(B, K, V, Spec, MD) ->
    Content = #r_content{metadata=MD, value=V},
    {#r_object{bucket=B, key=K, contents=[Content], vclock=[{'a',1}]},
        Spec}.


generate_multiple_compressibleobjects(Count, KeyNumber) ->
    S1 = "111111111111111",
    S2 = "222222222222222",
    S3 = "333333333333333",
    S4 = "aaaaaaaaaaaaaaa",
    S5 = "AAAAAAAAAAAAAAA",
    S6 = "GGGGGGGGGGGGGGG",
    S7 = "===============",
    S8 = "...............",
    Selector = [{1, S1}, {2, S2}, {3, S3}, {4, S4},
                {5, S5}, {6, S6}, {7, S7}, {8, S8}],
    L = lists:seq(1, 1024),
    V = lists:foldl(fun(_X, Acc) ->
                        {_, Str} = lists:keyfind(random:uniform(8), 1, Selector),
                        Acc ++ Str end,
                    "",
                    L),
    generate_multiple_objects(Count, KeyNumber, [], V).
    
generate_multiple_smallobjects(Count, KeyNumber) ->
    generate_multiple_objects(Count, KeyNumber, [], crypto:rand_bytes(512)).

generate_multiple_objects(Count, KeyNumber) ->
    generate_multiple_objects(Count, KeyNumber, [], crypto:rand_bytes(4096)).
    
generate_multiple_objects(0, _KeyNumber, ObjL, _Value) ->
    ObjL;
generate_multiple_objects(Count, KeyNumber, ObjL, Value) ->
    Obj = {"Bucket",
            "Key" ++ integer_to_list(KeyNumber),
            Value,
            [],
            [{"MDK", "MDV" ++ integer_to_list(KeyNumber)},
                {"MDK2", "MDV" ++ integer_to_list(KeyNumber)}]},
    {B1, K1, V1, Spec1, MD} = Obj,
    Content = #r_content{metadata=MD, value=V1},
    Obj1 = #r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
    generate_multiple_objects(Count - 1,
                                KeyNumber + 1,
                                ObjL ++ [{random:uniform(), Obj1, Spec1}],
                                Value).


load_objects(ChunkSize, GenList, Bookie, TestObject, Generator) ->
    lists:map(fun(KN) ->
                    ObjListA = Generator(ChunkSize, KN),
                    StartWatchA = os:timestamp(),
                    lists:foreach(fun({_RN, Obj, Spc}) ->
                            leveled_bookie:book_riakput(Bookie, Obj, Spc)
                            end,
                        ObjListA),
                    Time = timer:now_diff(os:timestamp(), StartWatchA),
                    io:format("~w objects loaded in ~w seconds~n",
                                [ChunkSize, Time/1000000]),
                    check_bookie_forobject(Bookie, TestObject),
                    lists:sublist(ObjListA, 1000) end,
                GenList).
