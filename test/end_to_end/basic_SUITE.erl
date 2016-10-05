-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("../include/leveled.hrl").
-export([all/0]).
-export([simple_put_fetch_head/1,
            many_put_fetch_head/1,
            journal_compaction/1,
            simple_snapshot/1]).

all() -> [simple_put_fetch_head,
            many_put_fetch_head,
            journal_compaction,
            simple_snapshot].


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
    CLs = lists:map(fun(KN) ->
                        ObjListA = generate_multiple_smallobjects(20000, KN),
                        StartWatchA = os:timestamp(),
                        lists:foreach(fun({_RN, Obj, Spc}) ->
                                leveled_bookie:book_riakput(Bookie2, Obj, Spc)
                                end,
                            ObjListA),
                        Time = timer:now_diff(os:timestamp(), StartWatchA),
                        io:format("20,000 objects loaded in ~w seconds~n",
                                    [Time/1000000]),
                        check_bookie_forobject(Bookie2, TestObject),
                        lists:sublist(ObjListA, 1000) end,
                    GenList),               
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


check_bookie_forlist(Bookie, ChkList) ->
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    R = leveled_bookie:book_riakget(Bookie,
                                                    Obj#r_object.bucket,
                                                    Obj#r_object.key),
                    R = {ok, Obj} end,
                ChkList).

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


simple_snapshot(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath, max_journalsize=3000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    ObjList1 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    SnapOpts = #bookie_options{snapshot_bookie=Bookie1},
    {ok, SnapBookie} = leveled_bookie:book_start(SnapOpts),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                        R = leveled_bookie:book_riakget(Bookie1,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                        R = {ok, Obj} end,
                    ChkList1),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                        R = leveled_bookie:book_riakget(SnapBookie,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                        io:format("Finding key ~s~n", [Obj#r_object.key]),
                        R = {ok, Obj} end,
                    ChkList1),
    ok = leveled_bookie:book_close(SnapBookie),
    ok = leveled_bookie:book_close(Bookie1),
    reset_filestructure().


reset_filestructure() ->
    RootPath  = "test",
    filelib:ensure_dir(RootPath ++ "/journal/"),
    filelib:ensure_dir(RootPath ++ "/ledger/"),
    leveled_inker:clean_testdir(RootPath ++ "/journal"),
    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    RootPath.


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


    