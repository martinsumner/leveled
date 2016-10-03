-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("../include/leveled.hrl").
-export([all/0]).
-export([simple_put_fetch/1,
            journal_compaction/1]).

all() -> [journal_compaction, simple_put_fetch].

simple_put_fetch(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie1,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = #bookie_options{root_path=RootPath,
                                 max_journalsize=3000000},
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie2,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    ObjList1 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie2, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                        R = leveled_bookie:book_riakget(Bookie2,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                        R = {ok, Obj} end,
                    ChkList1),
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie2,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    ok = leveled_bookie:book_close(Bookie2),
    reset_filestructure().

journal_compaction(_Config) ->
    RootPath = reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath,
                                 max_journalsize=4000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie1,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    ObjList1 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList1),
    ChkList1 = lists:sublist(lists:sort(ObjList1), 100),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                        R = leveled_bookie:book_riakget(Bookie1,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                        R = {ok, Obj} end,
                    ChkList1),
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie1,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    %% Now replace all the objects
    ObjList2 = generate_multiple_objects(5000, 2),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjList2),
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    ChkList3 = lists:sublist(lists:sort(ObjList2), 500),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                        R = leveled_bookie:book_riakget(Bookie1,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                        R = {ok, Obj} end,
                    ChkList3),
    ok = leveled_bookie:book_close(Bookie1),
    % Restart
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    {ok, TestObject} = leveled_bookie:book_riakget(Bookie2,
                                                    TestObject#r_object.bucket,
                                                    TestObject#r_object.key),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                        R = leveled_bookie:book_riakget(Bookie2,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                        R = {ok, Obj} end,
                    ChkList3),
    ok = leveled_bookie:book_close(Bookie2),
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
    Content = #r_content{metadata=MD, value=V1},
    {#r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
        Spec1}.

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


    