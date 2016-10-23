-module(testutil).

-include("../include/leveled.hrl").

-export([reset_filestructure/0,
            check_bucket_stats/2,
            check_forlist/2,
            check_forlist/3,
            check_formissinglist/2,
            check_forobject/2,
            check_formissingobject/3,
            generate_testobject/0,
            generate_testobject/5,
            generate_compressibleobjects/2,
            generate_smallobjects/2,
            generate_objects/2,
            generate_objects/5,
            generate_objects/6,
            set_object/5,
            get_key/1,
            get_value/1,
            get_compressiblevalue/0,
            get_randomindexes_generator/1,
            name_list/0,
            load_objects/5]).



reset_filestructure() ->
    % io:format("Waiting ~w ms to give a chance for all file closes " ++
    %             "to complete~n", [Wait]),
    % timer:sleep(Wait),
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
                                                        {riakbucket_stats,
                                                            Bucket}),
    {B1Size, B1Count} = Folder1(),
    io:format("Bucket fold completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), FoldSW1)]),
    io:format("Bucket ~s has size ~w and count ~w~n",
                [Bucket, B1Size, B1Count]),
    {B1Size, B1Count}.


check_forlist(Bookie, ChkList) ->
    check_forlist(Bookie, ChkList, false).

check_forlist(Bookie, ChkList, Log) ->
    SW = os:timestamp(),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    if
                        Log == true ->
                            io:format("Fetching Key ~s~n", [Obj#r_object.key]);
                        true ->
                            ok
                    end,
                    R = leveled_bookie:book_riakget(Bookie,
                                                    Obj#r_object.bucket,
                                                    Obj#r_object.key),
                    ok = case R of
                                {ok, Obj} ->
                                    ok;
                                not_found ->
                                    io:format("Object not found for key ~s~n",
                                                [Obj#r_object.key]),
                                    error
                            end
                    end,
                ChkList),
    io:format("Fetch check took ~w microseconds checking list of length ~w~n",
                    [timer:now_diff(os:timestamp(), SW), length(ChkList)]).

check_formissinglist(Bookie, ChkList) ->
    SW = os:timestamp(),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    R = leveled_bookie:book_riakget(Bookie,
                                                        Obj#r_object.bucket,
                                                        Obj#r_object.key),
                    R = not_found end,
                ChkList),
    io:format("Miss check took ~w microseconds checking list of length ~w~n",
                    [timer:now_diff(os:timestamp(), SW), length(ChkList)]).

check_forobject(Bookie, TestObject) ->
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

check_formissingobject(Bookie, Bucket, Key) ->
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


generate_compressibleobjects(Count, KeyNumber) ->
    V = get_compressiblevalue(),
    generate_objects(Count, KeyNumber, [], V).


get_compressiblevalue() ->
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
    lists:foldl(fun(_X, Acc) ->
                    {_, Str} = lists:keyfind(random:uniform(8), 1, Selector),
                    Acc ++ Str end,
                "",
                L).

generate_smallobjects(Count, KeyNumber) ->
    generate_objects(Count, KeyNumber, [], crypto:rand_bytes(512)).

generate_objects(Count, KeyNumber) ->
    generate_objects(Count, KeyNumber, [], crypto:rand_bytes(4096)).


generate_objects(Count, KeyNumber, ObjL, Value) ->
    generate_objects(Count, KeyNumber, ObjL, Value, fun() -> [] end).

generate_objects(Count, KeyNumber, ObjL, Value, IndexGen) ->
    generate_objects(Count, KeyNumber, ObjL, Value, IndexGen, "Bucket").

generate_objects(0, _KeyNumber, ObjL, _Value, _IndexGen, _Bucket) ->
    ObjL;
generate_objects(Count, uuid, ObjL, Value, IndexGen, Bucket) ->
    {Obj1, Spec1} = set_object(Bucket,
                                leveled_codec:generate_uuid(),
                                Value,
                                IndexGen),
    generate_objects(Count - 1,
                        uuid,
                        ObjL ++ [{random:uniform(), Obj1, Spec1}],
                        Value,
                        IndexGen,
                        Bucket);
generate_objects(Count, KeyNumber, ObjL, Value, IndexGen, Bucket) ->
    {Obj1, Spec1} = set_object(Bucket,
                                "Key" ++ integer_to_list(KeyNumber),
                                Value,
                                IndexGen),
    generate_objects(Count - 1,
                        KeyNumber + 1,
                        ObjL ++ [{random:uniform(), Obj1, Spec1}],
                        Value,
                        IndexGen,
                        Bucket).

set_object(Bucket, Key, Value, IndexGen) ->
    set_object(Bucket, Key, Value, IndexGen, []).

set_object(Bucket, Key, Value, IndexGen, Indexes2Remove) ->
    Obj = {Bucket,
            Key,
            Value,
            IndexGen() ++ lists:map(fun({add, IdxF, IdxV}) ->
                                            {remove, IdxF, IdxV} end,
                                        Indexes2Remove),
            [{"MDK", "MDV" ++ Key},
                {"MDK2", "MDV" ++ Key}]},
    {B1, K1, V1, Spec1, MD} = Obj,
    Content = #r_content{metadata=MD, value=V1},
    {#r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
        Spec1}.

get_key(Object) ->
    Object#r_object.key.

get_value(Object) ->
    [Content] = Object#r_object.contents,
    Content#r_content.value.

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
                    if
                        TestObject == no_check ->
                            ok;
                        true ->
                            check_forobject(Bookie, TestObject)
                    end,
                    lists:sublist(ObjListA, 1000) end,
                GenList).


get_randomindexes_generator(Count) ->
    Generator = fun() ->
            lists:map(fun(X) ->
                                {add,
                                    "idx" ++ integer_to_list(X) ++ "_bin",
                                    get_randomdate() ++ get_randomname()} end,
                        lists:seq(1, Count))
        end,
    Generator.

name_list() ->
    [{1, "Sophia"}, {2, "Emma"}, {3, "Olivia"}, {4, "Ava"},
            {5, "Isabella"}, {6, "Mia"}, {7, "Zoe"}, {8, "Lily"},
            {9, "Emily"}, {10, "Madelyn"}, {11, "Madison"}, {12, "Chloe"},
            {13, "Charlotte"}, {14, "Aubrey"}, {15, "Avery"},
            {16, "Abigail"}].

get_randomname() ->
    NameList = name_list(),
    N = random:uniform(16),
    {N, Name} = lists:keyfind(N, 1, NameList),
    Name.

get_randomdate() ->
    LowTime = 60000000000,
    HighTime = 70000000000,
    RandPoint = LowTime + random:uniform(HighTime - LowTime),
    Date = calendar:gregorian_seconds_to_datetime(RandPoint),
    {{Year, Month, Day}, {Hour, Minute, Second}} = Date,
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0w~2..0w~2..0w~2..0w",
                                    [Year, Month, Day, Hour, Minute, Second])).