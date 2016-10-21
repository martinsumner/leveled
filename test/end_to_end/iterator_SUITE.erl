-module(iterator_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").

-define(KEY_ONLY, {false, undefined}).
-define(RETURN_TERMS, {true, undefined}).

-export([all/0]).
-export([simple_load_with2i/1,
            simple_querycount/1,
            rotating_objects/1]).

all() -> [simple_load_with2i,
            simple_querycount,
            rotating_objects].


simple_load_with2i(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = #bookie_options{root_path=RootPath,
                                    max_journalsize=50000000},
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_formissingobject(Bookie1, "Bucket1", "Key2"),
    testutil:check_forobject(Bookie1, TestObject),
    ObjL1 = testutil:generate_objects(10000,
                                        uuid,
                                        [],
                                        testutil:get_compressiblevalue(),
                                        testutil:get_randomindexes_generator(8)),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjL1),
    ChkList1 = lists:sublist(lists:sort(ObjL1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().


simple_querycount(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Book1} = leveled_bookie:book_start(RootPath, 2500, 50000000),
    {TestObject, TestSpec} = testutil:generate_testobject("Bucket",
                                                            "Key1",
                                                            "Value1",
                                                            [],
                                                            {"MDK1", "MDV1"}),
    ok = leveled_bookie:book_riakput(Book1, TestObject, TestSpec),
    testutil:check_forobject(Book1, TestObject),
    testutil:check_formissingobject(Book1, "Bucket1", "Key2"),
    testutil:check_forobject(Book1, TestObject),
    lists:foreach(fun(_X) ->
                        V = testutil:get_compressiblevalue(),
                        Indexes = testutil:get_randomindexes_generator(8),
                        SW = os:timestamp(),
                        ObjL1 = testutil:generate_objects(10000,
                                                            uuid,
                                                            [],
                                                            V,
                                                            Indexes),
                        lists:foreach(fun({_RN, Obj, Spc}) ->
                                            leveled_bookie:book_riakput(Book1,
                                                                        Obj,
                                                                        Spc)
                                            end,
                                        ObjL1),
                        io:format("Put of 10000 objects with 8 index entries "
                                        ++
                                        "each completed in ~w microseconds~n",
                                    [timer:now_diff(os:timestamp(), SW)])
                        end,
                        lists:seq(1, 8)),
    testutil:check_forobject(Book1, TestObject),
    Total = lists:foldl(fun(X, Acc) ->
                                IdxF = "idx" ++ integer_to_list(X) ++ "_bin",
                                T = count_termsonindex("Bucket",
                                                        IdxF,
                                                        Book1,
                                                        ?KEY_ONLY),
                                io:format("~w terms found on index ~s~n",
                                            [T, IdxF]),
                                Acc + T
                                end,
                            0,
                            lists:seq(1, 8)),
    ok = case Total of
                640000 ->
                    ok
            end,
    Index1Count = count_termsonindex("Bucket",
                                        "idx1_bin",
                                        Book1,
                                        ?KEY_ONLY),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(RootPath, 2000, 50000000),
    Index1Count = count_termsonindex("Bucket",
                                        "idx1_bin",
                                        Book2,
                                        ?KEY_ONLY),
    NameList = testutil:name_list(),
    TotalNameByName = lists:foldl(fun({_X, Name}, Acc) ->
                                        {ok, Regex} = re:compile("[0-9]+" ++
                                                                    Name),
                                        SW = os:timestamp(),
                                        T = count_termsonindex("Bucket",
                                                                "idx1_bin",
                                                                Book2,
                                                                {false,
                                                                    Regex}),
                                        TD = timer:now_diff(os:timestamp(),
                                                                SW),
                                        io:format("~w terms found on " ++
                                                    "index idx1 with a " ++
                                                    "regex in ~w " ++
                                                    "microseconds~n",
                                                    [T, TD]),
                                        Acc + T
                                        end,
                                    0,
                                    NameList),
    ok = case TotalNameByName of
                Index1Count ->
                    ok
            end,
    {ok, RegMia} = re:compile("[0-9]+Mia"),
    {async,
        Mia2KFolder1} = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {"idx2_bin",
                                                                    "2000",
                                                                    "2000~"},
                                                                {false,
                                                                    RegMia}}),
    Mia2000Count1 = length(Mia2KFolder1()),
    {async,
        Mia2KFolder2} = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {"idx2_bin",
                                                                    "2000",
                                                                    "2001"},
                                                                {true,
                                                                    undefined}}),
    Mia2000Count2 = lists:foldl(fun({Term, _Key}, Acc) ->
                                    case re:run(Term, RegMia) of
                                        nomatch ->
                                            Acc;
                                        _ ->
                                            Acc + 1
                                    end end,
                                0,
                                Mia2KFolder2()),
    ok = case Mia2000Count2 of
                Mia2000Count1 when Mia2000Count1 > 0 ->
                    io:format("Mia2000 counts match at ~w~n",
                                [Mia2000Count1]),
                    ok
            end,
    {ok, RxMia2K} = re:compile("^2000[0-9]+Mia"),
    {async,
        Mia2KFolder3} = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {"idx2_bin",
                                                                    "1980",
                                                                    "2100"},
                                                                {false,
                                                                    RxMia2K}}),
    Mia2000Count1 = length(Mia2KFolder3()),
    
    V9 = testutil:get_compressiblevalue(),
    Indexes9 = testutil:get_randomindexes_generator(8),
    [{_RN, Obj9, Spc9}] = testutil:generate_objects(1, uuid, [], V9, Indexes9),
    ok = leveled_bookie:book_riakput(Book2, Obj9, Spc9),
    R9 = lists:map(fun({add, IdxF, IdxT}) ->
                        R = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {IdxF,
                                                                    IdxT,
                                                                    IdxT},
                                                                ?KEY_ONLY}),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            X when X > 0 ->
                                {IdxF, IdxT, X}
                        end
                        end,
                    Spc9),
    Spc9Del = lists:map(fun({add, IdxF, IdxT}) -> {remove, IdxF, IdxT} end,
                        Spc9),
    ok = leveled_bookie:book_riakput(Book2, Obj9, Spc9Del),
    lists:foreach(fun({IdxF, IdxT, X}) ->
                        R = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {IdxF,
                                                                    IdxT,
                                                                    IdxT},
                                                                ?KEY_ONLY}),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            Y ->
                                Y = X - 1
                        end
                        end,
                    R9),
    ok = leveled_bookie:book_close(Book2),
    {ok, Book3} = leveled_bookie:book_start(RootPath, 2000, 50000000),
    lists:foreach(fun({IdxF, IdxT, X}) ->
                        R = leveled_bookie:book_returnfolder(Book3,
                                                            {index_query,
                                                                "Bucket",
                                                                {IdxF,
                                                                    IdxT,
                                                                    IdxT},
                                                                ?KEY_ONLY}),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            Y ->
                                Y = X - 1
                        end
                        end,
                    R9),
    ok = leveled_bookie:book_riakput(Book3, Obj9, Spc9),
    {ok, Book4} = leveled_bookie:book_start(RootPath, 2000, 50000000),
    lists:foreach(fun({IdxF, IdxT, X}) ->
                        R = leveled_bookie:book_returnfolder(Book4,
                                                            {index_query,
                                                                "Bucket",
                                                                {IdxF,
                                                                    IdxT,
                                                                    IdxT},
                                                                ?KEY_ONLY}),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            X ->
                                ok
                        end
                        end,
                    R9),
    testutil:check_forobject(Book4, TestObject),
    ok = leveled_bookie:book_close(Book4),
    testutil:reset_filestructure().
    


count_termsonindex(Bucket, IdxField, Book, QType) ->
    lists:foldl(fun(X, Acc) ->
                        SW = os:timestamp(),
                        ST = integer_to_list(X),
                        ET = ST ++ "~",
                        R = leveled_bookie:book_returnfolder(Book,
                                                                {index_query,
                                                                    Bucket,
                                                                    {IdxField,
                                                                        ST,
                                                                        ET},
                                                                    QType}),
                        {async, Folder} = R,
                        Items = length(Folder()),
                        io:format("2i query from term ~s on index ~s took " ++
                                        "~w microseconds~n",
                                    [ST,
                                        IdxField,
                                        timer:now_diff(os:timestamp(), SW)]),
                        Acc + Items
                        end,
                    0,
                    lists:seq(1901, 2218)).


rotating_objects(_Config) ->
    RootPath = testutil:reset_filestructure(),
    ok = rotating_object_check(RootPath, "Bucket1", 10),
    ok = rotating_object_check(RootPath, "Bucket2", 200),
    ok = rotating_object_check(RootPath, "Bucket3", 800),
    ok = rotating_object_check(RootPath, "Bucket4", 1600),
    ok = rotating_object_check(RootPath, "Bucket5", 3200),
    ok = rotating_object_check(RootPath, "Bucket6", 9600),
    testutil:reset_filestructure().


rotating_object_check(RootPath, Bucket, NumberOfObjects) ->
    {ok, Book1} = leveled_bookie:book_start(RootPath, 2000, 5000000),
    {KSpcL1, V1} = put_indexed_objects(Book1, Bucket, NumberOfObjects),
    ok = check_indexed_objects(Book1, Bucket, KSpcL1, V1),
    {KSpcL2, V2} = put_altered_indexed_objects(Book1, Bucket, KSpcL1),
    ok = check_indexed_objects(Book1, Bucket, KSpcL2, V2),
    {KSpcL3, V3} = put_altered_indexed_objects(Book1, Bucket, KSpcL2),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(RootPath, 1000, 5000000),
    ok = check_indexed_objects(Book2, Bucket, KSpcL3, V3),
    {KSpcL4, V4} = put_altered_indexed_objects(Book2, Bucket, KSpcL3),
    ok = check_indexed_objects(Book2, Bucket, KSpcL4, V4),
    ok = leveled_bookie:book_close(Book2),
    ok.
    


check_indexed_objects(Book, B, KSpecL, V) ->
    % Check all objects match, return what should eb the results of an all
    % index query
    IdxR = lists:map(fun({K, Spc}) ->
                            {ok, O} = leveled_bookie:book_riakget(Book, B, K),
                            V = testutil:get_value(O),
                            {add,
                                "idx1_bin",
                                IdxVal} = lists:keyfind(add, 1, Spc),
                            {IdxVal, K} end,
                        KSpecL),
    % Check the all index query matxhes expectations
    R = leveled_bookie:book_returnfolder(Book,
                                            {index_query,
                                                B,
                                                {"idx1_bin",
                                                    "0",
                                                    "~"},
                                                ?RETURN_TERMS}),
    {async, Fldr} = R,
    QR = lists:sort(Fldr()),
    ER = lists:sort(IdxR),
    ok = if
                ER == QR ->
                    ok
            end,
    ok.


put_indexed_objects(Book, Bucket, Count) ->
    V = testutil:get_compressiblevalue(),
    IndexGen = testutil:get_randomindexes_generator(1),
    SW = os:timestamp(),
    ObjL1 = testutil:generate_objects(Count,
                                        uuid,
                                        [],
                                        V,
                                        IndexGen,
                                        Bucket),
    KSpecL = lists:map(fun({_RN, Obj, Spc}) ->
                            leveled_bookie:book_riakput(Book,
                                                        Obj,
                                                        Spc),
                            {testutil:get_key(Obj), Spc}
                            end,
                        ObjL1),
    io:format("Put of ~w objects with ~w index entries "
                    ++
                    "each completed in ~w microseconds~n",
                [Count, 1, timer:now_diff(os:timestamp(), SW)]),
    {KSpecL, V}.

put_altered_indexed_objects(Book, Bucket, KSpecL) ->
    IndexGen = testutil:get_randomindexes_generator(1),
    V = testutil:get_compressiblevalue(),
    RplKSpecL = lists:map(fun({K, Spc}) ->
                                AddSpc = lists:keyfind(add, 1, Spc),
                                {O, AltSpc} = testutil:set_object(Bucket,
                                                                    K,
                                                                    V,
                                                                    IndexGen,
                                                                    [AddSpc]),
                                ok = leveled_bookie:book_riakput(Book,
                                                                    O,
                                                                    AltSpc),
                                {K, AltSpc} end,
                            KSpecL),
    {RplKSpecL, V}.
    

