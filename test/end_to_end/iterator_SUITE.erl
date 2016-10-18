-module(iterator_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").

-export([all/0]).
-export([simple_load_with2i/1,
            simple_querycount/1]).

all() -> [simple_load_with2i,
            simple_querycount].


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
    StartOpts1 = #bookie_options{root_path=RootPath,
                                    max_journalsize=50000000},
    {ok, Book1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
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
                                                        {false, undefined}),
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
                                        {false, undefined}),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(StartOpts1),
    Index1Count = count_termsonindex("Bucket",
                                        "idx1_bin",
                                        Book2,
                                        {false, undefined}),
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
    RegMia = re:compile("[0-9]+Mia"),
    {async,
        Mia2KFolder1} = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {"idx2_bin",
                                                                    "2000L",
                                                                    "2000N~"},
                                                                {false,
                                                                    RegMia}}),
    Mia2000Count1 = length(Mia2KFolder1()),
    {async,
        Mia2KFolder2} = leveled_bookie:book_returnfolder(Book2,
                                                            {index_query,
                                                                "Bucket",
                                                                {"idx2_bin",
                                                                    "2000Ma",
                                                                    "2000Mz"},
                                                                {true,
                                                                    undefined}}),
    Mia2000Count2 = lists:foldl(fun({Term, _Key}, Acc) ->
                                    case Term of
                                        "2000Mia" ->
                                            Acc + 1;
                                        _ ->
                                            Acc
                                    end end,
                                0,
                                Mia2KFolder2()),
    ok = case Mia2000Count2 of
                Mia2000Count1 ->
                    ok
            end,
    ok = leveled_bookie:book_close(Book2),
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
