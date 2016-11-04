-module(iterator_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").

-define(KEY_ONLY, {false, undefined}).

-export([all/0]).
-export([small_load_with2i/1,
            query_count/1,
            rotating_objects/1]).

all() -> [
            small_load_with2i,
            query_count,
            rotating_objects
            ].


small_load_with2i(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 5000000}],
                    % low journal size to make sure > 1 created
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = leveled_bookie:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    testutil:check_formissingobject(Bookie1, "Bucket1", "Key2"),
    testutil:check_forobject(Bookie1, TestObject),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = testutil:get_randomindexes_generator(8),
    ObjL1 = testutil:generate_objects(10000,
                                        uuid,
                                        [],
                                        ObjectGen,
                                        IndexGen),
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        leveled_bookie:book_riakput(Bookie1, Obj, Spc) end,
                    ObjL1),
    ChkList1 = lists:sublist(lists:sort(ObjL1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    
    %% Delete the objects from the ChkList removing the indexes
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        DSpc = lists:map(fun({add, F, T}) -> {remove, F, T}
                                                                end,
                                            Spc),
                        {B, K} = leveled_codec:riakto_keydetails(Obj),
                        leveled_bookie:book_riakdelete(Bookie1, B, K, DSpc)
                        end,
                    ChkList1),
    %% Get the Buckets Keys and Hashes for the whole bucket
    FoldObjectsFun = fun(B, K, V, Acc) -> [{B, K, testutil:riak_hash(V)}|Acc]
                                            end,
    {async, HTreeF1} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_allkeys,
                                                            ?RIAK_TAG,
                                                            FoldObjectsFun}),
    KeyHashList1 = HTreeF1(),
    {async, HTreeF2} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_bybucket,
                                                            ?RIAK_TAG,
                                                            "Bucket",
                                                            FoldObjectsFun}),
    KeyHashList2 = HTreeF2(),
    {async, HTreeF3} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_byindex,
                                                            ?RIAK_TAG,
                                                            "Bucket",
                                                            {"idx1_bin",
                                                                "#", "~"},
                                                            FoldObjectsFun}),
    KeyHashList3 = HTreeF3(),
    true = 9901 == length(KeyHashList1), % also includes the test object
    true = 9900 == length(KeyHashList2),
    true = 9900 == length(KeyHashList3),
    
    SumIntegerFun = fun(_B, _K, V, Acc) ->
                                [C] = V#r_object.contents,
                                {I, _Bin} = C#r_content.value,
                                Acc + I
                                end,
    {async, Sum1} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_bybucket,
                                                            ?RIAK_TAG,
                                                            "Bucket",
                                                            {SumIntegerFun,
                                                                0}}),
    Total1 = Sum1(),
    true = Total1 > 100000, 
    
    ok = leveled_bookie:book_close(Bookie1),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    
    {async, Sum2} = leveled_bookie:book_returnfolder(Bookie2,
                                                        {foldobjects_bybucket,
                                                            ?RIAK_TAG,
                                                            "Bucket",
                                                            {SumIntegerFun,
                                                                0}}),
    Total2 = Sum2(),
    true = Total2 == Total1, 
    
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().


query_count(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Book1} = leveled_bookie:book_start(RootPath, 2000, 50000000),
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
    {ok, Book2} = leveled_bookie:book_start(RootPath, 1000, 50000000),
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
    ok = leveled_bookie:book_close(Book3),
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
    ok = testutil:rotating_object_check(RootPath, "Bucket1", 10),
    ok = testutil:rotating_object_check(RootPath, "Bucket2", 200),
    ok = testutil:rotating_object_check(RootPath, "Bucket3", 800),
    ok = testutil:rotating_object_check(RootPath, "Bucket4", 1600),
    ok = testutil:rotating_object_check(RootPath, "Bucket5", 3200),
    ok = testutil:rotating_object_check(RootPath, "Bucket6", 9600),
    testutil:reset_filestructure().




    

