-module(iterator_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").

-define(KEY_ONLY, {false, undefined}).

-export([all/0]).
-export([single_object_with2i/1,
            small_load_with2i/1,
            query_count/1,
            multibucket_fold/1,
            rotating_objects/1]).

all() -> [
            single_object_with2i,
            small_load_with2i,
            query_count,
            multibucket_fold,
            rotating_objects
            ].


single_object_with2i(_Config) ->
    % Load a single object with an integer and a binary
    % index and query for it
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 5000000},
                    {sync_strategy, testutil:sync_strategy()}],
                    % low journal size to make sure > 1 created
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, _TestSpec} = testutil:generate_testobject(),
    TestSpec = [{add, list_to_binary("integer_int"), 100},
                {add, list_to_binary("binary_bin"), <<100:32/integer>>}],
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    
    IdxQ1 = {index_query,
                "Bucket1",
                {fun testutil:foldkeysfun/3, []},
                {list_to_binary("binary_bin"),
                    <<99:32/integer>>, <<101:32/integer>>},
                {true, undefined}},
    {async, IdxFolder1} = leveled_bookie:book_returnfolder(Bookie1, IdxQ1),
    R1 = IdxFolder1(),
    io:format("R1 of ~w~n", [R1]),
    true = [{<<100:32/integer>>,"Key1"}] == R1,
    
    IdxQ2 = {index_query,
                "Bucket1",
                {fun testutil:foldkeysfun/3, []},
                {list_to_binary("integer_int"),
                    99, 101},
                {true, undefined}},
    {async, IdxFolder2} = leveled_bookie:book_returnfolder(Bookie1, IdxQ2),
    R2 = IdxFolder2(),
    io:format("R2 of ~w~n", [R2]),
    true = [{100,"Key1"}] == R2,
    
    IdxQ3 = {index_query,
                {"Bucket1", "Key1"},
                {fun testutil:foldkeysfun/3, []},
                {list_to_binary("integer_int"),
                    99, 101},
                {true, undefined}},
    {async, IdxFolder3} = leveled_bookie:book_returnfolder(Bookie1, IdxQ3),
    R3 = IdxFolder3(),
    io:format("R2 of ~w~n", [R3]),
    true = [{100,"Key1"}] == R3,
    
    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().

small_load_with2i(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 5000000},
                    {sync_strategy, testutil:sync_strategy()}],
                    % low journal size to make sure > 1 created
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
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
    testutil:riakload(Bookie1, ObjL1),
    ChkList1 = lists:sublist(lists:sort(ObjL1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    
    % Find all keys index, and then just the last key
    IdxQ1 = {index_query,
                "Bucket",
                {fun testutil:foldkeysfun/3, []},
                {"idx1_bin", "#", "|"},
                {true, undefined}},
    {async, IdxFolder} = leveled_bookie:book_returnfolder(Bookie1, IdxQ1),
    KeyList1 = lists:usort(IdxFolder()),
    true = 10000 == length(KeyList1),
    {LastTerm, LastKey} = lists:last(KeyList1),
    IdxQ2 = {index_query,
                {"Bucket", LastKey},
                {fun testutil:foldkeysfun/3, []},
                {"idx1_bin", LastTerm, "|"},
                {false, undefined}},
    {async, IdxFolderLK} = leveled_bookie:book_returnfolder(Bookie1, IdxQ2),
    KeyList2 = lists:usort(IdxFolderLK()),
    io:format("List should be last key ~w ~w~n", [LastKey, KeyList2]),
    true = 1 == length(KeyList2),
    
    %% Delete the objects from the ChkList removing the indexes
    lists:foreach(fun({_RN, Obj, Spc}) ->
                        DSpc = lists:map(fun({add, F, T}) -> {remove, F, T}
                                                                end,
                                            Spc),
                        {B, K} = {Obj#r_object.bucket, Obj#r_object.key},
                        testutil:book_riakdelete(Bookie1, B, K, DSpc)
                        end,
                    ChkList1),
    %% Get the Buckets Keys and Hashes for the whole bucket
    FoldObjectsFun = fun(B, K, V, Acc) -> [{B, K, erlang:phash2(V)}|Acc]
                                            end,
    {async, HTreeF1} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_allkeys,
                                                            ?RIAK_TAG,
                                                            FoldObjectsFun,
                                                            false}),
    KeyHashList1 = HTreeF1(),
    {async, HTreeF2} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_bybucket,
                                                            ?RIAK_TAG,
                                                            "Bucket",
                                                            all,
                                                            FoldObjectsFun,
                                                            false}),
    KeyHashList2 = HTreeF2(),
    {async, HTreeF3} = leveled_bookie:book_returnfolder(Bookie1,
                                                        {foldobjects_byindex,
                                                            ?RIAK_TAG,
                                                            "Bucket",
                                                            {"idx1_bin",
                                                                "#", "|"},
                                                            FoldObjectsFun,
                                                            false}),
    KeyHashList3 = HTreeF3(),
    true = 9901 == length(KeyHashList1), % also includes the test object
    true = 9900 == length(KeyHashList2),
    true = 9900 == length(KeyHashList3),
    
    SumIntFun = fun(_B, _K, Obj, Acc) ->
                        {I, _Bin} = testutil:get_value(Obj),
                        Acc + I
                        end,
    BucketObjQ = 
        {foldobjects_bybucket, ?RIAK_TAG, "Bucket", all, {SumIntFun, 0}, true},
    {async, Sum1} = leveled_bookie:book_returnfolder(Bookie1, BucketObjQ),
    Total1 = Sum1(),
    true = Total1 > 100000, 
    
    ok = leveled_bookie:book_close(Bookie1),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    
    {async, Sum2} = leveled_bookie:book_returnfolder(Bookie2, BucketObjQ),
    Total2 = Sum2(),
    true = Total2 == Total1, 
    
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,
    % this should find Bucket and Bucket1 - as we can now find string-based 
    % buckets using bucket_list - i.e. it isn't just binary buckets now
    BucketListQuery = {bucket_list,
                        ?RIAK_TAG,
                        {FoldBucketsFun, sets:new()}},
    {async, BL} = leveled_bookie:book_returnfolder(Bookie2, BucketListQuery),
    true = sets:size(BL()) == 2,
    
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().


query_count(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Book1} = leveled_bookie:book_start(RootPath,
                                            2000,
                                            50000000,
                                            testutil:sync_strategy()),
    BucketBin = list_to_binary("Bucket"),
    {TestObject, TestSpec} = testutil:generate_testobject(BucketBin,
                                                            term_to_binary("Key1"),
                                                            "Value1",
                                                            [],
                                                            [{"MDK1", "MDV1"}]),
    ok = testutil:book_riakput(Book1, TestObject, TestSpec),
    testutil:check_forobject(Book1, TestObject),
    testutil:check_formissingobject(Book1, "Bucket1", "Key2"),
    testutil:check_forobject(Book1, TestObject),
    lists:foreach(fun(_X) ->
                        V = testutil:get_compressiblevalue(),
                        Indexes = testutil:get_randomindexes_generator(8),
                        SW = os:timestamp(),
                        ObjL1 = testutil:generate_objects(10000,
                                                            binary_uuid,
                                                            [],
                                                            V,
                                                            Indexes),
                        testutil:riakload(Book1, ObjL1),
                        io:format("Put of 10000 objects with 8 index entries "
                                        ++
                                        "each completed in ~w microseconds~n",
                                    [timer:now_diff(os:timestamp(), SW)])
                        end,
                        lists:seq(1, 8)),
    testutil:check_forobject(Book1, TestObject),
    Total = lists:foldl(fun(X, Acc) ->
                                IdxF = "idx" ++ integer_to_list(X) ++ "_bin",
                                T = count_termsonindex(BucketBin,
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
    Index1Count = count_termsonindex(BucketBin,
                                        "idx1_bin",
                                        Book1,
                                        ?KEY_ONLY),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(RootPath,
                                            1000,
                                            50000000,
                                            testutil:sync_strategy()),
    Index1Count = count_termsonindex(BucketBin,
                                        "idx1_bin",
                                        Book2,
                                        ?KEY_ONLY),
    NameList = testutil:name_list(),
    TotalNameByName = lists:foldl(fun({_X, Name}, Acc) ->
                                        {ok, Regex} = re:compile("[0-9]+" ++
                                                                    Name),
                                        SW = os:timestamp(),
                                        T = count_termsonindex(BucketBin,
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
    Query1 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {"idx2_bin", "2000", "2000|"},
                {false, RegMia}},
    {async,
        Mia2KFolder1} = leveled_bookie:book_returnfolder(Book2, Query1),
    Mia2000Count1 = length(Mia2KFolder1()),
    Query2 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {"idx2_bin", "2000", "2001"},
                {true, undefined}},
    {async,
        Mia2KFolder2} = leveled_bookie:book_returnfolder(Book2, Query2),
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
    Query3 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {"idx2_bin", "1980", "2100"},
                {false, RxMia2K}},
    {async,
        Mia2KFolder3} = leveled_bookie:book_returnfolder(Book2, Query3),
    Mia2000Count1 = length(Mia2KFolder3()),
    
    V9 = testutil:get_compressiblevalue(),
    Indexes9 = testutil:get_randomindexes_generator(8),
    [{_RN, Obj9, Spc9}] = testutil:generate_objects(1,
                                                    binary_uuid,
                                                    [],
                                                    V9,
                                                    Indexes9),
    ok = testutil:book_riakput(Book2, Obj9, Spc9),
    R9 = lists:map(fun({add, IdxF, IdxT}) ->
                        Q = {index_query,
                                BucketBin,
                                {fun testutil:foldkeysfun/3, []},
                                {IdxF, IdxT, IdxT},
                                ?KEY_ONLY},
                        R = leveled_bookie:book_returnfolder(Book2, Q),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            X when X > 0 ->
                                {IdxF, IdxT, X}
                        end
                        end,
                    Spc9),
    Spc9Del = lists:map(fun({add, IdxF, IdxT}) -> {remove, IdxF, IdxT} end,
                        Spc9),
    ok = testutil:book_riakput(Book2, Obj9, Spc9Del),
    lists:foreach(fun({IdxF, IdxT, X}) ->
                        Q = {index_query,
                                BucketBin,
                                {fun testutil:foldkeysfun/3, []},
                                {IdxF, IdxT, IdxT},
                                ?KEY_ONLY},
                        R = leveled_bookie:book_returnfolder(Book2, Q),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            Y ->
                                Y = X - 1
                        end
                        end,
                    R9),
    ok = leveled_bookie:book_close(Book2),
    {ok, Book3} = leveled_bookie:book_start(RootPath,
                                            2000,
                                            50000000,
                                            testutil:sync_strategy()),
    lists:foreach(fun({IdxF, IdxT, X}) ->
                        Q = {index_query,
                                BucketBin,
                                {fun testutil:foldkeysfun/3, []},
                                {IdxF, IdxT, IdxT},
                                ?KEY_ONLY},
                        R = leveled_bookie:book_returnfolder(Book3, Q),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            Y ->
                                Y = X - 1
                        end
                        end,
                    R9),
    ok = testutil:book_riakput(Book3, Obj9, Spc9),
    ok = leveled_bookie:book_close(Book3),
    {ok, Book4} = leveled_bookie:book_start(RootPath,
                                            2000,
                                            50000000,
                                            testutil:sync_strategy()),
    lists:foreach(fun({IdxF, IdxT, X}) ->
                        Q = {index_query,
                                BucketBin,
                                {fun testutil:foldkeysfun/3, []},
                                {IdxF, IdxT, IdxT},
                                ?KEY_ONLY},
                        R = leveled_bookie:book_returnfolder(Book4, Q),
                        {async, Fldr} = R,
                        case length(Fldr()) of
                            X ->
                                ok
                        end
                        end,
                    R9),
    testutil:check_forobject(Book4, TestObject),
    
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,
    BucketListQuery = {bucket_list,
                        ?RIAK_TAG,
                        {FoldBucketsFun, sets:new()}},
    {async, BLF1} = leveled_bookie:book_returnfolder(Book4, BucketListQuery),
    SW_QA = os:timestamp(),
    BucketSet1 = BLF1(),
    io:format("Bucket set returned in ~w microseconds",
                [timer:now_diff(os:timestamp(), SW_QA)]),
    
    true = sets:size(BucketSet1) == 1, 
    
    ObjList10A = testutil:generate_objects(5000,
                                            binary_uuid,
                                            [],
                                            V9,
                                            Indexes9,
                                            "BucketA"),
    ObjList10B = testutil:generate_objects(5000,
                                            binary_uuid,
                                            [],
                                            V9,
                                            Indexes9,
                                            "BucketB"),
    ObjList10C = testutil:generate_objects(5000,
                                            binary_uuid,
                                            [],
                                            V9,
                                            Indexes9,
                                            "BucketC"),
    testutil:riakload(Book4, ObjList10A),
    testutil:riakload(Book4, ObjList10B),
    testutil:riakload(Book4, ObjList10C),
    {async, BLF2} = leveled_bookie:book_returnfolder(Book4, BucketListQuery),
    SW_QB = os:timestamp(),
    BucketSet2 = BLF2(),
    io:format("Bucket set returned in ~w microseconds",
                [timer:now_diff(os:timestamp(), SW_QB)]),
    true = sets:size(BucketSet2) == 4,
    
    ok = leveled_bookie:book_close(Book4),
    
    {ok, Book5} = leveled_bookie:book_start(RootPath,
                                            2000,
                                            50000000,
                                            testutil:sync_strategy()),
    {async, BLF3} = leveled_bookie:book_returnfolder(Book5, BucketListQuery),
    SW_QC = os:timestamp(),
    BucketSet3 = BLF3(),
    io:format("Bucket set returned in ~w microseconds",
                [timer:now_diff(os:timestamp(), SW_QC)]),
    true = sets:size(BucketSet3) == 4,
    
    ok = leveled_bookie:book_close(Book5),
    
    testutil:reset_filestructure().
    


count_termsonindex(Bucket, IdxField, Book, QType) ->
    lists:foldl(fun(X, Acc) ->
                        SW = os:timestamp(),
                        ST = integer_to_list(X),
                        ET = ST ++ "|",
                        Q = {index_query,
                                Bucket,
                                {fun testutil:foldkeysfun/3, []},
                                {IdxField, ST, ET},
                                QType},
                        R = leveled_bookie:book_returnfolder(Book, Q),
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
                    lists:seq(190, 221)).

multibucket_fold(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} = leveled_bookie:book_start(RootPath,
                                            2000,
                                            50000000,
                                            testutil:sync_strategy()),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 = testutil:generate_objects(13000,
                                        uuid,
                                        [],
                                        ObjectGen,
                                        IndexGen,
                                        <<"Bucket1">>),
    testutil:riakload(Bookie1, ObjL1),
    ObjL2 = testutil:generate_objects(17000,
                                        uuid,
                                        [],
                                        ObjectGen,
                                        IndexGen,
                                        <<"Bucket2">>),
    testutil:riakload(Bookie1, ObjL2),
    ObjL3 = testutil:generate_objects(7000,
                                        uuid,
                                        [],
                                        ObjectGen,
                                        IndexGen,
                                        <<"Bucket3">>),
    testutil:riakload(Bookie1, ObjL3),
    ObjL4 = testutil:generate_objects(23000,
                                        uuid,
                                        [],
                                        ObjectGen,
                                        IndexGen,
                                        <<"Bucket4">>),
    testutil:riakload(Bookie1, ObjL4),
    Q1 = {foldheads_bybucket,
                ?RIAK_TAG, 
                [<<"Bucket1">>, <<"Bucket4">>], bucket_list,
                fun(B, K, _PO, Acc) ->
                    [{B, K}|Acc]
                end,
                false, 
                true, 
                false},
    {async, R1} = leveled_bookie:book_returnfolder(Bookie1, Q1),
    O1 = length(R1()),
    io:format("Result R1 of length ~w~n", [O1]),
    
    Q2 = {foldheads_bybucket,
                ?RIAK_TAG, 
                [<<"Bucket2">>, <<"Bucket3">>], bucket_list,
                {fun(_B, _K, _PO, Acc) ->
                        Acc +1
                    end,
                    0},
                false, 
                true, 
                false},
    {async, R2} = leveled_bookie:book_returnfolder(Bookie1, Q2),
    O2 = R2(),
    io:format("Result R2 of ~w~n", [O2]),

    true = 36000 == O1,
    true = 24000 == O2,
    
    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().





rotating_objects(_Config) ->
    RootPath = testutil:reset_filestructure(),
    ok = testutil:rotating_object_check(RootPath, "Bucket1", 10),
    ok = testutil:rotating_object_check(RootPath, "Bucket2", 200),
    ok = testutil:rotating_object_check(RootPath, "Bucket3", 800),
    ok = testutil:rotating_object_check(RootPath, "Bucket4", 1600),
    ok = testutil:rotating_object_check(RootPath, "Bucket5", 3200),
    ok = testutil:rotating_object_check(RootPath, "Bucket6", 9600),
    testutil:reset_filestructure().




    

