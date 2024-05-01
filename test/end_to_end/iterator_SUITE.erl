-module(iterator_SUITE).

-include("include/leveled.hrl").

-define(KEY_ONLY, {false, undefined}).

-export([all/0]).
-export([expiring_indexes/1,
            breaking_folds/1,
            single_object_with2i/1,
            small_load_with2i/1,
            query_count/1,
            multibucket_fold/1,
            foldobjects_bybucket_range/1,
            rotating_objects/1,
            capture_and_filter_terms/1
        ]).

all() -> [
            expiring_indexes,
            breaking_folds,
            single_object_with2i,
            small_load_with2i,
            query_count,
            multibucket_fold,
            rotating_objects,
            foldobjects_bybucket_range,
            capture_and_filter_terms
            ].

expiring_indexes(_Config) ->
    % Add objects to the store with index entries, where the objects (and hence
    % the indexes have an expiry time.  Confirm that the indexes and the
    % objects are no longer present after the expiry time (and are present
    % before).  Confirm that replacing an object has the expected outcome, if
    % the IndexSpecs are updated as part of the request.
    KeyCount = 50000,
    Future = 120,
        % 2 minutes - if running tests on a slow machine, may need to increase
        % this value
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = 
        [{root_path, RootPath},
            {max_pencillercachesize, 16000},
            {max_journalobjectcount, 30000},
            {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    SW1 = os:timestamp(),
    timer:sleep(1000),

    V1 = <<"V1">>,
    Indexes9 = testutil:get_randomindexes_generator(2),
    TempRiakObjects =
        testutil:generate_objects(
            KeyCount, binary_uuid, [], V1, Indexes9, "riakBucket"),
    
    IBKL1 = testutil:stdload_expiring(Bookie1, KeyCount, Future),
    lists:foreach(
        fun({_RN, Obj, Spc}) ->
            testutil:book_tempriakput(
                Bookie1, Obj, Spc, leveled_util:integer_now() + Future)
        end,
        TempRiakObjects
    ),
    timer:sleep(1000),
        % Wait a second after last key so that none loaded in the last second
    LoadTime = timer:now_diff(os:timestamp(), SW1)/1000000,
    io:format("Load of ~w std objects in ~w seconds~n",  [KeyCount, LoadTime]),
    
    timer:sleep(1000),
    SW2 = os:timestamp(),

    FilterFun = fun({I, _B, _K}) -> lists:member(I, [5, 6, 7, 8]) end,
    LoadedEntriesInRange = lists:sort(lists:filter(FilterFun, IBKL1)),

    true = LoadTime < (Future - 20), 
        % need 20 seconds spare to run query
        % and add replacements

    {I0, B0, K0} = hd(IBKL1),
    false = FilterFun(hd(IBKL1)), 
        % The head entry should not have index between 5 and 8

    CountI0Fold =
        fun() ->
            leveled_bookie:book_indexfold(
                Bookie1,
                B0,
                {fun(_BF, _KT, Acc) -> Acc + 1 end, 0},
                {<<"temp_int">>, I0, I0},
                {true, undefined})
        end,
    {async, I0Counter1} = CountI0Fold(),
    I0Count1 = I0Counter1(),

    HeadFold =
        fun(LowTS, HighTS) ->
            leveled_bookie:book_headfold(
                Bookie1,
                ?RIAK_TAG,
                {range, <<"riakBucket">>, all},
                {fun(_B, _K, _V, Acc) ->  Acc + 1 end, 0},
                false, true, false,
                {testutil:convert_to_seconds(LowTS),
                    testutil:convert_to_seconds(HighTS)},
                false
            )
        end,
    {async, HeadCount0Fun} = HeadFold(SW1, SW2),
    {async, HeadCount1Fun} = HeadFold(SW2, os:timestamp()),
    HeadCounts = {HeadCount0Fun(), HeadCount1Fun()},
    io:format("HeadCounts ~w before expiry~n", [HeadCounts]),
    {KeyCount, 0} = HeadCounts,

    FoldFun = fun(BF, {IdxV, KeyF}, Acc) -> [{IdxV, BF, KeyF}|Acc] end,
    InitAcc = [],
    IndexFold = 
        fun() ->
            leveled_bookie:book_indexfold(
                Bookie1,
                B0,
                {FoldFun, InitAcc},
                {<<"temp_int">>, 5, 8},
                {true, undefined})
        end,

    {async, Folder1} = IndexFold(),
    QR1 = Folder1(),
    true = lists:sort(QR1) == LoadedEntriesInRange,
    % Replace object with one with an index value of 6
    testutil:stdload_object(
        Bookie1, B0, K0, 6, <<"value">>, leveled_util:integer_now() + 600),
    % Confirm that this has reduced the index entries in I0 by 1
    {async, I0Counter2} = CountI0Fold(),
    I0Count2 = I0Counter2(),
    io:format("Count with index value ~w changed from ~w to ~w~n",
                [I0, I0Count1, I0Count2]),
    true = I0Count2 == (I0Count1 - 1),
    % Now replace again, shortening the timeout to 10s,
    % this time index value of 6
    testutil:stdload_object(
        Bookie1, B0, K0, 5, <<"value">>, leveled_util:integer_now() + 10),
    timer:sleep(1000),
    {async, Folder2} = IndexFold(),
    QR2 = Folder2(),
    io:format("Query with additional entry length ~w~n", [length(QR2)]),
    true = lists:sort(QR2) == lists:sort([{5, B0, K0}|LoadedEntriesInRange]),
    % Wait for a 10s timeout plus a second to be sure
    timer:sleep(10000 + 1000),
    {async, Folder3} = IndexFold(),
    QR3 = Folder3(),
    % Now the entry should be missing (and the 600s TTL entry should not have
    % resurfaced)
    io:format("Query results length ~w following sleep~n", [length(QR3)]),
    true = lists:sort(QR3) == LoadedEntriesInRange,

    FoldTime = timer:now_diff(os:timestamp(), SW1)/1000000 - LoadTime,
    io:format("Query returned ~w entries in ~w seconds - 3 queries + 10s wait~n",
                [length(QR3), FoldTime]),
    
    SleepTime =
        (Future - (timer:now_diff(os:timestamp(), SW2) div (1000 * 1000))) + 1,

    io:format("Sleeping ~w s for all to expire~n", [SleepTime]),
    timer:sleep(SleepTime * 1000),

    % Index entries should now have expired
    {async, Folder4} = IndexFold(),
    QR4 = Folder4(),
    io:format("QR4 Unexpired indexes of length ~w~n", [length(QR4)]),
    timer:sleep(1000),
    {async, Folder5} = IndexFold(),
    QR5 = Folder5(),
    io:format("QR5 Unexpired indexes of length ~w~n", [length(QR5)]),
    
    true = QR4 == [],
    true = QR5 == [],

    {async, HeadCount0ExpFun} = HeadFold(SW1, SW2),
    {async, HeadCount1ExpFun} = HeadFold(SW2, os:timestamp()),
    HeadCountsExp = {HeadCount0ExpFun(), HeadCount1ExpFun()},
    io:format("HeadCounts ~w after expiry~n", [HeadCountsExp]),
    {0, 0} = HeadCountsExp,

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().


breaking_folds(_Config) ->
    % Run various iterators and show that they can be broken by throwing an
    % exception from within the fold
    KeyCount = 10000,

    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 10000000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    V1 = testutil:get_compressiblevalue_andinteger(),
    IndexGen = testutil:get_randomindexes_generator(8),
    ObjL1 =
        testutil:generate_objects(
            KeyCount, binary_uuid, [], V1, IndexGen),
    testutil:riakload(Bookie1, ObjL1),

    % Find all keys index, and then same again but stop at a midpoint using a
    % throw
    {async, IdxFolder} =
        leveled_bookie:book_indexfold(
            Bookie1,
            list_to_binary("Bucket"), 
            {fun testutil:foldkeysfun/3, []}, 
            {<<"idx1_bin">>, <<"#">>, <<"|">>},
            {true, undefined}),
    KeyList1 = lists:reverse(IdxFolder()),
    io:format("Index fold with result size ~w~n", [length(KeyList1)]),
    true = KeyCount == length(KeyList1),


    {MidTerm, MidKey} = lists:nth(KeyCount div 2, KeyList1),
    
    FoldKeyThrowFun =
        fun(_B, {Term, Key}, Acc) ->
            case {Term, Key} > {MidTerm, MidKey} of
                true ->
                    throw({stop_fold, Acc});
                false ->
                    [{Term, Key}|Acc]
            end
        end,
    {async, IdxFolderToMidK} =
        leveled_bookie:book_indexfold(
            Bookie1,
            list_to_binary("Bucket"), 
            {FoldKeyThrowFun, []}, 
            {<<"idx1_bin">>, <<"#">>, <<"|">>},
            {true, undefined}),
    CatchingFold =
        fun(AsyncFolder) ->
            try
                AsyncFolder()
            catch
                throw:{stop_fold, Acc} ->
                    Acc
            end
        end,

    KeyList2 = lists:reverse(CatchingFold(IdxFolderToMidK)),
    io:format("Index fold with result size ~w~n", [length(KeyList2)]),
    true = KeyCount div 2 == length(KeyList2),


    HeadFoldFun = 
        fun(_B, K, PO, Acc) ->
            {proxy_object, _MDBin, Size, _FF} = binary_to_term(PO),
            [{K, Size}|Acc]
        end,
    {async, HeadFolder} = 
        leveled_bookie:book_headfold(
            Bookie1, ?RIAK_TAG,  {HeadFoldFun, []}, true, true, false),
    KeySizeList1 = lists:reverse(HeadFolder()),
    io:format("Head fold with result size ~w~n", [length(KeySizeList1)]),
    true = KeyCount == length(KeySizeList1),

    {MidHeadKey, _MidSize} = lists:nth(KeyCount div 2, KeySizeList1),
    FoldThrowFun =
        fun(FoldFun) ->
            fun(B, K, PO, Acc) ->
                case K > MidHeadKey of
                    true ->
                        throw({stop_fold, Acc});
                    false ->
                        FoldFun(B, K, PO, Acc)
                end
            end
        end,
    {async, HeadFolderToMidK} = 
        leveled_bookie:book_headfold(
            Bookie1,
            ?RIAK_TAG, 
            {FoldThrowFun(HeadFoldFun), []}, 
            true, true, false),
    KeySizeList2 = lists:reverse(CatchingFold(HeadFolderToMidK)),
    io:format("Head fold with result size ~w~n", [length(KeySizeList2)]),
    true = KeyCount div 2 == length(KeySizeList2),    

    ObjFoldFun = 
        fun(_B, K, V, Acc) ->
            [{K,byte_size(V)}|Acc]
        end,
    {async, ObjectFolderKO} = 
        leveled_bookie:book_objectfold(
            Bookie1, ?RIAK_TAG, {ObjFoldFun, []}, false, key_order),
    ObjSizeList1 = lists:reverse(ObjectFolderKO()),
    io:format("Obj fold with result size ~w~n", [length(ObjSizeList1)]),
    true = KeyCount == length(ObjSizeList1),

    {async, ObjFolderToMidK} = 
        leveled_bookie:book_objectfold(
            Bookie1,
            ?RIAK_TAG, 
            {FoldThrowFun(ObjFoldFun), []},
            false,
            key_order),
    ObjSizeList2 = lists:reverse(CatchingFold(ObjFolderToMidK)),
    io:format("Object fold with result size ~w~n", [length(ObjSizeList2)]),
    true = KeyCount div 2 == length(ObjSizeList2),  

    % Object folds which are SQN order use a different path through the code,
    % so testing that here.  Note that it would not make sense to have a fold
    % that was terminated by reaching a point in the key range .. as results
    % will not be passed to the fold function in key order
    {async, ObjectFolderSO} = 
        leveled_bookie:book_objectfold(
            Bookie1, ?RIAK_TAG, {ObjFoldFun, []}, false, sqn_order),
    ObjSizeList1_SO = lists:reverse(ObjectFolderSO()),
    io:format("Obj fold with result size ~w~n", [length(ObjSizeList1_SO)]),
    true = KeyCount == length(ObjSizeList1_SO),

    % Exit fold when we've reached a thousand accumulated obects
    FoldThrowThousandFun =
        fun(FoldFun) ->
            fun(B, K, PO, Acc) ->
                case length(Acc) == 1000 of
                    true ->
                        throw({stop_fold, Acc});
                    false ->
                        FoldFun(B, K, PO, Acc)
                end
            end
        end,
    {async, ObjFolderTo1K} = 
        leveled_bookie:book_objectfold(
            Bookie1,
            ?RIAK_TAG, 
            {FoldThrowThousandFun(ObjFoldFun), []}, 
            false,
            sqn_order),
    ObjSizeList2_SO = lists:reverse(CatchingFold(ObjFolderTo1K)),
    io:format("Object fold with result size ~w~n", [length(ObjSizeList2_SO)]),
    true = 1000 == length(ObjSizeList2_SO),

    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    ObjL2 =
        testutil:generate_objects(
            10, binary_uuid, [], ObjectGen, IndexGen, "B2"),
    ObjL3 =
        testutil:generate_objects(
            10, binary_uuid, [], ObjectGen, IndexGen, "B3"),
    ObjL4 =
        testutil:generate_objects(
            10, binary_uuid, [], ObjectGen, IndexGen, "B4"),
    testutil:riakload(Bookie1, ObjL2),
    testutil:riakload(Bookie1, ObjL3),
    testutil:riakload(Bookie1, ObjL4),

    FBAccT = {fun(B, Acc) -> [B|Acc] end, []},
    {async, BucketFolder} = 
        leveled_bookie:book_bucketlist(Bookie1, ?RIAK_TAG, FBAccT, all),
    BucketList1 = lists:reverse(BucketFolder()),
    io:format("bucket list with result size ~w~n", [length(BucketList1)]),
    true = 4 == length(BucketList1),

    StopAt3Fun =
        fun(B, Acc) ->
            Acc0 = [B|Acc],
            case B of
                <<"B3">> ->
                    throw({stop_fold, Acc0});
                _ ->
                    Acc0
            end
        end,
    
    {async, StopAt3BucketFolder} = 
        leveled_bookie:book_bucketlist(
            Bookie1, ?RIAK_TAG, {StopAt3Fun, []}, all),
    BucketListSA3 = lists:reverse(CatchingFold(StopAt3BucketFolder)),
    io:format("bucket list with result ~w~n", [BucketListSA3]),
    true = [<<"B2">>, <<"B3">>] == BucketListSA3,

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().



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

    %% @TODO replace all index queries with new Top-Level API if tests
    %% pass
    {async, IdxFolder1} =
        leveled_bookie:book_indexfold(
            Bookie1,
            "Bucket1",
            {fun testutil:foldkeysfun/3, []},
            {list_to_binary("binary_bin"),
                <<99:32/integer>>, <<101:32/integer>>},
            {true, undefined}),
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
    ObjL1 =
        testutil:generate_objects(
            10000, uuid, [], ObjectGen, IndexGen),
    testutil:riakload(Bookie1, ObjL1),
    ChkList1 = lists:sublist(lists:sort(ObjL1), 100),
    testutil:check_forlist(Bookie1, ChkList1),
    testutil:check_forobject(Bookie1, TestObject),
    
    % Find all keys index, and then just the last key
    IdxQ1 = {index_query,
                "Bucket",
                {fun testutil:foldkeysfun/3, []},
                {<<"idx1_bin">>, <<"#">>, <<"|">>},
                {true, undefined}},
    {async, IdxFolder} = leveled_bookie:book_returnfolder(Bookie1, IdxQ1),
    KeyList1 = lists:usort(IdxFolder()),
    true = 10000 == length(KeyList1),
    {LastTerm, LastKey} = lists:last(KeyList1),
    IdxQ2 = {index_query,
                {"Bucket", LastKey},
                {fun testutil:foldkeysfun/3, []},
                {<<"idx1_bin">>, LastTerm, <<"|">>},
                {false, undefined}},
    {async, IdxFolderLK} = leveled_bookie:book_returnfolder(Bookie1, IdxQ2),
    KeyList2 = lists:usort(IdxFolderLK()),
    io:format("List should be last key ~w ~w~n", [LastKey, KeyList2]),
    true = 1 == length(KeyList2),
    
    %% Delete the objects from the ChkList removing the indexes
    lists:foreach(
        fun({_RN, Obj, Spc}) ->
            DSpc = lists:map(fun({add, F, T}) -> 
                                    {remove, F, T}
                                end,
                                Spc),
            {B, K} =
                {testutil:get_bucket(Obj), testutil:get_key(Obj)},
            testutil:book_riakdelete(Bookie1, B, K, DSpc)
        end,
        ChkList1),
    %% Get the Buckets Keys and Hashes for the whole bucket
    FoldObjectsFun =
        fun(B, K, V, Acc) -> [{B, K, erlang:phash2(V)}|Acc] end,

    {async, HTreeF1} =
        leveled_bookie:book_objectfold(
            Bookie1, ?RIAK_TAG, {FoldObjectsFun, []}, false),

    KeyHashList1 = HTreeF1(),
    {async, HTreeF2} =
        leveled_bookie:book_objectfold(
            Bookie1, ?RIAK_TAG, "Bucket", all, {FoldObjectsFun, []}, false),
    KeyHashList2 = HTreeF2(),
    {async, HTreeF3} =
        leveled_bookie:book_objectfold(
            Bookie1,
            ?RIAK_TAG,
            "Bucket",
            {<<"idx1_bin">>, <<"#">>, <<"|">>},
            {FoldObjectsFun, []},
            false),
    KeyHashList3 = HTreeF3(),
    true = 9901 == length(KeyHashList1), % also includes the test object
    true = 9900 == length(KeyHashList2),
    true = 9900 == length(KeyHashList3),
    
    SumIntFun =
        fun(_B, _K, Obj, Acc) ->
            {I, _Bin} = testutil:get_value(Obj),
            Acc + I
        end,
    BucketObjQ = 
        {foldobjects_bybucket, ?RIAK_TAG, "Bucket", all, {SumIntFun, 0}, true},
    {async, Sum1} = leveled_bookie:book_returnfolder(Bookie1, BucketObjQ),
    Total1 = Sum1(),
    io:format("Total from summing all I is ~w~n", [Total1]),
    SumFromObjLFun = 
        fun(Obj, Acc) ->
            {I, _Bin} = testutil:get_value_from_objectlistitem(Obj),
            Acc + I
        end,
    ObjL1Total = 
        lists:foldl(SumFromObjLFun, 0, ObjL1),
    ChkList1Total = 
        lists:foldl(SumFromObjLFun, 0, ChkList1),
    io:format(
        "Total in original object list ~w and from removed list ~w~n", 
        [ObjL1Total, ChkList1Total]),

    Total1 = ObjL1Total - ChkList1Total, 
    
    ok = leveled_bookie:book_close(Bookie1),
    
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    
    {async, Sum2} = leveled_bookie:book_returnfolder(Bookie2, BucketObjQ),
    Total2 = Sum2(),
    true = Total2 == Total1, 
    
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,

    % this should find Bucket and Bucket1 - as we can now find string-based 
    % buckets using bucket_list - i.e. it isn't just binary buckets now
    {async, BL} = leveled_bookie:book_bucketlist(Bookie2, ?RIAK_TAG, {FoldBucketsFun, sets:new()}, all),
    true = sets:size(BL()) == 2,

    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().


query_count(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Book1} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    BucketBin = list_to_binary("Bucket"),
    {TestObject, TestSpec} =
        testutil:generate_testobject(
            BucketBin, term_to_binary("Key1"), "Value1", [], [{"MDK1", "MDV1"}]),
    ok = testutil:book_riakput(Book1, TestObject, TestSpec),
    testutil:check_forobject(Book1, TestObject),
    testutil:check_formissingobject(Book1, "Bucket1", "Key2"),
    testutil:check_forobject(Book1, TestObject),
    lists:foreach(
        fun(_X) ->
            V = <<"TestValue">>,
            Indexes = testutil:get_randomindexes_generator(8),
            SW = os:timestamp(),
            ObjL1 =
                testutil:generate_objects(
                    10000, binary_uuid, [], V, Indexes),
            testutil:riakload(Book1, ObjL1),
            io:format(
                "Put of 10000 objects with 8 index entries "
                "each completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW)])
        end,
        lists:seq(1, 8)),
    testutil:check_forobject(Book1, TestObject),
    Total =
        lists:foldl(
            fun(X, Acc) ->
                IdxF = "idx" ++ integer_to_list(X) ++ "_bin",
                T =
                    count_termsonindex(
                        BucketBin, list_to_binary(IdxF), Book1, ?KEY_ONLY),
                io:format("~w terms found on index ~s~n", [T, IdxF]),
                Acc + T
            end,
            0,
            lists:seq(1, 8)),
    true = Total == 640000,
    Index1Count =
        count_termsonindex(
            BucketBin, <<"idx1_bin">>, Book1, ?KEY_ONLY),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} =
        leveled_bookie:book_start(
            RootPath, 1000, 50000000, testutil:sync_strategy()),
    Index1Count =
        count_termsonindex(
            BucketBin, <<"idx1_bin">>, Book2, ?KEY_ONLY),
    NameList = testutil:name_list(),
    TotalNameByName =
        lists:foldl(
            fun({_X, Name}, Acc) ->
                {ok, Regex} =
                    re:compile("[0-9]+" ++ Name),
                SW = os:timestamp(),
                T =
                    count_termsonindex(
                        BucketBin,
                        list_to_binary("idx1_bin"),
                        Book2,
                        {false, Regex}),
                TD = timer:now_diff(os:timestamp(), SW),
                io:format(
                    "~w terms found on  index idx1 with a "
                    "regex in ~w  microseconds~n",
                    [T, TD]),
                Acc + T
            end,
            0,
            NameList),
    true = TotalNameByName == Index1Count,
    {ok, RegMia} = re:compile("[0-9]+Mia"),
    Query1 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {<<"idx2_bin">>, <<"2000">>, <<"2000|">>},
                {false, RegMia}},
    {async,
        Mia2KFolder1} = leveled_bookie:book_returnfolder(Book2, Query1),
    Mia2000Count1 = length(Mia2KFolder1()),
    Query2 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {<<"idx2_bin">>, <<"2000">>, <<"2001">>},
                {true, undefined}},
    {async,
        Mia2KFolder2} = leveled_bookie:book_returnfolder(Book2, Query2),
    Mia2000Count2 =
        lists:foldl(
            fun({Term, _Key}, Acc) ->
                case leveled_util:regex_run(Term, RegMia, []) of
                    nomatch ->
                        Acc;
                    _ ->
                        Acc + 1
                end
            end,
            0,
            Mia2KFolder2()),
    ok = case Mia2000Count2 of
                Mia2000Count1 when Mia2000Count1 > 0 ->
                    io:format("Mia2000 counts match at ~w~n",
                                [Mia2000Count1]),
                    ok
            end,
    {ok, RxMia2K} = leveled_util:regex_compile("^2000[0-9]+Mia"),
    Query3 = {index_query,
                BucketBin,
                {fun testutil:foldkeysfun/3, []},
                {<<"idx2_bin">>, <<"1980">>, <<"2100">>},
                {false, RxMia2K}},
    {async,  Mia2KFolder3} = leveled_bookie:book_returnfolder(Book2, Query3),
    Mia2000Count1 = length(Mia2KFolder3()),
    {ok, RxMia2KPCRE} = re:compile("^2000[0-9]+Mia"),
    Query3PCRE =
        {index_query,
            BucketBin,
            {fun testutil:foldkeysfun/3, []},
            {<<"idx2_bin">>, <<"1980">>, <<"2100">>},
            {false, RxMia2KPCRE}},
    {async, Mia2KFolder3PCRE} =
        leveled_bookie:book_returnfolder(Book2, Query3PCRE),
    Mia2000Count1 = length(Mia2KFolder3PCRE()),
    
    V9 = testutil:get_compressiblevalue(),
    Indexes9 = testutil:get_randomindexes_generator(8),
    [{_RN, Obj9, Spc9}] =
        testutil:generate_objects(
            1, binary_uuid, [], V9, Indexes9),
    ok = testutil:book_riakput(Book2, Obj9, Spc9),
    R9 =
        lists:map(
            fun({add, IdxF, IdxT}) ->
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
    {ok, Book3} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    lists:foreach(
        fun({IdxF, IdxT, X}) ->
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
    {ok, Book4} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    lists:foreach(
        fun({IdxF, IdxT, X}) ->
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
    
    ObjList10A =
        testutil:generate_objects(
            5000, binary_uuid, [], V9, Indexes9, "BucketA"),
    ObjList10B =
        testutil:generate_objects(
            5000, binary_uuid, [], V9, Indexes9, "BucketB"),
    ObjList10C =
        testutil:generate_objects(
            5000, binary_uuid, [], V9, Indexes9, "BucketC"),
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
    
    {ok, Book5} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    {async, BLF3} = leveled_bookie:book_returnfolder(Book5, BucketListQuery),
    SW_QC = os:timestamp(),
    BucketSet3 = BLF3(),
    io:format("Bucket set returned in ~w microseconds",
                [timer:now_diff(os:timestamp(), SW_QC)]),
    true = sets:size(BucketSet3) == 4,
    
    ok = leveled_bookie:book_close(Book5),
    
    testutil:reset_filestructure().


capture_and_filter_terms(_Config) ->
    RootPath = testutil:reset_filestructure(),
    Bucket = {<<"Type1">>, <<"Bucket1">>},
    IdxName = <<"people_bin">>,
    {ok, Book1} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    V1 = <<"V1">>,
    IndexGen =
        fun() ->
            [{add, IdxName, list_to_binary(perf_SUITE:random_people_index())}]
        end,
    ObjL1 =
        testutil:generate_objects(
            100000, uuid, [], V1, IndexGen, Bucket),
    testutil:riakload(Book1, ObjL1),

    StartDoB = <<"19740301">>,
    EndDoB = <<"19761031">>,

    WillowLeedsFinder =
        "[^\\|]*\\|[0-9]{8}\\|[0-9]{0,8}\\|[^\\|]*#Willow[^\\|]*\\|"
        "[^\\|]*#LS[^\\|]*",

    SW0 = os:timestamp(),
    {ok, WillowLeedsPCRE} = re:compile(WillowLeedsFinder),
        
    QueryPCRE0 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {true, WillowLeedsPCRE}},
    {async, Runner0} = leveled_bookie:book_returnfolder(Book1, QueryPCRE0),
    Results0 = Runner0(),
    BornMid70s0 =
        lists:filtermap(
            fun({IdxValue, Key}) ->
                DoB =
                    list_to_binary(
                        lists:nth(
                            2,
                            string:tokens(binary_to_list(IdxValue), "|")
                        )
                    ),
                case (DoB >= StartDoB) andalso (DoB =< EndDoB) of
                    true ->
                        {true, Key};
                    false ->
                        false
                end
            end,
            Results0
        ),

    SW1 = os:timestamp(),
    
    WillowLeedsExtractor = 
        "[^\\|]*\\|(?P<dob>[0-9]{8})\\|[0-9]{0,8}\\|[^\\|]*#Willow[^\\|]*\\|"
        "[^\\|]*#LS[^\\|]*",
    FilterFun1 =
        fun(Captures) ->
            DoB = maps:get(<<"dob">>, Captures, notfound),
            (DoB >= StartDoB) andalso (DoB =< EndDoB)
        end,
    EvalFunPCRE =
        leveled_eval:generate_eval_function(
            "regex($term, :regex, pcre, ($dob))",
            #{<<"regex">> => list_to_binary(WillowLeedsExtractor)}
        ),
    
    QueryPCRE1 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {false, {eval, EvalFunPCRE, FilterFun1}}
    },
    {async, RunnerPCRE1} = leveled_bookie:book_returnfolder(Book1, QueryPCRE1),
    BornMid70sPCRE1 = RunnerPCRE1(),

    SW2 = os:timestamp(),

    EvalFunRE2 =
        leveled_eval:generate_eval_function(
            "regex($term, :regex, re2, ($dob))",
            #{<<"regex">> => list_to_binary(WillowLeedsExtractor)}
        ),
    QueryRE2_2 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {false, {eval, EvalFunRE2, FilterFun1}}
    },
    {async, RunnerRE2_2} = leveled_bookie:book_returnfolder(Book1, QueryRE2_2),
    BornMid70sRE2_2 = RunnerRE2_2(),

    SW3 = os:timestamp(),

    AllFun = fun(_) -> true end,
    QueryRE2_3 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {<<"dob">>, {eval, EvalFunRE2, AllFun}}
    },
    {async, RunnerRE2_3} = leveled_bookie:book_returnfolder(Book1, QueryRE2_3),
    Results3 = RunnerRE2_3(),
    BornMid70sRE2_3 =
        lists:filtermap(
            fun({DoB, Key}) ->
                case (DoB >= StartDoB) andalso (DoB =< EndDoB) of
                    true ->
                        {true, Key};
                    false ->
                        false
                end
            end,
            Results3
        ),

    SW4 = os:timestamp(),

    WillowLeedsDoubleExtractor = 
        "[^\\|]*\\|(?P<dob>[0-9]{8})\\|(?P<dod>[0-9]{0,8})\\|"
        "[^\\|]*#Willow[^\\|]*\\|[^\\|]*#LS[^\\|]*",
    EvalFunRE2_2 =
        leveled_eval:generate_eval_function(
            "regex($term, :regex, re2, ($dob, $dod))",
            #{<<"regex">> => list_to_binary(WillowLeedsDoubleExtractor)}
        ),
        
    FilterFun2 =
        fun(Captures) ->
            DoB = maps:get(<<"dob">>, Captures, notfound),
            (DoB >= StartDoB) andalso (DoB =< EndDoB)
        end,
    QueryRE2_4 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {false, {eval, EvalFunRE2_2, FilterFun2}}
    },
    {async, RunnerRE2_4} = leveled_bookie:book_returnfolder(Book1, QueryRE2_4),
    BornMid70sRE2_4 = RunnerRE2_4(),
    
    SW5 = os:timestamp(),

    QueryRE2_5 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {true, {eval, EvalFunRE2, FilterFun1}}
    },
    {async, RunnerRE2_5} = leveled_bookie:book_returnfolder(Book1, QueryRE2_5),
    {ok, WillowLeedsExtractorRE2} = re2:compile(WillowLeedsExtractor),
    BornMid70sRE2_5 =
        lists:filtermap(
            fun({T, K}) ->
                {match, _} =
                    leveled_util:regex_run(T, WillowLeedsExtractorRE2, []),
                {true, K}
            end,
            RunnerRE2_5()),

    SW8 = os:timestamp(),
    
    FilterExpression1 = "($dob BETWEEN \"19740301\" AND \"19761030\")",
    {ok, FilterExpressionParsed1} =
        leveled_filter:generate_filter_expression(
            FilterExpression1, maps:new()),
    FilterFun5 =
        fun(AttrMap) ->
            leveled_filter:apply_filter(FilterExpressionParsed1, AttrMap)
        end,
        
    QueryRE2_8 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {false, {eval, EvalFunRE2, FilterFun5}}
    },
    {async, RunnerRE2_8} = leveled_bookie:book_returnfolder(Book1, QueryRE2_8),
    BornMid70sRE2_8 = RunnerRE2_8(),

    SW9 = os:timestamp(),

    PreFilterRE =
        "[^\\|]*\\|(?P<dob>197[4-6]{1}[0-9]{4})\\|"
        "[0-9]{0,8}\\|[^\\|]*#Willow[^\\|]*\\|"
        "[^\\|]*#LS[^\\|]*",
    PreFilterEvalFun =
        leveled_eval:generate_eval_function(
            "regex($term, :regex, re2, ($dob))",
            #{<<"regex">> => list_to_binary(PreFilterRE)}
        ),
    
    QueryRE2_9 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {false, {eval, PreFilterEvalFun, FilterFun5}}
    },
    {async, RunnerRE2_9} = leveled_bookie:book_returnfolder(Book1, QueryRE2_9),
    BornMid70sRE2_9 = RunnerRE2_9(),

    SW10 = os:timestamp(),

    WillowLeedsExtractor = 
        "[^\\|]*\\|(?P<dob>[0-9]{8})\\|[0-9]{0,8}\\|[^\\|]*#Willow[^\\|]*\\|"
        "[^\\|]*#LS[^\\|]*",

    FilterExpression2 =
        "($dob BETWEEN \"19740301\" AND \"19761030\")"
        "AND (contains($gns, \"#Willow\") AND contains($pcs, \"#LS\"))",
    {ok, FilterExpressionParsed2} =
        leveled_filter:generate_filter_expression(
            FilterExpression2, maps:new()),
    FilterFun6 =
        fun(AttrMap) ->
            leveled_filter:apply_filter(FilterExpressionParsed2, AttrMap)
        end,
    {ok, EvalExpression2} =
        leveled_eval:generate_eval_expression(
            "delim($term, \"|\", ($surname, $dob, $dod, $gns, $pcs))",
            maps:new()
        ),
    EvalFun2 =
        fun(Term, Key) ->
            leveled_eval:apply_eval(EvalExpression2, Term, Key, maps:new())
        end,
    QueryRE2_10 =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {false, {eval, EvalFun2, FilterFun6}}
    },
    {async, RunnerRE2_10} = leveled_bookie:book_returnfolder(Book1, QueryRE2_10),
    BornMid70sRE2_10 = RunnerRE2_10(),

    SW11 = os:timestamp(),

    true = length(BornMid70s0) > 0,

    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sPCRE1),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_2),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_3),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_4),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_5),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_8),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_9),
    true = lists:sort(BornMid70s0) == lists:sort(BornMid70sRE2_10),

    maybe_log_toscreen(
        "~nFilter outside took ~w ms~n",
        [timer:now_diff(SW1, SW0) div 1000]),
    maybe_log_toscreen(
        "~nPCRE Capture filter inside took ~w ms~n",
        [timer:now_diff(SW2, SW1) div 1000]),
    maybe_log_toscreen(
        "~nRE2 Capture filter inside took ~w ms~n",
        [timer:now_diff(SW3, SW2) div 1000]),
    maybe_log_toscreen(
        "~nRE2 Capture filter outside took ~w ms~n",
        [timer:now_diff(SW4, SW3) div 1000]),
    maybe_log_toscreen(
        "~nRE2 double-capture filter outside took ~w ms~n",
        [timer:now_diff(SW5, SW4) div 1000]),
    maybe_log_toscreen(
        "~nRE2 single-capture filter with parsed filter expression took ~w ms~n",
        [timer:now_diff(SW9, SW8) div 1000]),
    maybe_log_toscreen(
        "~nRE2 single-capture pre-filter with parsed query string took ~w ms~n",
        [timer:now_diff(SW10, SW9) div 1000]),
    maybe_log_toscreen(
        "~nEval processed index with parsed filter expression took ~w ms~n",
        [timer:now_diff(SW11, SW10) div 1000]),


    QueryRE2_3_WrongCapture =
        {index_query,
            {Bucket, null},
            {fun testutil:foldkeysfun/3, []},
            {IdxName, <<"M">>, <<"Z">>},
            {<<"gns">>, {eval, EvalFunRE2, FilterFun6}}
        },
    {async, RunnerRE2_3_WC} =
        leveled_bookie:book_returnfolder(Book1, QueryRE2_3_WrongCapture),
    true = [] == RunnerRE2_3_WC(),

    ok = leveled_bookie:book_close(Book1),
    
    testutil:reset_filestructure().

maybe_log_toscreen(Log, Subs) ->
    io:format(
        % user,
        Log,
        Subs
    ).


count_termsonindex(Bucket, IdxField, Book, QType) ->
    lists:foldl(
        fun(X, Acc) ->
            SW = os:timestamp(),
            ST = list_to_binary(integer_to_list(X)),
            Pipe = <<"|">>,
            ET = <<ST/binary, Pipe/binary>>,
            Q = {index_query,
                    Bucket,
                    {fun testutil:foldkeysfun/3, []},
                    {IdxField, ST, ET},
                    QType},
            R = leveled_bookie:book_returnfolder(Book, Q),
            {async, Folder} = R,
            Items = length(Folder()),
            io:format(
                "2i query from term ~s on index ~s took ~w microseconds~n",
                [ST, IdxField, timer:now_diff(os:timestamp(), SW)]),
            Acc + Items
        end,
        0,
        lists:seq(190, 221)).

multibucket_fold(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    ObjectGen = <<"V1">>,
    IndexGen = fun() -> [] end,
    B1 = {<<"Type1">>, <<"Bucket1">>},
    B2 = <<"Bucket2">>,
    B3 = <<"Bucket3">>,
    B4 = {<<"Type2">>, <<"Bucket4">>},
    ObjL1 =
        testutil:generate_objects(
            13000, uuid, [], ObjectGen, IndexGen, B1),
    testutil:riakload(Bookie1, ObjL1),
    ObjL2 =
        testutil:generate_objects(
            17000, uuid, [], ObjectGen, IndexGen, B2),
    testutil:riakload(Bookie1, ObjL2),
    ObjL3 =
        testutil:generate_objects(
            7000, uuid, [], ObjectGen, IndexGen, B3),
    testutil:riakload(Bookie1, ObjL3),
    ObjL4 =
        testutil:generate_objects(
            23000, uuid, [], ObjectGen, IndexGen, B4),
    testutil:riakload(Bookie1, ObjL4),

    FF = fun(B, K, _PO, Acc) ->
                    [{B, K}|Acc]
         end,
    FoldAccT = {FF, []},

    {async, R1} = 
        leveled_bookie:book_headfold(Bookie1,
                                        ?RIAK_TAG,
                                        {bucket_list, 
                                        [{<<"Type1">>, <<"Bucket1">>}, 
                                            {<<"Type2">>, <<"Bucket4">>}]},
                                        FoldAccT,
                                        false,
                                        true,
                                        false),

    O1 = length(R1()),
    io:format("Result R1 of length ~w~n", [O1]),
    
    {async, R2} = 
        leveled_bookie:book_headfold(Bookie1,
                                        ?RIAK_TAG,
                                        {bucket_list, 
                                            [<<"Bucket2">>, 
                                                <<"Bucket3">>]},
                                        {fun(_B, _K, _PO, Acc) ->
                                                Acc +1
                                            end,
                                            0},
                                        false, true, false),
    O2 = R2(),
    io:format("Result R2 of ~w~n", [O2]),

    true = 36000 == O1,
    true = 24000 == O2,

    FoldBucketsFun = fun(B, Acc) -> [B|Acc] end,
    {async, Folder} = 
        leveled_bookie:book_bucketlist(Bookie1, 
                                        ?RIAK_TAG, 
                                        {FoldBucketsFun, []}, 
                                        all),
    BucketList = lists:reverse(Folder()),
    ExpectedBucketList = 
        [{<<"Type1">>, <<"Bucket1">>}, {<<"Type2">>, <<"Bucket4">>}, 
            <<"Bucket2">>, <<"Bucket3">>],
    io:format("BucketList ~w", [BucketList]),
    true = ExpectedBucketList == BucketList,

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

foldobjects_bybucket_range(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} =
        leveled_bookie:book_start(
            RootPath, 2000, 50000000, testutil:sync_strategy()),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 =
        testutil:generate_objects(
            1300, {fixed_binary, 1}, [], ObjectGen, IndexGen, <<"Bucket1">>),
    testutil:riakload(Bookie1, ObjL1),

    FoldKeysFun =
        fun(_B, K,_V, Acc) -> [ K |Acc] end,

    StartKey = testutil:fixed_bin_key(123),
    EndKey = testutil:fixed_bin_key(779),

    {async, Folder} =
        leveled_bookie:book_objectfold(
            Bookie1,
            ?RIAK_TAG,
            <<"Bucket1">>,
            {StartKey, EndKey}, {FoldKeysFun, []},
            true
        ),
    ResLen = length(Folder()),
    io:format("Length of Result of folder ~w~n", [ResLen]),
    true = 657 == ResLen,

    {async, AllFolder} =
        leveled_bookie:book_objectfold(
            Bookie1, ?RIAK_TAG, <<"Bucket1">>, all, {FoldKeysFun, []}, true),

    AllResLen = length(AllFolder()),
    io:format("Length of Result of all keys folder ~w~n", [AllResLen]),
    true = 1300 == AllResLen,

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().
