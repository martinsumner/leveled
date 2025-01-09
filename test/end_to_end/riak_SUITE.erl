-module(riak_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").

-export([all/0]).

-export(
    [
        test_large_lsm_merge/1,
        basic_riak/1,
        fetchclocks_modifiedbetween/1,
        crossbucket_aae/1,
        handoff/1,
        handoff_close/1,
        dollar_bucket_index/1,
        dollar_key_index/1,
        bigobject_memorycheck/1,
        summarisable_sstindex/1
    ]
).

all() -> [
            basic_riak,
            fetchclocks_modifiedbetween,
            crossbucket_aae,
            handoff,
            handoff_close,
            dollar_bucket_index,
            dollar_key_index,
            bigobject_memorycheck,
            summarisable_sstindex,
            test_large_lsm_merge
            ].

-define(MAGIC, 53). % riak_kv -> riak_object


test_large_lsm_merge(_Config) ->
    lsm_merge_tester(12).

lsm_merge_tester(LoopsPerBucket) ->
    RootPath = testutil:reset_filestructure("lsmMerge"),
    PutsPerLoop = 32000,
    SampleOneIn = 100,
    StartOpts1 =
        [
            {root_path, RootPath},
            {max_pencillercachesize, 16000},
            {max_sstslots, 48},
                % Make SST files smaller, to accelerate merges
            {max_mergebelow, 24},
            {sync_strategy, testutil:sync_strategy()},
            {log_level, warn},
            {compression_method, zstd},
            {
                forced_logs,
                [
                    b0015, b0016, b0017, b0018, p0032, sst12,
                    pc008, pc010, pc011, pc026,
                    p0018, p0024
                ]
            }
        ],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    LoadBucketFun =
        fun(Book, Bucket, Loops) ->
            V = testutil:get_compressiblevalue(),
            lists:foreach(
                fun(_I) ->
                    {_, V} =
                        testutil:put_indexed_objects(
                            Book,
                            Bucket,
                            PutsPerLoop,
                            V
                        )
                end,
                lists:seq(1, Loops)
            ),
        V
        end,

    V1 = LoadBucketFun(Bookie1, <<"B1">>, LoopsPerBucket),
    io:format("Completed load of ~s~n", [<<"B1">>]),
    V2 = LoadBucketFun(Bookie1, <<"B2">>, LoopsPerBucket),
    io:format("Completed load of ~s~n", [<<"B2">>]),
    ValueMap = #{<<"B1">> => V1, <<"B2">> => V2},

    CheckBucketFun =
        fun(Book) ->
            BookHeadFoldFun =
                fun(B, K, _Hd, {SampleKeys, CountAcc}) ->
                    UpdCntAcc =
                        maps:update_with(B, fun(C) -> C + 1 end, 1, CountAcc),
                    case rand:uniform(SampleOneIn) of
                        R when R == 1 ->
                            {[{B, K}|SampleKeys], UpdCntAcc};
                        _ ->
                            {SampleKeys, UpdCntAcc}
                    end
                end,
            {async, HeadFolder} =
                leveled_bookie:book_headfold(
                    Book, 
                    ?RIAK_TAG,
                    {BookHeadFoldFun, {[], maps:new()}},
                    true,
                    false,
                    false
                ), 
            {Time, R} = timer:tc(HeadFolder),
            io:format(
                "CheckBucketFold returned counts ~w in ~w ms~n",
                [element(2, R), Time div 1000]
            ),
            R
        end,

    {SampleKeysF1, CountMapF1} = CheckBucketFun(Bookie1),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B1">>, CountMapF1),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B2">>, CountMapF1),

    TestSampleKeyFun =
        fun(Book, Values) ->
            fun({B, K}) ->
                ExpectedV = maps:get(B, Values),
                {ok, Obj} = testutil:book_riakget(Book, B, K),
                true = ExpectedV == testutil:get_value(Obj)
            end
        end,

    {GT1, ok} =
        timer:tc(
            fun() ->
                lists:foreach(TestSampleKeyFun(Bookie1, ValueMap), SampleKeysF1)
            end
        ),
    io:format(
        "Returned ~w sample gets in ~w ms~n",
        [length(SampleKeysF1), GT1 div 1000]
    ),

    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} =
        leveled_bookie:book_start(
            lists:ukeysort(1, [{max_sstslots, 64}|StartOpts1])
        ),

    {SampleKeysF2, CountMapF2} = CheckBucketFun(Bookie2),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B1">>, CountMapF2),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B2">>, CountMapF2),

    {GT2, ok} =
        timer:tc(
            fun() ->
                lists:foreach(TestSampleKeyFun(Bookie2, ValueMap), SampleKeysF2)
            end
        ),
    io:format(
        "Returned ~w sample gets in ~w ms~n",
        [length(SampleKeysF2), GT2 div 1000]
    ),

    V3 = LoadBucketFun(Bookie2, <<"B3">>, LoopsPerBucket),
    io:format("Completed load of ~s~n", [<<"B3">>]),
    UpdValueMap = #{<<"B1">> => V1, <<"B2">> => V2, <<"B3">> => V3},

    {SampleKeysF3, CountMapF3} = CheckBucketFun(Bookie2),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B1">>, CountMapF3),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B2">>, CountMapF3),
    true = (LoopsPerBucket * PutsPerLoop) == maps:get(<<"B3">>, CountMapF3),

    {GT3, ok} =
        timer:tc(
            fun() ->
                lists:foreach(TestSampleKeyFun(Bookie2, UpdValueMap), SampleKeysF3)
            end
        ),
    io:format(
        "Returned ~w sample gets in ~w ms~n",
        [length(SampleKeysF3), GT3 div 1000]
    ),

    ok = leveled_bookie:book_destroy(Bookie2).

basic_riak(_Config) ->
    basic_riak_tester(<<"B0">>, 640000),
    basic_riak_tester({<<"Type0">>, <<"B0">>}, 80000).

basic_riak_tester(Bucket, KeyCount) ->
    % Key Count should be > 10K and divisible by 5
    io:format("Basic riak test with Bucket ~w KeyCount ~w~n",
                [Bucket, KeyCount]),
    IndexCount = 20,

    RootPath = testutil:reset_filestructure("basicRiak"),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, testutil:sync_strategy()},
                    {database_id, 32},
                    {stats_logfrequency, 5},
                    {stats_probability, 80}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    IndexGenFun =
        fun(ListID) ->
            fun() ->
                RandInt = leveled_rand:uniform(IndexCount),
                ID = integer_to_list(ListID),
                [{add, 
                    list_to_binary("integer" ++ ID ++ "_int"),
                    RandInt},
                    {add, 
                        list_to_binary("binary" ++ ID ++ "_bin"),
                        <<RandInt:32/integer>>}]
            end
        end,

    CountPerList = KeyCount div 5,

    ObjList1 = 
        testutil:generate_objects(CountPerList, 
                                    {fixed_binary, 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    IndexGenFun(1),
                                    Bucket),
    ObjList2 =
        testutil:generate_objects(CountPerList, 
                                    {fixed_binary, CountPerList + 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    IndexGenFun(2),
                                    Bucket),
    
    ObjList3 =
        testutil:generate_objects(CountPerList, 
                                    {fixed_binary, 2 * CountPerList + 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    IndexGenFun(3),
                                    Bucket),
    
    ObjList4 =
        testutil:generate_objects(CountPerList, 
                                    {fixed_binary, 3 * CountPerList + 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    IndexGenFun(4),
                                    Bucket),
    
    ObjList5 =
        testutil:generate_objects(CountPerList, 
                                    {fixed_binary, 4 * CountPerList + 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    IndexGenFun(5),
                                    Bucket),
    
    % Mix with the ordering on the load, just in case ordering hides issues
    testutil:riakload(Bookie1, ObjList4),
    testutil:riakload(Bookie1, ObjList1),
    testutil:riakload(Bookie1, ObjList3),
    testutil:riakload(Bookie1, ObjList5),
    testutil:riakload(Bookie1, ObjList2), 
        % This needs to stay last,
        % as the last key of this needs to be the last key added
        % so that headfold check, checks something in memory

    % Take a subset, and do some HEAD/GET requests
    SubList1 = lists:sublist(lists:ukeysort(1, ObjList1), 1000),
    SubList5 = lists:sublist(lists:ukeysort(1, ObjList5), 1000),

    ok = testutil:check_forlist(Bookie1, SubList1),
    ok = testutil:check_forlist(Bookie1, SubList5),
    ok = testutil:checkhead_forlist(Bookie1, SubList1),
    ok = testutil:checkhead_forlist(Bookie1, SubList5),

    FoldKeysFun =  fun(_B, K, Acc) -> [K|Acc] end,
    IntIndexFold =
        fun(Idx, Book) ->
            fun(IC, CountAcc) ->
                ID = integer_to_list(Idx),
                Index = list_to_binary("integer" ++ ID ++ "_int"),
                {async, R} = 
                    leveled_bookie:book_indexfold(Book,
                                                    {Bucket, <<>>},
                                                    {FoldKeysFun, []},
                                                    {Index, 
                                                        IC, 
                                                        IC},
                                                    {true, undefined}),
                KTL = R(),
                CountAcc + length(KTL)
            end
        end,
    BinIndexFold =
        fun(Idx, Book) ->
            fun(IC, CountAcc) ->
                ID = integer_to_list(Idx),
                Index = list_to_binary("binary" ++ ID ++ "_bin"),
                {async, R} = 
                    leveled_bookie:book_indexfold(Book,
                                                    {Bucket, <<>>},
                                                    {FoldKeysFun, []},
                                                    {Index, 
                                                        <<IC:32/integer>>, 
                                                        <<IC:32/integer>>},
                                                    {true, undefined}),
                KTL = R(),
                CountAcc + length(KTL)
            end
        end,

    SWA = os:timestamp(),
    TotalIndexEntries2 =
        lists:foldl(IntIndexFold(2, Bookie1), 0, lists:seq(1, IndexCount)),
    io:format("~w queries returned count=~w in ~w ms~n",
                [IndexCount, 
                    TotalIndexEntries2,
                    timer:now_diff(os:timestamp(), SWA)/1000]),
    true = TotalIndexEntries2 == length(ObjList2),
    SWB = os:timestamp(),

    TotalIndexEntries4 =
        lists:foldl(IntIndexFold(4, Bookie1), 0, lists:seq(1, IndexCount)),
    io:format("~w queries returned count=~w in ~w ms~n",
                [IndexCount, 
                    TotalIndexEntries4,
                    timer:now_diff(os:timestamp(), SWB)/1000]),
    true = TotalIndexEntries4 == length(ObjList4),
    
    SWC = os:timestamp(),
    TotalIndexEntries3 =
        lists:foldl(BinIndexFold(3, Bookie1), 0, lists:seq(1, IndexCount)),
    io:format("~w queries returned count=~w in ~w ms~n",
                [IndexCount, 
                    TotalIndexEntries3,
                    timer:now_diff(os:timestamp(), SWC)/1000]),
    true = TotalIndexEntries3 == length(ObjList3),
    
    ok = leveled_bookie:book_close(Bookie1),

    StartOpts2 = [{root_path, RootPath},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 12000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),

    ok = testutil:check_forlist(Bookie2, SubList5),
    ok = testutil:checkhead_forlist(Bookie2, SubList1),
    TotalIndexEntries4B =
        lists:foldl(IntIndexFold(4, Bookie2), 0, lists:seq(1, IndexCount)),
    true = TotalIndexEntries4B == length(ObjList4),
    TotalIndexEntries3B =
        lists:foldl(BinIndexFold(3, Bookie2), 0, lists:seq(1, IndexCount)),
    true = TotalIndexEntries3B == length(ObjList3),

    HeadFoldFun = fun(B, K, _Hd, Acc) -> [{B, K}|Acc] end,
    [{_I1, Obj1, _Spc1}|_Rest1] = ObjList1,
    [{_I2, Obj2, _Spc2}|_Rest2] = ObjList2,
    [{_I3, Obj3, _Spc3}|_Rest3] = ObjList3,
    [{_I4, Obj4, _Spc4}|_Rest4] = ObjList4,
    [{_I5, Obj5, _Spc5}|_Rest5] = ObjList5,
    {_I2L, Obj2L, _Spc2L} = lists:last(ObjList2),

    SegList =
        lists:map(fun(Obj) -> testutil:get_aae_segment(Obj) end, 
                    [Obj1, Obj2, Obj3, Obj4, Obj5, Obj2L]),
    BKList = 
        lists:map(fun(Obj) -> 
                        {testutil:get_bucket(Obj), testutil:get_key(Obj)}
                    end, 
                    [Obj1, Obj2, Obj3, Obj4, Obj5, Obj2L]),
    
    {async, HeadR} =
        leveled_bookie:book_headfold(Bookie2, 
                                        ?RIAK_TAG,
                                        {HeadFoldFun, []},
                                        true, false,
                                        SegList),
    SW_SL0 = os:timestamp(),
    KLBySeg = HeadR(),
    io:format("SegList Headfold returned ~w heads in ~w ms~n", 
                [length(KLBySeg),
                    timer:now_diff(os:timestamp(), SW_SL0)/1000]),
    true = length(KLBySeg) < KeyCount div 1000, % not too many false answers
    KLBySegRem = lists:subtract(KLBySeg, BKList),
    true = length(KLBySeg) - length(KLBySegRem) == length(BKList),

    {async, HeadRFalsePositive} =
        leveled_bookie:book_headfold(Bookie2, 
                                        ?RIAK_TAG,
                                        {HeadFoldFun, []},
                                        true, false,
                                        SegList ++ lists:seq(1, 256)),
                                        % Make it a large seg list
    SW_SL1 = os:timestamp(),
    KLByXcessSeg = HeadRFalsePositive(),
    io:format("SegList Headfold with xcess segments returned ~w heads in ~w ms~n",
                [length(KLByXcessSeg),
                    timer:now_diff(os:timestamp(), SW_SL1)/1000]),
    true = length(KLByXcessSeg) < KeyCount div 10, % Still not too many false answers
    KLByXcessSegRem = lists:subtract(KLByXcessSeg, BKList),
    true = length(KLByXcessSeg) - length(KLByXcessSegRem) == length(BKList),

    ok = leveled_bookie:book_destroy(Bookie2).

summarisable_sstindex(_Config) ->
    RootPathA = testutil:reset_filestructure("SummarisableSSTIndex"),
    KeyCount = 200000,
    IndexGen = fun() -> [] end,
    Bucket = {<<"test_type">>, <<"test_bucket">>},
    KeyGen = fun(I) -> list_to_binary(io_lib:format("~10..0w", [I])) end,
    ObjListToSort =
        lists:map(
            fun(I) -> 
                {leveled_rand:uniform(KeyCount * 10),
                testutil:set_object(
                    Bucket, KeyGen(I), integer_to_binary(I), IndexGen, [])}
                end,
            lists:seq(1, KeyCount)),
    UnsortedList =
        lists:map(
            fun({I, {O, S}}) -> {I, O, S} end,
            lists:keysort(1, ObjListToSort)),
    true = KeyCount == length(UnsortedList),
    StartOpts1 = [{root_path, RootPathA},
        {max_journalsize, 500000000},
        {max_pencillercachesize, 8000},
        {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    testutil:riakload(Bookie1, UnsortedList),
    FoldAccT = {fun(_B, K, Acc) -> [K|Acc] end, []},
    KeyRangeCheckFun =
        fun(SK, EK) ->
            {async, FoldFun} =
                leveled_bookie:book_keylist(
                    Bookie1,
                    ?RIAK_TAG,
                    Bucket,
                    {SK, EK},
                    FoldAccT,
                    undefined),
            QueryList = FoldFun(),
            io:format(
                "QueryCount ~w against total ~w for range ~p ~p~n",
                [length(QueryList), KeyCount, SK, EK]),
            QueryList
        end,
    
        true = KeyCount == length(KeyRangeCheckFun(<<"00">>, <<"02">>)),
    true = KeyCount == length(KeyRangeCheckFun(<<"000">>, <<"002">>)),
    true = KeyCount == length(KeyRangeCheckFun(<<"0000">>, <<"0002">>)),
    true =
        (KeyCount - 1) ==
            length(KeyRangeCheckFun(<<"00000">>, <<"00002">>)),
    true =
        (KeyCount - 1) ==
            length(KeyRangeCheckFun(<<"00000">>, <<"000020">>)),
    true =
        (KeyCount - 1) ==
            length(KeyRangeCheckFun(<<"00000">>, <<"0000200">>)),
    true =
        (KeyCount div 2) ==
            length(KeyRangeCheckFun(<<"00001">>, <<"00002">>)),
    true =
        1 == length(KeyRangeCheckFun(<<"00002">>, <<"000021">>)),
    true =
        ((KeyCount div 10) - 1) ==
            length(KeyRangeCheckFun(<<"000000">>, <<"000002">>)),
    true =
        (KeyCount div 20) ==
            length(KeyRangeCheckFun(<<"000001">>, <<"000002">>)),
    true =
        ((KeyCount div 100) - 1) ==
            length(KeyRangeCheckFun(<<"0000000">>, <<"0000002">>)),

    lists:foreach(
        fun(I) ->
            StartKey = KeyGen(I),
            EndKey = KeyGen(I + 200 - 1),
            true = 200 == length(KeyRangeCheckFun(StartKey, EndKey))
        end,
        lists:map(
            fun(_I) -> leveled_rand:uniform(KeyCount - 200) end,
            lists:seq(1, 100))),

    IdxObjKeyCount = 50000,
    TermGen =
        fun(I, C) ->
            list_to_binary(
                lists:flatten(
                    io_lib:format("~10..0w", [I]) ++ integer_to_list(C)))
        end,
    SequentialIndexGen =
        fun(I) ->
            fun() ->
                lists:map(
                    fun(C) ->
                        {add, <<"indexf_bin">>,TermGen(I, C)}
                    end,
                    lists:seq(1, 8))
            end
        end,
    IdxObjListToSort =
        lists:map(
            fun(I) -> 
                {leveled_rand:uniform(KeyCount * 10),
                    testutil:set_object(
                        Bucket,
                        KeyGen(I),
                        integer_to_binary(I - KeyCount),
                        SequentialIndexGen(I - KeyCount),
                        [])}
                end,
            lists:seq(KeyCount + 1, KeyCount + IdxObjKeyCount)),
    UnsortedIdxObjList =
        lists:map(
            fun({I, {O, S}}) -> {I, O, S} end,
            lists:keysort(1, IdxObjListToSort)),
    testutil:riakload(Bookie1, UnsortedIdxObjList),
    IdxCount = IdxObjKeyCount * 8,

    IdxQueryFun =
        fun(StartTerm, EndTerm) ->
            {async, FoldFun} = 
                leveled_bookie:book_indexfold(
                    Bookie1, {Bucket, <<>>}, FoldAccT,
                    {<<"indexf_bin">>, StartTerm, EndTerm},
                    {true, undefined}),
            IdxQueryList = FoldFun(),
            io:format(
                "IdxQueryCount ~w for range ~p ~p~n",
                [length(IdxQueryList), StartTerm, EndTerm]),
            IdxQueryList
        end,
    true = IdxCount == length(IdxQueryFun(<<"00">>, <<"05">>)),
    true = IdxCount == length(IdxQueryFun(<<"000">>, <<"005">>)),
    true = IdxCount == length(IdxQueryFun(<<"0000">>, <<"0005">>)),
    true = IdxCount == length(IdxQueryFun(<<"00000">>, <<"00005">>)),
    true =
        (IdxCount - 8) ==
            length(IdxQueryFun(<<"000000">>, <<"000005">>)),
    true =
        (IdxCount - 8) ==
            length(IdxQueryFun(<<"000000">>, <<"0000050">>)),
    true =
        (IdxCount - 8) ==
            length(IdxQueryFun(<<"000000">>, <<"00000500">>)),
    true = 8 == length(IdxQueryFun(<<"000005">>, <<"0000051">>)),

    lists:foreach(
        fun(I) ->
            StartTerm = TermGen(I, 0),
            EndTerm = TermGen(I + 20, 9),
            true = 168 == length(IdxQueryFun(StartTerm, EndTerm))
        end,
        lists:map(
            fun(_I) ->
                leveled_rand:uniform(IdxObjKeyCount - 20)
            end,
            lists:seq(1, 100))),
    lists:foreach(
        fun(I) ->
            StartTerm = TermGen(I, 0),
            EndTerm = TermGen(I + 10, 9),
            true = 88 == length(IdxQueryFun(StartTerm, EndTerm))
        end,
        lists:map(
            fun(_I) ->
                leveled_rand:uniform(IdxObjKeyCount - 10)
            end,
            lists:seq(1, 100))),

    io:format("Redo object count checks:~n"),
    NewKeyCount = KeyCount + IdxObjKeyCount,
    true = NewKeyCount == length(KeyRangeCheckFun(<<"00">>, <<"025">>)),
    true = NewKeyCount == length(KeyRangeCheckFun(<<"000">>, <<"0025">>)),
    true = NewKeyCount == length(KeyRangeCheckFun(<<"0000">>, <<"00025">>)),
    true =
        (NewKeyCount - 1) ==
            length(KeyRangeCheckFun(<<"00000">>, <<"000025">>)),
    true = 1 == length(KeyRangeCheckFun(<<"000025">>, <<"0000251">>)),

    lists:foreach(
        fun(I) ->
            StartKey = KeyGen(I),
            EndKey = KeyGen(I + 200 - 1),
            true = 200 == length(KeyRangeCheckFun(StartKey, EndKey))
        end,
        lists:map(
            fun(_I) -> leveled_rand:uniform(KeyCount - 200) end,
            lists:seq(1, 100))),
    
    ok = leveled_bookie:book_destroy(Bookie1).


fetchclocks_modifiedbetween(_Config) ->
    RootPathA = testutil:reset_filestructure("fetchClockA"),
    RootPathB = testutil:reset_filestructure("fetchClockB"),
    StartOpts1A = [{root_path, RootPathA},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 8000},
                    {sync_strategy, testutil:sync_strategy()}],
    StartOpts1B = [{root_path, RootPathB},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 12000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie1A} = leveled_bookie:book_start(StartOpts1A),
    {ok, Bookie1B} = leveled_bookie:book_start(StartOpts1B),

    ObjList0 = 
        testutil:generate_objects(100000, 
                                    {fixed_binary, 1}, [],
                                    leveled_rand:rand_bytes(32),
                                    fun() -> [] end,
                                    <<"BaselineB">>),

    ObjL1StartTS = testutil:convert_to_seconds(os:timestamp()),
    ObjList1 = 
        testutil:generate_objects(20000, 
                                    {fixed_binary, 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    fun() -> [] end,
                                    <<"B0">>),
    timer:sleep(1000),
    ObjL1EndTS = testutil:convert_to_seconds(os:timestamp()),
    timer:sleep(1000),

    _ObjL2StartTS = testutil:convert_to_seconds(os:timestamp()),
    ObjList2 = 
        testutil:generate_objects(15000, 
                                    {fixed_binary, 20001}, [],
                                    leveled_rand:rand_bytes(512),
                                    fun() -> [] end,
                                    <<"B0">>),
    timer:sleep(1000),
    _ObjList2EndTS = testutil:convert_to_seconds(os:timestamp()),
    timer:sleep(1000),

    ObjL3StartTS = testutil:convert_to_seconds(os:timestamp()),
    ObjList3 = 
        testutil:generate_objects(35000, 
                                    {fixed_binary, 35001}, [],
                                    leveled_rand:rand_bytes(512),
                                    fun() -> [] end,
                                    <<"B0">>),
    timer:sleep(1000),
    ObjL3EndTS = testutil:convert_to_seconds(os:timestamp()),
    timer:sleep(1000),

    ObjL4StartTS = testutil:convert_to_seconds(os:timestamp()),
    ObjList4 = 
        testutil:generate_objects(30000, 
                                    {fixed_binary, 70001}, [],
                                    leveled_rand:rand_bytes(512),
                                    fun() -> [] end,
                                    <<"B0">>),
    timer:sleep(1000),
    _ObjL4EndTS = testutil:convert_to_seconds(os:timestamp()),
    timer:sleep(1000),

    ObjL5StartTS = testutil:convert_to_seconds(os:timestamp()),
    ObjList5 = 
        testutil:generate_objects(8000, 
                                    {fixed_binary, 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    fun() -> [] end,
                                    <<"B1">>),
    timer:sleep(1000),
    _ObjL5EndTS = testutil:convert_to_seconds(os:timestamp()),
    timer:sleep(1000),

    ObjL6StartTS = testutil:convert_to_seconds(os:timestamp()),
    ObjList6 = 
        testutil:generate_objects(7000, 
                                    {fixed_binary, 1}, [],
                                    leveled_rand:rand_bytes(512),
                                    fun() -> [] end,
                                    <<"B2">>),
    timer:sleep(1000),
    ObjL6EndTS = testutil:convert_to_seconds(os:timestamp()),
    timer:sleep(1000),

    testutil:riakload(Bookie1A, ObjList5),
    testutil:riakload(Bookie1A, ObjList1),
    testutil:riakload(Bookie1A, ObjList2),
    testutil:riakload(Bookie1A, ObjList3),
    testutil:riakload(Bookie1A, ObjList4),
    testutil:riakload(Bookie1A, ObjList6),

    testutil:riakload(Bookie1B, ObjList0),
    testutil:riakload(Bookie1B, ObjList4),
    testutil:riakload(Bookie1B, ObjList5),
    testutil:riakload(Bookie1B, ObjList1),
    testutil:riakload(Bookie1B, ObjList6),
    testutil:riakload(Bookie1B, ObjList3),
    
    RevertFixedBinKey = 
        fun(FBK) ->
            <<$K, $e, $y, KeyNumber:64/integer>> = FBK,
            KeyNumber
        end,
    StoreFoldFun = 
        fun(_B, K, _V, {_LK, AccC}) ->
            {RevertFixedBinKey(K), AccC + 1}
        end,

    KeyRangeFun = 
        fun(StartNumber, EndNumber) ->
            {range,
                <<"B0">>,
                {testutil:fixed_bin_key(StartNumber), 
                    testutil:fixed_bin_key(EndNumber)}}
        end,
    
    % Count with max object count
    FoldRangesFun =
        fun(FoldTarget, ModRange, EndNumber, MaxCount) ->
            fun(_I, {LKN, KC}) ->
                {async, Runner} = 
                    leveled_bookie:book_headfold(FoldTarget,
                                                    ?RIAK_TAG,
                                                    KeyRangeFun(LKN + 1,
                                                                EndNumber),
                                                    {StoreFoldFun, {LKN, KC}},
                                                    false,
                                                    true,
                                                    false,
                                                    ModRange,
                                                    MaxCount),
                {_, {LKN0, KC0}} = Runner(),
                {LKN0, KC0}
            end
        end,

    R1A = lists:foldl(FoldRangesFun(Bookie1A, false, 50000, 13000),
                        {0, 0}, lists:seq(1, 4)),
    io:format("R1A ~w~n", [R1A]),
    true = {50000, 50000} == R1A,
    
    R1B = lists:foldl(FoldRangesFun(Bookie1B, false, 50000, 13000),
                        {0, 0}, lists:seq(1, 3)),
    io:format("R1B ~w~n", [R1B]),
    true = {50000, 35000} == R1B,

    R2A = lists:foldl(FoldRangesFun(Bookie1A, 
                                    {ObjL3StartTS, ObjL3EndTS},
                                    60000,
                                    13000),
                        {10000, 0}, lists:seq(1, 2)),
    io:format("R2A ~w~n", [R2A]),
    true = {60000, 25000} == R2A,
    R2A_SR = lists:foldl(FoldRangesFun(Bookie1A, 
                                    {ObjL3StartTS, ObjL3EndTS},
                                    60000,
                                    13000),
                        {10000, 0}, lists:seq(1, 1)), % Only single rotation
    io:format("R2A_SingleRotation ~w~n", [R2A_SR]),
    true = {48000, 13000} == R2A_SR, % Hit at max results
    R2B = lists:foldl(FoldRangesFun(Bookie1B, 
                                    {ObjL3StartTS, ObjL3EndTS},
                                    60000,
                                    13000),
                        {10000, 0}, lists:seq(1, 2)),
    io:format("R2B ~w~n", [R1B]),
    true = {60000, 25000} == R2B,

    CrudeStoreFoldFun = 
        fun(LowLMD, HighLMD) ->
            fun(_B, K, V, {LK, AccC}) ->
                % Value is proxy_object?  Can we get the metadata and
                % read the last modified date?  The do a non-accelerated
                % fold to check that it is slower
                {proxy_object, MDBin, _Size, _Fetcher} = binary_to_term(V),
                LMDTS = testutil:get_lastmodified(MDBin),
                LMD = testutil:convert_to_seconds(LMDTS),
                case (LMD >= LowLMD) and (LMD =< HighLMD) of
                    true ->
                        {RevertFixedBinKey(K), AccC + 1};
                    false ->
                        {LK, AccC}
                end
            end
        end,

    io:format("Comparing queries for Obj1 TS range ~w ~w~n",
                [ObjL1StartTS, ObjL1EndTS]),

    PlusFilterTimes =
        lists:map(
            fun(_I) -> 
                time_filtered_query(
                    FoldRangesFun, Bookie1A, ObjL1StartTS, ObjL1EndTS)
            end,
            lists:seq(1, 4)),
    PlusFilterTime = lists:sum(PlusFilterTimes) div 4,
    
    NoFilterStart = os:timestamp(),
    {async, R3A_NoFilterRunner} = 
        leveled_bookie:book_headfold(Bookie1A,
                                        ?RIAK_TAG,
                                        KeyRangeFun(1, 100000),
                                        {CrudeStoreFoldFun(ObjL1StartTS, 
                                                            ObjL1EndTS),
                                            {0, 0}},
                                        false,
                                        true,
                                        false),
    R3A_NoFilter = R3A_NoFilterRunner(),
    NoFilterTime = timer:now_diff(os:timestamp(), NoFilterStart) div 1000,
    io:format("R3A_NoFilter ~w~n", [R3A_NoFilter]),
    true = {20000, 20000} == R3A_NoFilter,
    io:format("Filtered query ~w ms and unfiltered query ~w ms~n", 
                [PlusFilterTime, NoFilterTime]),
    true = NoFilterTime > PlusFilterTime,

    SimpleCountFun =
        fun(BucketList) ->
            fun(B, _K, _V, AccC) -> 
                case lists:member(B, BucketList) of
                    true -> AccC + 1;
                    false -> AccC
                end
            end
        end,

    {async, R4A_MultiBucketRunner} = 
        leveled_bookie:book_headfold(Bookie1A,
                                        ?RIAK_TAG,
                                        {bucket_list, [<<"B0">>, <<"B2">>]},
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                            % Range includes ObjjL5 LMDs, 
                                            % but these ar enot in bucket list
                                        false),
    R4A_MultiBucket = R4A_MultiBucketRunner(),
    io:format("R4A_MultiBucket ~w ~n", [R4A_MultiBucket]),
    true = R4A_MultiBucket == 37000,

    {async, R5A_MultiBucketRunner} = 
        leveled_bookie:book_headfold(Bookie1A,
                                        ?RIAK_TAG,
                                        {bucket_list, [<<"B2">>, <<"B0">>]},
                                            % Reverse the buckets in the bucket
                                            % list
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    R5A_MultiBucket = R5A_MultiBucketRunner(),
    io:format("R5A_MultiBucket ~w ~n", [R5A_MultiBucket]),
    true = R5A_MultiBucket == 37000,


    {async, R5B_MultiBucketRunner} = 
        leveled_bookie:book_headfold(Bookie1B,
                                        ?RIAK_TAG,
                                        {bucket_list,
                                            [<<"BaselineB">>, <<"B2">>, <<"B0">>]},
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    R5B_MultiBucket = R5B_MultiBucketRunner(),
    io:format("R5B_MultiBucket ~w ~n", [R5B_MultiBucket]),
    true = R5B_MultiBucket == 37000,

    testutil:update_some_objects(Bookie1A, ObjList1, 1000),
    R6A_PlusFilter = lists:foldl(FoldRangesFun(Bookie1A, 
                                    {ObjL1StartTS, ObjL1EndTS},
                                    100000,
                                    100000),
                        {0, 0}, lists:seq(1, 1)),
    io:format("R6A_PlusFilter ~w~n", [R6A_PlusFilter]),
    true = 19000 == element(2, R6A_PlusFilter),

    % Hit limit of max count before trying next bucket, with and without a
    % timestamp filter
    {async, R7A_MultiBucketRunner} = 
        leveled_bookie:book_headfold(Bookie1A,
                                        ?RIAK_TAG,
                                        {bucket_list, [<<"B1">>, <<"B2">>]},
                                        {SimpleCountFun([<<"B1">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL5StartTS, ObjL6EndTS},
                                        5000),
    R7A_MultiBucket = R7A_MultiBucketRunner(),
    io:format("R7A_MultiBucket ~w ~n", [R7A_MultiBucket]),
    true = R7A_MultiBucket == {0, 5000},

    {async, R8A_MultiBucketRunner} = 
        leveled_bookie:book_headfold(Bookie1A,
                                        ?RIAK_TAG,
                                        {bucket_list, [<<"B1">>, <<"B2">>]},
                                        {SimpleCountFun([<<"B1">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        false,
                                        5000),
    R8A_MultiBucket = R8A_MultiBucketRunner(),
    io:format("R8A_MultiBucket ~w ~n", [R8A_MultiBucket]),
    true = R8A_MultiBucket == {0, 5000},

    ok = leveled_bookie:book_close(Bookie1B),

    io:format("Double query to generate index cache and use~n"),
    {ok, Bookie1BS} = leveled_bookie:book_start(StartOpts1B),
    
    TooLate = testutil:convert_to_seconds(os:timestamp()),

    lmdrange_tester(Bookie1BS, SimpleCountFun,
                    ObjL4StartTS, ObjL6StartTS, ObjL6EndTS, TooLate),

    io:format("Push tested keys down levels with new objects~n"),
    ObjList7 = 
        testutil:generate_objects(200000, 
                                    {fixed_binary, 1}, [],
                                    leveled_rand:rand_bytes(32),
                                    fun() -> [] end,
                                    <<"B1.9">>),
    testutil:riakload(Bookie1BS, ObjList7),

    lmdrange_tester(Bookie1BS, SimpleCountFun,
                    ObjL4StartTS, ObjL6StartTS, ObjL6EndTS, TooLate),

    ok = leveled_bookie:book_destroy(Bookie1A),
    ok = leveled_bookie:book_destroy(Bookie1BS).

time_filtered_query(FoldRangesFun, Bookie, ObjL1StartTS, ObjL1EndTS) ->
    PlusFilterStart = os:timestamp(),
    R3A_PlusFilter = lists:foldl(FoldRangesFun(Bookie, 
                                    {ObjL1StartTS, ObjL1EndTS},
                                    100000,
                                    100000),
                        {0, 0}, lists:seq(1, 1)),
    PlusFilterTime = timer:now_diff(os:timestamp(), PlusFilterStart) div 1000,
    io:format("R3A_PlusFilter ~w in ~w~n", [R3A_PlusFilter, PlusFilterTime]),
    true = {20000, 20000} == R3A_PlusFilter,
    PlusFilterTime.

lmdrange_tester(Bookie1BS, SimpleCountFun,
                ObjL4StartTS, ObjL6StartTS, ObjL6EndTS, TooLate) ->
    {async, R5B_MultiBucketRunner0} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        all,
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    R5B_MultiBucket0 = R5B_MultiBucketRunner0(),
    io:format("R5B_MultiBucket0 ~w ~n", [R5B_MultiBucket0]),
    true = R5B_MultiBucket0 == 37000,
    {async, R5B_MultiBucketRunner1} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        all,
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    R5B_MultiBucket1 = R5B_MultiBucketRunner1(),
    io:format("R5B_MultiBucket1 ~w ~n", [R5B_MultiBucket1]),
    true = R5B_MultiBucket1 == 37000,
    SimpleMinMaxFun = 
        fun(B, K, _V, Acc) ->
            case lists:keyfind(B, 1, Acc) of
                {B, MinK, MaxK} ->
                    lists:ukeysort(1, [{B, min(K, MinK), max(K, MaxK)}|Acc]);
                false ->
                    lists:ukeysort(1, [{B, K, K}|Acc])
            end
        end,
    {async, R5B_MultiBucketRunner2} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        {bucket_list, [<<"B0">>, <<"B2">>]},
                                        {SimpleMinMaxFun, []},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    [{<<"B0">>, MinB0K, MaxB0K}, {<<"B2">>, MinB2K, MaxB2K}] =
        R5B_MultiBucketRunner2(),
    io:format("Found Min and Max Keys~n"),
    io:format("B ~s MinK ~s MaxK ~s~n", [<<"B0">>, MinB0K, MaxB0K]),
    io:format("B ~s MinK ~s MaxK ~s~n", [<<"B2">>, MinB2K, MaxB2K]),
    {async, R5B_MultiBucketRunner3a} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        {range, <<"B0">>, {MinB0K, MaxB0K}},
                                        {SimpleCountFun([<<"B0">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    {async, R5B_MultiBucketRunner3b} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        {range, <<"B2">>, {MinB2K, MaxB2K}},
                                        {SimpleCountFun([<<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL4StartTS, ObjL6EndTS},
                                        false),
    R5B_MultiBucket3a = R5B_MultiBucketRunner3a(),
    io:format("R5B_MultiBucket3a ~w ~n", [R5B_MultiBucket3a]),
    R5B_MultiBucket3b = R5B_MultiBucketRunner3b(),
    io:format("R5B_MultiBucket3b ~w ~n", [R5B_MultiBucket3b]),
    true = (R5B_MultiBucket3a + R5B_MultiBucket3b) == 37000,

    io:format("Query outside of time range~n"),
    {async, R5B_MultiBucketRunner4} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        all,
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL6EndTS,
                                            TooLate},
                                        false),
    R5B_MultiBucket4 = R5B_MultiBucketRunner4(),
    io:format("R5B_MultiBucket4 ~w ~n", [R5B_MultiBucket4]),
    true = R5B_MultiBucket4 == 0,

    io:format("Query with one foot inside of time range~n"),
    {async, R5B_MultiBucketRunner5} = 
        leveled_bookie:book_headfold(Bookie1BS,
                                        ?RIAK_TAG,
                                        all,
                                        {SimpleCountFun([<<"B0">>, <<"B2">>]), 0},
                                        false,
                                        true,
                                        false,
                                        {ObjL6StartTS,
                                            TooLate},
                                        false),
    R5B_MultiBucket5 = R5B_MultiBucketRunner5(),
    io:format("R5B_MultiBucket5 ~w ~n", [R5B_MultiBucket5]),
    true = R5B_MultiBucket5 == 7000.


crossbucket_aae(_Config) ->
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),

    % Start the first database, load a test object, close it, start it again
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, riak_sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {B1, K1, V1, S1, MD} = {<<"Bucket">>,
                                <<"Key1.1.4567.4321">>,
                                <<"Value1">>,
                                [],
                                [{<<"MDK1">>, <<"MDV1">>}]},
    {TestObject, TestSpec} = testutil:generate_testobject(B1, K1, V1, S1, MD),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPathA},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 32000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forobject(Bookie2, TestObject),

    % Generate 200K objects to be used within the test, and load them into
    % the first store (outputting the generated objects as a list of lists)
    % to be used elsewhere

    GenList = 
        [{binary, 2}, {binary, 40002}, {binary, 80002}, {binary, 120002}],
    CLs = testutil:load_objects(40000,
                                GenList,
                                Bookie2,
                                TestObject,
                                fun testutil:generate_smallobjects/2,
                                40000),

    %% Check all the objects are found - used to trigger HEAD performance log
    ok = testutil:checkhead_forlist(Bookie2, lists:nth(1, CLs)),

    test_segfilter_query(Bookie2, CLs),

    % Start a new store, and load the same objects (except fot the original
    % test object) into this store
    %
    % This is now the comparison part of the test

    StartOpts3 = [{root_path, RootPathB},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie3, ObjL) end, CLs),
    test_singledelta_stores(Bookie2, Bookie3, small, {B1, K1}),
    test_singledelta_stores(Bookie2, Bookie3, medium, {B1, K1}),
    test_singledelta_stores(Bookie2, Bookie3, xsmall, {B1, K1}),
    test_singledelta_stores(Bookie2, Bookie3, xxsmall, {B1, K1}),

    % Test with a newly opened book (i.e with no block indexes cached)
    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie2A} = leveled_bookie:book_start(StartOpts2),

    test_segfilter_query(Bookie2A, CLs),
    test_segfilter_query(Bookie2A, CLs),

    test_singledelta_stores(Bookie2A, Bookie3, small, {B1, K1}),

    ok = leveled_bookie:book_close(Bookie2A),
    ok = leveled_bookie:book_close(Bookie3).


test_segfilter_query(Bookie, CLs) ->
    % This part of the test tests an issue with accelerating folds by segment
    % list, when there is more than one key with a matching segment in the 
    % slot.  Previously this was not handled correctly - and this test part
    % of the test detects this, by finding slices of keys which are probably
    % in the same slot 
    SW0 = os:timestamp(),
    SliceSize = 20,

    CL1 = lists:sublist(lists:nth(1, CLs), 100, SliceSize),
    CL2 = lists:sublist(lists:nth(2, CLs), 100, SliceSize),
    CL3 = lists:sublist(lists:nth(3, CLs), 100, SliceSize),
    CL4 = lists:sublist(lists:nth(4, CLs), 100, SliceSize),

    SegMapFun = 
        fun({_RN, RiakObject, _Spc}) ->
            B = testutil:get_bucket(RiakObject),
            K = testutil:get_key(RiakObject),
            leveled_tictac:keyto_segment32(<<B/binary, K/binary>>)
        end,
    BKMapFun = 
        fun({_RN, RiakObject, _Spc}) ->
            B = testutil:get_bucket(RiakObject),
            K = testutil:get_key(RiakObject),
            {B, K}
        end,

    SL1 = lists:map(SegMapFun, CL1),
    SL2 = lists:map(SegMapFun, CL2),
    SL3 = lists:map(SegMapFun, CL3),
    SL4 = lists:map(SegMapFun, CL4),

    BK1 = lists:map(BKMapFun, CL1),
    BK2 = lists:map(BKMapFun, CL2),
    BK3 = lists:map(BKMapFun, CL3),
    BK4 = lists:map(BKMapFun, CL4),

    HeadSegmentFolderGen =
        fun(SegL, BKL) ->
            {foldheads_allkeys,
                ?RIAK_TAG,
                {fun(B, K, _PO, Acc) -> 
                        case lists:member({B, K}, BKL) of 
                            true ->
                                Acc + 1;
                            false ->
                                Acc
                        end
                        end,  0},
                false, true, SegL, false, false}
        end,

    {async, SL1Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL1, BK1)),
    {async, SL2Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL2, BK2)),
    {async, SL3Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL3, BK3)),
    {async, SL4Folder} =
        leveled_bookie:book_returnfolder(Bookie, 
                                            HeadSegmentFolderGen(SL4, BK4)),

    Results = [SL1Folder(), SL2Folder(), SL3Folder(), SL4Folder()],
    io:format("SegList folders returned results of ~w " ++ 
                "for SliceSize ~w in ~w ms~n",
                [Results, SliceSize,
                    timer:now_diff(os:timestamp(), SW0)/1000]),
    lists:foreach(fun(R) -> true = R == SliceSize end, Results).


test_singledelta_stores(BookA, BookB, TreeSize, DeltaKey) ->
    io:format("Test for single delta with tree size ~w~n", [TreeSize]),
    % Now run a tictac query against both stores to see the extent to which
    % state between stores is consistent
    TicTacFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {fun head_tictac_foldfun/4, 
                {0, leveled_tictac:new_tree(test, TreeSize)}},
            false, true, false, false, false},
    % tictac query by bucket (should be same result as all stores)
    TicTacByBucketFolder = 
        {foldheads_bybucket, 
                ?RIAK_TAG, <<"Bucket">>, 
                all, 
                {fun head_tictac_foldfun/4, 
                    {0, leveled_tictac:new_tree(test, TreeSize)}},
                false, false, false, false, false},

    DLs = check_tictacfold(BookA, BookB, 
                            TicTacFolder, 
                            DeltaKey, 
                            TreeSize),
    DLs = check_tictacfold(BookA, BookB, 
                            TicTacByBucketFolder, 
                            DeltaKey, 
                            TreeSize),
    
    HeadSegmentFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true, false, false, false},
    
    SW_SL0 = os:timestamp(),
    {async, BookASegFolder} =
        leveled_bookie:book_returnfolder(BookA, HeadSegmentFolder),
    {async, BookBSegFolder} =
        leveled_bookie:book_returnfolder(BookB, HeadSegmentFolder),
    BookASegList = BookASegFolder(),
    BookBSegList = BookBSegFolder(),
    Time_SL0 = timer:now_diff(os:timestamp(), SW_SL0)/1000,
    io:format("Two unfiltered segment list folds took ~w milliseconds ~n", 
                [Time_SL0]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList), length(BookBSegList)]),

    Delta = lists:subtract(BookASegList, BookBSegList),
    true = length(Delta) == 1,

    SegFilterList = leveled_tictac:generate_segmentfilter_list(DLs, TreeSize),
    
    SuperHeadSegmentFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true, SegFilterList, false, false},
    
    SW_SL1 = os:timestamp(),
    {async, BookASegFolder1} =
        leveled_bookie:book_returnfolder(BookA, SuperHeadSegmentFolder),
    {async, BookBSegFolder1} =
        leveled_bookie:book_returnfolder(BookB, SuperHeadSegmentFolder),
    BookASegList1 = BookASegFolder1(),
    BookBSegList1 = BookBSegFolder1(),
    Time_SL1 = timer:now_diff(os:timestamp(), SW_SL1)/1000,
    io:format("Two filtered segment list folds took ~w milliseconds ~n", 
                [Time_SL1]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList1), length(BookBSegList1)]),
    
    SuperHeadSegmentFolderCP = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            true, true, SegFilterList, false, false},
    
    SW_SL1CP = os:timestamp(),
    {async, BookASegFolder1CP} =
        leveled_bookie:book_returnfolder(BookA, SuperHeadSegmentFolderCP),
    {async, BookBSegFolder1CP} =
        leveled_bookie:book_returnfolder(BookB, SuperHeadSegmentFolderCP),
    BookASegList1CP = BookASegFolder1CP(),
    BookBSegList1CP = BookBSegFolder1CP(),
    Time_SL1CP = timer:now_diff(os:timestamp(), SW_SL1CP)/1000,
    io:format("Two filtered segment list folds " ++ 
                "with presence check took ~w milliseconds ~n", 
                [Time_SL1CP]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList1CP), length(BookBSegList1CP)]),
    

    FalseMatchFilter = DLs ++ [1, 100, 101, 1000, 1001],
    SegFilterListF = 
        leveled_tictac:generate_segmentfilter_list(FalseMatchFilter, TreeSize),
    SuperHeadSegmentFolderF = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {get_segment_folder(DLs, TreeSize),  []},
            false, true, SegFilterListF, false, false},
    
    SW_SL1F = os:timestamp(),
    {async, BookASegFolder1F} =
        leveled_bookie:book_returnfolder(BookA, SuperHeadSegmentFolderF),
    {async, BookBSegFolder1F} =
        leveled_bookie:book_returnfolder(BookB, SuperHeadSegmentFolderF),
    BookASegList1F = BookASegFolder1F(),
    BookBSegList1F = BookBSegFolder1F(),
    Time_SL1F = timer:now_diff(os:timestamp(), SW_SL1F)/1000,
    io:format("Two filtered segment list folds " ++ 
                " with false positives took ~w milliseconds ~n", 
                [Time_SL1F]),
    io:format("Segment lists found of lengths ~w ~w~n", 
                [length(BookASegList1F), length(BookBSegList1F)]),

    Delta1F = lists:subtract(BookASegList1F, BookBSegList1F),
    io:format("Delta found of ~w~n", [Delta1F]),
    true = length(Delta1F) == 1.


get_segment_folder(SegmentList, TreeSize) ->
    fun(B, K, PO, KeysAndClocksAcc) ->
        SegmentH = leveled_tictac:keyto_segment32(<<B/binary, K/binary>>),
        Segment = leveled_tictac:get_segment(SegmentH, TreeSize),
        case lists:member(Segment, SegmentList) of
            true ->
                {VC, _Sz, _SC} = summary_from_binary(PO),
                [{B, K, VC}|KeysAndClocksAcc];
            false ->
                KeysAndClocksAcc
        end     
    end.

head_tictac_foldfun(B, K, PO, {Count, TreeAcc}) ->
    ExtractFun = 
        fun({BBin, KBin}, Obj) ->
            {VC, _Sz, _SC} = summary_from_binary(Obj),
            {<<BBin/binary, KBin/binary>>, lists:sort(VC)}
        end,
    {Count + 1, 
        leveled_tictac:add_kv(TreeAcc, {B, K}, PO, ExtractFun)}.


check_tictacfold(BookA, BookB, HeadTicTacFolder, DeltaKey, TreeSize) ->
    SW_TT0 = os:timestamp(),
    {async, BookATreeFolder} =
        leveled_bookie:book_returnfolder(BookA, HeadTicTacFolder),
    {async, BookBTreeFolder} =
        leveled_bookie:book_returnfolder(BookB, HeadTicTacFolder),
    {CountA, BookATree} = BookATreeFolder(),
    {CountB, BookBTree} = BookBTreeFolder(),
    Time_TT0 = timer:now_diff(os:timestamp(), SW_TT0)/1000,
    io:format("Two tree folds took ~w milliseconds ~n", [Time_TT0]),

    io:format("Fold over keys revealed counts of ~w and ~w~n", 
                [CountA, CountB]),

    DLs = leveled_tictac:find_dirtyleaves(BookATree, BookBTree),
    io:format("Found dirty leaves with Riak fold_heads of ~w~n",
                [length(DLs)]),
    case DeltaKey of
        {B1, K1} ->
            % There should be a single delta between the stores
            1 = CountA - CountB,
            true = length(DLs) == 1,
            ExpSeg = leveled_tictac:keyto_segment32(<<B1/binary, K1/binary>>),
            TreeSeg = leveled_tictac:get_segment(ExpSeg, TreeSize),
            [ActualSeg] = DLs,
            true = TreeSeg == ActualSeg;
        none ->
            0 = CountA - CountB,
            true = length(DLs) == 0
    end,
    DLs.


summary_from_binary(<<131, _Rest/binary>>=ObjBin) ->
    {proxy_object, HeadBin, ObjSize, _Fetcher} = binary_to_term(ObjBin),
    summary_from_binary(HeadBin, ObjSize);
summary_from_binary(ObjBin) when is_binary(ObjBin) ->
    summary_from_binary(ObjBin, byte_size(ObjBin)).

summary_from_binary(ObjBin, ObjSize) ->
    <<?MAGIC:8/integer, 
        1:8/integer, 
        VclockLen:32/integer, VclockBin:VclockLen/binary, 
        SibCount:32/integer, 
        _Rest/binary>> = ObjBin,
    {lists:usort(binary_to_term(VclockBin)), ObjSize, SibCount}.



handoff(_Config) ->
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),

    % Start the first database, load a test object, close it, start it again
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    % Add some none Riak objects in - which should be ignored in folds.
    Hashes = testutil:stdload(Bookie1, 1000),
    % Generate 200K objects to be used within the test, and load them into
    % the first store (outputting the generated objects as a list of lists)
    % to be used elsewhere

    GenList = 
        [binary_uuid, binary_uuid, binary_uuid, binary_uuid],
    [CL0, CL1, CL2, CL3] = 
        testutil:load_objects(40000,
                                GenList,
                                Bookie1,
                                no_check,
                                fun testutil:generate_smallobjects/2,
                                40000),
    
    % Update an delete some objects
    testutil:update_some_objects(Bookie1, CL0, 1000),
    testutil:update_some_objects(Bookie1, CL1, 20000),
    testutil:delete_some_objects(Bookie1, CL2, 10000),
    testutil:delete_some_objects(Bookie1, CL3, 4000),

    % Compact the journal
    ok = leveled_bookie:book_compactjournal(Bookie1, 30000),
    testutil:wait_for_compaction(Bookie1),

    % Start two new empty stores
    StartOpts2 = [{root_path, RootPathB},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, none}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    StartOpts3 = [{root_path, RootPathC},
                    {max_pencillercachesize, 30000},
                    {sync_strategy, none}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    StartOpts4 = [{root_path, RootPathD},
                    {max_pencillercachesize, 30000},
                    {sync_strategy, none}],
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts4),

    FoldStObjectsFun = 
        fun(B, K, V, Acc) ->
            [{B, K, erlang:phash2(V)}|Acc]
        end,

    FoldObjectsFun = 
        fun(Book) ->
            fun(B, K, Obj, ok) ->
                leveled_bookie:book_put(Book, B, K, Obj, [], ?RIAK_TAG),
                ok
            end
        end,
    
    % Handoff the data from the first store to the other three stores
    {async, Handoff2} =
        leveled_bookie:book_objectfold(Bookie1,
                                       ?RIAK_TAG,
                                       {FoldObjectsFun(Bookie2), ok},
                                       false,
                                       key_order),
    SW2 = os:timestamp(),
    ok = Handoff2(),
    Time_HO2 = timer:now_diff(os:timestamp(), SW2)/1000,
    io:format("Handoff to Book2 in key_order took ~w milliseconds ~n", 
                [Time_HO2]),
    SW3 = os:timestamp(),
    {async, Handoff3} =
        leveled_bookie:book_objectfold(Bookie1,
                                       ?RIAK_TAG,
                                       {FoldObjectsFun(Bookie3), ok},
                                       true,
                                       sqn_order),
    ok = Handoff3(),
    Time_HO3 = timer:now_diff(os:timestamp(), SW3)/1000,
    io:format("Handoff to Book3 in sqn_order took ~w milliseconds ~n", 
                [Time_HO3]),
    SW4 = os:timestamp(),
    {async, Handoff4} =
        leveled_bookie:book_objectfold(Bookie1,
                                       ?RIAK_TAG,
                                       {FoldObjectsFun(Bookie4), ok},
                                       true,
                                       sqn_order),

    ok = Handoff4(),
    Time_HO4 = timer:now_diff(os:timestamp(), SW4)/1000,
    io:format("Handoff to Book4 in sqn_order took ~w milliseconds ~n", 
                [Time_HO4]),

    % Run tictac folds to confirm all stores consistent after handoff
    TreeSize = xxsmall,

    TicTacFolder = 
        {foldheads_allkeys,
            ?RIAK_TAG,
            {fun head_tictac_foldfun/4, 
                {0, leveled_tictac:new_tree(test, TreeSize)}},
            false, true, false, false, false},
    check_tictacfold(Bookie1, Bookie2, TicTacFolder, none, TreeSize),
    check_tictacfold(Bookie2, Bookie3, TicTacFolder, none, TreeSize),
    check_tictacfold(Bookie3, Bookie4, TicTacFolder, none, TreeSize),

    StdFolder = 
        {foldobjects_allkeys,
            ?STD_TAG,
            FoldStObjectsFun,
            true, 
            sqn_order},
    
    {async, StdFold1} = leveled_bookie:book_returnfolder(Bookie1, StdFolder),
    {async, StdFold2} = leveled_bookie:book_returnfolder(Bookie2, StdFolder),
    {async, StdFold3} = leveled_bookie:book_returnfolder(Bookie3, StdFolder),
    {async, StdFold4} = leveled_bookie:book_returnfolder(Bookie4, StdFolder),
    StdFoldOut1 = lists:sort(StdFold1()),
    StdFoldOut2 = lists:sort(StdFold2()),
    StdFoldOut3 = lists:sort(StdFold3()),
    StdFoldOut4 = lists:sort(StdFold4()),
    true = StdFoldOut1 == lists:sort(Hashes),
    true = StdFoldOut2 == [],
    true = StdFoldOut3 == [],
    true = StdFoldOut4 == [],

    % Shutdown
    ok = leveled_bookie:book_close(Bookie1),
    ok = leveled_bookie:book_close(Bookie2),
    ok = leveled_bookie:book_close(Bookie3),
    ok = leveled_bookie:book_close(Bookie4).

%% @doc test that the riak specific $key index can be iterated using
%% leveled's existing folders
dollar_key_index(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} = leveled_bookie:book_start(RootPath,
                                              2000,
                                              50000000,
                                              testutil:sync_strategy()),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 = testutil:generate_objects(1300,
                                      {fixed_binary, 1},
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket1">>),
    testutil:riakload(Bookie1, ObjL1),

    FoldKeysFun = fun(_B, K, Acc) ->
                          [ K |Acc]
                  end,

    StartKey = testutil:fixed_bin_key(123),
    EndKey = testutil:fixed_bin_key(779),

    {async, Folder} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket1">>,
                                    {StartKey, EndKey},
                                    {FoldKeysFun, []}
                                    ),
    ResLen = length(Folder()),
    io:format("Length of Result of folder ~w~n", [ResLen]),
    true = 657 == ResLen,

    {ok, REMatch} = re:compile("K.y"),
    {ok, REMiss} = re:compile("key"),
    
    {async, FolderREMatch} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket1">>,
                                    {StartKey, EndKey},
                                    {FoldKeysFun, []},
                                    REMatch),
    {async, FolderREMiss} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket1">>,
                                    {StartKey, EndKey},
                                    {FoldKeysFun, []},
                                    REMiss),
                                                
    true = 657 == length(FolderREMatch()),
    true = 0 == length(FolderREMiss()),

    % Delete an object - and check that it does not show in 
    % $key index query
    DeleteFun =
        fun(KeyID) ->
            ok = leveled_bookie:book_put(Bookie1, 
                                            <<"Bucket1">>, 
                                            testutil:fixed_bin_key(KeyID), 
                                            delete, [],
                                            ?RIAK_TAG)
        end,
    DelList = [200, 400, 600, 800, 1200],
    lists:foreach(DeleteFun, DelList),
    
    {async, DeleteFolder0} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket1">>,
                                    {StartKey, EndKey},
                                    {FoldKeysFun, []}
                                    ),
    ResultsDeleteFolder0 = length(DeleteFolder0()),
    io:format("Length of Result of folder ~w~n", [ResultsDeleteFolder0]),
    true = 657 - 3 == ResultsDeleteFolder0,

    {async, DeleteFolder1} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket1">>,
                                    {testutil:fixed_bin_key(1151), 
                                        testutil:fixed_bin_key(1250)},
                                    {FoldKeysFun, []}
                                    ),
    ResultsDeleteFolder1 = length(DeleteFolder1()),
    io:format("Length of Result of folder ~w~n", [ResultsDeleteFolder1]),
    true = 100 -1 == ResultsDeleteFolder1,

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().

handoff_close(_Config) ->
    RootPath = testutil:reset_filestructure(),
    KeyCount = 500000,
    Bucket = {<<"BType">>, <<"BName">>},
    StartOpts1 =
        [
            {root_path, RootPath},
            {max_journalobjectcount, KeyCount + 1},
            {max_pencillercachesize, 12000},
            {sync_strategy, testutil:sync_strategy()}
        ],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    ObjList1 = 
        testutil:generate_objects(
            KeyCount div 10, 
            {fixed_binary, 1}, [],
            leveled_rand:rand_bytes(512),
            fun() -> [] end,
            Bucket
        ),
    ObjList2 = 
        testutil:generate_objects(
            KeyCount - (KeyCount div 10), 
            {fixed_binary, KeyCount div 10 + 1}, [],
            leveled_rand:rand_bytes(512),
            fun() -> [] end,
            Bucket
        ),
    testutil:riakload(Bookie1, ObjList1),
    FoldObjectsFun =
        fun(_, _, _, Acc) ->
            [os:timestamp()|Acc]
        end,
    {async, Runner} =
        leveled_bookie:book_objectfold(
            Bookie1,
            ?RIAK_TAG,
            {FoldObjectsFun, []},
            true,
            sqn_order
        ),
    testutil:riakload(Bookie1, ObjList2),
    TSList = Runner(),
    QueryCompletionTime = os:timestamp(),
    LastTS = hd(TSList),
    io:format(
        "Found ~w objects with Last TS ~w completion time ~w~n",
        [length(TSList), LastTS, QueryCompletionTime]
    ),
    true = KeyCount div 10 == length(TSList),
    TimeSinceLastObjectTouchedMS =
        timer:now_diff(QueryCompletionTime, LastTS) div 1000,
    true = TimeSinceLastObjectTouchedMS < 1000,
    leveled_bookie:book_destroy(Bookie1),
    testutil:reset_filestructure().


%% @doc test that the riak specific $bucket indexes can be iterated
%% using leveled's existing folders
dollar_bucket_index(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie1} = leveled_bookie:book_start(RootPath,
                                              2000,
                                              50000000,
                                              testutil:sync_strategy()),
    ObjectGen = testutil:get_compressiblevalue_andinteger(),
    IndexGen = fun() -> [] end,
    ObjL1 = testutil:generate_objects(1300,
                                      uuid,
                                      [],
                                      ObjectGen,
                                      IndexGen,
                                      <<"Bucket1">>),
    testutil:riakload(Bookie1, ObjL1),
    ObjL2 = testutil:generate_objects(1700,
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

    FoldKeysFun = fun(B, K, Acc) ->
                          [{B, K}|Acc]
                  end,
    FoldAccT = {FoldKeysFun, []},

    {async, Folder} = 
        leveled_bookie:book_keylist(Bookie1, 
                                    ?RIAK_TAG, 
                                    <<"Bucket2">>, 
                                    FoldAccT),
    Results = Folder(),
    true = 1700 == length(Results),
    
    {<<"Bucket2">>, SampleKey} = lists:nth(100, Results),
    UUID = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
    {ok, RESingleMatch} = re:compile(SampleKey),
    {ok, REAllMatch} = re:compile(UUID),
    {ok, REMiss} = re:compile("no_key"),

    {async, FolderREMiss} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket2">>,
                                    {null, null},
                                    {FoldKeysFun, []},
                                    REMiss),
    {async, FolderRESingleMatch} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket2">>,
                                    {null, null},
                                    {FoldKeysFun, []},
                                    RESingleMatch),
    {async, FolderREAllMatch} = 
        leveled_bookie:book_keylist(Bookie1,
                                    ?RIAK_TAG,
                                    <<"Bucket2">>,
                                    {null, null},
                                    {FoldKeysFun, []},
                                    REAllMatch),
    
    true = 0 == length(FolderREMiss()),
    true = 1 == length(FolderRESingleMatch()),
    true = 1700 == length(FolderREAllMatch()),

    ok = leveled_bookie:book_close(Bookie1),
    testutil:reset_filestructure().


bigobject_memorycheck(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(RootPath,
                                              200,
                                              1000000000,
                                              testutil:sync_strategy()),
    Bucket = <<"B">>,
    IndexGen = fun() -> [] end,
    ObjPutFun = 
        fun(I) ->
            Key = base64:encode(<<I:32/integer>>),
            Value = leveled_rand:rand_bytes(1024 * 1024),
                % a big object each time!
            {Obj, Spc} = testutil:set_object(Bucket, Key, Value, IndexGen, []),
            testutil:book_riakput(Bookie, Obj, Spc)
        end,
    lists:foreach(ObjPutFun, lists:seq(1, 700)),
    {ok, _Ink, Pcl} = leveled_bookie:book_returnactors(Bookie),
    {binary, BL} = process_info(Pcl, binary),
    {memory, M0} = process_info(Pcl, memory),
    B0 = lists:foldl(fun({_R, Sz, _C}, Acc) -> Acc + Sz end, 0, BL),
    io:format("Pcl binary memory ~w ~w memory ~w~n", [B0, length(BL), M0]),
    true = B0 < 500 * 4000,
    true = M0 < 500 * 4000,
    % All processes
    {_TotalCDBBinMem, _TotalCDBProcesses} = cdb_memory_check(),
    ok = leveled_bookie:book_close(Bookie),
    {ok, BookieR} = leveled_bookie:book_start(RootPath,
                                              2000,
                                              1000000000,
                                              testutil:sync_strategy()),
    {RS_TotalCDBBinMem, _RS_TotalCDBProcesses} = cdb_memory_check(),
    true = RS_TotalCDBBinMem < 1024 * 1024,
        % No binary object references exist after startup
    ok = leveled_bookie:book_close(BookieR),
    testutil:reset_filestructure(). 


cdb_memory_check() ->
    TotalCDBProcesses =
        lists:filter(fun(P) ->
                        {dictionary, PD} = 
                            process_info(P, dictionary),
                        case lists:keyfind('$initial_call', 1, PD) of
                            {'$initial_call',{leveled_cdb,init,1}} ->
                                true;
                            _ ->
                                false
                        end
                        end,
                        processes()),
    TotalCDBBinMem =
        lists:foldl(fun(P, Acc) ->
                        BinMem = calc_total_binary_memory(P),
                        io:format("Memory for pid ~w is ~w~n", [P, BinMem]),
                        BinMem + Acc
                    end,
                        0,
                        TotalCDBProcesses),
    io:format("Total binary memory ~w in ~w CDB processes~n",
                [TotalCDBBinMem, length(TotalCDBProcesses)]),
    {TotalCDBBinMem, TotalCDBProcesses}.

calc_total_binary_memory(Pid) ->
    {binary, BL} = process_info(Pid, binary),
    TBM = lists:foldl(fun({_R, Sz, _C}, Acc) -> Acc + Sz end, 0, BL),
    case TBM > 1000000 of
        true ->
            FilteredBL =
                lists:filter(fun(BMD) -> element(2, BMD) > 1024 end, BL),
            io:format("Big-ref details for ~w ~w~n", [Pid, FilteredBL]);
        false ->
            ok
    end,
    TBM.