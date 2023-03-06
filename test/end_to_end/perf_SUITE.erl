-module(perf_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([bigpcl_bucketlist/1, stall/1
            ]).

all() -> [bigpcl_bucketlist, stall].


stall(_Config) ->
    %% https://github.com/martinsumner/leveled/issues/402
    %% There may be circumstances whereby an excessively large L0 file
    %% is created
    IndexCount = 20,

    RootPath = testutil:reset_filestructure("stallTest"),
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, testutil:sync_strategy()},
                    {database_id, 32},
                    {stats_logfrequency, 5},
                    {stats_probability, 80}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),

    IndexGenFun =
        fun() ->
            RandInt = leveled_rand:uniform(IndexCount),
            [{add, list_to_binary("integerID1_int"), RandInt},
            {add, list_to_binary("binaryID1_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID2_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID3_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID4_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID5_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID6_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID7_bin"), <<RandInt:32/integer>>},
            {add, list_to_binary("binaryID8_bin"), <<RandInt:32/integer>>}
        ]
        end,

    io:format("Create then persist a list of objects~n"),
    ObjList = 
        testutil:generate_objects(
            150000,
            {fixed_binary, 1}, [],
            leveled_rand:rand_bytes(64),
            IndexGenFun,
            <<"StallTest">>),
    {ObjList1, ObjList2} =
        lists:partition(
            fun(_) ->
                rand:uniform(5) == 1
            end,
            ObjList
        ),
    io:format("Load 1 in 5 objects at random~n"),
    ok = testutil:riakload(Bookie1, ObjList1),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    io:format("Block the penciller as if l0 pending~n"),
    io:format("Puts should now backup in the ledger cache~n"),
    {ok, _Ink2, Pcl2} = leveled_bookie:book_returnactors(Bookie2),
    false = 
        lists:foldl(
            fun(_, L0P) ->
                case L0P of
                    false ->
                        false;
                    true ->
                        timer:sleep(1000),
                        leveled_penciller:pcl_l0pending(Pcl2, true)
                end
            end,
            true,
            lists:seq(1, 20)),
    io:format("Load remaining - 4 in 5 - objects~n"),
    ok = testutil:riakload(Bookie2, ObjList2, 1),
    io:format("Unblock the penciller~n"),
    true = leveled_penciller:pcl_l0pending(Pcl2, false),
    io:format("Push a few more objects to prompt merge~n"),
    ObjList3 = 
        testutil:generate_objects(
            10,
            {fixed_binary, 1}, [],
            leveled_rand:rand_bytes(64),
            IndexGenFun,
            <<"OtherBucket">>),
    ok = testutil:riakload(Bookie2, ObjList3),
    io:format("Wait and see what happens~n"),
    timer:sleep(60000),
    ok = leveled_bookie:book_destroy(Bookie2).

    
bigpcl_bucketlist(_Config) ->
    %% https://github.com/martinsumner/leveled/issues/326
    %% In OTP 22+ there appear to be issues with anonymous functions which
    %% have a reference to loop state, requiring a copy of all the loop state
    %% to be made when returning the function.
    %% This test creates  alarge loop state on the leveled_penciller to prove
    %% this.
    %% The problem can be resolved simply by renaming the element of the loop
    %% state using within the anonymous function.
    RootPath = testutil:reset_filestructure(),
    BucketCount = 500,
    ObjectCount = 100,
    StartOpts1 = [{root_path, RootPath},
                    {max_journalsize, 50000000},
                    {cache_size, 4000},
                    {max_pencillercachesize, 128000},
                    {max_sstslots, 256},
                    {sync_strategy, testutil:sync_strategy()},
                    {compression_point, on_compact}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    BucketList =
        lists:map(fun(I) -> list_to_binary(integer_to_list(I)) end,
                    lists:seq(1, BucketCount)),

    MapFun =
        fun(B) ->
            testutil:generate_objects(ObjectCount, 1, [], 
                                        leveled_rand:rand_bytes(100), 
                                        fun() -> [] end, 
                                        B)
        end,
    ObjLofL = lists:map(MapFun, BucketList),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie1, ObjL) end, ObjLofL),
    BucketFold =
        fun(B, _K, _V, Acc) ->
            case sets:is_element(B, Acc) of
                true ->
                    Acc;
                false ->
                    sets:add_element(B, Acc)
            end
        end,
    FBAccT = {BucketFold, sets:new()},

    {async, BucketFolder1} = 
        leveled_bookie:book_headfold(Bookie1,
                                        ?RIAK_TAG,
                                        {bucket_list, BucketList},
                                        FBAccT,
                                        false, false, false),

    {FoldTime1, BucketList1} = timer:tc(BucketFolder1, []),
    true = BucketCount == sets:size(BucketList1),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),
    
    {async, BucketFolder2} = 
        leveled_bookie:book_headfold(Bookie2,
                                        ?RIAK_TAG,
                                        {bucket_list, BucketList},
                                        FBAccT,
                                        false, false, false),
    {FoldTime2, BucketList2} = timer:tc(BucketFolder2, []),
    true = BucketCount == sets:size(BucketList2),

    io:format("Fold pre-close ~w ms post-close ~w ms~n",
                [FoldTime1 div 1000, FoldTime2 div 1000]),

    true = FoldTime1 < 10 * FoldTime2,
    %% The fold in-memory should be the same order of magnitude of response
    %% time as the fold post-persistence

    ok = leveled_bookie:book_destroy(Bookie2).

