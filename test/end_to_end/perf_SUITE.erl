-module(perf_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([bigpcl_bucketlist/1
            ]).

all() -> [bigpcl_bucketlist].

    
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

