-module(tictac_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([
            many_put_compare/1
            ]).

all() -> [
            many_put_compare
            ].


many_put_compare(_Config) ->
    RootPathA = testutil:reset_filestructure("testA"),
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, riak_sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {TestObject, TestSpec} = testutil:generate_testobject(),
    ok = testutil:book_riakput(Bookie1, TestObject, TestSpec),
    testutil:check_forobject(Bookie1, TestObject),
    ok = leveled_bookie:book_close(Bookie1),
    StartOpts2 = [{root_path, RootPathA},
                    {max_journalsize, 500000000},
                    {max_pencillercachesize, 32000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts2),
    testutil:check_forobject(Bookie2, TestObject),
    GenList = [2, 20002, 40002, 60002, 80002,
                100002, 120002, 140002, 160002, 180002],
    CLs = testutil:load_objects(20000,
                                GenList,
                                Bookie2,
                                TestObject,
                                fun testutil:generate_smallobjects/2,
                                20000),
    
    RootPathB = testutil:reset_filestructure("testB"),
    StartOpts3 = [{root_path, RootPathB},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie3, ObjL) end, CLs),
    
    TicTacQ = {tictactree_obj,
                {o_rkv, "Bucket", null, null, false},
                fun(_B, _K) -> accumulate end},
    {async, TreeAFolder} = leveled_bookie:book_returnfolder(Bookie2, TicTacQ),
    {async, TreeBFolder} = leveled_bookie:book_returnfolder(Bookie3, TicTacQ),
    SWA0 = os:timestamp(),
    TreeA = TreeAFolder(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWA0)]),
    SWB0 = os:timestamp(),
    TreeB = TreeBFolder(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWB0)]),
    SWC0 = os:timestamp(),
    SegList = leveled_tictac:find_dirtyleaves(TreeA, TreeB),
    io:format("Compare tictac trees with 200K objects  in ~w~n",
                [timer:now_diff(os:timestamp(), SWC0)]),
    io:format("Tree comparison shows ~w different leaves~n",
                [length(SegList)]),
    true = length(SegList) == 1,
    
    ok = leveled_bookie:book_destroy(Bookie2),
    ok = leveled_bookie:book_destroy(Bookie3).
