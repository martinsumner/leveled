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
    % Test requires multiple different databases, so want to mount them all
    % on individual file paths
    RootPathA = testutil:reset_filestructure("testA"),
    RootPathB = testutil:reset_filestructure("testB"),
    RootPathC = testutil:reset_filestructure("testC"),
    RootPathD = testutil:reset_filestructure("testD"),
    
    % Start the first database, load a test object, close it, start it again
    StartOpts1 = [{root_path, RootPathA},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, riak_sync}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    {B1, K1, V1, S1, MD} = {"Bucket",
                                "Key1.1.4567.4321",
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
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
    
    % Generate 200K objects to be sued within the test, and load them into
    % the first store (outputting the generated objects as a list of lists)
    % to be used elsewhere
    
    GenList = [2, 20002, 40002, 60002, 80002,
                100002, 120002, 140002, 160002, 180002],
    CLs = testutil:load_objects(20000,
                                GenList,
                                Bookie2,
                                TestObject,
                                fun testutil:generate_smallobjects/2,
                                20000),
    
    % Start a new store, and load the same objects (except fot the original
    % test object) into this store
    
    StartOpts3 = [{root_path, RootPathB},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 16000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie3} = leveled_bookie:book_start(StartOpts3),
    lists:foreach(fun(ObjL) -> testutil:riakload(Bookie3, ObjL) end, CLs),
    
    % Now run a tictac query against both stores to see th extent to which
    % state between stores is consistent
    
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
    SegList0 = leveled_tictac:find_dirtyleaves(TreeA, TreeB),
    io:format("Compare tictac trees with 200K objects  in ~w~n",
                [timer:now_diff(os:timestamp(), SWC0)]),
    io:format("Tree comparison shows ~w different leaves~n",
                [length(SegList0)]),
    AltList = leveled_tictac:find_dirtyleaves(TreeA, 
                                                leveled_tictac:new_tree(0)),
    io:format("Tree comparison shows ~w altered leaves~n",
                [length(AltList)]),
    true = length(SegList0) == 1, 
    % only the test object should be different
    true = length(AltList) > 100000, 
    % check there are a significant number of differences from empty
    
    FoldKeysFun =
        fun(SegListToFind) ->
            fun(_B, K, Acc) ->
                Seg = leveled_tictac:get_segment(K),
                case lists:member(Seg, SegListToFind) of
                    true ->
                        [K|Acc];
                    false ->
                        Acc
                end
            end
        end,
    SegQuery = {keylist, o_rkv, "Bucket", {FoldKeysFun(SegList0), []}},
    {async, SegKeyFinder} =
        leveled_bookie:book_returnfolder(Bookie2, SegQuery),
    SWSKL0 = os:timestamp(),
    SegKeyList = SegKeyFinder(),
    io:format("Finding ~w keys in ~w dirty segments in ~w~n",
                [length(SegKeyList),
                    length(SegList0),
                    timer:now_diff(os:timestamp(), SWSKL0)]),
    
    true = length(SegKeyList) >= 1,
    true = length(SegKeyList) < 10,
    true = lists:member("Key1.1.4567.4321", SegKeyList),
    
    % Now remove the object which represents the difference between these
    % stores and confirm that the tictac trees will now match
    
    testutil:book_riakdelete(Bookie2, B1, K1, []),
    {async, TreeAFolder0} = leveled_bookie:book_returnfolder(Bookie2, TicTacQ),
    SWA1 = os:timestamp(),
    TreeA0 = TreeAFolder0(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWA1)]),

    SegList1 = leveled_tictac:find_dirtyleaves(TreeA0, TreeB),
    io:format("Tree comparison following delete shows ~w different leaves~n",
                [length(SegList1)]),
    true = length(SegList1) == 0,
    % Removed test object so tictac trees should match
    
    ok = testutil:book_riakput(Bookie3, TestObject, TestSpec),
    {async, TreeBFolder0} = leveled_bookie:book_returnfolder(Bookie3, TicTacQ),
    SWB1 = os:timestamp(),
    TreeB0 = TreeBFolder0(),
    io:format("Build tictac tree with 200K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWB1)]),
    SegList2 = leveled_tictac:find_dirtyleaves(TreeA0, TreeB0),
    true = SegList2 == SegList0,
    % There is an identical difference now the difference is on Bookie3 not
    % Bookie 2 (compared to it being in Bookie2 not Bookie3)

    ok = leveled_bookie:book_close(Bookie3),
    
    % Replace Bookie 3 with two stores Bookie 4 and Bookie 5 where the ojects
    % have been randomly split between the stores
    
    StartOpts4 = [{root_path, RootPathC},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie4} = leveled_bookie:book_start(StartOpts4),
    StartOpts5 = [{root_path, RootPathD},
                    {max_journalsize, 200000000},
                    {max_pencillercachesize, 24000},
                    {sync_strategy, testutil:sync_strategy()}],
    {ok, Bookie5} = leveled_bookie:book_start(StartOpts5),
    
    SplitFun =
        fun(Obj) ->
            case erlang:phash2(Obj) rem 2 of
                0 ->
                    true;
                1 ->
                    false
            end
        end,
    lists:foreach(fun(ObjL) ->
                        {ObjLA, ObjLB} = lists:partition(SplitFun, ObjL),
                        testutil:riakload(Bookie4, ObjLA),
                        testutil:riakload(Bookie5, ObjLB)
                    end,
                    CLs),
    
    % query both the stores, then merge the trees - the result should be the
    % same as the result from the tree created aginst the store with both
    % partitions
    
    {async, TreeC0Folder} = leveled_bookie:book_returnfolder(Bookie4, TicTacQ),
    {async, TreeC1Folder} = leveled_bookie:book_returnfolder(Bookie5, TicTacQ),
    SWD0 = os:timestamp(),
    TreeC0 = TreeC0Folder(),
    io:format("Build tictac tree with 100K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWD0)]),
    SWD1 = os:timestamp(),
    TreeC1 = TreeC1Folder(),
    io:format("Build tictac tree with 100K objects in ~w~n",
                [timer:now_diff(os:timestamp(), SWD1)]),
    
    TreeC2 = leveled_tictac:merge_trees(TreeC0, TreeC1),
    SegList3 = leveled_tictac:find_dirtyleaves(TreeC2, TreeB),
    io:format("Tree comparison following delete shows ~w different leaves~n",
                [length(SegList3)]),
    true = length(SegList3) == 0,
    
    
    ok = leveled_bookie:book_close(Bookie2),
    ok = leveled_bookie:book_close(Bookie4),
    ok = leveled_bookie:book_close(Bookie5).
    
