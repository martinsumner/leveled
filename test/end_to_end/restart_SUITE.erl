-module(restart_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([retain_strategy/1
            ]).

all() -> [
            retain_strategy
            ].

retain_strategy(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BookOpts = #bookie_options{root_path=RootPath,
                                cache_size=1000,
                                max_journalsize=5000000,
                                reload_strategy=[{?RIAK_TAG, retain}]},
    BookOptsAlt = BookOpts#bookie_options{max_run_length=6,
                                            max_journalsize=100000},
    {ok, Spcl3, LastV3} = rotating_object_check(BookOpts, "Bucket3", 800),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3}]),
    {ok, Spcl4, LastV4} = rotating_object_check(BookOpts, "Bucket4", 1600),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4}]),
    {ok, Spcl5, LastV5} = rotating_object_check(BookOpts, "Bucket5", 3200),
    ok = restart_from_blankledger(BookOptsAlt, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket5", Spcl5, LastV5}]),
    {ok, Spcl6, LastV6} = rotating_object_check(BookOpts, "Bucket6", 6400),
    ok = restart_from_blankledger(BookOpts, [{"Bucket3", Spcl3, LastV3},
                                                {"Bucket4", Spcl4, LastV4},
                                                {"Bucket5", Spcl5, LastV5},
                                                {"Bucket6", Spcl6, LastV6}]),
    testutil:reset_filestructure().



rotating_object_check(BookOpts, B, NumberOfObjects) ->
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    {KSpcL1, V1} = testutil:put_indexed_objects(Book1, B, NumberOfObjects),
    ok = testutil:check_indexed_objects(Book1,
                                        B,
                                        KSpcL1,
                                        V1),
    {KSpcL2, V2} = testutil:put_altered_indexed_objects(Book1,
                                                        B,
                                                        KSpcL1,
                                                        false),
    ok = testutil:check_indexed_objects(Book1,
                                        B,
                                        KSpcL1 ++ KSpcL2,
                                        V2),
    {KSpcL3, V3} = testutil:put_altered_indexed_objects(Book1,
                                                        B,
                                                        KSpcL2,
                                                        false),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3,
                                        V3),
    {KSpcL4, V4} = testutil:put_altered_indexed_objects(Book2,
                                                        B,
                                                        KSpcL3,
                                                        false),
    io:format("Bucket complete - checking index before compaction~n"),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4,
                                        V4),
    
    ok = leveled_bookie:book_compactjournal(Book2, 30000),
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(20000),
                                F(Book2)
                        end end,
                    true,
                    lists:seq(1, 15)),
    io:format("Waiting for journal deletes~n"),
    timer:sleep(20000),
    
    io:format("Checking index following compaction~n"),
    ok = testutil:check_indexed_objects(Book2,
                                        B,
                                        KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4,
                                        V4),
    
    ok = leveled_bookie:book_close(Book2),
    {ok, KSpcL1 ++ KSpcL2 ++ KSpcL3 ++ KSpcL4, V4}.
    
    
restart_from_blankledger(BookOpts, B_SpcL) ->
    leveled_penciller:clean_testdir(BookOpts#bookie_options.root_path ++
                                    "/ledger"),
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    io:format("Checking index following restart~n"),
    lists:foreach(fun({B, SpcL, V}) ->
                        ok = testutil:check_indexed_objects(Book1, B, SpcL, V)
                        end,
                    B_SpcL),
    ok = leveled_bookie:book_close(Book1),
    ok.