-module(appdefined_SUITE).
-include("leveled.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
            application_defined_tag/1,
            bespoketag_recalc/1
            ]).

all() -> [
            application_defined_tag,
            bespoketag_recalc
            ].

init_per_suite(Config) ->
    testutil:init_per_suite([{suite, "appdefined"}|Config]),
    Config.

end_per_suite(Config) ->
    testutil:end_per_suite(Config).

application_defined_tag(_Config) ->
    T1 = os:timestamp(),
    application_defined_tag_tester(40000, ?STD_TAG, [], false),
    io:format("Completed with std tag in ~w ms~n",
                [timer:now_diff(os:timestamp(), T1)/1000]),
    
    T2 = os:timestamp(),
    application_defined_tag_tester(40000, bespoke_tag1, [], false),
    io:format("Completed with app tag but not function in ~w ms~n",
                [timer:now_diff(os:timestamp(), T2)/1000]),
    
    ExtractMDFun = 
        fun(Tag, Size, Obj) ->
            [{hash, Hash}, {shard, Shard}, {random, Random}, {value, _V}]
                = Obj,
            case Tag of
                bespoke_tag1 ->
                    {{Hash, Size, [{shard, Shard}, {random, Random}]}, []};
                bespoke_tag2 ->
                    {{Hash, Size, [{shard, Shard}]}, [os:timestamp()]}
            end
        end,
    
    T3 = os:timestamp(),
    application_defined_tag_tester(40000, ?STD_TAG,
                                    [{extract_metadata, ExtractMDFun}],
                                    false),
    io:format("Completed with std tag and override function in ~w ms~n",
                [timer:now_diff(os:timestamp(), T3)/1000]),
    
    T4 = os:timestamp(),
    application_defined_tag_tester(40000, bespoke_tag1,
                                    [{extract_metadata, ExtractMDFun}],
                                    true),
    io:format("Completed with app tag and override function in ~w ms~n",
                [timer:now_diff(os:timestamp(), T4)/1000]),
    

    T5 = os:timestamp(),
    application_defined_tag_tester(40000, bespoke_tag2,
                                    [{extract_metadata, ExtractMDFun}],
                                    true),
    io:format("Completed with app tag and override function in ~w ms~n",
                [timer:now_diff(os:timestamp(), T5)/1000]).


application_defined_tag_tester(KeyCount, Tag, Functions, ExpectMD) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts1 = [{root_path, RootPath},
                    {sync_strategy, testutil:sync_strategy()},
                    {log_level, warn},
                    {reload_strategy,
                        [{bespoke_tag1, retain}, {bespoke_tag2, retain}]},
                    {override_functions, Functions}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    Value = crypto:strong_rand_bytes(512),
    MapFun = 
        fun(C) -> 
            {C, object_generator(C, Value)}
        end,
    CBKVL = lists:map(MapFun, lists:seq(1, KeyCount)),

    PutFun =
        fun({_C, {B, K, O}}) ->
            R = leveled_bookie:book_put(Bookie1, B, K, O, [], Tag),
            case R of
                ok -> ok;
                pause -> timer:sleep(100)
            end
        end,
    lists:foreach(PutFun, CBKVL),

    CheckFun =
        fun(Book) ->
            fun({C, {B, K, O}}) ->
                {ok, O} = leveled_bookie:book_get(Book, B, K, Tag),
                {ok, H} = leveled_bookie:book_head(Book, B, K, Tag),
                MD = element(3, H),
                case ExpectMD of
                    true ->
                        true =
                            {shard, C rem 10} == lists:keyfind(shard, 1, MD);
                    false ->
                        true = 
                            undefined == MD
                end
            end
        end,
    
    lists:foreach(CheckFun(Bookie1), CBKVL),

    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts1),

    lists:foreach(CheckFun(Bookie2), CBKVL),

    ok = leveled_bookie:book_close(Bookie2).


object_generator(Count, V) ->
    Hash = erlang:phash2({count, V}),
    Random = rand:uniform(1000),
    Key = list_to_binary(leveled_util:generate_uuid()),
    Bucket = <<"B">>,
    {Bucket, 
        Key,
        [{hash, Hash}, {shard, Count rem 10},
            {random, Random}, {value, V}]}.



bespoketag_recalc(_Config) ->
    %% Get a sensible behaviour using the recalc compaction strategy with a
    %% bespoke tag

    RootPath = testutil:reset_filestructure(),
    B0 = <<"B0">>,
    KeyCount = 7000,

    ExtractMDFun = 
        fun(bespoke_tag, Size, Obj) ->
            [{index, IL}, {value, _V}] = Obj,
            {{erlang:phash2(term_to_binary(Obj)), 
                    Size,
                    {index, IL}},
                [os:timestamp()]}
        end,
    CalcIndexFun =
        fun(bespoke_tag, UpdMeta, PrvMeta) ->
            % io:format("UpdMeta ~w PrvMeta ~w~n", [UpdMeta, PrvMeta]),
            {index, UpdIndexes} = element(3, UpdMeta),
            IndexDeltas =
                case PrvMeta of
                    not_present ->
                        UpdIndexes;
                    PrvMeta when is_tuple(PrvMeta) ->
                        {index, PrvIndexes} = element(3, PrvMeta),
                        lists:subtract(UpdIndexes, PrvIndexes)
                end,
            lists:map(fun(I) -> {add, <<"temp_int">>, I} end, IndexDeltas)
        end,

    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalobjectcount, 6000},
                    {max_pencillercachesize, 8000},
                    {sync_strategy, testutil:sync_strategy()},
                    {reload_strategy, [{bespoke_tag, recalc}]},
                    {override_functions,
                        [{extract_metadata, ExtractMDFun},
                            {diff_indexspecs, CalcIndexFun}]}],
    
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    LoadFun =
        fun(Book, MustFind) ->
            fun(I) ->
                testutil:stdload_object(Book,
                                        B0, integer_to_binary(I rem KeyCount),
                                        I, erlang:phash2({value, I}),
                                        infinity, bespoke_tag, false, MustFind)
            end
        end,
    lists:foreach(LoadFun(Book1, false), lists:seq(1, KeyCount)),
    lists:foreach(LoadFun(Book1, true), lists:seq(KeyCount + 1, KeyCount * 2)),

    FoldFun =
        fun(_B0, {IV0, _K0}, Acc) -> 
            case IV0 - 1 of
                Acc ->
                    Acc + 1;
                _Unexpected ->
                    % io:format("Eh? - ~w ~w~n", [Unexpected, Acc]),
                    Acc + 1
            end
        end,

    CountFold =
        fun(Book, CurrentCount) ->
            leveled_bookie:book_indexfold(Book,
                                            B0,
                                            {FoldFun, 0},
                                            {<<"temp_int">>, 0, CurrentCount},
                                            {true, undefined})
        end,

    {async, FolderA} = CountFold(Book1, 2 * KeyCount),
    CountA = FolderA(),
    io:format("Counted double index entries ~w - everything loaded OK~n",
                [CountA]),
    true = 2 * KeyCount == CountA,

    ok = leveled_bookie:book_close(Book1),

    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    lists:foreach(LoadFun(Book2, true), lists:seq(KeyCount * 2 + 1, KeyCount * 3)),

    {async, FolderB} = CountFold(Book2, 3 * KeyCount),
    CountB = FolderB(),
    true = 3 * KeyCount == CountB,

    testutil:compact_and_wait(Book2),
    ok = leveled_bookie:book_close(Book2),

    io:format("Restart from blank ledger~n"),

    leveled_penciller:clean_testdir(proplists:get_value(root_path, BookOpts) ++
                                    "/ledger"),
    {ok, Book3} = leveled_bookie:book_start(BookOpts),

    {async, FolderC} = CountFold(Book3, 3 * KeyCount),
    CountC = FolderC(),
    io:format("All index entries ~w present - recalc ok~n",
                [CountC]),
    true = 3 * KeyCount == CountC,

    ok = leveled_bookie:book_close(Book3),
    
    testutil:reset_filestructure().