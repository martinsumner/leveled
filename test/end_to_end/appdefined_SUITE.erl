-module(appdefined_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("include/leveled.hrl").
-export([all/0]).
-export([application_defined_tag/1
            ]).

all() -> [
            application_defined_tag
            ].



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
                    {override_functions, Functions}],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts1),
    Value = leveled_rand:rand_bytes(512),
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
    Random = leveled_rand:uniform(1000),
    Key = list_to_binary(leveled_util:generate_uuid()),
    Bucket = <<"B">>,
    {Bucket, 
        Key,
        [{hash, Hash}, {shard, Count rem 10},
            {random, Random}, {value, V}]}.