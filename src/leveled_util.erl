%% -------- Utility Functions ---------
%%
%% Generally helpful funtions within leveled
%%

-module(leveled_util).

-export([generate_uuid/0,
            integer_now/0,
            integer_time/1,
            magic_hash/1,
            t2b/1,
            safe_rename/4]).

-define(WRITE_OPS, [binary, raw, read, write]).


-spec generate_uuid() -> list().
%% @doc
%% Generate a new globally unique ID as a string.
%% Credit to
%% https://github.com/afiskon/erlang-uuid-v4/blob/master/src/uuid.erl
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = leveled_rand:rand_bytes(16),
    L = io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b", 
                        [A, B, C band 16#0fff, D band 16#3fff bor 16#8000, E]),
    binary_to_list(list_to_binary(L)).

-spec integer_now() -> non_neg_integer().
%% @doc
%% Return now in gregorian seconds
integer_now() ->
    integer_time(os:timestamp()).

-spec integer_time (erlang:timestamp()) -> non_neg_integer().
%% @doc
%% Return a given time in gergorian seconds
integer_time(TS) ->
    DT = calendar:now_to_universal_time(TS),
    calendar:datetime_to_gregorian_seconds(DT).


-spec magic_hash(any()) -> 0..16#FFFFFFFF.
%% @doc 
%% Use DJ Bernstein magic hash function. Note, this is more expensive than
%% phash2 but provides a much more balanced result.
%%
%% Hash function contains mysterious constants, some explanation here as to
%% what they are -
%% http://stackoverflow.com/questions/10696223/reason-for-5381-number-in-djb-hash-function
magic_hash({binary, BinaryKey}) ->
    H = 5381,
    hash1(H, BinaryKey);
magic_hash(AnyKey) ->
    BK = t2b(AnyKey),
    magic_hash({binary, BK}).

hash1(H, <<>>) -> 
    H;
hash1(H0, <<I:32/integer, Rest/bytes>>) ->
    H1 = ((H0 bsl 5) + H0) bxor ((I band 16#FF000000) bsr 24),
    H2 = ((H1 bsl 5) + H1) bxor ((I band 16#00FF0000) bsr 16),
    H3 = ((H2 bsl 5) + H2) bxor ((I band 16#0000FF00) bsr 8),
    H4 = (((H3 bsl 5) + H3) bxor (I band 16#000000FF)) band 16#FFFFFFFF,
    hash1(H4, Rest);
hash1(H, <<I:8/integer, Rest/bytes>>) ->
    hash1((((H bsl 5) + H) band 16#FFFFFFFF) bxor I, Rest).

-spec t2b(term()) -> binary().
%% @doc
%% term_to_binary with options necessary to ensure backwards compatability
%% in the handling of atoms (within OTP 26).
%% See https://github.com/martinsumner/leveled/issues/407
%% If the binary() which is outputted is to be hashed for comparison, then
%% this must be used.
t2b(Term) ->
    term_to_binary(Term, [{minor_version, 1}]).


-spec safe_rename(string(), string(), binary(), boolean()) -> ok.
%% @doc
%% Write a file, sync it and rename it (and for super-safe mode read it back)
%% An attempt to prevent crashes leaving files with empty or partially written
%% values
safe_rename(TempFN, RealFN, BinData, ReadCheck) ->
    {ok, TempFH} = file:open(TempFN, ?WRITE_OPS),
    ok = file:write(TempFH, BinData),
    ok = file:sync(TempFH),
    ok = file:close(TempFH),
    ok = file:rename(TempFN, RealFN),
    case ReadCheck of
        true ->
            {ok, ReadBack} = file:read_file(RealFN),
            true = (ReadBack == BinData),
            ok;
        false ->
            ok
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_AREA, "test/test_area/util/").

magichashperf_test() ->
    N = 100000,
    Bucket =
        {<<"LongerBucketType">>,
            <<"LongerBucketName">>,
            <<"FurtherBucketInfo">>},
    KeyPrefix = "LongerkeyPrefix",
    KeyFun =
        fun(X) ->
            K =
                {o_rkv,
                    Bucket,
                    list_to_binary(KeyPrefix ++ integer_to_list(X)),
                    null},
            {K, X}
        end,
    KL = lists:map(KeyFun, lists:seq(1, N)),
    {TimeMH, HL1} = timer:tc(lists, map, [fun(K) -> magic_hash(K) end, KL]),
    io:format(
        user, "~w keys magic hashed in ~w microseconds~n", [N, TimeMH]),
    {TimePH, _Hl2} = timer:tc(lists, map, [fun(K) -> erlang:phash2(K) end, KL]),
    io:format(
        user, "~w keys phash2 hashed in ~w microseconds~n", [N, TimePH]),
    {TimeMH2, HL1} = timer:tc(lists, map, [fun(K) -> magic_hash(K) end, KL]),
    io:format(
        user, "~w keys magic hashed in ~w microseconds~n", [N, TimeMH2]).


safe_rename_test() ->
    ok = filelib:ensure_dir(?TEST_AREA),
    TempFN = filename:join(?TEST_AREA, "test_manifest0.pnd"),
    RealFN = filename:join(?TEST_AREA, "test_manifest0.man"),
    ok = safe_rename(TempFN, RealFN, <<1:128/integer>>, false),
    ?assertMatch({ok, <<1:128/integer>>}, file:read_file(RealFN)),
    TempFN1 = filename:join(?TEST_AREA, "test_manifest1.pnd"),
    RealFN1 = filename:join(?TEST_AREA, "test_manifest1.man"),
    ok = safe_rename(TempFN1, RealFN1, <<2:128/integer>>, true),
    ?assertMatch({ok, <<2:128/integer>>}, file:read_file(RealFN1)).


-endif.
