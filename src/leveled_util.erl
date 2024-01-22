%% -------- Utility Functions ---------
%%
%% Generally helpful funtions within leveled
%%

-module(leveled_util).

-export([generate_uuid/0,
            integer_now/0,
            integer_time/1,
            magic_hash/1,
            magic_erlhash/1,
            t2b/1,
            safe_rename/4]).

-export([setup_checker/1, int_checker/3, find_pos/3]).

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

-spec magic_hash(term()) -> 0..16#FFFFFFFF.
magic_hash(Key) when is_binary(Key) ->
    magic_nifhash(Key);
magic_hash(Key) ->
    magic_hash(t2b(Key)).

-spec magic_erlhash(binary()) -> 0..16#FFFFFFFF.
%% @doc 
%% Use DJ Bernstein magic hash function. Note, this is more expensive than
%% phash2 but provides a much more balanced result.
%%
%% Hash function contains mysterious constants, some explanation here as to
%% what they are -
%% http://stackoverflow.com/questions/10696223/reason-for-5381-number-in-djb-hash-function
magic_erlhash(BinaryKey) when is_binary(BinaryKey) ->
    H = 5381,
    hash1(H, BinaryKey).

hash1(H, <<>>) -> 
    H;
hash1(H, <<B:8/integer, Rest/bytes>>) ->
    H1 = ((H bsl 5) + H) band 16#FFFFFFFF,
    hash1(H1 bxor B, Rest).

-spec magic_nifhash(binary()) -> 0..16#FFFFFFFF.
magic_nifhash(BinaryKey) when is_binary(BinaryKey) ->
    leveled_nif:magic_chash(BinaryKey) band 16#FFFFFFFF.


-spec t2b(term()) -> binary().
%% @doc
%% term_to_binary with options necessary to ensure backwards compatability
%% in the handling of atoms (within OTP 26).
%% See https://github.com/martinsumner/leveled/issues/407
%% If the binary() which is outputted is to be hashed for comparison, then
%% this must be used.
t2b(Term) ->
    term_to_binary(Term, [{minor_version, 1}]).

-spec setup_checker(list(32768..16#FFFF)) -> {binary(), 32768..16#FFFF}.
setup_checker(IntList) ->
    SortedList = lists:reverse(lists:usort(IntList)),
    {
        lists:foldl(
            fun(I, B) -> <<I:16/integer, B/binary>> end,
            <<>>,
            SortedList),
        hd(SortedList)
    }.

-spec int_checker(binary(), 32768..16#FFFF, 32768..16#FFFF) -> boolean().
int_checker(CheckBin, Hash, Max) ->
    leveled_nif:int_checker(CheckBin, Hash, Max).

-spec find_pos(binary(), binary(), 32768..16#FFFF) -> list(0..127).
find_pos(PosBin, CheckBin, MaxHash) ->
    lists:map(
        fun(I) ->  I - 1 end,
        leveled_nif:pos_finder(PosBin, CheckBin, MaxHash)).

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


checker_test() ->
    {Bin, Max} = setup_checker(lists:seq(64, 96) ++ lists:seq(512, 544)),
    ?assertMatch(false, int_checker(Bin, 0, Max)),
    ?assertMatch(true, int_checker(Bin, 64, Max)),
    ?assertMatch(true, int_checker(Bin, 70, Max)),
    ?assertMatch(true, int_checker(Bin, 96, Max)),
    ?assertMatch(false, int_checker(Bin, 97, Max)),
    ?assertMatch(false, int_checker(Bin, 101, Max)),
    ?assertMatch(true, int_checker(Bin, 512, Max)),
    ?assertMatch(true, int_checker(Bin, 540, Max)),
    ?assertMatch(false, int_checker(Bin, 600, Max)),
    ?assertMatch(false, int_checker(Bin, 65535, Max)),
    ?assertException(error, badarg, int_checker(Bin, 65536, Max)).


find_pos_test() ->
    PosBin =
        lists:foldl(
            fun(I, AccBin) -> <<(I + 32768):16/integer, AccBin/binary>> end,
            <<>>,
            lists:reverse(lists:seq(1, 128))
        ),
    {CheckBin1, Max1} = setup_checker([32768 + 1]),
    {CheckBin2, Max2} = setup_checker([32768 + 2]),
    {CheckBinMiss, MaxMiss} = setup_checker([32768 + 200]),
    ?assertMatch([1], leveled_nif:pos_finder(PosBin, CheckBin1, Max1)),
    ?assertMatch([2], leveled_nif:pos_finder(PosBin, CheckBin2, Max2)),
    ?assertMatch([], leveled_nif:pos_finder(PosBin, CheckBinMiss, MaxMiss)),
    {CheckBin3, Max3} = setup_checker([32768 + 3, 32768 + 4, 32768 + 100]),
    ?assertMatch([3, 4, 100], leveled_nif:pos_finder(PosBin, CheckBin3, Max3)).

magichashperf_test_() ->
    {timeout, 10, fun magichashperf_tester/0}.

magichashperf_tester() ->
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
    {TimePH1, _Hl1} = timer:tc(lists, map, [fun(K) -> erlang:phash2(t2b(K)) end, KL]),
    io:format(user, "~w keys phash2 hashed in ~w microseconds~n", [N, TimePH1]),
    {TimeMH1, HL1ERL} = timer:tc(lists, map, [fun(K) -> magic_erlhash(t2b(K)) end, KL]),
    io:format(user, "~w keys magic hashed in ~w microseconds~n", [N, TimeMH1]),
    {TimeMH2, HL1NIF} = timer:tc(lists, map, [fun(K) -> magic_nifhash(t2b(K)) end, KL]),
    io:format(user, "~w keys magic chashed in ~w microseconds~n", [N, TimeMH2]),
    ?assert(HL1ERL == HL1NIF).


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
