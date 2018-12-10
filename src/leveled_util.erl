%% -------- Utility Functions ---------
%%
%% Generally helpful funtions within leveled
%%

-module(leveled_util).


-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").


-export([generate_uuid/0,
            integer_now/0,
            integer_time/1,
            magic_hash/1]).

-export([string_right/3,
         string_str/2]).

-ifdef(OTP_RELEASE).

-if(?OTP_RELEASE >= 21).
-else.
-define(LEGACY_OTP, true).
-endif.

-endif.  % (OTP_RELEASE)


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


-spec magic_hash(any()) -> integer().
%% @doc 
%% Use DJ Bernstein magic hash function. Note, this is more expensive than
%% phash2 but provides a much more balanced result.
%%
%% Hash function contains mysterious constants, some explanation here as to
%% what they are -
%% http://stackoverflow.com/questions/10696223/reason-for-5381-number-in-djb-hash-function
magic_hash({binary, BinaryKey}) ->
    H = 5381,
    hash1(H, BinaryKey) band 16#FFFFFFFF;
magic_hash(AnyKey) ->
    BK = term_to_binary(AnyKey),
    magic_hash({binary, BK}).

hash1(H, <<>>) -> 
    H;
hash1(H, <<B:8/integer, Rest/bytes>>) ->
    H1 = H * 33,
    H2 = H1 bxor B,
    hash1(H2, Rest).

%% A number of string functions have become deprecated in OTP 21
%%
-ifdef(LEGACY_OTP).

string_right(String, Len, Char) ->
    string:right(String, Len, Char).

string_str(S, Sub) ->
    string:str(S, Sub).

-else.

string_right(String, Len, Char) when is_list(String), is_integer(Char) ->
    Slen = length(String),
    if
	Slen > Len -> lists:nthtail(Slen-Len, String);
	Slen < Len -> chars(Char, Len-Slen, String);
	Slen =:= Len -> String
    end.

chars(C, N, Tail) when N > 0 ->
    chars(C, N-1, [C|Tail]);
chars(C, 0, Tail) when is_integer(C) ->
    Tail.


string_str(S, Sub) when is_list(Sub) -> str(S, Sub, 1).

str([C|S], [C|Sub], I) ->
    case l_prefix(Sub, S) of
        true -> I;
        false -> str(S, [C|Sub], I+1)
    end;
str([_|S], Sub, I) -> str(S, Sub, I+1);
str([], _Sub, _I) -> 0.

l_prefix([C|Pre], [C|String]) -> l_prefix(Pre, String);
l_prefix([], String) when is_list(String) -> true;
l_prefix(Pre, String) when is_list(Pre), is_list(String) -> false.

-endif.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


magichashperf_test() ->
    KeyFun =
        fun(X) ->
            K = {o, "Bucket", "Key" ++ integer_to_list(X), null},
            {K, X}
        end,
    KL = lists:map(KeyFun, lists:seq(1, 1000)),
    {TimeMH, _HL1} = timer:tc(lists, map, [fun(K) -> magic_hash(K) end, KL]),
    io:format(user, "1000 keys magic hashed in ~w microseconds~n", [TimeMH]),
    {TimePH, _Hl2} = timer:tc(lists, map, [fun(K) -> erlang:phash2(K) end, KL]),
    io:format(user, "1000 keys phash2 hashed in ~w microseconds~n", [TimePH]),
    {TimeMH2, _HL1} = timer:tc(lists, map, [fun(K) -> magic_hash(K) end, KL]),
    io:format(user, "1000 keys magic hashed in ~w microseconds~n", [TimeMH2]).

-endif.
