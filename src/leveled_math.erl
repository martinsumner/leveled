%% Handle missing log2 prior to OTP18

-module(leveled_math).

%% API
-export([
    log2/1
]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Use log2
%%%===================================================================
-ifndef(no_log2).

log2(X) ->
    math:log2(X).

-else.
%%%===================================================================
%%% Old (r18) random style functions
%%%===================================================================

log2(X) ->
    math:log(X) / 0.6931471805599453.

-endif.

-ifdef(TEST).

log2_test() ->
    ?assertMatch(8, round(log2(256))),
    ?assertMatch(16, round(log2(65536))).

-endif.
