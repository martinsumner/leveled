%% Generalized random module that offers a backwards compatible API
%% around some of the changes in rand, crypto and for time units.

-module(leveled_rand).

%% API
-export([
         uniform/0,
         uniform/1,
         seed/0,
         rand_bytes/1
        ]).


-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% New (r19+) rand style functions
%%%===================================================================
-ifndef(old_rand).
uniform() ->
    rand:uniform().

uniform(N) ->
    rand:uniform(N).

seed() ->
    ok.

rand_bytes(Size) ->
    crypto:strong_rand_bytes(Size).

-else.
%%%===================================================================
%%% Old (r18) random style functions
%%%===================================================================
uniform() ->
    random:uniform().

uniform(N) ->
    random:uniform(N).

seed() ->
    SW = os:timestamp(),
    random:seed(erlang:phash2(self()), element(2, SW), element(3, SW)).

rand_bytes(Size) ->
    crypto:rand_bytes(Size).

-endif.


-ifdef(TEST).

rand_test() ->
    ?assertMatch(true, uniform() < 1).

-endif.
