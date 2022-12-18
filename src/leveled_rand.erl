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

%%%===================================================================
%%% New (r19+) rand style functions
%%%===================================================================
uniform() ->
    rand:uniform().

uniform(N) ->
    rand:uniform(N).

seed() ->
    ok.

rand_bytes(Size) ->
    crypto:strong_rand_bytes(Size).

