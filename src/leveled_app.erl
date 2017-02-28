-module(leveled_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%% Currently this is just to keep dialyzer happy
%% Run the store diretcly using leveled_bookie:book_start/4 or bookie_start/1

start(_StartType, _StartArgs) ->
    leveled_sup:start_link().

stop(_State) ->
    ok.
