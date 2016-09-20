

-module(leveled_iclerk).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        clerk_new/1,
        clerk_compact/3,
        clerk_remove/2,
        clerk_stop/1,
        code_change/3]).      

-include_lib("eunit/include/eunit.hrl").

-define(KEYS_TO_CHECK, 100).

-record(state, {owner :: pid()}).

%%%============================================================================
%%% API
%%%============================================================================

clerk_new(Owner) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    ok = gen_server:call(Pid, {register, Owner}, infinity),
    {ok, Pid}.
    
clerk_compact(Pid, InkerManifest, Penciller) ->
    gen_server:cast(Pid, {compact, InkerManifest, Penciller}),
    ok.

clerk_remove(Pid, Removals) ->
    gen_server:cast(Pid, {remove, Removals}),
    ok.

clerk_stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register, Owner}, _From, State) ->
    {reply, ok, State#state{owner=Owner}}.

handle_cast({compact, InkerManifest, Penciller, Timeout}, State) ->
    ok = journal_compact(InkerManifest, Penciller, Timeout, State#state.owner),
    {noreply, State};
handle_cast({remove, _Removals}, State) ->
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

journal_compact(_InkerManifest, _Penciller, _Timeout, _Owner) ->
    ok.

check_all_files(_InkerManifest) ->
    ok.

window_closed(_Timeout) ->
    true.


%%%============================================================================
%%% Test
%%%============================================================================
