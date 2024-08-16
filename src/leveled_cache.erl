%% -------- Cache ---------
%%
%% When leveled is used by Riak, the GET process requires a HEAD followed by
%% a GET request to the fastest responder.  So 1:3 such HEAD requests will
%% immediately lead to a GET
%% 
%% The cache is an attempt to accelerate this - for a very small numnber of
%% recent HEAD requests, the SQN will be cached, so that the underlying
%% penciller request is not required.
%% 
%% Two data structures required, a list of entries and a map of entries.
%% 
%% A request to cache_head(LK, Head) will check to see if there is space in
%% the map, if there is add {LK, Head} to the map and prepend LK to the list.
%% 
%% If there is not space, then first split the list in two, and first delete
%% all entries from the second half of the list from the map, making the first
%% half th enew list
%% 
%% A request to purge_head(LK) can be sent after a PUT/DELETE operation, and
%% will simply remove the Key if it is present.
%% 
%% A request to fetch_head(LK) will return the HEAD if it is present.

-module(leveled_cache).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        format_status/1]).

-export([
    start/1,
    cache_head/3,
    purge_head/2,
    fetch_head/2,
    cache_size/1,
    stop/1
]).

-define(TIME_UNIT, millisecond).

-type read_cache() :: pid() | undefined.

-record(state,
    {
        size_limit = 64 :: pos_integer(),
        cache = maps:new() ::
            redacted|
                #{leveled_codec:ledger_key() =>
                    {pos_integer(), leveled_codec:ledger_value()}}
    }
).

-export_type([read_cache/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec start(pos_integer()) -> {ok, pid()}.
start(CacheSize) ->
    gen_server:start_link(?MODULE, [CacheSize], []).

-spec cache_head(
    read_cache(), leveled_codec:ledger_key(), leveled_codec:ledger_value())
        -> ok.
cache_head(undefined, _LK, _Head) ->
    ok;
cache_head(Cache, LK, Head) ->
    gen_server:cast(Cache, {cache_head, LK, Head}).

-spec purge_head(read_cache(), leveled_codec:ledger_key()) -> ok.
purge_head(undefined, _LK) ->
    ok;
purge_head(Cache, LK) ->
    gen_server:cast(Cache, {purge_head, LK}).

-spec fetch_head(
   read_cache(), leveled_codec:ledger_key())
        -> leveled_codec:ledger_value()|not_cached.
fetch_head(undefined, _LK) ->
    not_cached;
fetch_head(Cache, LK) ->
    gen_server:call(Cache, {fetch_head, LK}, infinity).

-spec cache_size(read_cache()) -> non_neg_integer().
cache_size(Cache) ->
    gen_server:call(Cache, cache_size, infinity).

-spec stop(read_cache()) -> ok.
stop(undefined) ->
    ok;
stop(Cache) ->
    gen_server:stop(Cache).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([CacheSize]) ->
    {ok, #state{size_limit = CacheSize}}.

handle_cast({purge_head, LedgerKey}, State) ->
    case maps:is_key(LedgerKey, State#state.cache) of
        true ->
            {noreply,
                State#state{
                    cache =
                        maps:remove(LedgerKey, State#state.cache)
                }
            };
        false ->
            {noreply, State}
    end;
handle_cast({cache_head, LedgerKey, Head}, State) ->
    case maps:size(State#state.cache) of
        S when S >= State#state.size_limit ->
            {ToPrune, _ToKeep} =
                lists:split(
                    State#state.size_limit div 2,
                    lists:keysort(
                        2,
                        maps:to_list(State#state.cache)
                    )
                ),
            PurgedCache =
                maps:put(
                    LedgerKey,
                    {os:system_time(?TIME_UNIT), Head},
                    maps:without(
                        lists:map(fun({K, _V}) -> K end, ToPrune),
                        State#state.cache
                    )
                ),
            {noreply, State#state{cache = PurgedCache}, hibernate};
        _ ->
            UpdCache =
                maps:put(
                    LedgerKey,
                    {os:system_time(?TIME_UNIT), Head},
                    State#state.cache
                ),
            {noreply, State#state{cache = UpdCache}}
    end.

handle_call({fetch_head, LedgerKey}, _From, State) ->
    {_TS, Result} = maps:get(LedgerKey, State#state.cache, {0, not_cached}),
    case Result of
        not_cached ->
            {reply, not_cached, State};
        Head ->
            {
                reply,
                Head,
                State#state{
                    cache =
                        maps:update(
                            LedgerKey,
                            {os:system_time(millisecond), Head},
                            State#state.cache
                        )
                }
            }
    end;
handle_call(cache_size, _From, State) ->
    {reply, maps:size(State#state.cache), State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(Status) ->
    case maps:get(reason, Status, normal) of
        terminate ->
            State = maps:get(state, Status),
            maps:update(
                state,
                State#state{cache = redacted},
                Status
            );
        _ ->
            Status
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).

format_status_test() ->
    {ok, Cache} = start(1024),
    {status, Cache, {module, gen_server}, SItemL} = sys:get_status(Cache),
    {data,[{"State", S}]} = lists:nth(3, lists:nth(5, SItemL)),
    ?assertMatch(1024, S#state.size_limit),
    ?assert(is_map(S#state.cache)),
    Status = format_status(#{reason => terminate, state => S}),
    ST = maps:get(state, Status),
    ?assertMatch(redacted, ST#state.cache).

basic_cache_test() ->
    {ok, Cache} = start(128),
    lists:foreach(
        fun(I) ->
            LK = {o, <<"B">>, <<I:32/integer>>, null},
            LV = {I, {active, infinity}, no_lookup, null, os:timestamp()},
            cache_head(Cache, LK, LV)
        end,
        lists:seq(1, 128)
    ),
    ?assertMatch(128, cache_size(Cache)),
    lists:foreach(
        fun(I) ->
            LK = {o, <<"B">>, <<I:32/integer>>, null},
            LV = fetch_head(Cache, LK),
            ?assertMatch(I, element(1, LV))
        end,
        lists:seq(1, 64)
    ),
    timer:sleep(2),
    lists:foreach(
        fun(I) ->
            LK = {o, <<"B">>, <<I:32/integer>>, null},
            LV = fetch_head(Cache, LK),
            ?assertMatch(I, element(1, LV))
        end,
        lists:seq(65, 128)
    ),
    NLK = {o, <<"B">>, <<129:32/integer>>, null},
    NLV = {129, {active, infinity}, no_lookup, null, os:timestamp()},
    cache_head(Cache, NLK, NLV),
    ?assertMatch(65, cache_size(Cache)),
    lists:foreach(
        fun(I) ->
            LK = {o, <<"B">>, <<I:32/integer>>, null},
            ?assertMatch(not_cached, fetch_head(Cache, LK))
        end,
        lists:seq(1, 64)
    ),
    lists:foreach(
        fun(I) ->
            LK = {o, <<"B">>, <<I:32/integer>>, null},
            LV = fetch_head(Cache, LK),
            ?assertMatch(I, element(1, LV))
        end,
        lists:seq(65, 129)
    ),
    purge_head(Cache, NLK),
    ?assertMatch(64, cache_size(Cache)),
    cache_head(Cache, NLK, NLV),
    ?assertMatch(65, cache_size(Cache)),
    stop(Cache).

-endif.