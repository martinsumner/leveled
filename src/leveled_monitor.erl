%% -------- MONITOR ---------
%%
%% The bookie's monitor is a process dedciated to gathering and reporting
%% stats related to performance of the store.
%% 
%% The monitor was introduced as a sedicated process to reduce the number of
%% LoopState mutations otherwise necessary to track statistics, requiring
%% State copies even on read events.

-module(leveled_monitor).

-behaviour(gen_server).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    monitor_start/0,
    monitor_start/2,
    add_stat/2,
    report_stats/2,
    monitor_close/1,
    maybe_time/1,
    step_time/1,
    log_level/2,
    log_add/2,
    log_remove/2]).

-include_lib("eunit/include/eunit.hrl").

-define(
    LOG_ORDER,
    [bookie_get, bookie_put, bookie_head, bookie_snap,
        pcl_fetch, sst_fetch, cdb_get]).
-define(LOG_FREQUENCY_SECONDS, 30).


-record(bookie_get_timings,
    {sample_count = 0 :: non_neg_integer(),
    head_time = 0 :: non_neg_integer(),
    body_time = 0 :: non_neg_integer(),
    fetch_count = 0 :: non_neg_integer()}).

-record(bookie_head_timings,
    {sample_count = 0 :: non_neg_integer(),
    fetch_time = 0 :: non_neg_integer(),
    rsp_time = 0 :: non_neg_integer(),
    fetch_count = 0 :: non_neg_integer(),
    cache_count = 0 :: non_neg_integer()}).

-record(bookie_put_timings,
    {sample_count = 0 :: non_neg_integer(),
    mem_time = 0 :: non_neg_integer(),
    ink_time = 0 :: non_neg_integer(),
    total_size = 0 :: non_neg_integer()}).

-record(bookie_snap_timings,
    {sample_count = 0 :: non_neg_integer(),
    bookie_time = 0 :: non_neg_integer(),
    pcl_time = 0 :: non_neg_integer()}).

-record(pcl_fetch_timings, 
    {sample_count = 0 :: non_neg_integer(),
    foundmem_time = 0 :: non_neg_integer(),
    found0_time = 0 :: non_neg_integer(),
    found1_time = 0 :: non_neg_integer(),
    found2_time = 0 :: non_neg_integer(),
    found3_time = 0 :: non_neg_integer(),
    foundlower_time = 0 :: non_neg_integer(),
    notfound_time = 0 :: non_neg_integer(),
    foundmem_count = 0 :: non_neg_integer(),
    found0_count = 0 :: non_neg_integer(),
    found1_count = 0 :: non_neg_integer(),
    found2_count = 0 :: non_neg_integer(),
    found3_count = 0 :: non_neg_integer(),
    foundlower_count = 0 :: non_neg_integer(),
    notfound_count = 0 :: non_neg_integer()}).

-record(sst_fetch_timings, 
    {sample_count = 0 :: non_neg_integer(),
    fetchcache_time = 0 :: non_neg_integer(),
    slotcached_time = 0 :: non_neg_integer(),
    slotnoncached_time = 0 :: non_neg_integer(),
    notfound_time = 0 :: non_neg_integer(),
    fetchcache_count = 0 :: non_neg_integer(),
    slotcached_count = 0 :: non_neg_integer(),
    slotnoncached_count = 0 :: non_neg_integer(),
    notfound_count = 0 :: non_neg_integer()}).

-record(cdb_get_timings,
    {sample_count = 0 :: non_neg_integer(),
    cycle_count = 0 :: non_neg_integer(),
    index_time = 0 :: non_neg_integer(),
    read_time = 0 :: non_neg_integer()}).

-record(state, 
    {bookie_get_timings = #bookie_get_timings{} :: bookie_get_timings(),
    bookie_head_timings = #bookie_head_timings{} :: bookie_head_timings(),
    bookie_put_timings = #bookie_put_timings{} :: bookie_put_timings(),
    bookie_snap_timings = #bookie_snap_timings{} :: bookie_snap_timings(),
    pcl_fetch_timings = #pcl_fetch_timings{} :: pcl_fetch_timings(),
    sst_fetch_timings = [] :: list(sst_fetch_timings()),
    cdb_get_timings = #cdb_get_timings{} :: cdb_get_timings(),
    log_frequency = ?LOG_FREQUENCY_SECONDS :: pos_integer(),
    log_order = [] :: list(log_types())}).      


-type bookie_get_timings() :: #bookie_get_timings{}.
-type bookie_head_timings() :: #bookie_head_timings{}.
-type bookie_put_timings() :: #bookie_put_timings{}.
-type bookie_snap_timings() :: #bookie_snap_timings{}.
-type pcl_fetch_timings() :: #pcl_fetch_timings{}.
-type cdb_get_timings() :: #cdb_get_timings{}.
-type sst_fetch_timings() ::
    {leveled_pmanifest:lsm_level(), #sst_fetch_timings{}}.
-type log_types() ::
    bookie_head|bookie_get|bookie_put|bookie_snap|pcl_fetch|sst_fetch|cdb_get.
-type pcl_level() :: mem|leveled_pmanifest:lsm_level().
-type sst_fetch_type() ::
    fetch_cache|slot_cachedblock|slot_noncachedblock|not_found.
-type microsecs() :: pos_integer().
-type byte_size() :: pos_integer().
-type monitor() :: {no_monitor, 0}|{pid(), 0..100}.
-type timing() :: no_timing|pos_integer().


-type bookie_get_update() ::
    {bookie_get_update, microsecs(), microsecs()|not_found}.
-type bookie_head_update() ::
    {bookie_head_update, microsecs(), microsecs()|not_found, 0..1}.
-type bookie_put_update() ::
    {bookie_put_update, microsecs(), microsecs(), byte_size()}.
-type bookie_snap_update() ::
    {bookie_snap_update, microsecs(), microsecs()}.
-type pcl_fetch_update() ::
    {pcl_fetch_update, not_found|pcl_level(), microsecs()}.
-type sst_fetch_update() ::
    {sst_fetch_update,
        leveled_pmanifest:lsm_level(), sst_fetch_type(), microsecs()}.
-type cdb_get_update() ::
    {cdb_get_update, pos_integer(), microsecs(), microsecs()}.
-type statistic() ::
    bookie_get_update()|bookie_head_update()|bookie_put_update()|
        bookie_snap_update()|
        pcl_fetch_update()|sst_fetch_update()|cdb_get_update().

-export_type([monitor/0, timing/0, sst_fetch_type/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec monitor_start() -> {ok, pid()}.
monitor_start() ->
    monitor_start(?LOG_FREQUENCY_SECONDS, ?LOG_ORDER).

-spec monitor_start(pos_integer(), list(log_types())) -> {ok, pid()}.
monitor_start(LogFreq, LogOrder) ->
    gen_server:start_link(
        ?MODULE, [leveled_log:get_opts(), LogFreq, LogOrder], []).

-spec add_stat(pid(), statistic()) -> ok.
add_stat(Watcher, Statistic) ->
    gen_server:cast(Watcher, Statistic).

-spec report_stats(pid(), log_types()) -> ok.
report_stats(Watcher, StatsType) ->
    gen_server:cast(Watcher, {report_stats, StatsType}).

-spec monitor_close(pid()|no_monitor) -> ok.
monitor_close(no_monitor) ->
    ok;
monitor_close(Watcher) ->
    gen_server:call(Watcher, close, 60000).

-spec log_level(pid(), leveled_log:log_level()) -> ok.
log_level(Pid, LogLevel) ->
    gen_server:cast(Pid, {log_level, LogLevel}).

-spec log_add(pid(), list(string())) -> ok.
log_add(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {log_add, ForcedLogs}).

-spec log_remove(pid(), list(string())) -> ok.
log_remove(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {log_remove, ForcedLogs}).

-spec maybe_time(monitor()) -> os:timestamp()|no_timing.
maybe_time({_Pid, TimingProbability}) ->
    case leveled_rand:uniform(100) of
        N when N =< TimingProbability ->
            os:timestamp();
        _ ->
            no_timing
    end.

-spec step_time(
    os:timestamp()|no_timing) ->
        {pos_integer(), os:timestamp()}|{no_timing, no_timing}.
step_time(no_timing) ->
    {no_timing, no_timing};
step_time(TS) ->
    Now = os:timestamp(),
    {timer:now_diff(Now, TS), Now}.


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([LogOpts, LogFrequency, LogOrder]) ->
    leveled_log:save(LogOpts),
    leveled_rand:seed(),
    RandomLogOrder = 
        lists:map(
            fun({_R, SL}) -> SL end,
            lists:keysort(
                1,
                lists:map(
                    fun(L) -> {leveled_rand:uniform(), L} end,
                    LogOrder))),
    InitialJitter = leveled_rand:uniform(2 * 1000 * LogFrequency),
    erlang:send_after(InitialJitter, self(), report_next_stats),
    {ok, #state{log_frequency = LogFrequency, log_order = RandomLogOrder}}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({bookie_head_update, FetchTime, RspTime, CacheHit}, State) ->
    Timings = State#state.bookie_head_timings,
    SC0 = Timings#bookie_head_timings.sample_count + 1,
    CC0 = Timings#bookie_head_timings.cache_count + CacheHit, 
    {FC0, PT0, RT0} =
        case RspTime of
            not_found ->
                {Timings#bookie_head_timings.fetch_count,
                    Timings#bookie_head_timings.fetch_time + FetchTime,
                    Timings#bookie_head_timings.rsp_time};
            RspTime ->
                {Timings#bookie_head_timings.fetch_count + 1,
                    Timings#bookie_head_timings.fetch_time + FetchTime,
                    Timings#bookie_head_timings.rsp_time + RspTime}
        end,
    UpdTimings =
        #bookie_head_timings{
            sample_count = SC0,
            fetch_time = PT0,
            rsp_time = RT0,
            fetch_count = FC0,
            cache_count = CC0
        },
    {noreply, State#state{bookie_head_timings = UpdTimings}};
handle_cast({bookie_get_update, HeadTime, BodyTime}, State) ->
    Timings = State#state.bookie_get_timings,
    SC0 = Timings#bookie_get_timings.sample_count + 1,
    {FC0, HT0, BT0} =
        case BodyTime of
            not_found ->
                {Timings#bookie_get_timings.fetch_count,
                    Timings#bookie_get_timings.head_time + HeadTime,
                    Timings#bookie_get_timings.body_time};
            BodyTime ->
                {Timings#bookie_get_timings.fetch_count + 1,
                    Timings#bookie_get_timings.head_time + HeadTime,
                    Timings#bookie_get_timings.body_time + BodyTime}
        end,
    UpdTimings =
        #bookie_get_timings{
            sample_count = SC0,
            head_time = HT0,
            body_time = BT0,
            fetch_count = FC0
        },
    {noreply, State#state{bookie_get_timings = UpdTimings}};
handle_cast({bookie_put_update, MemTime, InkTime, Size}, State) ->
    Timings = State#state.bookie_put_timings,
    SC0 = Timings#bookie_put_timings.sample_count + 1,
    SZ0 = Timings#bookie_put_timings.total_size + Size,
    MT0 = Timings#bookie_put_timings.mem_time + MemTime,
    IT0 = Timings#bookie_put_timings.ink_time + InkTime,
    UpdTimings =
        #bookie_put_timings{
            sample_count = SC0,
            mem_time = MT0,
            ink_time = IT0,
            total_size = SZ0
        },
    {noreply, State#state{bookie_put_timings = UpdTimings}};
handle_cast({bookie_snap_update, BookieTime, PCLTime}, State) ->
    Timings = State#state.bookie_snap_timings,
    SC0 = Timings#bookie_snap_timings.sample_count + 1,
    BT0 = Timings#bookie_snap_timings.bookie_time + BookieTime,
    PT0 = Timings#bookie_snap_timings.pcl_time + PCLTime,
    UpdTimings =
        #bookie_snap_timings{
            sample_count = SC0,
            bookie_time = BT0,
            pcl_time = PT0
        },
    {noreply, State#state{bookie_snap_timings = UpdTimings}};
handle_cast({pcl_fetch_update, Level, FetchTime}, State) ->
    Timings = State#state.pcl_fetch_timings,
    SC0 = Timings#pcl_fetch_timings.sample_count + 1,
    UpdTimings =
        case Level of
            not_found ->
                Timings#pcl_fetch_timings{
                    notfound_count =
                        Timings#pcl_fetch_timings.notfound_count + 1,
                    notfound_time =
                        Timings#pcl_fetch_timings.notfound_time + FetchTime
                };
            memory ->
                Timings#pcl_fetch_timings{
                    foundmem_count =
                        Timings#pcl_fetch_timings.foundmem_count + 1,
                    foundmem_time =
                        Timings#pcl_fetch_timings.foundmem_time + FetchTime
                };
            0 ->
                Timings#pcl_fetch_timings{
                    found0_count =
                        Timings#pcl_fetch_timings.found0_count + 1,
                    found0_time =
                        Timings#pcl_fetch_timings.found0_time + FetchTime
                };
            1 ->
                Timings#pcl_fetch_timings{
                    found1_count =
                        Timings#pcl_fetch_timings.found1_count + 1,
                    found1_time =
                        Timings#pcl_fetch_timings.found1_time + FetchTime
                };
            2 ->
                Timings#pcl_fetch_timings{
                    found2_count =
                        Timings#pcl_fetch_timings.found2_count + 1,
                    found2_time =
                        Timings#pcl_fetch_timings.found2_time + FetchTime
                };
            3 ->
                Timings#pcl_fetch_timings{
                    found3_count =
                        Timings#pcl_fetch_timings.found3_count + 1,
                    found3_time =
                        Timings#pcl_fetch_timings.found3_time + FetchTime
                };
            N when N  > 3 ->
                Timings#pcl_fetch_timings{
                    foundlower_count =
                        Timings#pcl_fetch_timings.foundlower_count + 1,
                    foundlower_time =
                        Timings#pcl_fetch_timings.foundlower_time + FetchTime
                }
        end,              
    UpdTimings0 = UpdTimings#pcl_fetch_timings{sample_count = SC0},
    {noreply, State#state{pcl_fetch_timings = UpdTimings0}};
handle_cast({sst_fetch_update, Level, FetchPoint, FetchTime}, State) ->
    Timings =
        case lists:keyfind(Level, 1, State#state.sst_fetch_timings) of
            {Level, PrvTimings} ->
                PrvTimings;
            false ->
                #sst_fetch_timings{}
        end,
    SC0 = Timings#sst_fetch_timings.sample_count + 1,
    UpdTimings =
        case FetchPoint of
            not_found ->
                Timings#sst_fetch_timings{
                    notfound_count =
                        Timings#sst_fetch_timings.notfound_count + 1,
                    notfound_time =
                        Timings#sst_fetch_timings.notfound_time + FetchTime
                };
            fetch_cache ->
                Timings#sst_fetch_timings{
                    fetchcache_count =
                        Timings#sst_fetch_timings.fetchcache_count + 1,
                    fetchcache_time =
                        Timings#sst_fetch_timings.fetchcache_time + FetchTime
                };
            slot_cachedblock ->
                Timings#sst_fetch_timings{
                    slotcached_count =
                        Timings#sst_fetch_timings.slotcached_count + 1,
                    slotcached_time =
                        Timings#sst_fetch_timings.slotcached_time + FetchTime
                };
            slot_noncachedblock ->
                Timings#sst_fetch_timings{
                    slotnoncached_count =
                        Timings#sst_fetch_timings.slotnoncached_count + 1,
                    slotnoncached_time =
                        Timings#sst_fetch_timings.slotnoncached_time + FetchTime
                }
        end,
    UpdLevel = {Level, UpdTimings#sst_fetch_timings{sample_count = SC0}},
    UpdLevels = 
        lists:ukeysort(1, [UpdLevel|State#state.sst_fetch_timings]),
    {noreply, State#state{sst_fetch_timings = UpdLevels}};
handle_cast({cdb_get_update, CycleCount, IndexTime, ReadTime}, State) ->
    Timings = State#state.cdb_get_timings,
    SC0 = Timings#cdb_get_timings.sample_count + 1,
    CC0 = Timings#cdb_get_timings.cycle_count + CycleCount,
    IT0 = Timings#cdb_get_timings.index_time + IndexTime,
    RT0 = Timings#cdb_get_timings.read_time + ReadTime,
    UpdTimings =
        #cdb_get_timings{
            sample_count = SC0,
            cycle_count = CC0,
            index_time = IT0,
            read_time = RT0
        },
    {noreply, State#state{cdb_get_timings = UpdTimings}};
handle_cast({report_stats, bookie_get}, State) ->
    Timings = State#state.bookie_get_timings,
    leveled_log:log(
        "B0016",
        [Timings#bookie_get_timings.sample_count,
            Timings#bookie_get_timings.head_time,
            Timings#bookie_get_timings.body_time,
            Timings#bookie_get_timings.fetch_count]),
    {noreply, State#state{bookie_get_timings = #bookie_get_timings{}}};
handle_cast({report_stats, bookie_head}, State) ->
    Timings = State#state.bookie_head_timings,
    leveled_log:log(
        "B0018",
        [Timings#bookie_head_timings.sample_count,
            Timings#bookie_head_timings.fetch_time,
            Timings#bookie_head_timings.rsp_time,
            Timings#bookie_head_timings.fetch_count,
            Timings#bookie_head_timings.cache_count]),
    {noreply, State#state{bookie_head_timings = #bookie_head_timings{}}};
handle_cast({report_stats, bookie_put}, State) ->
    Timings = State#state.bookie_put_timings,
    leveled_log:log(
        "B0015",
        [Timings#bookie_put_timings.sample_count,
            Timings#bookie_put_timings.mem_time,
            Timings#bookie_put_timings.ink_time,
            Timings#bookie_put_timings.total_size]),
    {noreply, State#state{bookie_put_timings = #bookie_put_timings{}}};
handle_cast({report_stats, bookie_snap}, State) ->
    Timings = State#state.bookie_snap_timings,
    leveled_log:log(
        "B0017",
        [Timings#bookie_snap_timings.sample_count,
            Timings#bookie_snap_timings.bookie_time,
            Timings#bookie_snap_timings.pcl_time]),
    {noreply, State#state{bookie_snap_timings = #bookie_snap_timings{}}};
handle_cast({report_stats, pcl_fetch}, State) ->
    Timings = State#state.pcl_fetch_timings,
    leveled_log:log(
        "P0032",
        [Timings#pcl_fetch_timings.sample_count,
            Timings#pcl_fetch_timings.foundmem_time,
            Timings#pcl_fetch_timings.found0_time,
            Timings#pcl_fetch_timings.found1_time,
            Timings#pcl_fetch_timings.found2_time,
            Timings#pcl_fetch_timings.found3_time,
            Timings#pcl_fetch_timings.foundlower_time,
            Timings#pcl_fetch_timings.notfound_time,
            Timings#pcl_fetch_timings.foundmem_count,
            Timings#pcl_fetch_timings.found0_count,
            Timings#pcl_fetch_timings.found1_count,
            Timings#pcl_fetch_timings.found2_count,
            Timings#pcl_fetch_timings.found3_count,
            Timings#pcl_fetch_timings.foundlower_count,
            Timings#pcl_fetch_timings.notfound_count]),
    {noreply, State#state{pcl_fetch_timings = #pcl_fetch_timings{}}};
handle_cast({report_stats, sst_fetch}, State) ->
    LogFun =
        fun({Level, Timings}) ->
            leveled_log:log(
                "SST12",
                [Level,
                    Timings#sst_fetch_timings.sample_count,
                    Timings#sst_fetch_timings.notfound_time,
                    Timings#sst_fetch_timings.fetchcache_time,
                    Timings#sst_fetch_timings.slotcached_time,
                    Timings#sst_fetch_timings.slotnoncached_time,
                    Timings#sst_fetch_timings.notfound_count,
                    Timings#sst_fetch_timings.fetchcache_count,
                    Timings#sst_fetch_timings.slotcached_count,
                    Timings#sst_fetch_timings.slotnoncached_count])
        end,
    lists:foreach(LogFun, State#state.sst_fetch_timings),
    {noreply, State#state{sst_fetch_timings = []}};
handle_cast({report_stats, cdb_get}, State) ->
    Timings = State#state.cdb_get_timings,
    leveled_log:log(
        "CDB19",
        [Timings#cdb_get_timings.sample_count,
            Timings#cdb_get_timings.cycle_count,
            Timings#cdb_get_timings.index_time,
            Timings#cdb_get_timings.read_time]),
    {noreply, State#state{cdb_get_timings = #cdb_get_timings{}}};
handle_cast({log_level, LogLevel}, State) ->
    ok = leveled_log:set_loglevel(LogLevel),
    {noreply, State};
handle_cast({log_add, ForcedLogs}, State) ->
    ok = leveled_log:add_forcedlogs(ForcedLogs),
    {noreply, State};
handle_cast({log_remove, ForcedLogs}, State) ->
    ok = leveled_log:remove_forcedlogs(ForcedLogs),
    {noreply, State}.

handle_info(report_next_stats, State) ->
    erlang:send_after(
        State#state.log_frequency * 1000, self(), report_next_stats),
    case State#state.log_order of
        [] ->
            {noreply, State};
        [NextStat|TailLogOrder] ->
            ok = report_stats(self(), NextStat),
            {noreply, State#state{log_order = TailLogOrder ++ [NextStat]}}
    end.

terminate(_Reason, _State) ->
    ok.
    
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

coverage_cheat_test() ->
    {ok, M} = monitor_start(1, []),
    timer:sleep(2000),
    {ok, _State1} = code_change(null, #state{}, null),
    % Can close, so empty log_order hasn't crashed
    ok = monitor_close(M).

-endif.