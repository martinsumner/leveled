%% Module to abstract from choice of logger, and allow use of logReferences
%% for fast lookup

-module(leveled_log).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([log/2,
            log_timer/3,
            log_randomtimer/4]).

-export([set_loglevel/1, 
            set_databaseid/1,
            add_forcedlogs/1,
            remove_forcedlogs/1,
            get_opts/0,
            save/1,
            return_settings/0]).

-ifdef(TEST).
-export([format_time/1, log_prefix/5]).
-endif.

-record(log_options,
    {log_level = info :: log_level(), 
        forced_logs = [] :: [atom()],
        database_id = 0 :: non_neg_integer()}).

-type log_level()  ::  debug | info | warn | error | critical.
-type log_options() :: #log_options{}.

-export_type([log_options/0, log_level/0]).

-define(LOG_LEVELS, [debug, info, warn, error, critical]).
-define(DEFAULT_LOG_LEVEL, error).

-define(LOGBASE,
    #{
        g0001 => 
            {info, <<"Generic log point">>},
        g0002 =>
            {info, <<"Generic log point with term ~w">>},
        d0001 =>
            {info, <<"Generic debug log">>},
        b0001 =>
            {info, <<"Bookie starting with Ink ~w Pcl ~w">>},
        b0002 =>
            {info, <<"Snapshot starting with Ink ~w Pcl ~w">>},
        b0003 =>
            {info, <<"Bookie closing for reason ~w">>},
        b0004 =>
            {warn, <<"Bookie snapshot exiting as master store ~w is down for reason ~p">>},
        b0005 =>
            {info, <<"LedgerSQN=~w at startup">>},
        b0006 =>
            {info, <<"Reached end of load batch with SQN ~w">>},
        b0008 =>
            {info, <<"Bucket list finds no more results">>},
        b0009 =>
            {debug, <<"Bucket list finds Bucket ~w">>},
        b0011 =>
            {warn, <<"Call to destroy the store and so all files to be removed">>},
        b0013 =>
            {warn, <<"Long running task took ~w microseconds with task_type=~w">>},
        b0015 =>
            {info, <<"Put timing with sample_count=~w ink_time=~w prep_time=~w mem_time=~w with total_object_size=~w with sample_period=~w seconds">>},
        b0016 =>
            {info, <<"Get timing with sample_count=~w and head_time=~w body_time=~w with fetch_count=~w with sample_period=~w seconds">>},
        b0017 =>
            {info, <<"Snapshot timing with sample_count=~w and bookie_time=~w pcl_time=~w with sample_period=~w seconds">>},
        b0018 =>
            {info, <<"Positive HEAD responses timed with sample_count=~w and cache_count=~w found_count=~w fetch_ledger_time=~w fetch_ledgercache_time=~w rsp_time=~w notfound_time=~w with sample_period=~w seconds">>},
        b0019 =>
            {warn, <<"Use of book_indexfold with constraint of Bucket ~w with no StartKey is deprecated">>},
        b0020 =>
            {warn, <<"Ratio of penciller cache size ~w to bookie's memory cache size ~w is larger than expected">>},
        r0001 =>
            {debug, <<"Object fold to process batch of ~w objects">>},
        p0001 =>
            {debug, <<"Ledger snapshot ~w registered">>},
        p0003 =>
            {debug, <<"Ledger snapshot ~w released">>},
        p0004 =>
            {debug, <<"Remaining ledger snapshots are ~w">>},
        p0005 =>
            {debug, <<"Delete confirmed as file ~s is removed from Manifest">>},
        p0007 =>
            {debug, <<"Shutdown complete for cloned Penciller for reason ~w">>},
        p0008 =>
            {info, <<"Penciller closing for reason ~w">>},
        p0010 =>
            {info, <<"level zero discarded_count=~w on close of Penciller">>},
        p0011 =>
            {debug, <<"Shutdown complete for Penciller for reason ~w">>},
        p0012 =>
            {info, <<"Store to be started based on manifest sequence number of ~w">>},
        p0013 =>
            {info, <<"Seqence number of 0 indicates no valid manifest">>},
        p0014 =>
            {info, <<"Maximum sequence number of ~w found in nonzero levels">>},
        p0015 =>
            {info, <<"L0 file found ~s">>},
        p0016 =>
            {info, <<"L0 file had maximum sequence number of ~w">>},
        p0017 =>
            {info, <<"No L0 file found">>},
        p0018 =>
            {info, <<"Response to push_mem of returned with cache_size=~w L0_pending=~w merge_backlog=~w cachelines_full=~w">>},
        p0019 =>
            {info, <<"Rolling level zero to filename ~s at ledger sqn ~w">>},
        p0024 =>
            {info, <<"Outstanding compaction work items of ~w with backlog status of ~w L0 full ~w">>},
        p0029 =>
            {info, <<"L0 completion confirmed and will transition to not pending">>},
        p0030 =>
            {warn, <<"We're doomed - intention recorded to destroy all files">>},
        p0031 =>
            {info, <<"Completion of update to levelzero with cache_size=~w level0_due=~w change_pending=~w MinSQN=~w MaxSQN=~w">>},
        p0032 =>
            {info, <<"Fetch head timing with sample_count=~w and level timings of foundmem_time=~w found0_time=~w found1_time=~w found2_time=~w found3_time=~w foundlower_time=~w missed_time=~w with counts of foundmem_count=~w found0_count=~w found1_count=~w found2_count=~w found3_count=~w foundlower_count=~w missed_count=~w with sample_period=~w seconds">>},
        p0033 =>
            {error, <<"Corrupted manifest file at path ~s to be ignored due to error ~s">>},
        p0035 =>
            {info, <<"Startup with Manifest SQN of ~w">>},
        p0037 =>
            {debug, <<"Merging of penciller L0 tree from size ~w complete">>},
        p0038 =>
            {info, <<"Timeout of snapshot with pid=~w at SQN=~w at TS ~w set to timeout=~w">>},
        p0039 =>
            {debug, <<"Failed to release pid=~w leaving SnapshotCount=~w and MinSQN=~w">>},
        p0040 =>
            {info, <<"Archiving filename ~s as unused at startup">>},
        p0041 =>
            {info, <<"Penciller manifest switched from SQN ~w to ~w">>},
        p0042 =>
            {info, <<"Deferring shutdown due to snapshot_count=~w">>},
        pc001 =>
            {info, <<"Penciller's clerk ~w started with owner ~w">>},
        pc005 =>
            {info, <<"Penciller's Clerk ~w shutdown now complete for reason ~w">>},
        pc007 =>
            {debug, <<"Clerk prompting Penciller regarding manifest change">>},
        pc008 =>
            {info, <<"Merge from level ~w to merge into ~w files below">>},
        pc009 =>
            {debug, <<"File ~s to simply switch levels to level ~w">>},
        pc010 =>
            {info, <<"Merge to be commenced for FileToMerge=~s with MSN=~w">>},
        pc011 =>
            {info, <<"Merge completed with MSN=~w to Level=~w and FileCounter=~w">>},
        pc012 =>
            {debug, <<"File to be created as part of MSN=~w Filename=~s IsBasement=~w">>},
        pc013 =>
            {warn, <<"Merge resulted in empty file ~s">>},
        pc015 =>
            {info, <<"File created">>},
        pc016 =>
            {info, <<"Slow fetch from SFT ~w of ~w us at level ~w with result ~w">>},
        pc017 =>
            {debug, <<"Notified clerk of manifest change">>},
        pc018 =>
            {info, <<"Saved manifest file">>},
        pc019 =>
            {debug, <<"After ~s level ~w is ~w">>},
        pc021 =>
            {debug, <<"Prompting deletions at ManifestSQN=~w">>},
        pc022 =>
            {debug, <<"Storing reference to deletions at ManifestSQN=~w">>},
        pc023 =>
            {info, <<"At level=~w file_count=~w avg_mem=~w file with most memory fn=~s p=~w mem=~w">>},
        pc024 =>
            {info, <<"Grooming compaction picked file with tomb_count=~w">>},
        pc025 =>
            {info, <<"At level=~w file_count=~w average words for heap_block_size=~w heap_size=~w recent_size=~w bin_vheap_size=~w">>},
        pm002 =>
            {info, <<"Completed dump of L0 cache to list of l0cache_size=~w">>},
        sst03 =>
            {info, <<"Opening SST file with filename ~s slot_count=~w and max sqn ~w">>},
        sst04 =>
            {debug, <<"Exit called for reason ~w on filename ~s">>},
        sst05 =>
            {warn, <<"Rename rogue filename ~s to ~s">>},
        sst06 =>
            {debug, <<"File ~s has been set for delete">>},
        sst07 =>
            {info, <<"Exit called and now clearing ~s">>},
        sst08 =>
            {info, <<"Completed creation of ~s at level ~w with max sqn ~w">>},
        sst09 =>
            {warn, <<"Read request exposes slot with bad CRC">>},
        sst10 =>
            {debug, <<"Expansion sought to support pointer to pid ~w status ~w">>},
        sst11 =>
            {info, <<"Level zero creation timings in microseconds pmem_fetch=~w merge_lists=~w build_slots=~w build_summary=~w read_switch=~w">>},
        sst12 =>
            {info, <<"SST Timings at level=~w for sample_count=~w at timing points notfound_time=~w fetchcache_time=~w slotcached_time=~w slotnoncached_time=~w exiting at points notfound_count=~w fetchcache_count=~w slotcached_count=~w slotnoncached_count=~w with sample_period=~w seconds">>},
        sst13 =>
            {info, <<"SST merge list build timings of fold_toslot=~w slot_hashlist=~w slot_serialise=~w slot_finish=~w is_basement=~w level=~w">>},
        sst14 =>
            {debug, <<"File ~s has completed BIC">>},
        i0001 =>
            {info, <<"Unexpected failure to fetch value for Key=~w SQN=~w with reason ~w">>},
        i0002 =>
            {debug, <<"Journal snapshot ~w registered at SQN ~w">>},
        i0003 =>
            {debug, <<"Journal snapshot ~w released">>},
        i0004 =>
            {info, <<"Remaining number of journal snapshots is ~w">>},
        i0005 =>
            {info, <<"Inker closing journal for reason ~w">>},
        i0006 =>
            {info, <<"Close triggered with journal_sqn=~w and manifest_sqn=~w">>},
        i0007 =>
            {info, <<"Inker manifest when closing is:">>},
        i0008 =>
            {info, <<"Put to new active journal required roll and manifest write">>},
        i0009 =>
            {info, <<"Updated manifest on startup:">>},
        i0010 =>
            {info, <<"Unchanged manifest on startup:">>},
        i0011 =>
            {info, <<"Manifest is empty, starting from manifest SQN 1">>},
        i0012 =>
            {info, <<"Head manifest entry ~s is complete so new active journal required">>},
        i0013 =>
            {info, <<"File ~s to be removed from manifest">>},
        i0014 =>
            {info, <<"On startup loading from filename ~s from SQN ~w">>},
        i0015 =>
            {info, <<"Opening manifest file at ~s with SQN ~w">>},
        i0016 =>
            {info, <<"Writing new version of manifest for manifestSQN=~w">>},
        i0017 =>
            {debug, <<"At SQN=~w journal has filename ~s">>},
        i0018 =>
            {warn, <<"We're doomed - intention recorded to destroy all files">>},
        i0020 =>
            {info, <<"Journal backup completed to path=~s with file_count=~w">>},
        i0021 =>
            {info, <<"Ingoring filename=~s with SQN=~w and JournalSQN=~w">>},
        i0022 =>
            {info, <<"Removing filename=~s from backup folder as not in backup">>},
        i0023 =>
            {info, <<"Backup commencing into folder with ~w existing files">>},
        i0024 =>
            {info, <<"Prompted roll at NewSQN=~w">>},
        i0025 =>
            {warn, <<"Journal SQN of ~w is below Ledger SQN of ~w anti-entropy will be required">>},
        i0026 =>
            {info, <<"Deferring shutdown due to snapshot_count=~w">>},
        i0027 =>
            {debug, <<"Shutdown complete for cloned Inker for reason ~w">>},
        i0028 =>
            {debug, <<"Shutdown complete for Inker for reason ~w">>},
        ic001 =>
            {info, <<"Closed for reason ~w so maybe leaving garbage">>},
        ic002 =>
            {info, <<"Clerk updating Inker as compaction complete of ~w files">>},
        ic003 =>
            {info, <<"Scoring of compaction runs complete with highest score=~w with run of run_length=~w">>},
        ic004 =>
            {info, <<"Score=~w with mean_byte_jump=~w for filename ~s">>},
        ic005 =>
            {info, <<"Compaction to be performed on file_count=~w with compaction_score=~w">>},
        ic006 =>
            {info, <<"Filename ~s is part of compaction run">>},
        ic007 =>
            {info, <<"Clerk has completed compaction process">>},
        ic008 =>
            {info, <<"Compaction source ~s has yielded ~w positions">>},
        ic009 =>
            {info, <<"Generate journal for compaction with filename ~s">>},
        ic010 =>
            {info, <<"Clearing journal with filename ~s">>},
        ic011 =>
            {info, <<"Not clearing filename ~s as modified delta is only ~w seconds">>},
        ic012 =>
            {warn, <<"Tag ~w not found in Strategy ~w - maybe corrupted">>},
        ic013 =>
            {warn, "File with name ~s to be ignored in manifest as scanning for first key returned empty - maybe corrupted"},
        ic014 =>
            {info, <<"Compaction to be run with strategy ~w and max_run_length ~w">>},
        cdb01 =>
            {info, <<"Opening file for writing with filename ~s">>},
        cdb02 =>
            {info, <<"Opening file for reading with filename ~s">>},
        cdb03 =>
            {info, <<"Re-opening file for reading with filename ~s">>},
        cdb04 =>
            {info, <<"Deletion confirmed for file ~s at ManifestSQN ~w">>},
        cdb05 =>
            {info, <<"Closing of filename ~s from state ~w for reason ~w">>},
        cdb06 =>
            {warn, <<"File to be truncated at last position of ~w with end of file at ~w">>},
        cdb07 =>
            {info, <<"Hashtree index computed">>},
        cdb08 =>
            {info, <<"Renaming file from ~s to ~s for which existence is ~w">>},
        cdb09 =>
            {info, <<"Failure to read Key/Value at Position ~w in scan this may be the end of the file">>},
        cdb10 =>
            {warn, <<"CRC check failed due to error=~s">>},
        cdb12 =>
            {info, <<"Hashtree index written">>},
        cdb13 =>
            {debug, <<"Write options of ~w">>},
        cdb14 =>
            {info, <<"Microsecond timings for hashtree build of to_list=~w sort=~w build=~w">>},
        cdb15 =>
            {info, <<"Collision in search for hash ~w">>},
        cdb18 =>
            {info, <<"Handled return and write of hashtable">>},
        cdb19 =>
            {info, <<"Sample timings in microseconds for sample_count=~w with totals of cycle_count=~w index_time=~w read_time=~w with sample_period=~w seconds">>},
        cdb20 =>
            {warn, <<"Error ~w caught when safe reading a file to length ~w">>},
        cdb21 =>
            {warn, <<"File ~s to be deleted but already gone">>}
    }).


%%%============================================================================
%%% Manage Log Options
%%%============================================================================

-spec set_loglevel(log_level()) -> ok.
%% @doc
%% Set the log level for this PID
set_loglevel(LogLevel) when is_atom(LogLevel) ->
    LO = get_opts(),
    UpdLO = LO#log_options{log_level = LogLevel},
    save(UpdLO).

-spec set_databaseid(non_neg_integer()) -> ok.
%% @doc
%% Set the Database ID for this PID
set_databaseid(DBid) when is_integer(DBid) ->
    LO = get_opts(),
    UpdLO = LO#log_options{database_id = DBid},
    save(UpdLO).

-spec add_forcedlogs(list(atom())) -> ok.
%% @doc
%% Add a forced log to the list of forced logs. this will cause the log of this
%% logReference to be logged even if the log_level of the process would not
%% otherwise require it to be logged - e.g. to fire an 'INFO' log when running
%% at an 'ERROR' log level.
add_forcedlogs(LogRefs) ->
    LO = get_opts(),
    ForcedLogs = LO#log_options.forced_logs,
    UpdLO = LO#log_options{forced_logs = lists:usort(LogRefs ++ ForcedLogs)},
    save(UpdLO).

-spec remove_forcedlogs(list(atom())) -> ok.
%% @doc
%% Remove a forced log from the list of forced logs
remove_forcedlogs(LogRefs) ->
    LO = get_opts(),
    ForcedLogs = LO#log_options.forced_logs,
    UpdLO = LO#log_options{forced_logs = lists:subtract(ForcedLogs, LogRefs)},
    save(UpdLO).

-spec save(log_options()) -> ok.
%% @doc
%% Save the log options to the process dictionary
save(#log_options{} = LO) ->
    put('$leveled_log_options', LO),
    ok.

-spec get_opts() -> log_options().
%% @doc
%% Retrieve the log options from the process dictionary if present.
get_opts() ->
    case get('$leveled_log_options') of
        #log_options{} = LO ->
            LO;
        _ ->
            #log_options{log_level = ?DEFAULT_LOG_LEVEL,
                         forced_logs = []}
    end.

-spec return_settings() -> {log_level(), list(string())}.
%% @doc
%% Return the settings outside of the record
return_settings() ->
    LO = get_opts(),
    {LO#log_options.log_level, LO#log_options.forced_logs}.

%%%============================================================================
%%% Prompt Logs
%%%============================================================================

-spec log(atom(), list()) -> ok.
log(LogReference, Subs) ->
    log(LogReference, Subs, ?LOG_LEVELS).

log(LogRef, Subs, SupportedLogLevels) ->
    {LogLevel, Log} = maps:get(LogRef, ?LOGBASE),
    LogOpts = get_opts(),
    case should_i_log(LogLevel, SupportedLogLevels, LogRef, LogOpts) of
        true ->
            DBid = LogOpts#log_options.database_id,
            Prefix =
                log_prefix(
                    localtime_ms(), LogLevel, LogRef, DBid, self()),
            Suffix = <<"~n">>,
            io:format(iolist_to_binary([Prefix, Log, Suffix]), Subs);
        false ->
            ok
    end.

should_i_log(LogLevel, Levels, LogRef) ->
    should_i_log(LogLevel, Levels, LogRef, get_opts()).

should_i_log(LogLevel, Levels, LogRef, LogOpts) ->
    #log_options{log_level = CurLevel, forced_logs = ForcedLogs} = LogOpts,
    case lists:member(LogRef, ForcedLogs) of
        true ->
            true;
        false ->
            if CurLevel == LogLevel ->
                    true;
               true ->
                    is_active_level(Levels, CurLevel, LogLevel)
            end
    end.

is_active_level([L|_], L, _) -> true;
is_active_level([L|_], _, L) -> false;
is_active_level([_|T], C, L) -> is_active_level(T, C, L).

-spec log_timer(atom(), list(), erlang:timestamp()) -> ok.
log_timer(LogReference, Subs, StartTime) ->
    log_timer(LogReference, Subs, StartTime, ?LOG_LEVELS).

log_timer(LogRef, Subs, StartTime, SupportedLevels) ->
    {LogLevel, Log} = maps:get(LogRef, ?LOGBASE),
    LogOpts = get_opts(),
    case should_i_log(LogLevel, SupportedLevels, LogRef, LogOpts) of
        true ->
            DBid = LogOpts#log_options.database_id,
            Prefix =
                log_prefix(
                        localtime_ms(), LogLevel, LogRef, DBid, self()),
            Suffix = <<"~n">>,
            Duration = duration_text(StartTime),
            io:format(
                iolist_to_binary([Prefix, Log, Duration, Suffix]),
                Subs);
        false ->
            ok
    end.

-spec log_randomtimer(atom(), list(), erlang:timestamp(), float()) -> ok.
log_randomtimer(LogReference, Subs, StartTime, RandomProb) ->
    R = leveled_rand:uniform(),
    case R < RandomProb of
        true ->
            log_timer(LogReference, Subs, StartTime);
        false ->
            ok
    end.

localtime_ms() ->
    {_, _, Micro} = Now = os:timestamp(),
    {Date, {Hours, Minutes, Seconds}} = calendar:now_to_local_time(Now),
    {Date, {Hours, Minutes, Seconds, Micro div 1000 rem 1000}}.

-spec log_prefix(
    tuple(), atom(), atom(), non_neg_integer(), pid()) -> io_lib:chars().
log_prefix({{Y, M, D}, {H, Mi, S, Ms}}, LogLevel, LogRef, DBid, Pid) ->
    [integer_to_list(Y), $-, i2l(M), $-, i2l(D),
    $T, i2l(H), $:, i2l(Mi), $:, i2l(S), $., i3l(Ms),
    " log_level=", atom_to_list(LogLevel), " log_ref=", atom_to_list(LogRef),
    " db_id=", integer_to_list(DBid), " pid=", pid_to_list(Pid), " "].

-spec i2l(non_neg_integer()) -> list().
i2l(I) when I < 10 ->
    [$0, $0+I];
i2l(I) ->
    integer_to_list(I).

-spec i3l(non_neg_integer()) -> list().
i3l(I) when I < 100 ->
    [$0 | i2l(I)];
i3l(I) ->
    integer_to_list(I).

-spec duration_text(erlang:timestamp()) -> io_lib:chars().
duration_text(StartTime) ->
    case timer:now_diff(os:timestamp(), StartTime) of
        US when US > 1000 ->
            [" with us_duration=", integer_to_list(US),
            " or ms_duration=", integer_to_list(US div 1000)];
        US ->
            [" with us_duration=", integer_to_list(US)]
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

format_time({{Y, M, D}, {H, Mi, S, Ms}}) ->
    io_lib:format("~b-~2..0b-~2..0b", [Y, M, D]) ++ "T" ++
        io_lib:format("~2..0b:~2..0b:~2..0b.~3..0b", [H, Mi, S, Ms]).

prefix_compare_test() ->
    Time = localtime_ms(),
    DBid = 64,
    LogLevel = info,
    LogRef = b0001,
    {TS0, OldTS} =
        timer:tc(?MODULE, format_time, [Time]),
    {TS1, NewPrefix} =
        timer:tc(?MODULE, log_prefix, [Time, LogLevel, LogRef, DBid, self()]),
    {NewTS, _Rest} = lists:split(23, lists:flatten(NewPrefix)),
    ?assertMatch(OldTS, NewTS),
    io:format(user, "~nTimestamp timings old ~w new ~w~n", [TS0, TS1]).

log_test() ->
    log(d0001, []),
    log_timer(d0001, [], os:timestamp()).

log_warn_test() ->
    ok = log(g0001, [], [warn, error]),
    ok = log_timer(g0001, [], os:timestamp(), [warn, error]).

shouldilog_test() ->
    ok = set_loglevel(debug),
    ?assertMatch(true, should_i_log(info, ?LOG_LEVELS, g0001)),
    ok = set_loglevel(info),
    ?assertMatch(true, should_i_log(info, ?LOG_LEVELS, g0001)),
    ok = add_forcedlogs([g0001]),
    ok = set_loglevel(error),
    ?assertMatch(true, should_i_log(info, ?LOG_LEVELS, g0001)),
    ?assertMatch(false, should_i_log(info, ?LOG_LEVELS, g0002)),
    ok = remove_forcedlogs([g0001]),
    ok = set_loglevel(info),
    ?assertMatch(false, should_i_log(debug, ?LOG_LEVELS, d0001)).

badloglevel_test() ->
    % Set a bad log level - and everything logs
    ?assertMatch(true, is_active_level(?LOG_LEVELS, debug, unsupported)),
    ?assertMatch(true, is_active_level(?LOG_LEVELS, critical, unsupported)).

timing_test() ->
    % Timing test
    % Previous LOGBASE used list with string-based keys and values
    % The size of the LOGBASE was 19,342 words (>150KB), and logs took
    % o(100) microseconds.
    % Changing the LOGBASE ot a map with binary-based keys and values does not
    % appear to improve the speed of logging, but does reduce the size of the
    % LOGBASE to just over 2,000 words (so an order of magnitude improvement)
    timer:sleep(10),
    io:format(user, "Log timings:~n", []),
    io:format(user, "Logbase size ~w~n", [erts_debug:flat_size(?LOGBASE)]),
    io:format(
        user,
        "Front log timing ~p~n",
        [timer:tc(fun() -> log(cdb21, ["test_file"]) end)]
    ),
    io:format(
        user,
        "Mid log timing ~p~n",
        [timer:tc(fun() -> log(pc013, ["test_file"]) end)]
    ),
    io:format(
        user,
        "End log timing ~p~n",
        [timer:tc(fun() -> log(b0003, ["testing"]) end)]
    ),
    io:format(
        user,
        "Big log timing ~p~n",
        [timer:tc(fun() -> log(sst13, [100,100,100,100,true,1]) end)]
    ),
    io:format(
        user,
        "Timer log timing ~p~n",
        [timer:tc(fun() -> log_timer(pc015, [], os:timestamp()) end)]
    ).

-endif.
