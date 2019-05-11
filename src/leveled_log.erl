%% Module to abstract from choice of logger, and allow use of logReferences
%% for fast lookup

-module(leveled_log).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([log/2,
            log_timer/3,
            log_randomtimer/4]).

-export([set_loglevel/1, 
            add_forcedlogs/1,
            remove_forcedlogs/1,
            get_opts/0,
            save/1,
            return_settings/0]).


-record(log_options, {log_level = info :: log_level(), 
                        forced_logs = [] :: [string()]}).

-type log_level()  ::  debug | info | warn | error | critical.
-type log_options() :: #log_options{}.

-export_type([log_options/0, log_level/0]).

-define(LOG_LEVELS, [debug, info, warn, error, critical]).
-define(DEFAULT_LOG_LEVEL, error).

-define(LOGBASE, [

    {"G0001",
        {info, "Generic log point"}},
    {"G0002",
        {info, "Generic log point with term ~w"}},
    {"D0001",
        {debug, "Generic debug log"}},
    
    {"B0001",
        {info, "Bookie starting with Ink ~w Pcl ~w"}},
    {"B0002",
        {info, "Snapshot starting with Ink ~w Pcl ~w"}},
    {"B0003",
        {info, "Bookie closing for reason ~w"}},
    {"B0004",
        {info, "Initialised PCL clone and length of increment in snapshot is ~w"}},
    {"B0005",
        {info, "LedgerSQN=~w at startup"}},
    {"B0006",
        {info, "Reached end of load batch with SQN ~w"}},
    {"B0007",
        {info, "Skipping as exceeded MaxSQN ~w with SQN ~w"}},
    {"B0008",
        {info, "Bucket list finds no more results"}},
    {"B0009",
        {debug, "Bucket list finds Bucket ~w"}},
    {"B0011",
        {warn, "Call to destroy the store and so all files to be removed"}},
    {"B0013",
        {warn, "Long running task took ~w microseconds with task_type=~w"}},
    {"B0015",
        {info, "Put timing with sample_count=~w and mem_time=~w ink_time=~w"
                ++ " with total_object_size=~w"}},
    {"B0016",
        {info, "Get timing with sample_count=~w and head_time=~w body_time=~w"
                ++ " with fetch_count=~w"}},
    {"B0017",
        {info, "Fold timing with sample_count=~w and setup_time=~w"}},
    {"B0018",
        {info, "Positive HEAD responses timed with sample_count=~w and "
                ++ " pcl_time=~w rsp_time=~w"}},
    {"B0019",
        {warn, "Use of book_indexfold with constraint of Bucket ~w with "
                    ++ "no StartKey is deprecated"}},
    {"B0020",
        {warn, "Ratio of penciller cache size ~w to bookie's memory "
                    ++ "cache size ~w is larger than expected"}},

    {"R0001",
        {debug, "Object fold to process batch of ~w objects"}},
    
    {"P0001",
        {debug, "Ledger snapshot ~w registered"}},
    {"P0003",
        {debug, "Ledger snapshot ~w released"}},
    {"P0004",
        {debug, "Remaining ledger snapshots are ~w"}},
    {"P0005",
        {debug, "Delete confirmed as file ~s is removed from Manifest"}},
    {"P0006",
        {info, "Orphaned reply after timeout on L0 file write ~s"}},
    {"P0007",
        {debug, "Sent release message for cloned Penciller following close for "
                ++ "reason ~w"}},
    {"P0008",
        {info, "Penciller closing for reason ~w"}},
    {"P0010",
        {info, "discarded=~w level zero on close of Penciller"}},
    {"P0011",
        {info, "Shutdown complete for Penciller for reason ~w"}},
    {"P0012",
        {info, "Store to be started based on manifest sequence number of ~w"}},
    {"P0013",
        {warn, "Seqence number of 0 indicates no valid manifest"}},
    {"P0014",
        {info, "Maximum sequence number of ~w found in nonzero levels"}},
    {"P0015",
        {info, "L0 file found ~s"}},
    {"P0016",
        {info, "L0 file had maximum sequence number of ~w"}},
    {"P0017",
        {info, "No L0 file found"}},
    {"P0018",
        {info, "Response to push_mem of ~w with "
                    ++ "L0 pending ~w and merge backlog ~w"}},
    {"P0019",
        {info, "Rolling level zero to filename ~s at ledger sqn ~w"}},
    {"P0021",
        {info, "Allocation of work blocked as L0 pending"}},
    {"P0022",
        {info, "Manifest at Level ~w"}},
    {"P0023",
        {info, "Manifest entry of startkey ~s ~s ~s endkey ~s ~s ~s "
                ++ "filename=~s~n"}},
    {"P0024",
        {info, "Outstanding compaction work items of ~w with backlog status "
                    ++ "of ~w"}},
    {"P0025",
        {info, "Merge to sqn ~w from Level ~w completed"}},
    {"P0026",
        {info, "Merge has been commmitted at sequence number ~w"}},
    {"P0027",
        {info, "Rename of manifest from ~s ~w to ~s ~w"}},
    {"P0028",
        {debug, "Adding cleared file ~s to deletion list"}},
    {"P0029",
        {info, "L0 completion confirmed and will transition to not pending"}},
    {"P0030",
        {warn, "We're doomed - intention recorded to destroy all files"}},
    {"P0031",
        {info, "Completion of update to levelzero"
                    ++ " with cache_size=~w level0_due=~w"
                    ++ " and change_pending=~w"}},
    {"P0032",
        {info, "Fetch head timing with sample_count=~w and level timings of"
                    ++ " foundmem_time=~w found0_time=~w found1_time=~w" 
                    ++ " found2_time=~w found3_time=~w foundlower_time=~w" 
                    ++ " missed_time=~w"
                    ++ " with counts of"
                    ++ " foundmem_count=~w found0_count=~w found1_count=~w" 
                    ++ " found2_count=~w found3_count=~w foundlower_count=~w"
                    ++ " missed_count=~w"}},
    {"P0033",
        {error, "Corrupted manifest file at path ~s to be ignored "
                    ++ "due to error ~w"}},
    {"P0034",
        {warn, "Snapshot with pid ~w timed out and so deletion will "
                    ++ "continue regardless"}},
    {"P0035",
        {info, "Startup with Manifest SQN of ~w"}},
    {"P0036",
        {info, "Garbage collection on manifest removes key for filename ~s"}},
    {"P0037",
        {debug, "Merging of penciller L0 tree from size ~w complete"}},
    {"P0038",
        {info, "Timeout of snapshot with pid=~w at SQN=~w at TS ~w "
                    ++ "set to timeout=~w"}},
    {"P0039",
        {info, "Failed to release pid=~w "
                    ++ "leaving SnapshotCount=~w and MinSQN=~w"}},
    {"P0040",
        {info, "Archiving filename ~s as unused at startup"}},
    {"P0041",
        {info, "Penciller manifest switched from SQN ~w to ~w"}},
    {"P0042",
        {warn, "Cache full so attempting roll memory with l0_size=~w"}},
        
    {"PC001",
        {info, "Penciller's clerk ~w started with owner ~w"}},
    {"PC002",
        {info, "Request for manifest change from clerk on closing"}},
    {"PC003",
        {info, "Confirmation of manifest change on closing"}},
    {"PC004",
        {info, "Prompted confirmation of manifest change"}},
    {"PC005",
        {info, "Penciller's Clerk ~w shutdown now complete for reason ~w"}},
    {"PC006",
        {debug, "Work prompted but none needed"}},
    {"PC007",
        {debug, "Clerk prompting Penciller regarding manifest change"}},
    {"PC008",
        {info, "Merge from level ~w to merge into ~w files below"}},
    {"PC009",
        {info, "File ~s to simply switch levels to level ~w"}},
    {"PC010",
        {info, "Merge to be commenced for FileToMerge=~s with MSN=~w"}},
    {"PC011",
        {info, "Merge completed with MSN=~w to Level=~w and FileCounter=~w"}},
    {"PC012",
        {debug, "File to be created as part of MSN=~w Filename=~s "
                    ++ "IsBasement=~w"}},
    {"PC013",
        {warn, "Merge resulted in empty file ~s"}},
    {"PC015",
        {info, "File created"}},
    {"PC016",
        {info, "Slow fetch from SFT ~w of ~w us at level ~w "
                    ++ "with result ~w"}},
    {"PC017",
        {info, "Notified clerk of manifest change"}},
    {"PC018",
        {info, "Saved manifest file"}},
    {"PC019",
        {debug, "After ~s level ~w is ~w"}},
    {"PC020",
        {warn, "Empty prompt deletions at ManifestSQN=~w"}},
    {"PC021",
        {info, "Prompting deletions at ManifestSQN=~w"}},
    {"PC022",
        {info, "Storing reference to deletions at ManifestSQN=~w"}},
    {"PM002",
        {info, "Completed dump of L0 cache to list of l0cache_size=~w"}},
    
    {"SST01",
        {info, "SST timing for result=~w is sample=~w total=~w and max=~w"}},
    {"SST02",
        {error, "False result returned from SST with filename ~s as "
                    ++ "slot ~w has failed crc check"}},
    {"SST03",
        {info, "Opening SST file with filename ~s slot_count=~w and"
                ++ " max sqn ~w"}},
    {"SST04",
        {debug, "Exit called for reason ~w on filename ~s"}},
    {"SST05",
        {warn, "Rename rogue filename ~s to ~s"}},
    {"SST06",
        {debug, "File ~s has been set for delete"}},
    {"SST07",
        {info, "Exit called and now clearing ~s"}},
    {"SST08",
        {info, "Completed creation of ~s at level ~w with max sqn ~w"}},
    {"SST09",
        {warn, "Read request exposes slot with bad CRC"}},
    {"SST10",
        {debug, "Expansion sought to support pointer to pid ~w status ~w"}},
    {"SST11",
        {info, "Level zero creation timings in microseconds "
                ++ "pmem_fetch=~w merge_lists=~w build_slots=~w " 
                ++ "build_summary=~w read_switch=~w"}},
    {"SST12",
        {info, "SST Timings at level=~w for sample_count=~w"
                ++ " at timing points index_query_time=~w"
                ++ " lookup_cache_time=~w slot_index_time=~w "
                ++ " fetch_cache_time=~w slot_fetch_time=~w"
                ++ " noncached_block_fetch_time=~w"
                ++ " exiting at points slot_index=~w"
                ++ " fetch_cache=~w slot_fetch=~w noncached_block_fetch=~w"}},
    {"SST13",
        {info, "SST merge list build timings of"
                ++ " fold_toslot=~w slot_hashlist=~w"
                ++ " slot_serialise=~w slot_finish=~w"
                ++ " is_basement=~w level=~w"}},
    
    {"I0001",
        {info, "Unexpected failure to fetch value for Key=~w SQN=~w "
                ++ "with reason ~w"}},
    {"I0002",
        {debug, "Journal snapshot ~w registered at SQN ~w"}},
    {"I0003",
        {debug, "Journal snapshot ~w released"}},
    {"I0004",
        {info, "Remaining number of journal snapshots is ~w"}},
    {"I0005",
        {info, "Inker closing journal for reason ~w"}},
    {"I0006",
        {info, "Close triggered with journal_sqn=~w and manifest_sqn=~w"}},
    {"I0007",
        {info, "Inker manifest when closing is:"}},
    {"I0008",
        {info, "Put to new active journal required roll and manifest write"}},
    {"I0009",
        {info, "Updated manifest on startup:"}},
    {"I0010",
        {info, "Unchanged manifest on startup:"}},
    {"I0011",
        {info, "Manifest is empty, starting from manifest SQN 1"}},
    {"I0012",
        {info, "Head manifest entry ~s is complete so new active journal "
                ++ "required"}},
    {"I0013",
        {info, "File ~s to be removed from manifest"}},
    {"I0014",
        {info, "On startup loading from filename ~s from SQN ~w"}},
    {"I0015",
        {info, "Opening manifest file at ~s with SQN ~w"}},
    {"I0016",
        {info, "Writing new version of manifest for manifestSQN=~w"}},
    {"I0017",
        {info, "At SQN=~w journal has filename ~s"}},
    {"I0018",
        {warn, "We're doomed - intention recorded to destroy all files"}},
    {"I0019",
        {info, "After ~w PUTs total prepare time is ~w total cdb time is ~w "
                ++ "and max prepare time is ~w and max cdb time is ~w"}},
    {"I0020",
        {info, "Journal backup completed to path=~s with file_count=~w"}},
    {"I0021",
        {info, "Ingoring filename=~s with SQN=~w and JournalSQN=~w"}},
    {"I0022",
        {info, "Removing filename=~s from backup folder as not in backup"}},
    {"I0023",
        {info, "Backup commencing into folder with ~w existing files"}},
    {"I0024",
        {info, "Prompted roll at NewSQN=~w"}},
    {"I0025",
        {warn, "Journal SQN of ~w is below Ledger SQN of ~w " ++
                "anti-entropy will be required"}},

    {"IC001",
        {info, "Closed for reason ~w so maybe leaving garbage"}},
    {"IC002",
        {info, "Clerk updating Inker as compaction complete of ~w files"}},
    {"IC003",
        {info, "Scoring of compaction runs complete with highest score=~w " 
                ++ "with run of run_length=~w"}},
    {"IC004",
        {info, "Score for filename ~s is ~w"}},
    {"IC005",
        {info, "Compaction to be performed on ~w files with score of ~w"}},
    {"IC006",
        {info, "Filename ~s is part of compaction run"}},
    {"IC007",
        {info, "Clerk has completed compaction process"}},
    {"IC008",
        {info, "Compaction source ~s has yielded ~w positions"}},
    {"IC009",
        {info, "Generate journal for compaction with filename ~s"}},
    {"IC010",
        {info, "Clearing journal with filename ~s"}},
    {"IC011",
        {info, "Not clearing filename ~s as modified delta is only ~w seconds"}},
    {"IC012",
        {warn, "Tag ~w not found in Strategy ~w - maybe corrupted"}},
    {"IC013",
        {warn, "File with name ~s to be ignored in manifest as scanning for "
                ++ "first key returned empty - maybe corrupted"}},

    {"CDB01",
        {info, "Opening file for writing with filename ~s"}},
    {"CDB02",
        {info, "Opening file for reading with filename ~s"}},
    {"CDB03",
        {info, "Re-opening file for reading with filename ~s"}},
    {"CDB04",
        {info, "Deletion confirmed for file ~s at ManifestSQN ~w"}},
    {"CDB05",
        {info, "Closing of filename ~s from state ~w for reason ~w"}},
    {"CDB06",
        {warn, "File to be truncated at last position of ~w with end of "
                ++ "file at ~w"}},
    {"CDB07",
        {info, "Hashtree computed"}},
    {"CDB08",
        {info, "Renaming file from ~s to ~s for which existence is ~w"}},
    {"CDB09",
        {info, "Failure to read Key/Value at Position ~w in scan " ++ 
                "this may be the end of the file"}},
    {"CDB10",
        {info, "CRC check failed due to mismatch"}},
    {"CDB11",
        {info, "CRC check failed due to size"}},
    {"CDB12",
        {info, "HashTree written"}},
    {"CDB13",
        {debug, "Write options of ~w"}},
    {"CDB14",
        {info, "Microsecond timings for hashtree build of "
                ++ "to_list=~w sort=~w build=~w"}},
    {"CDB15",
        {info, "Collision in search for hash ~w"}},
    {"CDB16",
        {info, "CDB scan from start ~w in file with end ~w and last_key ~w"}},
    {"CDB17",
        {info, "After ~w PUTs total_write_time=~w total_sync_time=~w "
                ++ "and max_write_time=~w and max_sync_time=~w"}},
    {"CDB18",
        {info, "Handled return and write of hashtable"}},
    {"CDB19",
        {info, "Sample timings in microseconds for sample_count=~w " 
                    ++ "with totals of cycle_count=~w "
                    ++ "fetch_time=~w index_time=~w"}},
    {"CDB20",
        {warn, "Error ~w caught when safe reading a file to length ~w"}},
    {"CDB21",
        {warn, "File ~s to be deleted but already gone"}}
        
    ]).


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

-spec add_forcedlogs(list(string())) -> ok.
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

-spec remove_forcedlogs(list(string())) -> ok.
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


log(LogReference, Subs) ->
    log(LogReference, Subs, ?LOG_LEVELS).

log(LogRef, Subs, SupportedLogLevels) ->
    case lists:keyfind(LogRef, 1, ?LOGBASE) of
        {LogRef, {LogLevel, LogText}} ->
            case should_i_log(LogLevel, SupportedLogLevels, LogRef) of
                true ->
                    io:format(format_time() ++ " "
                                ++ atom_to_list(LogLevel) ++ " "
                                ++ LogRef ++ " ~w "
                                ++ LogText ++ "~n",
                              [self()|Subs]);
                false ->
                    ok
            end;
        false ->
            ok
    end.

should_i_log(LogLevel, Levels, LogRef) ->
    #log_options{log_level = CurLevel, forced_logs = ForcedLogs} = get_opts(),
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

log_timer(LogReference, Subs, StartTime) ->
    log_timer(LogReference, Subs, StartTime, ?LOG_LEVELS).

log_timer(LogRef, Subs, StartTime, SupportedLogLevels) ->
    case lists:keyfind(LogRef, 1, ?LOGBASE) of
        {LogRef, {LogLevel, LogText}} ->
            case should_i_log(LogLevel, SupportedLogLevels, LogRef) of
                true ->
                    DurationText =
                        case timer:now_diff(os:timestamp(), StartTime) of
                            US when US > 1000 ->
                                " with us_duration=" ++ integer_to_list(US) ++
                                " or ms_duration="
                                ++ integer_to_list(US div 1000);
                            US ->
                                " with us_duration=" ++ integer_to_list(US)
                        end,
                    io:format(format_time() ++ " "
                                ++ atom_to_list(LogLevel) ++ " "
                                ++ LogRef ++ " ~w "
                                ++ LogText
                                ++ DurationText ++ "~n",
                                [self()|Subs]);
                false ->
                    ok
            end;
        false ->
            ok
    end.

log_randomtimer(LogReference, Subs, StartTime, RandomProb) ->
    R = leveled_rand:uniform(),
    case R < RandomProb of
        true ->
            log_timer(LogReference, Subs, StartTime);
        false ->
            ok
    end.

format_time() ->
    format_time(localtime_ms()).

localtime_ms() ->
    {_, _, Micro} = Now = os:timestamp(),
    {Date, {Hours, Minutes, Seconds}} = calendar:now_to_local_time(Now),
    {Date, {Hours, Minutes, Seconds, Micro div 1000 rem 1000}}.

format_time({{Y, M, D}, {H, Mi, S, Ms}}) ->
    io_lib:format("~b-~2..0b-~2..0b", [Y, M, D]) ++ "T" ++
        io_lib:format("~2..0b:~2..0b:~2..0b.~3..0b", [H, Mi, S, Ms]).


%%%============================================================================
%%% Test
%%%============================================================================



-ifdef(TEST).

log_test() ->
    log("D0001", []),
    log_timer("D0001", [], os:timestamp()).

log_warn_test() ->
    ok = log("G0001", [], [warn, error]),
    ok = log("G8888", [], [info, warn, error]),
    ok = log_timer("G0001", [], os:timestamp(), [warn, error]),
    ok = log_timer("G8888", [], os:timestamp(), [info, warn, error]).

shouldilog_test() ->
    ok = set_loglevel(debug),
    ?assertMatch(true, should_i_log(info, ?LOG_LEVELS, "G0001")),
    ok = set_loglevel(info),
    ?assertMatch(true, should_i_log(info, ?LOG_LEVELS, "G0001")),
    ok = add_forcedlogs(["G0001"]),
    ok = set_loglevel(error),
    ?assertMatch(true, should_i_log(info, ?LOG_LEVELS, "G0001")),
    ?assertMatch(false, should_i_log(info, ?LOG_LEVELS, "G0002")),
    ok = remove_forcedlogs(["G0001"]),
    ok = set_loglevel(info),
    ?assertMatch(false, should_i_log(debug, ?LOG_LEVELS, "D0001")).

badloglevel_test() ->
    % Set a bad log level - and everything logs
    ?assertMatch(true, is_active_level(?LOG_LEVELS, debug, unsupported)),
    ?assertMatch(true, is_active_level(?LOG_LEVELS, critical, unsupported)).

-endif.
