%% Module to abstract from choice of logger, and allow use of logReferences
%% for fast lookup

-module(leveled_log).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([log/2,
            log_timer/3,
            put_timing/4,
            head_timing/4,
            get_timing/3,
            sst_timing/3]).         

-define(PUT_LOGPOINT, 20000).
-define(HEAD_LOGPOINT, 160000).
-define(GET_LOGPOINT, 160000).
-define(SST_LOGPOINT, 20000).
-define(LOG_LEVEL, [info, warn, error, critical]).
-define(SAMPLE_RATE, 16).

-define(LOGBASE, dict:from_list([

    {"G0001",
        {info, "Generic log point"}},
    {"D0001",
        {debug, "Generic debug log"}},
    
    {"B0001",
        {info, "Bookie starting with Ink ~w Pcl ~w"}},
    {"B0002",
        {info, "Snapshot starting with Ink ~w Pcl ~w"}},
    {"B0003",
        {info, "Bookie closing for reason ~w"}},
    {"B0004",
        {info, "Length of increment in snapshot is ~w"}},
    {"B0005",
        {info, "LedgerSQN=~w at startup"}},
    {"B0006",
        {info, "Reached end of load batch with SQN ~w"}},
    {"B0007",
        {info, "Skipping as exceeded MaxSQN ~w with SQN ~w"}},
    {"B0008",
        {info, "Bucket list finds no more results"}},
    {"B0009",
        {info, "Bucket list finds Bucket ~w"}},
    {"B0010",
        {info, "Bucket list finds non-binary Bucket ~w"}},
    {"B0011",
        {warn, "Call to destroy the store and so all files to be removed"}},
    {"B0012",
        {info, "After ~w PUTs total inker time is ~w total ledger time is ~w "
                ++ "and max inker time is ~w and max ledger time is ~w"}},
    {"B0013",
        {warn, "Long running task took ~w microseconds with task of type ~w"}},
    {"B0014",
        {info, "Get timing for result ~w is sample ~w total ~w and max ~w"}},
    
    {"P0001",
        {info, "Ledger snapshot ~w registered"}},
    {"P0003",
        {info, "Ledger snapshot ~w released"}},
    {"P0004",
        {info, "Remaining ledger snapshots are ~w"}},
    {"P0005",
        {info, "Delete confirmed as file ~s is removed from Manifest"}},
    {"P0006",
        {info, "Orphaned reply after timeout on L0 file write ~s"}},
    {"P0007",
        {debug, "Sent release message for cloned Penciller following close for "
                ++ "reason ~w"}},
    {"P0008",
        {info, "Penciller closing for reason ~w"}},
    {"P0010",
        {info, "No level zero action on close of Penciller ~w"}},
    {"P0011",
        {info, "Shutdown complete for Penciller"}},
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
        {info, "Completion of update to levelzero"}},
    {"P0032",
        {info, "Head timing for result ~w is sample ~w total ~w and max ~w"}},
    {"P0033",
        {error, "Corrupted manifest file at path ~s to be ignored "
                    ++ "due to error ~w"}},
    {"P0034",
        {warn, "Snapshot with pid ~w timed out and so deletion will "
                    ++ "continue regardless"}},
    {"P0035",
        {info, "Startup with Manifest SQN of ~w"}},
    {"P0036",
        {info, "Garbage collection on mnaifest removes key for filename ~s"}},
        
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
        {info, "Clerk prompting Penciller regarding manifest change"}},
    {"PC008",
        {info, "Merge from level ~w to merge into ~w files below"}},
    {"PC009",
        {info, "File ~s to simply switch levels to level ~w"}},
    {"PC010",
        {info, "Merge to be commenced for FileToMerge=~s with MSN=~w"}},
    {"PC011",
        {info, "Merge completed with MSN=~w to Level=~w and FileCounter=~w"}},
    {"PC012",
        {info, "File to be created as part of MSN=~w Filename=~s "
                    ++ "IsBasement=~w"}},
    {"PC013",
        {warn, "Merge resulted in empty file ~s"}},
    {"PC015",
        {info, "File created"}},
    {"PC016",
        {info, "Slow fetch from SFT ~w of ~w microseconds with result ~w"}},
    {"PC017",
        {info, "Notified clerk of manifest change"}},
    {"PC018",
        {info, "Saved manifest file"}},
    {"PC019",
        {debug, "After ~s level ~w is ~w"}},
    
    {"I0001",
        {info, "Unexpected failure to fetch value for Key=~w SQN=~w "
                ++ "with reason ~w"}},
    {"I0002",
        {info, "Journal snapshot ~w registered at SQN ~w"}},
    {"I0003",
        {info, "Journal snapshot ~w released"}},
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
    
    {"IC001",
        {info, "Closed for reason ~w so maybe leaving garbage"}},
    {"IC002",
        {info, "Clerk updating Inker as compaction complete of ~w files"}},
    {"IC003",
        {info, "No compaction run as highest score=~w"}},
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
    
    {"PM002",
        {info, "Completed dump of L0 cache to list of size ~w"}},
    
    {"SST01",
        {info, "SST timing for result ~w is sample ~w total ~w and max ~w"}},
    {"SST02",
        {error, "False result returned from SST with filename ~s as "
                    ++ "slot ~w has failed crc check"}},
    {"SST03",
        {info, "Opening SST file with filename ~s keys ~w slots ~w and"
                ++ " max sqn ~w"}},
    {"SST04",
        {info, "Exit called for reason ~w on filename ~s"}},
    {"SST05",
        {warn, "Rename rogue filename ~s to ~s"}},
    {"SST06",
        {info, "File ~s has been set for delete"}},
    {"SST07",
        {info, "Exit called and now clearing ~s"}},
    {"SST08",
        {info, "Completed creation of ~s at level ~w with max sqn ~w"}},
    {"SST09",
        {warn, "Read request exposes slot with bad CRC"}},
    {"SST10",
        {info, "Expansion sought to support pointer to pid ~w status ~w"}},
    
    {"CDB01",
        {info, "Opening file for writing with filename ~s"}},
    {"CDB02",
        {info, "Opening file for reading with filename ~s"}},
    {"CDB03",
        {info, "Re-opening file for reading with filename ~s"}},
    {"CDB04",
        {info, "Deletion confirmed for file ~s at ManifestSQN ~w"}},
    {"CDB05",
        {info, "Closing of filename ~s for Reason ~w"}},
    {"CDB06",
        {info, "File to be truncated at last position of ~w with end of "
                ++ "file at ~w"}},
    {"CDB07",
        {info, "Hashtree computed"}},
    {"CDB08",
        {info, "Renaming file from ~s to ~s for which existence is ~w"}},
    {"CDB09",
        {info, "Failure to read Key/Value at Position ~w in scan"}},
    {"CDB10",
        {info, "CRC check failed due to mismatch"}},
    {"CDB11",
        {info, "CRC check failed due to size"}},
    {"CDB12",
        {info, "HashTree written"}},
    {"CDB13",
        {info, "Write options of ~w"}},
    {"CDB14",
        {info, "Microsecond timings for hashtree build of "
                ++ "to_list ~w sort ~w build ~w"}},
    {"CDB15",
        {info, "Cycle count of ~w in hashtable search higher than expected"
                ++ " in search for hash ~w with result ~w"}},
    {"CDB16",
        {info, "CDB scan from start ~w in file with end ~w and last_key ~w"}},
    {"CDB17",
        {info, "After ~w PUTs total write time is ~w total sync time is ~w "
                ++ "and max write time is ~w and max sync time is ~w"}},
    {"CDB18",
        {info, "Handled return and write of hashtable"}}
        ])).


log(LogReference, Subs) ->
    {ok, {LogLevel, LogText}} = dict:find(LogReference, ?LOGBASE),
    case lists:member(LogLevel, ?LOG_LEVEL) of
        true ->
            io:format(LogReference ++ " ~w " ++ LogText ++ "~n",
                        [self()|Subs]);
        false ->
            ok
    end.


log_timer(LogReference, Subs, StartTime) ->
    {ok, {LogLevel, LogText}} = dict:find(LogReference, ?LOGBASE),
    case lists:member(LogLevel, ?LOG_LEVEL) of
        true ->
            MicroS = timer:now_diff(os:timestamp(), StartTime),
            {Unit, Time} = case MicroS of
                                MicroS when MicroS < 1000 ->
                                    {"microsec", MicroS};
                                MicroS ->
                                    {"ms", MicroS div 1000}
                            end,
            io:format(LogReference ++ " ~w " ++ LogText
                            ++ " with time taken ~w " ++ Unit ++ "~n",
                        [self()|Subs] ++ [Time]);
        false ->
            ok
    end.

%% Make a log of put timings split out by actor - one log for every
%% PUT_LOGPOINT puts

put_timing(_Actor, undefined, T0, T1) ->
    {1, {T0, T1}, {T0, T1}};
put_timing(Actor, {?PUT_LOGPOINT, {Total0, Total1}, {Max0, Max1}}, T0, T1) ->
    RN = random:uniform(?HEAD_LOGPOINT),
    case RN > ?HEAD_LOGPOINT div 2 of
        true ->
            % log at the timing point less than half the time
            LogRef =
                case Actor of
                    bookie -> "B0012";
                    inker -> "I0019";
                    journal -> "CDB17"
                end,
            log(LogRef, [?PUT_LOGPOINT, Total0, Total1, Max0, Max1]),
            put_timing(Actor, undefined, T0, T1);
        false ->
            % Log some other random time
            put_timing(Actor, {RN, {Total0, Total1}, {Max0, Max1}}, T0, T1)
    end;
put_timing(_Actor, {N, {Total0, Total1}, {Max0, Max1}}, T0, T1) ->
    {N + 1, {Total0 + T0, Total1 + T1}, {max(Max0, T0), max(Max1, T1)}}.

%% Make a log of penciller head timings split out by level and result - one
%% log for every HEAD_LOGPOINT puts
%% Returns a tuple of {Count, TimingDict} to be stored on the process state
head_timing(undefined, SW, Level, R) ->
    T0 = timer:now_diff(os:timestamp(), SW),
    head_timing_int(undefined, T0, Level, R);
head_timing({N, HeadTimingD}, SW, Level, R) ->
    case N band (?SAMPLE_RATE - 1) of
        0 ->
            T0 = timer:now_diff(os:timestamp(), SW),
            head_timing_int({N, HeadTimingD}, T0, Level, R);
        _ ->
            % Not to be sampled this time
            {N + 1, HeadTimingD}
    end.

head_timing_int(undefined, T0, Level, R) -> 
    Key = head_key(R, Level),
    NewDFun = fun(K, Acc) ->
                case K of
                    Key ->
                        dict:store(K, [1, T0, T0], Acc);
                    _ ->
                        dict:store(K, [0, 0, 0], Acc)
                end end,
    {1, lists:foldl(NewDFun, dict:new(), head_keylist())};
head_timing_int({?HEAD_LOGPOINT, HeadTimingD}, T0, Level, R) ->
    RN = random:uniform(?HEAD_LOGPOINT),
    case RN > ?HEAD_LOGPOINT div 2 of
        true ->
            % log at the timing point less than half the time
            LogFun  = fun(K) -> log("P0032", [K|dict:fetch(K, HeadTimingD)]) end,
            lists:foreach(LogFun, head_keylist()),
            head_timing_int(undefined, T0, Level, R);
        false ->
            % Log some other time - reset to RN not 0 to stagger logs out over
            % time between the vnodes
            head_timing_int({RN, HeadTimingD}, T0, Level, R)
    end;
head_timing_int({N, HeadTimingD}, T0, Level, R) ->
    Key = head_key(R, Level),
    [Count0, Total0, Max0] = dict:fetch(Key, HeadTimingD),
    {N + 1,
        dict:store(Key, [Count0 + 1, Total0 + T0, max(Max0, T0)],
        HeadTimingD)}.
                                    
head_key(not_present, _Level) ->
    not_present;
head_key(found, 0) ->
    found_0;
head_key(found, 1) ->
    found_1;
head_key(found, 2) ->
    found_2;
head_key(found, Level) when Level > 2 ->
    found_lower.

head_keylist() ->
    [not_present, found_lower, found_0, found_1, found_2].


sst_timing(undefined, SW, TimerType) ->
    T0 = timer:now_diff(os:timestamp(), SW),
    gen_timing_int(undefined,
                    T0,
                    TimerType,
                    fun sst_keylist/0,
                    ?SST_LOGPOINT,
                    "SST01");
sst_timing({N, SSTTimerD}, SW, TimerType) ->
    case N band (?SAMPLE_RATE - 1) of
        0 ->
            T0 = timer:now_diff(os:timestamp(), SW),
            gen_timing_int({N, SSTTimerD},
                            T0,
                            TimerType,
                            fun sst_keylist/0,
                            ?SST_LOGPOINT,
                            "SST01");
        _ ->
            % Not to be sampled this time
            {N + 1, SSTTimerD}
    end.

sst_keylist() ->
    [slot_bloom, slot_fetch].


get_timing(undefined, SW, TimerType) ->
    T0 = timer:now_diff(os:timestamp(), SW),
    gen_timing_int(undefined,
                    T0,
                    TimerType,
                    fun get_keylist/0,
                    ?GET_LOGPOINT,
                    "B0014");
get_timing({N, GetTimerD}, SW, TimerType) ->
    case N band (?SAMPLE_RATE - 1) of
        0 ->
            T0 = timer:now_diff(os:timestamp(), SW),
            gen_timing_int({N, GetTimerD},
                            T0,
                            TimerType,
                            fun get_keylist/0,
                            ?GET_LOGPOINT,
                            "B0014");
        _ ->
            % Not to be sampled this time
            {N + 1, GetTimerD}
    end.

get_keylist() ->
    [head_not_present, head_found, fetch].


gen_timing_int(undefined, T0, TimerType, KeyListFun, _LogPoint, _LogRef) ->
    NewDFun = fun(K, Acc) ->
                case K of
                    TimerType ->
                        dict:store(K, [1, T0, T0], Acc);
                    _ ->
                        dict:store(K, [0, 0, 0], Acc)
                end end,
    {1, lists:foldl(NewDFun, dict:new(), KeyListFun())};
gen_timing_int({LogPoint, TimerD}, T0, TimerType, KeyListFun, LogPoint,
                                                                    LogRef) ->
    RN = random:uniform(LogPoint),
    case RN > LogPoint div 2 of
        true ->
            % log at the timing point less than half the time
            LogFun  = fun(K) -> log(LogRef, [K|dict:fetch(K, TimerD)]) end,
            lists:foreach(LogFun, KeyListFun()),
            gen_timing_int(undefined, T0, TimerType,
                            KeyListFun, LogPoint, LogRef);
        false ->
            % Log some other time - reset to RN not 0 to stagger logs out over
            % time between the vnodes
            gen_timing_int({RN, TimerD}, T0, TimerType,
                            KeyListFun, LogPoint, LogRef)
    end;
gen_timing_int({N, TimerD}, T0, TimerType, _KeyListFun, _LogPoint, _LogRef) ->
    [Count0, Total0, Max0] = dict:fetch(TimerType, TimerD),
    {N + 1, 
        dict:store(TimerType, 
                    [Count0 + 1, Total0 + T0, max(Max0, T0)], 
                    TimerD)}.




%%%============================================================================
%%% Test
%%%============================================================================



-ifdef(TEST).

log_test() ->
    log("D0001", []),
    log_timer("D0001", [], os:timestamp()).

head_timing_test() ->
    SW = os:timestamp(),
    HeadTimer0 = lists:foldl(fun(_X, Acc) -> head_timing(Acc, SW, 2, found) end,
                                undefined,
                                lists:seq(0, 47)),
    HeadTimer1 = head_timing(HeadTimer0, SW, 3, found),
    {N, D} = HeadTimer1,
    ?assertMatch(49, N),
    ?assertMatch(3, lists:nth(1, dict:fetch(found_2, D))),
    ?assertMatch(1, lists:nth(1, dict:fetch(found_lower, D))).

-endif.