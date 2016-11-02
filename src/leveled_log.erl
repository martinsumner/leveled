%% Module to abstract from choice of logger, and allow use of logReferences
%% for fast lookup

-module(leveled_log).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([log/2,
            log_timer/3]).         

-define(LOG_LEVEL, [info, warn, error, critical]).
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
    
    {"P0001",
        {info, "Ledger snapshot ~w registered"}},
    {"P0002",
        {info, "Handling of push completed with L0 cache size now ~w"}},
    {"P0003",
        {info, "Ledger snapshot ~w released"}},
    {"P0004",
        {info, "Remaining ledger snapshots are ~w"}},
    {"P0005",
        {info, "Delete confirmed as file is removed from " ++ "
                unreferenced files ~w"}},
    {"P0006",
        {info, "Orphaned reply after timeout on L0 file write ~s"}},
    {"P0007",
        {info, "Sent release message for cloned Penciller following close for "
                ++ "reason ~w"}},
    {"P0008",
        {info, "Penciller closing for reason ~w"}},
    {"P0009",
        {info, "Level 0 cache empty at close of Penciller"}},
    {"P0010",
        {info, "No level zero action on close of Penciller"}},
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
        {info, "Respone to push_mem of ~w ~s"}},
    {"P0019",
        {info, "Rolling level zero to filename ~s"}},
    {"P0020",
        {info, "Work at Level ~w to be scheduled for ~w with ~w "
                ++ "queue items outstanding"}},
    {"P0021",
        {info, "Allocation of work blocked as L0 pending"}},
    {"P0022",
        {info, "Manifest at Level ~w"}},
    {"P0023",
        {info, "Manifest entry of startkey ~s ~s ~s endkey ~s ~s ~s "
                ++ "filename=~s~n"}},
    {"P0024",
        {info, "Outstanding compaction work items of ~w at level ~w"}},
    {"P0025",
        {info, "Merge to sqn ~w from Level ~w completed"}},
    {"P0026",
        {info, "Merge has been commmitted at sequence number ~w"}},
    {"P0027",
        {info, "Rename of manifest from ~s ~w to ~s ~w"}},
    {"P0028",
        {info, "Adding cleared file ~s to deletion list"}},
    
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
        {info, "Work prompted but none needed ~w"}},
    {"PC007",
        {info, "Clerk prompting Penciller regarding manifest change"}},
    {"PC008",
        {info, "Merge from level ~w to merge into ~w files below"}},
    {"PC009",
        {info, "File ~s to simply switch levels to level ~w"}},
    {"PC010",
        {info, "Merge to be commenced for FileToMerge=~s with MSN=~w"}},
    {"PC011",
        {info, "Merge completed with MSN=~w Level=~w and FileCounter=~w"}},
    {"PC012",
        {info, "File to be created as part of MSN=~w Filename=~s"}},
    {"PC013",
        {warn, "Merge resulted in empty file ~s"}},
    {"PC014",
        {info, "Empty file ~s to be cleared"}},
    {"PC015",
        {info, "File created"}}
    
        ])).


log(LogReference, Subs) ->
    {ok, {LogLevel, LogText}} = dict:find(LogReference, ?LOGBASE),
    case lists:member(LogLevel, ?LOG_LEVEL) of
        true ->
            io:format(LogText ++ "~n", Subs);
        false ->
            ok
    end.

log_timer(LogReference, Subs, StartTime) ->
    {ok, {LogLevel, LogText}} = dict:find(LogReference, ?LOGBASE),
    case lists:member(LogLevel, ?LOG_LEVEL) of
        true ->
            MicroS = timer:now_diff(os:timestamp(), StartTime),
            {Unit, Time} = case MicroS of
                                MicroS when MicroS < 100 ->
                                    {"microsec", MicroS};
                                MicroS ->
                                    {"ms", MicroS div 1000}
                            end,
            io:format(LogText ++ " with time taken ~w " ++ Unit ++ "~n",
                        Subs ++ [Time]);
        false ->
            ok
    end.



%%%============================================================================
%%% Test
%%%============================================================================



-ifdef(TEST).

log_test() ->
    ?assertMatch(ok, log("D0001", [])),
    ?assertMatch(ok, log_timer("D0001", [], os:timestamp())).

-endif.