%% -------- PENCILLER ---------
%%
%% The penciller is repsonsible for writing and re-writing the ledger - a
%% persisted, ordered view of non-recent Keys and Metadata which have been
%% added to the store.
%% - The penciller maintains a manifest of all the files within the current
%% Ledger.
%% - The Penciller queues re-write (compaction) work up to be managed by Clerks
%% - The Penciller mainatins a register of iterators who have requested
%% snapshots of the Ledger
%% - The accepts new dumps (in the form of immutable ets tables) from the
%% Bookie, and calls the Bookie once the process of pencilling this data in
%% the Ledger is complete - and the Bookie is free to forget about the data
%%
%% -------- LEDGER ---------
%%
%% The Ledger is divided into many levels
%% L0: ETS tables are received from the Bookie and merged into a single ETS
%% table, until that table is the size of a SFT file, and it is then persisted
%% as a SFT file at this level.  Once the persistence is completed, the ETS
%% table can be dropped.  There can be only one SFT file at Level 0, so
%% the work to merge that file to the lower level must be the highest priority,
%% as otherwise the database will stall.
%% L1 TO L7: May contain multiple non-overlapping PIDs managing sft files.
%% Compaction work should be sheduled if the number of files exceeds the target
%% size of the level, where the target size is 8 ^ n.
%%
%% The most recent revision of a Key can be found by checking each level until
%% the key is found.  To check a level the correct file must be sought from the
%% manifest for that level, and then a call is made to that file.  If the Key
%% is not present then every level should be checked.
%%
%% If a compaction change takes the size of a level beyond the target size,
%% then compaction work for that level + 1 should be added to the compaction
%% work queue.
%% Compaction work is fetched by the Pencllier's Clerk because:
%% - it has timed out due to a period of inactivity
%% - it has been triggered by the a cast to indicate the arrival of high
%% priority compaction work
%% The Penciller's Clerk (which performs compaction worker) will always call
%% the Penciller to find out the highest priority work currently in the queue
%% whenever it has either completed work, or a timeout has occurred since it
%% was informed there was no work to do.
%%
%% When the clerk picks work off the queue it will take the current manifest
%% for the level and level - 1.  The clerk will choose which file to compact
%% from level - 1, and once the compaction is complete will call to the
%% Penciller with the new version of the manifest to be written.
%%
%% Once the new version of the manifest had been persisted, the state of any
%% deleted files will be changed to pending deletion.  In pending deletion they
%% will call the Penciller on a timeout to confirm that they are no longer in
%% use (by any iterators).
%%
%% ---------- PUSH ----------
%%
%% The Penciller must support the PUSH of an ETS table from the Bookie.  The
%% call to PUSH should be immediately acknowledged, and then work should be
%% completed to merge the ETS table into the L0 ETS table.
%%
%% The Penciller MUST NOT accept a new PUSH if the Clerk has commenced the
%% conversion of the current ETS table into a SFT file, but not completed this
%% change.  This should prompt a stall.
%%
%% ---------- FETCH ----------
%%
%% On request to fetch a key the Penciller should look first in the L0 ETS 
%% table, and then look in the SFT files Level by Level, consulting the
%% Manifest to determine which file should be checked at each level.
%%
%% ---------- SNAPSHOT ----------
%%
%% Iterators may request a snapshot of the database.  To provide a snapshot
%% the Penciller must snapshot the ETS table, and then send this with a copy
%% of the manifest.
%%
%% Iterators requesting snapshots are registered by the Penciller, so that SFT
%% files valid at the point of the snapshot until either the iterator is
%% completed or has timed out.
%%
%% ---------- ON STARTUP ----------
%%
%% On Startup the Bookie with ask the Penciller to initiate the Ledger first.
%% To initiate the Ledger the must consult the manifest, and then start a SFT
%% management process for each file in the manifest.
%%
%% The penciller should then try and read any persisted ETS table in the
%% on_shutdown folder. The Penciller must then discover the highest sequence
%% number in the ledger, and respond to the Bookie with that sequence number.
%%
%% The Bookie will ask the Inker for any Keys seen beyond that sequence number
%% before the startup of the overall store can be completed.
%%
%% ---------- ON SHUTDOWN ----------
%%
%% On a controlled shutdown the Penciller should attempt to write any in-memory
%% ETS table to disk into the special ..on_shutdown folder
%%
%% ---------- FOLDER STRUCTURE ----------
%%
%% The following folders are used by the Penciller
%% $ROOT/ledger_manifest/ - used for keeping manifest files
%% $ROOT/ledger_onshutdown/ - containing the persisted view of the ETS table
%% written on controlled shutdown
%% $ROOT/ledger_files/ - containing individual SFT files
%%
%% In larger stores there could be a large number of files in the ledger_file
%% folder - perhaps o(1000).  It is assumed that modern file systems should
%% handle this efficiently.
%%
%% ---------- COMPACTION & MANIFEST UPDATES ----------
%%
%% The Penciller can have one and only one Clerk for performing compaction
%% work.  When the Clerk has requested and taken work, it should perform the
%5 compaction work starting the new SFT process to manage the new Ledger state
%% and then write a new manifest file that represents that state with using
%% The MergeID as the filename <MergeID>.pnd.
%%
%% Prior to completing the work the previous manifest file should be renamed
%% to the filename <OldMergeID>.bak, and any .bak files other than the
%% the most recent n files should be deleted.
%% 
%% The Penciller on accepting the change should rename the manifest file to
%% '<MergeID>.crr'.
%%
%% On startup, the Penciller should look first for a *.crr file, and if
%% one is not present it should promot the most recently modified *.bak -
%% checking first that all files referenced in it are still present.
%%
%% The pace at which the store can accept updates will be dependent on the
%% speed at which the Penciller's Clerk can merge files at lower levels plus
%% the time it takes to merge from Level 0.  As if a clerk has commenced
%% compaction work at a lower level and then immediately a L0 SFT file is
%% written the Penciller will need to wait for this compaction work to
%% complete and the L0 file to be compacted before the ETS table can be
%% allowed to again reach capacity


-module(leveled_penciller).

%% -behaviour(gen_server).

-export([return_work/2, commit_manifest_change/5]).

-include_lib("eunit/include/eunit.hrl").

-define(LEVEL_SCALEFACTOR, [{0, 0}, {1, 8}, {2, 64}, {3, 512},
                            {4, 4096}, {5, 32768}, {6, 262144}, {7, infinity}]).
-define(MAX_LEVELS, 8).
-define(MAX_WORK_WAIT, 300).
-define(MANIFEST_FP, "ledger_manifest").
-define(FILES_FP, "ledger_files").
-define(SHUTDOWN_FP, "ledger_onshutdown").
-define(CURRENT_FILEX, "crr").
-define(PENDING_FILEX, "pnd").
-define(BACKUP_FILEX, "bak").
-define(ARCHIVE_FILEX, "arc").

-record(state, {manifest :: list(),
                ongoing_work :: list(),
				manifest_sqn :: integer(),
                registered_iterators :: list(),
                unreferenced_files :: list(),
                root_path :: string(),
                mem :: ets:tid()}).



%% Work out what the current work queue should be
%%
%% The work queue should have a lower level work at the front, and no work
%% should be added to the queue if a compaction worker has already been asked
%% to look at work at that level

return_work(State, From) ->
    case State#state.ongoing_work of
        [] ->        
            WorkQueue = assess_workqueue([],
                                            0,
                                            State#state.manifest,
                                            []),
            case length(WorkQueue) of
                L when L > 0 ->
                    [{SrcLevel, Manifest}|OtherWork] = WorkQueue,
                    io:format("Work at Level ~w to be scheduled for ~w
                                with ~w queue items outstanding~n",
                                [SrcLevel, From, length(OtherWork)]),
                    {State#state{ongoing_work={SrcLevel, From, os:timestamp()}},
                        {SrcLevel, Manifest}};
                _ ->
                    {State, none}
            end;
        [{SrcLevel, OtherFrom, _TS}|T] ->
            io:format("Ongoing work requested by ~w but work
                        outstanding from Level ~w and Clerk ~w with
                        ~w other items outstanding~n",
                        [From, SrcLevel, OtherFrom, length(T)]),
            {State, none}
    end.

assess_workqueue(WorkQ, ?MAX_LEVELS - 1, _Manifest, _OngoingWork) ->
    WorkQ;
assess_workqueue(WorkQ, LevelToAssess, Manifest, OngoingWork)->
    MaxFiles = get_item(LevelToAssess, ?LEVEL_SCALEFACTOR, 0),
    FileCount = length(get_item(LevelToAssess, Manifest, [])),
    NewWQ = maybe_append_work(WorkQ, LevelToAssess, Manifest, MaxFiles,
                                FileCount, OngoingWork),
    assess_workqueue(NewWQ, LevelToAssess + 1, Manifest, OngoingWork).


maybe_append_work(WorkQ, Level, Manifest,
                    MaxFiles, FileCount, OngoingWork)
                        when FileCount > MaxFiles ->
    io:format("Outstanding compaction work items of ~w at level ~w~n",
                [FileCount - MaxFiles, Level]),
    case lists:keyfind(Level, 1, OngoingWork) of
        {Level, Pid, TS} ->
            io:format("Work will not be added to queue due to
                        outstanding work with ~w assigned at ~w~n", [Pid, TS]),
            WorkQ;
        false ->
            lists:append(WorkQ, [{Level, Manifest}])
    end;
maybe_append_work(WorkQ, Level, _Manifest,
                    _MaxFiles, FileCount, _OngoingWork) ->
    io:format("No compaction work due to file count ~w at level ~w~n",
                [FileCount, Level]),
    WorkQ.


get_item(Index, List, Default) ->
    case lists:keysearch(Index, 1, List) of
        {value, {Index, Value}} ->
            Value;
        false ->
            Default
    end.


%% Request a manifest change
%% Should be passed the
%% - {SrcLevel, NewManifest, ClearedFiles, MergeID, From, State}
%% To complete a manifest change need to:
%% - Update the Manifest Sequence Number (msn)
%% - Confirm this Pid has a current element of manifest work outstanding at
%% that level
%% - Rename the manifest file created under the MergeID (<mergeID>.manifest)
%% to the filename current.manifest
%% - Update the state of the LevelFileRef lists
%% - Add the ClearedFiles to the list of files to be cleared (as a tuple with
%% the new msn)


commit_manifest_change(NewManifest, ClearedFiles, MergeID, From, State) ->
    NewMSN = State#state.manifest_sqn +  1,
    OngoingWork = State#state.ongoing_work,
    RootPath = State#state.root_path,
    UnreferencedFiles = State#state.unreferenced_files,
    case OngoingWork of
        {SrcLevel, From, TS} ->
            io:format("Merge ~s completed in ~w microseconds at Level ~w~n",
                [MergeID, timer:diff_now(os:timestamp(), TS), SrcLevel]),
            ok = rename_manifest_files(RootPath, MergeID),
            UnreferencedFilesUpd = update_deletions(ClearedFiles,
                                                        NewMSN,
                                                        UnreferencedFiles),
            io:format("Merge ~s has been commmitted at sequence number ~w~n",
                        [MergeID, NewMSN]),
            {ok, State#state{ongoing_work=null,
                                manifest_sqn=NewMSN,
                                manifest=NewManifest,
                                unreferenced_files=UnreferencedFilesUpd}};
        _ ->
            io:format("Merge commit ~s not matched to known work~n",
                        [MergeID]),
            {error, State}
    end.    
    


rename_manifest_files(RootPath, MergeID) ->
    ManifestFP = RootPath ++ "/" ++ ?MANIFEST_FP ++ "/",
    ok = file:rename(ManifestFP ++ MergeID
                            ++ "." ++ ?PENDING_FILEX,
                        ManifestFP ++ MergeID 
                            ++ "." ++ ?CURRENT_FILEX),
    ok.

update_deletions([], _NewMSN, UnreferencedFiles) ->
    UnreferencedFiles;
update_deletions([ClearedFile|Tail], MSN, UnreferencedFiles) ->
    update_deletions(Tail,
                        MSN,
                        lists:append(UnreferencedFiles, [{ClearedFile, MSN}])).

%%%============================================================================
%%% Test
%%%============================================================================


compaction_work_assessment_test() ->
    L0 = [{{o, "B1", "K1"}, {o, "B3", "K3"}, dummy_pid}],
    L1 = [{{o, "B1", "K1"}, {o, "B2", "K2"}, dummy_pid},
            {{o, "B2", "K3"}, {o, "B4", "K4"}, dummy_pid}],
    Manifest = [{0, L0}, {1, L1}],
    OngoingWork1 = [],
    WorkQ1 = assess_workqueue([], 0, Manifest, OngoingWork1),
    ?assertMatch(WorkQ1, [{0, Manifest}]),
    OngoingWork2 = [{0, dummy_pid, os:timestamp()}],
    WorkQ2 = assess_workqueue([], 0, Manifest, OngoingWork2),
    ?assertMatch(WorkQ2, []),
    L1Alt = lists:append(L1,
                        [{{o, "B5", "K0001"}, {o, "B5", "K9999"}, dummy_pid},
                        {{o, "B6", "K0001"}, {o, "B6", "K9999"}, dummy_pid},
                        {{o, "B7", "K0001"}, {o, "B7", "K9999"}, dummy_pid},
                        {{o, "B8", "K0001"}, {o, "B8", "K9999"}, dummy_pid},
                        {{o, "B9", "K0001"}, {o, "B9", "K9999"}, dummy_pid},
                        {{o, "BA", "K0001"}, {o, "BA", "K9999"}, dummy_pid},
                        {{o, "BB", "K0001"}, {o, "BB", "K9999"}, dummy_pid}]),
    Manifest3 = [{0, []}, {1, L1Alt}],
    WorkQ3 = assess_workqueue([], 0, Manifest3, OngoingWork1),
    ?assertMatch(WorkQ3, [{1, Manifest3}]),
    WorkQ4 = assess_workqueue([], 0, Manifest3, OngoingWork2),
    ?assertMatch(WorkQ4, [{1, Manifest3}]).
