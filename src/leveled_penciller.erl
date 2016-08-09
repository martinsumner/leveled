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
%% the next Manifest sequence number as the filename:
%% - nonzero_<ManifestSQN#>.pnd
%% 
%% The Penciller on accepting the change should rename the manifest file to -
%% - nonzero_<ManifestSQN#>.crr
%%
%% On startup, the Penciller should look for the nonzero_*.crr file with the
%% highest such manifest sequence number.
%%
%% The pace at which the store can accept updates will be dependent on the
%% speed at which the Penciller's Clerk can merge files at lower levels plus
%% the time it takes to merge from Level 0.  As if a clerk has commenced
%% compaction work at a lower level and then immediately a L0 SFT file is
%% written the Penciller will need to wait for this compaction work to
%% complete and the L0 file to be compacted before the ETS table can be
%% allowed to again reach capacity
%%
%% The writing of L0 files do not require the involvement of the clerk.
%% The L0 files are prompted directly by the penciller when the in-memory ets
%% table has reached capacity.  When there is a next push into memory the
%% penciller calls to check that the file is now active (which may pause if the
%% write is ongoing the acceptence of the push), and if so it can clear the ets
%% table and build a new table starting with the remainder, and the keys from
%% the latest push.
%%


-module(leveled_penciller).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        pcl_new/0,
        pcl_start/1,
        pcl_pushmem/2,
        pcl_fetch/2,
        pcl_workforclerk/1,
        pcl_requestmanifestchange/2,
        commit_manifest_change/3]).

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
-define(MEMTABLE, mem).
-define(MAX_TABLESIZE, 32000).
-define(L0PEND_RESET, {false, [], none}).

-record(state, {manifest = [] :: list(),
                ongoing_work = [] :: list(),
				manifest_sqn = 0 :: integer(),
                levelzero_sqn =0 :: integer(),
                registered_iterators = [] :: list(),
                unreferenced_files = [] :: list(),
                root_path = "../test/" :: string(),
                table_size = 0 :: integer(),
                clerk :: pid(),
                levelzero_pending = {false, [], none} :: tuple(),
                memtable}).


%%%============================================================================
%%% API
%%%============================================================================

pcl_new() ->
    gen_server:start(?MODULE, [], []).

pcl_start(_RootDir) ->
    %% TODO
    %% Need to call startup to rebuild from disk
    ok.

pcl_pushmem(Pid, DumpList) ->
    %% Bookie to dump memory onto penciller
    gen_server:call(Pid, {push_mem, DumpList}, infinity).
    
pcl_fetch(Pid, Key) ->
    gen_server:call(Pid, {fetch, Key}, infinity).

pcl_workforclerk(Pid) ->
    gen_server:call(Pid, work_for_clerk, infinity).

pcl_requestmanifestchange(Pid, WorkItem) ->
    gen_server:call(Pid, {manifest_change, WorkItem}, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    TID = ets:new(?MEMTABLE, [ordered_set, private]),
    {ok, #state{memtable=TID}}.

handle_call({push_mem, DumpList}, _From, State) ->
    {TableSize, Manifest, L0Pend} = case State#state.levelzero_pending of
        {true, Remainder, {StartKey, EndKey, Pid}} ->
            %% Need to handle not error scenarios?
            %% N.B. Sync call - so will be ready
            ok = leveled_sft:sft_checkready(Pid),
            %% Reset ETS, but re-insert any remainder
            true = ets:delete_all_objects(State#state.memtable),
            true = ets:insert(State#state.memtable, Remainder),
            {length(Remainder),
                lists:keystore(0,
                                1,
                                State#state.manifest,
                                {0, [{StartKey, EndKey, Pid}]}),
                ?L0PEND_RESET};
        {false, _, _} ->
            {State#state.table_size,
                State#state.manifest,
                State#state.levelzero_pending};
        Unexpected ->
            io:format("Unexpected value of ~w~n", [Unexpected]),
            error
    end,    
    case do_push_to_mem(DumpList, TableSize, State#state.memtable) of
        {twist, ApproxTableSize} ->
            {reply, ok, State#state{table_size=ApproxTableSize,
                                        manifest=Manifest,
                                        levelzero_pending=L0Pend}};
        {roll, ApproxTableSize} ->
            case {get_item(0, Manifest, []), L0Pend} of
                {[], ?L0PEND_RESET} ->
                    L0SN = State#state.levelzero_sqn + 1,
                    FileName = State#state.root_path
                                ++ ?FILES_FP ++ "/"
                                ++ integer_to_list(L0SN),
                    SFT = leveled_sft:sft_new(FileName,
                                                ets:tab2list(State#state.memtable),
                                                [],
                                                0,
                                                #sft_options{wait=false}),
                    {ok, L0Pid, Reply} = SFT,
                    {{KL1Rem, []}, L0StartKey, L0EndKey} = Reply,
                    {reply, ok, State#state{levelzero_pending={true,
                                                                KL1Rem,
                                                                {L0StartKey,
                                                                    L0EndKey,
                                                                    L0Pid}},
                                                table_size=ApproxTableSize,
                                                levelzero_sqn=L0SN}};
                _ ->
                    io:format("Memory has exceeded limit but L0 file is still
                                awaiting compaction ~n"),
                    {reply, pause, State#state{table_size=ApproxTableSize,
                                                manifest=Manifest,
                                                levelzero_pending=L0Pend}}
            end
    end;
handle_call({fetch, Key}, _From, State) ->
    {reply, fetch(Key, State#state.manifest, State#state.memtable), State};
handle_call(work_for_clerk, From, State) ->
    {UpdState, Work} = return_work(State, From),
    {reply, Work, UpdState}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================


fetch(Key, Manifest, TID) ->
    case ets:lookup(TID, Key) of
        [Object] ->
            Object;
        [] ->
            fetch(Key, Manifest, 0, fun leveled_sft:sft_get/2)
    end.

fetch(_Key, _Manifest, ?MAX_LEVELS + 1, _FetchFun) ->
    not_present;
fetch(Key, Manifest, Level, FetchFun) ->
    LevelManifest = get_item(Level, Manifest, []),
    case lists:foldl(fun(File, Acc) ->
                        case Acc of
                            not_present when
                                    Key >= File#manifest_entry.start_key,
                                    File#manifest_entry.end_key >= Key ->
                                File#manifest_entry.owner;
                            PidFound ->
                                PidFound
                        end end,
                        not_present,
                        LevelManifest) of
        not_present ->
            fetch(Key, Manifest, Level + 1, FetchFun);
        FileToCheck ->
            case FetchFun(FileToCheck, Key) of
                not_present ->
                    fetch(Key, Manifest, Level + 1, FetchFun);
                ObjectFound ->
                    ObjectFound
            end
    end.

do_push_to_mem(DumpList, TableSize, MemTable) ->
    ets:insert(MemTable, DumpList),
    case TableSize + length(DumpList) of
        ApproxTableSize when ApproxTableSize > ?MAX_TABLESIZE ->
            case ets:info(MemTable, size) of
                ActTableSize when ActTableSize > ?MAX_TABLESIZE ->
                    {roll, ActTableSize};
                ActTableSize ->
                    io:format("Table size is actually ~w~n", [ActTableSize]),
                    {twist, ActTableSize}
            end;
        ApproxTableSize ->
            io:format("Table size is approximately ~w~n", [ApproxTableSize]),
            {twist, ApproxTableSize}
    end.



%% Work out what the current work queue should be
%%
%% The work queue should have a lower level work at the front, and no work
%% should be added to the queue if a compaction worker has already been asked
%% to look at work at that level

return_work(State, From) ->
    WorkQueue = assess_workqueue([],
                                    0,
                                    State#state.manifest),
    case length(WorkQueue) of
        L when L > 0 ->
            [{SrcLevel, Manifest}|OtherWork] = WorkQueue,
            io:format("Work at Level ~w to be scheduled for ~w
                        with ~w queue items outstanding~n",
                        [SrcLevel, From, length(OtherWork)]),
            case State#state.ongoing_work of     
                [] ->
                    %% No work currently outstanding
                    %% Can allocate work
                    NextSQN = State#state.manifest_sqn + 1,
                    FP = filepath(State#state.root_path,
                                    NextSQN,
                                    new_merge_files),
                    ManFile = filepath(State#state.root_path,
                                    NextSQN,
                                    pending_manifest),
                    WI = #penciller_work{next_sqn=NextSQN,
                                            clerk=From,
                                            src_level=SrcLevel,
                                            manifest=Manifest,
                                            start_time = os:timestamp(),
                                            ledger_filepath = FP,
                                            manifest_file = ManFile},
                    {State#state{ongoing_work=[WI]}, WI};
                [OutstandingWork] ->
                    %% Still awaiting a response 
                    io:format("Ongoing work requested by ~w but work
                                outstanding from Level ~w and Clerk ~w 
                                at sequence number ~w~n",
                                [From,
                                    OutstandingWork#penciller_work.src_level,
                                    OutstandingWork#penciller_work.clerk,
                                    OutstandingWork#penciller_work.next_sqn]),
                    {State, none}
            end;
        _ ->
            {State, none}
    end.




assess_workqueue(WorkQ, ?MAX_LEVELS - 1, _Manifest) ->
    WorkQ;
assess_workqueue(WorkQ, LevelToAssess, Manifest)->
    MaxFiles = get_item(LevelToAssess, ?LEVEL_SCALEFACTOR, 0),
    FileCount = length(get_item(LevelToAssess, Manifest, [])),
    NewWQ = maybe_append_work(WorkQ, LevelToAssess, Manifest, MaxFiles,
                                FileCount),
    assess_workqueue(NewWQ, LevelToAssess + 1, Manifest).


maybe_append_work(WorkQ, Level, Manifest,
                    MaxFiles, FileCount)
                        when FileCount > MaxFiles ->
    io:format("Outstanding compaction work items of ~w at level ~w~n",
                [FileCount - MaxFiles, Level]),
    lists:append(WorkQ, [{Level, Manifest}]);
maybe_append_work(WorkQ, Level, _Manifest,
                    _MaxFiles, FileCount) ->
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
%% The clerk should have completed the work, and created a new manifest
%% and persisted the new view of the manifest
%%
%% To complete the change of manifest:
%% - the state of the manifest file needs to be changed from pending to current
%% - the list of unreferenced files needs to be updated on State
%% - the current manifest needs to be update don State
%% - the list of ongoing work needs to be cleared of this item


commit_manifest_change(ReturnedWorkItem, From, State) ->
    NewMSN = State#state.manifest_sqn +  1,
    [SentWorkItem] = State#state.ongoing_work,
    RootPath = State#state.root_path,
    UnreferencedFiles = State#state.unreferenced_files,
    
    case {SentWorkItem#penciller_work.next_sqn,
            SentWorkItem#penciller_work.clerk} of
        {NewMSN, From} ->
            MTime = timer:diff_now(os:timestamp(),
                                    SentWorkItem#penciller_work.start_time),
            io:format("Merge to sqn ~w completed in ~w microseconds
                        at Level ~w~n",
                        [SentWorkItem#penciller_work.next_sqn,
                            MTime,
                            SentWorkItem#penciller_work.src_level]),
            ok = rename_manifest_files(RootPath, NewMSN),
            FilesToDelete = ReturnedWorkItem#penciller_work.unreferenced_files,
            UnreferencedFilesUpd = update_deletions(FilesToDelete,
                                                        NewMSN,
                                                        UnreferencedFiles),
            io:format("Merge has been commmitted at sequence number ~w~n",
                        [NewMSN]),
            NewManifest = ReturnedWorkItem#penciller_work.new_manifest,
            {ok, State#state{ongoing_work=null,
                                manifest_sqn=NewMSN,
                                manifest=NewManifest,
                                unreferenced_files=UnreferencedFilesUpd}};
        {MaybeWrongMSN, MaybeWrongClerk} ->
            io:format("Merge commit from ~w at sqn ~w not matched to expected
                        clerk ~w or sqn ~w~n",
                        [From, NewMSN, MaybeWrongClerk, MaybeWrongMSN]),
            {error, State}
    end.


rename_manifest_files(RootPath, NewMSN) ->
    file:rename(filepath(RootPath, NewMSN, pending_manifest),
                    filepath(RootPath, NewMSN, current_manifest)).

filepath(RootPath, NewMSN, pending_manifest) ->
    RootPath ++ "/" ++ ?MANIFEST_FP ++ "/" ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?PENDING_FILEX;
filepath(RootPath, NewMSN, current_manifest) ->
    RootPath ++ "/" ++ ?MANIFEST_FP ++ "/" ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?CURRENT_FILEX;
filepath(RootPath, NewMSN, new_merge_files) ->
    RootPath ++ "/" ++ ?FILES_FP ++ "/" ++ integer_to_list(NewMSN).
 
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
    WorkQ1 = assess_workqueue([], 0, Manifest),
    ?assertMatch(WorkQ1, [{0, Manifest}]),
    L1Alt = lists:append(L1,
                        [{{o, "B5", "K0001"}, {o, "B5", "K9999"}, dummy_pid},
                        {{o, "B6", "K0001"}, {o, "B6", "K9999"}, dummy_pid},
                        {{o, "B7", "K0001"}, {o, "B7", "K9999"}, dummy_pid},
                        {{o, "B8", "K0001"}, {o, "B8", "K9999"}, dummy_pid},
                        {{o, "B9", "K0001"}, {o, "B9", "K9999"}, dummy_pid},
                        {{o, "BA", "K0001"}, {o, "BA", "K9999"}, dummy_pid},
                        {{o, "BB", "K0001"}, {o, "BB", "K9999"}, dummy_pid}]),
    Manifest3 = [{0, []}, {1, L1Alt}],
    WorkQ3 = assess_workqueue([], 0, Manifest3),
    ?assertMatch(WorkQ3, [{1, Manifest3}]).
