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
%% The penciller should then try and read any Level 0 file which has the
%% manifest sequence number one higher than the last store in the manifest.
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
        pcl_start/1,
        pcl_pushmem/2,
        pcl_fetch/2,
        pcl_workforclerk/1,
        pcl_requestmanifestchange/2,
        pcl_confirmdelete/2,
        pcl_prompt/1,
        pcl_close/1,
        pcl_getstartupsequencenumber/1]).

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
                ledger_sqn = 0 :: integer(),
                registered_iterators = [] :: list(),
                unreferenced_files = [] :: list(),
                root_path = "../test" :: string(),
                table_size = 0 :: integer(),
                clerk :: pid(),
                levelzero_pending = {false, [], none} :: tuple(),
                memtable,
                backlog = false :: boolean()}).


%%%============================================================================
%%% API
%%%============================================================================
 
pcl_start(RootDir) ->
    gen_server:start(?MODULE, [RootDir], []).

pcl_pushmem(Pid, DumpList) ->
    %% Bookie to dump memory onto penciller
    gen_server:call(Pid, {push_mem, DumpList}, infinity).
    
pcl_fetch(Pid, Key) ->
    gen_server:call(Pid, {fetch, Key}, infinity).

pcl_workforclerk(Pid) ->
    gen_server:call(Pid, work_for_clerk, infinity).

pcl_requestmanifestchange(Pid, WorkItem) ->
    gen_server:call(Pid, {manifest_change, WorkItem}, infinity).

pcl_confirmdelete(Pid, FileName) ->
    gen_server:call(Pid, {confirm_delete, FileName}).

pcl_prompt(Pid) ->
    gen_server:call(Pid, prompt_compaction).

pcl_getstartupsequencenumber(Pid) ->
    gen_server:call(Pid, get_startup_sqn).

pcl_close(Pid) ->
    gen_server:call(Pid, close).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([RootPath]) ->
    TID = ets:new(?MEMTABLE, [ordered_set, private]),
    {ok, Clerk} = leveled_clerk:clerk_new(self()),
    InitState = #state{memtable=TID, clerk=Clerk, root_path=RootPath},
    
    %% Open manifest
    ManifestPath = InitState#state.root_path ++ "/" ++ ?MANIFEST_FP ++ "/",
    {ok, Filenames} = file:list_dir(ManifestPath),
    CurrRegex = "nonzero_(?<MSN>[0-9]+)\\." ++ ?CURRENT_FILEX,
    ValidManSQNs = lists:foldl(fun(FN, Acc) ->
                                    case re:run(FN,
                                                CurrRegex,
                                                [{capture, ['MSN'], list}]) of
                                        nomatch ->
                                            Acc;
                                        {match, [Int]} when is_list(Int) ->
                                            Acc ++ [list_to_integer(Int)];
                                        _ ->
                                            Acc
                                    end end,
                                    [],
                                    Filenames),
    TopManSQN = lists:foldl(fun(X, MaxSQN) -> max(X, MaxSQN) end,
                            0,
                            ValidManSQNs),
    io:format("Store to be started based on " ++
                "manifest sequence number of ~w~n", [TopManSQN]),
    case TopManSQN of
        0 ->
            io:format("Seqence number of 0 indicates no valid manifest~n"),
            {ok, InitState};
        _ ->
            {ok, Bin} = file:read_file(filepath(InitState#state.root_path,
                                                    TopManSQN,
                                                    current_manifest)),
            Manifest = binary_to_term(Bin),
            {UpdManifest, MaxSQN} = open_all_filesinmanifest(Manifest),
            io:format("Maximum sequence number of ~w "
                        ++ "found in nonzero levels~n",
                        [MaxSQN]),
            
            %% TODO
            %% Find any L0 File left outstanding
            L0FN = filepath(RootPath,
                            TopManSQN + 1,
                            new_merge_files) ++ "_0_0.sft",
            case filelib:is_file(L0FN) of
                true ->
                    io:format("L0 file found ~s~n", [L0FN]),
                    {ok,
                        L0Pid,
                        {L0StartKey, L0EndKey}} = leveled_sft:sft_open(L0FN),
                    L0SQN = leveled_sft:sft_getmaxsequencenumber(L0Pid),
                    ManifestEntry = #manifest_entry{start_key=L0StartKey,
                                                        end_key=L0EndKey,
                                                        owner=L0Pid,
                                                        filename=L0FN},
                    UpdManifest2 = lists:keystore(0,
                                                    1,
                                                    UpdManifest,
                                                    {0, [ManifestEntry]}),
                    io:format("L0 file had maximum sequence number of ~w~n",
                                [L0SQN]),
                    {ok,
                        InitState#state{manifest=UpdManifest2,
                                            manifest_sqn=TopManSQN,
                                            ledger_sqn=max(MaxSQN, L0SQN)}};
                false ->
                    io:format("No L0 file found~n"),
                    {ok,
                        InitState#state{manifest=UpdManifest,
                                            manifest_sqn=TopManSQN,
                                            ledger_sqn=MaxSQN}}
            end
    end.
    

handle_call({push_mem, DumpList}, _From, State) ->
    StartWatch = os:timestamp(),
    Response = case assess_sqn(DumpList) of
        {MinSQN, MaxSQN} when MaxSQN > MinSQN,
                                MinSQN >= State#state.ledger_sqn ->
            case push_to_memory(DumpList, State) of
                {ok, UpdState} ->
                    {reply, ok, UpdState};
                {{pause, Reason, Details}, UpdState} ->
                    io:format("Excess work due to - " ++ Reason, Details),
                    {reply, pause, UpdState#state{backlog=true,
                                                    ledger_sqn=MaxSQN}}
            end;
        {MinSQN, MaxSQN} ->
            io:format("Mismatch of sequence number expectations with push "
                        ++ "having sequence numbers between ~w and ~w "
                        ++ "but current sequence number is ~w~n",
                        [MinSQN, MaxSQN, State#state.ledger_sqn]),
            {reply, refused, State}
    end,
    io:format("Push completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),StartWatch)]),
    Response;
handle_call({fetch, Key}, _From, State) ->
    {reply, fetch(Key, State#state.manifest, State#state.memtable), State};
handle_call(work_for_clerk, From, State) ->
    {UpdState, Work} = return_work(State, From),
    {reply, {Work, UpdState#state.backlog}, UpdState};
handle_call({confirm_delete, FileName}, _From, State) ->
    Reply = confirm_delete(FileName,
                            State#state.unreferenced_files,
                            State#state.registered_iterators),
    case Reply of
        true ->
            UF1 = lists:keydelete(FileName, 1, State#state.unreferenced_files),
            {reply, true, State#state{unreferenced_files=UF1}};
        _ ->
            {reply, Reply, State}
    end;
handle_call(prompt_compaction, _From, State) ->
    case push_to_memory([], State) of
        {ok, UpdState} ->
            {reply, ok, UpdState#state{backlog=false}};
        {{pause, Reason, Details}, UpdState} ->
            io:format("Excess work due to - " ++ Reason, Details),
            {reply, pause, UpdState#state{backlog=true}}
    end;
handle_call({manifest_change, WI}, _From, State) ->
    {ok, UpdState} = commit_manifest_change(WI, State),
    {reply, ok, UpdState};
handle_call(get_startup_sqn, _From, State) ->
    {reply, State#state.ledger_sqn, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.
            
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% When a Penciller shuts down it isn't safe to try an manage the safe
    %% finishing of any outstanding work.  The last commmitted manifest will
    %% be used.
    %%
    %% Level 0 files lie outside of the manifest, and so if there is no L0
    %% file present it is safe to write the current contents of memory.  If
    %% there is a L0 file present - then the memory can be dropped (it is
    %% recoverable from the ledger, and there should not be a lot to recover
    %% as presumably the ETS file has been recently flushed, hence the presence
    %% of a L0 file).
    %%
    %% The penciller should close each file in the unreferenced files, and
    %% then each file in the manifest, and cast a close on the clerk.
    %% The cast may not succeed as the clerk could be synchronously calling
    %% the penciller looking for a manifest commit
    %%
    leveled_clerk:clerk_stop(State#state.clerk),
    Dump = ets:tab2list(State#state.memtable),
    case {State#state.levelzero_pending,
            get_item(0, State#state.manifest, []), length(Dump)} of
        {{false, _, _}, [], L} when L > 0 ->
            MSN = State#state.manifest_sqn + 1,
            FileName = State#state.root_path
                        ++ "/" ++ ?FILES_FP ++ "/"
                        ++ integer_to_list(MSN) ++ "_0_0",
            {ok,
                L0Pid,
                {{KR1, _}, _SK, _HK}} = leveled_sft:sft_new(FileName ++ ".pnd",
                                                                Dump,
                                                                [],
                                                                0),
            io:format("Dump of memory on close to filename ~s with"
                        ++ " remainder ~w~n", [FileName, length(KR1)]),
            leveled_sft:sft_close(L0Pid),
            file:rename(FileName ++ ".pnd", FileName ++ ".sft");
        {{false, _, _}, [], L} when L == 0 ->
            io:format("No keys to dump from memory when closing~n");
        _ ->
            io:format("No opportunity to persist memory before closing "
                        ++ "with ~w keys discarded~n", [length(Dump)])
    end,
    ok = close_files(0, State#state.manifest),
    lists:foreach(fun({_FN, Pid, _SN}) -> leveled_sft:sft_close(Pid) end,
                    State#state.unreferenced_files),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

push_to_memory(DumpList, State) ->
    {TableSize, UpdState} = case State#state.levelzero_pending of
        {true, Remainder, {StartKey, EndKey, Pid}} ->
            %% Need to handle error scenarios?
            %% N.B. Sync call - so will be ready
            {ok, SrcFN} = leveled_sft:sft_checkready(Pid),
            %% Reset ETS, but re-insert any remainder
            true = ets:delete_all_objects(State#state.memtable),
            true = ets:insert(State#state.memtable, Remainder),
            ManifestEntry = #manifest_entry{start_key=StartKey,
                                                end_key=EndKey,
                                                owner=Pid,
                                                filename=SrcFN},
            {length(Remainder),
                State#state{manifest=lists:keystore(0,
                                                    1,
                                                    State#state.manifest,
                                                    {0, [ManifestEntry]}),
                            levelzero_pending=?L0PEND_RESET}};
        {false, _, _} ->
            {State#state.table_size, State}
    end,
    
    %% Prompt clerk to ask about work - do this for every push_mem
    ok = leveled_clerk:clerk_prompt(UpdState#state.clerk, penciller),    
    
    case do_push_to_mem(DumpList, TableSize, UpdState#state.memtable) of
        {twist, ApproxTableSize} ->
            {ok, UpdState#state{table_size=ApproxTableSize}};
        {roll, ApproxTableSize} ->
            L0 = get_item(0, UpdState#state.manifest, []),
            case {L0, manifest_locked(UpdState)} of
                {[], false} ->
                    MSN = UpdState#state.manifest_sqn + 1,
                    FileName = UpdState#state.root_path
                                ++ "/" ++ ?FILES_FP ++ "/"
                                ++ integer_to_list(MSN) ++ "_0_0",
                    Dump = ets:tab2list(UpdState#state.memtable),
                    L0_SFT = leveled_sft:sft_new(FileName,
                                                    Dump,
                                                    [],
                                                    0,
                                                    #sft_options{wait=false}),
                    {ok, L0Pid, Reply} = L0_SFT,
                    {{KL1Rem, []}, L0StartKey, L0EndKey} = Reply,
                    Backlog = length(KL1Rem),
                    Rsp =
                        if
                            Backlog > ?MAX_TABLESIZE ->
                                {pause,
                                    "Backlog of ~w in memory table~n",
                                    [Backlog]};
                            true ->
                                ok
                        end,
                    {Rsp,
                        UpdState#state{levelzero_pending={true,
                                                        KL1Rem,
                                                        {L0StartKey,
                                                            L0EndKey,
                                                            L0Pid}},
                                        table_size=ApproxTableSize,
                                        manifest_sqn=MSN}};
                {[], true} ->
                    {{pause,
                            "L0 file write blocked by change at sqn=~w~n",
                            [UpdState#state.manifest_sqn]},
                        UpdState#state{table_size=ApproxTableSize}};
                _ ->
                    {{pause,
                        "L0 file write blocked by L0 file in manifest~n",
                        []},
                        UpdState#state{table_size=ApproxTableSize}}
            end
    end.

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


%% Manifest lock - don't have two changes to the manifest happening
%% concurrently

manifest_locked(State) ->
    if
        length(State#state.ongoing_work) > 0 ->
            true;
        true ->
            case State#state.levelzero_pending of
                {true, _, _} ->
                    true;
                _ ->
                    false
            end
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
            io:format("Work at Level ~w to be scheduled for ~w with ~w " ++
                        "queue items outstanding~n",
                        [SrcLevel, From, length(OtherWork)]),
            case {manifest_locked(State), State#state.ongoing_work} of     
                {false, _} ->
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
                {true, [OutstandingWork]} ->
                    %% Still awaiting a response 
                    io:format("Ongoing work requested by ~w " ++
                                "but work outstanding from Level ~w " ++
                                "and Clerk ~w at sequence number ~w~n",
                                [From,
                                    OutstandingWork#penciller_work.src_level,
                                    OutstandingWork#penciller_work.clerk,
                                    OutstandingWork#penciller_work.next_sqn]),
                    {State, none};
                {true, _} ->
                    %% Manifest locked
                    io:format("Manifest locked but no work outstanding " ++
                                "with clerk~n"),
                    {State, none}
            end;
        _ ->
            {State, none}
    end.


close_files(?MAX_LEVELS - 1, _Manifest) ->
    ok;
close_files(Level, Manifest) ->
    LevelList = get_item(Level, Manifest, []),
    lists:foreach(fun(F) -> leveled_sft:sft_close(F#manifest_entry.owner) end,
                    LevelList),
    close_files(Level + 1, Manifest).


open_all_filesinmanifest(Manifest) ->
    open_all_filesinmanifest({Manifest, 0}, 0).

open_all_filesinmanifest(Result, ?MAX_LEVELS - 1) ->
    Result;
open_all_filesinmanifest({Manifest, TopSQN}, Level) ->
    LevelList = get_item(Level, Manifest, []),
    %% The Pids in the saved manifest related to now closed references
    %% Need to roll over the manifest at this level starting new processes to
    %5 replace them
    LvlR = lists:foldl(fun(F, {FL, FL_SQN}) ->
                            FN = F#manifest_entry.filename,
                            {ok, P, _Keys} = leveled_sft:sft_open(FN),
                            F_SQN = leveled_sft:sft_getmaxsequencenumber(P),
                            {lists:append(FL,
                                        [F#manifest_entry{owner = P}]),
                                max(FL_SQN, F_SQN)}
                            end,
                            {[], 0},
                            LevelList),
    %% Result is tuple of revised file list for this level in manifest, and
    %% the maximum sequence number seen at this level
    {LvlFL, LvlSQN} = LvlR, 
    UpdManifest = lists:keystore(Level, 1, Manifest, {Level, LvlFL}),
    open_all_filesinmanifest({UpdManifest, max(TopSQN, LvlSQN)}, Level + 1).

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
maybe_append_work(WorkQ, _Level, _Manifest,
                    _MaxFiles, _FileCount) ->
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


commit_manifest_change(ReturnedWorkItem, State) ->
    NewMSN = State#state.manifest_sqn +  1,
    [SentWorkItem] = State#state.ongoing_work,
    RootPath = State#state.root_path,
    UnreferencedFiles = State#state.unreferenced_files,
    
    case {SentWorkItem#penciller_work.next_sqn,
            SentWorkItem#penciller_work.clerk} of
        {NewMSN, _From} ->
            MTime = timer:now_diff(os:timestamp(),
                                    SentWorkItem#penciller_work.start_time),
            io:format("Merge to sqn ~w completed in ~w microseconds " ++
                        "at Level ~w~n",
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
            %% io:format("Updated manifest is ~w~n", [NewManifest]),
            {ok, State#state{ongoing_work=[],
                                manifest_sqn=NewMSN,
                                manifest=NewManifest,
                                unreferenced_files=UnreferencedFilesUpd}};
        {MaybeWrongMSN, From} ->
            io:format("Merge commit at sqn ~w not matched to expected" ++
                        " sqn ~w from Clerk ~w~n",
                        [NewMSN, MaybeWrongMSN, From]),
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
    io:format("Adding cleared file ~s to deletion list ~n",
                [ClearedFile#manifest_entry.filename]),
    update_deletions(Tail,
                        MSN,
                        lists:append(UnreferencedFiles,
                            [{ClearedFile#manifest_entry.filename,
                                ClearedFile#manifest_entry.owner,
                                MSN}])).

confirm_delete(Filename, UnreferencedFiles, RegisteredIterators) ->
    case lists:keyfind(Filename, 1, UnreferencedFiles) of
        false ->
            false;
        {Filename, _Pid, MSN} ->
            LowSQN = lists:foldl(fun({_, SQN}, MinSQN) -> min(SQN, MinSQN) end,
                                    infinity,
                                    RegisteredIterators),
            if
                MSN >= LowSQN ->
                    false;
                true ->
                    true
            end
    end.



assess_sqn(DumpList) ->
    assess_sqn(DumpList, infinity, 0).

assess_sqn([], MinSQN, MaxSQN) ->
    {MinSQN, MaxSQN};
assess_sqn([HeadKey|Tail], MinSQN, MaxSQN) ->
    {_K, SQN} = leveled_sft:strip_to_key_seqn_only(HeadKey),
    assess_sqn(Tail, min(MinSQN, SQN), max(MaxSQN, SQN)).


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

confirm_delete_test() ->
    Filename = 'test.sft',
    UnreferencedFiles = [{'other.sft', dummy_owner, 15},
                            {Filename, dummy_owner, 10}],
    RegisteredIterators1 = [{dummy_pid, 16}, {dummy_pid, 12}],
    R1 = confirm_delete(Filename, UnreferencedFiles, RegisteredIterators1),
    ?assertMatch(R1, true),
    RegisteredIterators2 = [{dummy_pid, 10}, {dummy_pid, 12}],
    R2 = confirm_delete(Filename, UnreferencedFiles, RegisteredIterators2),
    ?assertMatch(R2, false),
    RegisteredIterators3 = [{dummy_pid, 9}, {dummy_pid, 12}],
    R3 = confirm_delete(Filename, UnreferencedFiles, RegisteredIterators3),
    ?assertMatch(R3, false).