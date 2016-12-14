%% -------- PENCILLER ---------
%%
%% The penciller is responsible for writing and re-writing the ledger - a
%% persisted, ordered view of non-recent Keys and Metadata which have been
%% added to the store.
%% - The penciller maintains a manifest of all the files within the current
%% Ledger.
%% - The Penciller provides re-write (compaction) work up to be managed by
%% the Penciller's Clerk
%% - The Penciller can be cloned and maintains a register of clones who have
%% requested snapshots of the Ledger
%% - The accepts new dumps (in the form of a gb_tree) from the Bookie, and
%% calls the Bookie once the process of pencilling this data in the Ledger is
%% complete - and the Bookie is free to forget about the data
%% - The Penciller's persistence of the ledger may not be reliable, in that it
%% may lose data but only in sequence from a particular sequence number.  On
%% startup the Penciller will inform the Bookie of the highest sequence number
%% it has, and the Bookie should load any missing data from that point out of
%% the journal.
%%
%% -------- LEDGER ---------
%%
%% The Ledger is divided into many levels
%% - L0: New keys are received from the Bookie and merged into a single
%% gb_tree, until that tree is the size of a SFT file, and it is then persisted
%% as a SFT file at this level.  L0 SFT files can be larger than the normal 
%% maximum size - so we don't have to consider problems of either having more
%% than one L0 file (and handling what happens on a crash between writing the
%% files when the second may have overlapping sequence numbers), or having a
%% remainder with overlapping in sequence numbers in memory after the file is
%% written.   Once the persistence is completed, the L0 tree can be erased.
%% There can be only one SFT file at Level 0, so the work to merge that file
%% to the lower level must be the highest priority, as otherwise writes to the
%% ledger will stall, when there is next a need to persist.
%% - L1 TO L7: May contain multiple processes managing non-overlapping sft
%% files.  Compaction work should be sheduled if the number of files exceeds
%% the target size of the level, where the target size is 8 ^ n.
%%
%% The most recent revision of a Key can be found by checking each level until
%% the key is found.  To check a level the correct file must be sought from the
%% manifest for that level, and then a call is made to that file.  If the Key
%% is not present then every level should be checked.
%%
%% If a compaction change takes the size of a level beyond the target size,
%% then compaction work for that level + 1 should be added to the compaction
%% work queue.
%% Compaction work is fetched by the Penciller's Clerk because:
%% - it has timed out due to a period of inactivity
%% - it has been triggered by the a cast to indicate the arrival of high
%% priority compaction work
%% The Penciller's Clerk (which performs compaction worker) will always call
%% the Penciller to find out the highest priority work currently required
%% whenever it has either completed work, or a timeout has occurred since it
%% was informed there was no work to do.
%%
%% When the clerk picks work it will take the current manifest, and the
%% Penciller assumes the manifest sequence number is to be incremented.
%% When the clerk has completed the work it can request that the manifest
%% change be committed by the Penciller.  The commit is made through changing
%% the filename of the new manifest - so the Penciller is not held up by the
%% process of wiritng a file, just altering file system metadata.
%%
%% ---------- PUSH ----------
%%
%% The Penciller must support the PUSH of a dump of keys from the Bookie.  The
%% call to PUSH should be immediately acknowledged, and then work should be
%% completed to merge the tree into the L0 tree.
%%
%% The Penciller MUST NOT accept a new PUSH if the Clerk has commenced the
%% conversion of the current L0 tree into a SFT file, but not completed this
%% change.  The Penciller in this case returns the push, and the Bookie should
%% continue to grow the cache before trying again.
%%
%% ---------- FETCH ----------
%%
%% On request to fetch a key the Penciller should look first in the in-memory
%% L0 tree, then look in the SFT files Level by Level (including level 0),
%% consulting the Manifest to determine which file should be checked at each
%% level.
%%
%% ---------- SNAPSHOT ----------
%%
%% Iterators may request a snapshot of the database.  A snapshot is a cloned
%% Penciller seeded not from disk, but by the in-memory L0 gb_tree and the
%% in-memory manifest, allowing for direct reference for the SFT file processes.
%%
%% Clones formed to support snapshots are registered by the Penciller, so that
%% SFT files valid at the point of the snapshot until either the iterator is
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
%% ETS table to a L0 SFT file, assuming one is nto already pending.  If one is
%% already pending then the Penciller will not persist this part of the Ledger.
%%
%% ---------- FOLDER STRUCTURE ----------
%%
%% The following folders are used by the Penciller
%% $ROOT/ledger/ledger_manifest/ - used for keeping manifest files
%% $ROOT/ledger/ledger_files/ - containing individual SFT files
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
%% highest such manifest sequence number.  This will be started as the
%% manifest, together with any _0_0.sft file found at that Manifest SQN.
%% Level zero files are not kept in the persisted manifest, and adding a L0
%% file does not advanced the Manifest SQN.
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
%% The L0 files are prompted directly by the penciller when the in-memory tree
%% has reached capacity.  This places the penciller in a levelzero_pending
%% state, and in this state it must return new pushes.  Once the SFT file has
%% been completed it will confirm completion to the penciller which can then
%% revert the levelzero_pending state, add the file to the manifest and clear
%% the current level zero in-memory view.
%%


-module(leveled_penciller).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        pcl_start/1,
        pcl_pushmem/2,
        pcl_fetchlevelzero/2,
        pcl_fetch/2,
        pcl_fetch/3,
        pcl_fetchkeys/5,
        pcl_fetchnextkey/5,
        pcl_checksequencenumber/3,
        pcl_checksequencenumber/4,
        pcl_workforclerk/1,
        pcl_promptmanifestchange/2,
        pcl_confirml0complete/4,
        pcl_confirmdelete/2,
        pcl_close/1,
        pcl_doom/1,
        pcl_registersnapshot/2,
        pcl_releasesnapshot/2,
        pcl_loadsnapshot/2,
        pcl_getstartupsequencenumber/1,
        clean_testdir/1]).

-include_lib("eunit/include/eunit.hrl").

-define(LEVEL_SCALEFACTOR, [{0, 0}, {1, 8}, {2, 64}, {3, 512},
                            {4, 4096}, {5, 32768}, {6, 262144},
                            {7, infinity}]).
-define(MAX_LEVELS, 8).
-define(MAX_WORK_WAIT, 300).
-define(MANIFEST_FP, "ledger_manifest").
-define(FILES_FP, "ledger_files").
-define(CURRENT_FILEX, "crr").
-define(PENDING_FILEX, "pnd").
-define(MEMTABLE, mem).
-define(MAX_TABLESIZE, 16000). % This is less than max - but COIN_SIDECOUNT
-define(SUPER_MAX_TABLE_SIZE, 22000).
-define(PROMPT_WAIT_ONL0, 5).
-define(WORKQUEUE_BACKLOG_TOLERANCE, 4).
-define(COIN_SIDECOUNT, 5).

-record(state, {manifest = [] :: list(),
				manifest_sqn = 0 :: integer(),
                ledger_sqn = 0 :: integer(), % The highest SQN added to L0
                persisted_sqn = 0 :: integer(), % The highest SQN persisted
                registered_snapshots = [] :: list(),
                unreferenced_files = [] :: list(),
                root_path = "../test" :: string(),
                
                clerk :: pid(),
                
                levelzero_pending = false :: boolean(),
                levelzero_constructor :: pid(),
                levelzero_cache = [] :: list(), % a list of skiplists
                levelzero_size = 0 :: integer(),
                levelzero_maxcachesize :: integer(),
                levelzero_cointoss = false :: boolean(),
                levelzero_index, % may be none or an ETS table reference
                
                is_snapshot = false :: boolean(),
                snapshot_fully_loaded = false :: boolean(),
                source_penciller :: pid(),
                levelzero_astree :: list(),
                
                ongoing_work = [] :: list(),
                work_backlog = false :: boolean()}).


%%%============================================================================
%%% API
%%%============================================================================

 
pcl_start(PCLopts) ->
    gen_server:start(?MODULE, [PCLopts], []).

pcl_pushmem(Pid, LedgerCache) ->
    %% Bookie to dump memory onto penciller
    gen_server:call(Pid, {push_mem, LedgerCache}, infinity).

pcl_fetchlevelzero(Pid, Slot) ->
    %% Timeout to cause crash of L0 file when it can't get the close signal
    %% as it is deadlocked making this call.
    %%
    %% If the timeout gets hit outside of close scenario the Penciller will
    %% be stuck in L0 pending
    gen_server:call(Pid, {fetch_levelzero, Slot}, 60000).
    
pcl_fetch(Pid, Key) ->
    Hash = leveled_codec:magic_hash(Key),
    if
        Hash /= no_lookup ->
            gen_server:call(Pid, {fetch, Key, Hash}, infinity)
    end.

pcl_fetch(Pid, Key, Hash) ->
    gen_server:call(Pid, {fetch, Key, Hash}, infinity).

pcl_fetchkeys(Pid, StartKey, EndKey, AccFun, InitAcc) ->
    gen_server:call(Pid,
                    {fetch_keys, StartKey, EndKey, AccFun, InitAcc, -1},
                    infinity).

pcl_fetchnextkey(Pid, StartKey, EndKey, AccFun, InitAcc) ->
    gen_server:call(Pid,
                    {fetch_keys, StartKey, EndKey, AccFun, InitAcc, 1},
                    infinity).

pcl_checksequencenumber(Pid, Key, SQN) ->
    Hash = leveled_codec:magic_hash(Key),
    if
        Hash /= no_lookup ->
            gen_server:call(Pid, {check_sqn, Key, Hash, SQN}, infinity)
    end.

pcl_checksequencenumber(Pid, Key, Hash, SQN) ->
    gen_server:call(Pid, {check_sqn, Key, Hash, SQN}, infinity).

pcl_workforclerk(Pid) ->
    gen_server:call(Pid, work_for_clerk, infinity).

pcl_promptmanifestchange(Pid, WI) ->
    gen_server:cast(Pid, {manifest_change, WI}).

pcl_confirml0complete(Pid, FN, StartKey, EndKey) ->
    gen_server:cast(Pid, {levelzero_complete, FN, StartKey, EndKey}).

pcl_confirmdelete(Pid, FileName) ->
    gen_server:cast(Pid, {confirm_delete, FileName}).

pcl_getstartupsequencenumber(Pid) ->
    gen_server:call(Pid, get_startup_sqn, infinity).

pcl_registersnapshot(Pid, Snapshot) ->
    gen_server:call(Pid, {register_snapshot, Snapshot}, infinity).

pcl_releasesnapshot(Pid, Snapshot) ->
    gen_server:cast(Pid, {release_snapshot, Snapshot}).

pcl_loadsnapshot(Pid, Increment) ->
    gen_server:call(Pid, {load_snapshot, Increment}, infinity).


pcl_close(Pid) ->
    gen_server:call(Pid, close, 60000).

pcl_doom(Pid) ->
    gen_server:call(Pid, doom, 60000).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([PCLopts]) ->
    case {PCLopts#penciller_options.root_path,
            PCLopts#penciller_options.start_snapshot} of
        {undefined, true} ->
            SrcPenciller = PCLopts#penciller_options.source_penciller,
            {ok, State} = pcl_registersnapshot(SrcPenciller, self()),
            leveled_log:log("P0001", [self()]),
            {ok, State#state{is_snapshot=true, source_penciller=SrcPenciller}};
            %% Need to do something about timeout
        {_RootPath, false} ->
            start_from_file(PCLopts)
    end.    
    

handle_call({push_mem, {PushedTree, MinSQN, MaxSQN}},
                From,
                State=#state{is_snapshot=Snap}) when Snap == false ->
    % The push_mem process is as follows:
    %
    % 1 - Receive a gb_tree containing the latest Key/Value pairs (note that
    % we mean value from the perspective of the Ledger, not the full value
    % stored in the Inker)
    %
    % 2 - Check to see if there is a levelzero file pending.  If so, the
    % update must be returned.  If not the update can be accepted
    %
    % 3 - The Penciller can now reply to the Bookie to show if the push has
    % been accepted
    %
    % 4 - Update the cache:
    % a) Append the cache to the list
    % b) Add hashes for all the elements to the index
    %
    % Check the approximate size of the cache.  If it is over the maximum size,
    % trigger a backgroun L0 file write and update state of levelzero_pending.
    case State#state.levelzero_pending or State#state.work_backlog of
        true ->
            leveled_log:log("P0018", [returned,
                                        State#state.levelzero_pending,
                                        State#state.work_backlog]),
            {reply, returned, State};
        false ->
            leveled_log:log("P0018", [ok, false, false]),
            gen_server:reply(From, ok),
            {noreply, update_levelzero(State#state.levelzero_size,
                                        {PushedTree, MinSQN, MaxSQN},
                                        State#state.ledger_sqn,
                                        State#state.levelzero_cache,
                                        State)}
    end;
handle_call({fetch, Key, Hash}, _From, State) ->
    {reply,
        fetch_mem(Key,
                    Hash,
                    State#state.manifest,
                    State#state.levelzero_cache,
                    State#state.levelzero_index),
        State};
handle_call({check_sqn, Key, Hash, SQN}, _From, State) ->
    {reply,
        compare_to_sqn(fetch_mem(Key,
                                    Hash,
                                    State#state.manifest,
                                    State#state.levelzero_cache,
                                    State#state.levelzero_index),
                        SQN),
        State};
handle_call({fetch_keys, StartKey, EndKey, AccFun, InitAcc, MaxKeys},
                _From,
                State=#state{snapshot_fully_loaded=Ready})
                                                        when Ready == true ->
    L0AsList =
        case State#state.levelzero_astree of
            undefined ->
                leveled_pmem:merge_trees(StartKey,
                                            EndKey,
                                            State#state.levelzero_cache,
                                            leveled_skiplist:empty());
            List ->
                List
        end,
    SFTiter = initiate_rangequery_frommanifest(StartKey,
                                                EndKey,
                                                State#state.manifest),
    Acc = keyfolder({L0AsList, SFTiter},
                        {StartKey, EndKey},
                        {AccFun, InitAcc},
                        MaxKeys),
    {reply, Acc, State#state{levelzero_astree = L0AsList}};
handle_call(work_for_clerk, From, State) ->
    {UpdState, Work} = return_work(State, From),
    {reply, Work, UpdState};
handle_call(get_startup_sqn, _From, State) ->
    {reply, State#state.persisted_sqn, State};
handle_call({register_snapshot, Snapshot}, _From, State) ->
    Rs = [{Snapshot, State#state.manifest_sqn}|State#state.registered_snapshots],
    {reply, {ok, State}, State#state{registered_snapshots = Rs}};
handle_call({load_snapshot, {BookieIncrTree, MinSQN, MaxSQN}}, _From, State) ->
    L0D = leveled_pmem:add_to_cache(State#state.levelzero_size,
                                        {BookieIncrTree, MinSQN, MaxSQN},
                                        State#state.ledger_sqn,
                                        State#state.levelzero_cache),
    {LedgerSQN, L0Size, L0Cache} = L0D,
    {reply, ok, State#state{levelzero_cache=L0Cache,
                                levelzero_size=L0Size,
                                levelzero_index=none,
                                ledger_sqn=LedgerSQN,
                                snapshot_fully_loaded=true}};
handle_call({fetch_levelzero, Slot}, _From, State) ->
    {reply, lists:nth(Slot, State#state.levelzero_cache), State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(doom, _From, State) ->
    leveled_log:log("P0030", []),
    ManifestFP = State#state.root_path ++ "/" ++ ?MANIFEST_FP ++ "/",
    FilesFP = State#state.root_path ++ "/" ++ ?FILES_FP ++ "/",
    {stop, normal, {ok, [ManifestFP, FilesFP]}, State}.

handle_cast({manifest_change, WI}, State) ->
    {ok, UpdState} = commit_manifest_change(WI, State),
    ok = leveled_pclerk:clerk_manifestchange(State#state.clerk,
                                                confirm,
                                                false),
    {noreply, UpdState};
handle_cast({release_snapshot, Snapshot}, State) ->
    Rs = lists:keydelete(Snapshot, 1, State#state.registered_snapshots),
    leveled_log:log("P0003", [Snapshot]),
    leveled_log:log("P0004", [Rs]),
    {noreply, State#state{registered_snapshots=Rs}};
handle_cast({confirm_delete, FileName}, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->    
    Reply = confirm_delete(FileName,
                            State#state.unreferenced_files,
                            State#state.registered_snapshots),
    case Reply of
        {true, Pid} ->
            UF1 = lists:keydelete(FileName, 1, State#state.unreferenced_files),
            leveled_log:log("P0005", [FileName]),
            ok = leveled_sft:sft_deleteconfirmed(Pid),
            {noreply, State#state{unreferenced_files=UF1}};
        _ ->
            {noreply, State}
    end;
handle_cast({levelzero_complete, FN, StartKey, EndKey}, State) ->
    leveled_log:log("P0029", []),
    ManEntry = #manifest_entry{start_key=StartKey,
                                end_key=EndKey,
                                owner=State#state.levelzero_constructor,
                                filename=FN},
    UpdMan = lists:keystore(0, 1, State#state.manifest, {0, [ManEntry]}),
    % Prompt clerk to ask about work - do this for every L0 roll
    leveled_pmem:clear_index(State#state.levelzero_index),
    ok = leveled_pclerk:clerk_prompt(State#state.clerk),
    {noreply, State#state{levelzero_cache=[],
                            levelzero_pending=false,
                            levelzero_constructor=undefined,
                            levelzero_size=0,
                            manifest=UpdMan,
                            persisted_sqn=State#state.ledger_sqn}}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State=#state{is_snapshot=Snap}) when Snap == true ->
    ok = pcl_releasesnapshot(State#state.source_penciller, self()),
    leveled_log:log("P0007", [Reason]),   
    ok;
terminate(Reason, State) ->
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
    leveled_log:log("P0008", [Reason]),
    MC = leveled_pclerk:clerk_manifestchange(State#state.clerk,
                                                return,
                                                true),
    UpdState = case MC of
                    {ok, WI} ->
                        {ok, NewState} = commit_manifest_change(WI, State),
                        Clerk = State#state.clerk,
                        ok = leveled_pclerk:clerk_manifestchange(Clerk,
                                                                    confirm,
                                                                    true),
                        NewState;
                    no_change ->
                        State
                end,
    case {UpdState#state.levelzero_pending,
            get_item(0, UpdState#state.manifest, []),
            UpdState#state.levelzero_size} of
        {false, [], 0} ->
           leveled_log:log("P0009", []);
        {false, [], _N} ->
            L0Pid = roll_memory(UpdState, true),
            ok = leveled_sft:sft_close(L0Pid);
        StatusTuple ->
            leveled_log:log("P0010", [StatusTuple])
    end,
    
    % Tidy shutdown of individual files
    ok = close_files(0, UpdState#state.manifest),
    lists:foreach(fun({_FN, Pid, _SN}) ->
                            ok = leveled_sft:sft_close(Pid) end,
                    UpdState#state.unreferenced_files),
    leveled_log:log("P0011", []),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================


start_from_file(PCLopts) ->
    RootPath = PCLopts#penciller_options.root_path,
    MaxTableSize = case PCLopts#penciller_options.max_inmemory_tablesize of
                        undefined ->
                            ?MAX_TABLESIZE;
                        M ->
                            M
                    end,
    
    {ok, MergeClerk} = leveled_pclerk:clerk_new(self()),
    
    CoinToss = PCLopts#penciller_options.levelzero_cointoss,
    % Used to randomly defer the writing of L0 file.  Intended to help with
    % vnode syncronisation issues (e.g. stop them all by default merging to
    % level zero concurrently)
    
    InitState = #state{clerk=MergeClerk,
                        root_path=RootPath,
                        levelzero_maxcachesize=MaxTableSize,
                        levelzero_cointoss=CoinToss,
                        levelzero_index=leveled_pmem:new_index()},
    
    %% Open manifest
    ManifestPath = InitState#state.root_path ++ "/" ++ ?MANIFEST_FP ++ "/",
    ok = filelib:ensure_dir(ManifestPath),
    {ok, Filenames} = file:list_dir(ManifestPath),
    CurrRegex = "nonzero_(?<MSN>[0-9]+)\\." ++ ?CURRENT_FILEX,
    ValidManSQNs = lists:foldl(fun(FN, Acc) ->
                                    case re:run(FN,
                                                CurrRegex,
                                                [{capture, ['MSN'], list}]) of
                                        nomatch ->
                                            Acc;
                                        {match, [Int]} when is_list(Int) ->
                                            Acc ++ [list_to_integer(Int)]
                                    end
                                    end,
                                    [],
                                    Filenames),
    TopManSQN = lists:foldl(fun(X, MaxSQN) -> max(X, MaxSQN) end,
                            0,
                            ValidManSQNs),
    leveled_log:log("P0012", [TopManSQN]),
    ManUpdate = case TopManSQN of
                    0 ->
                        leveled_log:log("P0013", []),
                        {[], 0};
                    _ ->
                        CurrManFile = filepath(InitState#state.root_path,
                                                TopManSQN,
                                                current_manifest),
                        {ok, Bin} = file:read_file(CurrManFile),
                        Manifest = binary_to_term(Bin),
                        open_all_filesinmanifest(Manifest)
                end,
        
    {UpdManifest, MaxSQN} = ManUpdate,
    leveled_log:log("P0014", [MaxSQN]),
            
    %% Find any L0 files
    L0FN = filepath(RootPath, TopManSQN, new_merge_files) ++ "_0_0.sft",
    case filelib:is_file(L0FN) of
        true ->
            leveled_log:log("P0015", [L0FN]),
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
            leveled_log:log("P0016", [L0SQN]),
            LedgerSQN = max(MaxSQN, L0SQN),
            {ok,
                InitState#state{manifest=UpdManifest2,
                                    manifest_sqn=TopManSQN,
                                    ledger_sqn=LedgerSQN,
                                    persisted_sqn=LedgerSQN}};
        false ->
            leveled_log:log("P0017", []),
            {ok,
                InitState#state{manifest=UpdManifest,
                                    manifest_sqn=TopManSQN,
                                    ledger_sqn=MaxSQN,
                                    persisted_sqn=MaxSQN}}
    end.



update_levelzero(L0Size, {PushedTree, MinSQN, MaxSQN},
                                                LedgerSQN, L0Cache, State) ->
    SW = os:timestamp(),
    Update = leveled_pmem:add_to_cache(L0Size,
                                        {PushedTree, MinSQN, MaxSQN},
                                        LedgerSQN,
                                        L0Cache),
    leveled_pmem:add_to_index(PushedTree, State#state.levelzero_index),
    
    {UpdMaxSQN, NewL0Size, UpdL0Cache} = Update,
    if
        UpdMaxSQN >= LedgerSQN ->
            UpdState = State#state{levelzero_cache=UpdL0Cache,
                                    levelzero_size=NewL0Size,
                                    ledger_sqn=UpdMaxSQN},
            CacheTooBig = NewL0Size > State#state.levelzero_maxcachesize,
            CacheMuchTooBig = NewL0Size > ?SUPER_MAX_TABLE_SIZE,
            Level0Free = length(get_item(0, State#state.manifest, [])) == 0,
            RandomFactor =
                case State#state.levelzero_cointoss of
                    true ->
                        case random:uniform(?COIN_SIDECOUNT) of
                            1 ->
                                true;
                            _ ->
                                false
                        end;
                    false ->
                        true
                end,
            JitterCheck = RandomFactor or CacheMuchTooBig,
            case {CacheTooBig, Level0Free, JitterCheck} of
                {true, true, true}  ->
                    L0Constructor = roll_memory(UpdState, false),
                    leveled_log:log_timer("P0031", [], SW),
                    UpdState#state{levelzero_pending=true,
                                    levelzero_constructor=L0Constructor};
                _ ->
                    leveled_log:log_timer("P0031", [], SW),
                    UpdState
            end;
        
        NewL0Size == L0Size ->
            leveled_log:log_timer("P0031", [], SW),
            State#state{levelzero_cache=L0Cache,
                        levelzero_size=L0Size,
                        ledger_sqn=LedgerSQN}
    end.


%% Casting a large object (the levelzero cache) to the gen_server did not lead
%% to an immediate return as expected.  With 32K keys in the TreeList it could
%% take around 35-40ms.
%%
%% To avoid blocking this gen_server, the SFT file can request each item of the
%% cache one at a time.
%%
%% The Wait is set to false to use a cast when calling this in normal operation
%% where as the Wait of true is used at shutdown

roll_memory(State, false) ->
    FileName = levelzero_filename(State),
    leveled_log:log("P0019", [FileName]),
    Opts = #sft_options{wait=false, penciller=self()},
    PCL = self(),
    FetchFun = fun(Slot) -> pcl_fetchlevelzero(PCL, Slot) end,
    % FetchFun = fun(Slot) -> lists:nth(Slot, State#state.levelzero_cache) end,
    R = leveled_sft:sft_newfroml0cache(FileName,
                                        length(State#state.levelzero_cache),
                                        FetchFun,
                                        Opts),
    {ok, Constructor, _} = R,
    Constructor;
roll_memory(State, true) ->
    FileName = levelzero_filename(State),
    Opts = #sft_options{wait=true},
    FetchFun = fun(Slot) -> lists:nth(Slot, State#state.levelzero_cache) end,
    R = leveled_sft:sft_newfroml0cache(FileName,
                                        length(State#state.levelzero_cache),
                                        FetchFun,
                                        Opts),
    {ok, Constructor, _} = R,
    Constructor.

levelzero_filename(State) ->
    MSN = State#state.manifest_sqn,
    FileName = State#state.root_path
                ++ "/" ++ ?FILES_FP ++ "/"
                ++ integer_to_list(MSN) ++ "_0_0",
    FileName.



fetch_mem(Key, Hash, Manifest, L0Cache, none) ->
    L0Check = leveled_pmem:check_levelzero(Key, Hash, L0Cache),
    case L0Check of
        {false, not_found} ->
            fetch(Key, Manifest, 0, fun leveled_sft:sft_get/2);
        {true, KV} ->
            KV
    end;
fetch_mem(Key, Hash, Manifest, L0Cache, L0Index) ->
    case leveled_pmem:check_index(Hash, L0Index) of
        true ->
            fetch_mem(Key, Hash, Manifest, L0Cache, none);
        false ->
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
                            FoundDetails ->
                                FoundDetails
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
    

compare_to_sqn(Obj, SQN) ->
    case Obj of
        not_present ->
            false;
        Obj ->
            SQNToCompare = leveled_codec:strip_to_seqonly(Obj),
            if
                SQNToCompare > SQN ->
                    false;
                true ->
                    true
            end
    end.


%% Work out what the current work queue should be
%%
%% The work queue should have a lower level work at the front, and no work
%% should be added to the queue if a compaction worker has already been asked
%% to look at work at that level
%%
%% The full queue is calculated for logging purposes only

return_work(State, From) ->
    {WorkQ, BasementL} = assess_workqueue([], 0, State#state.manifest, 0),
    case length(WorkQ) of
        L when L > 0 ->
            Excess = lists:foldl(fun({_, _, OH}, Acc) -> Acc+OH end, 0, WorkQ),
            [{SrcLevel, Manifest, _Overhead}|_OtherWork] = WorkQ,
            leveled_log:log("P0020", [SrcLevel, From, Excess]),
            IsBasement = if
                                SrcLevel + 1 == BasementL ->
                                    true;
                                true ->
                                    false
                            end,
            Backlog = Excess >= ?WORKQUEUE_BACKLOG_TOLERANCE,
            case State#state.levelzero_pending of
                true ->
                    % Once the L0 file is completed there will be more work
                    % - so don't be busy doing other work now
                    leveled_log:log("P0021", []),
                    {State#state{work_backlog=Backlog}, none};
                false ->
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
                                            manifest_file = ManFile,
                                            target_is_basement = IsBasement},
                    {State#state{ongoing_work=[WI], work_backlog=Backlog}, WI}
            end;
        _ ->
            {State#state{work_backlog=false}, none}
    end.


close_files(?MAX_LEVELS - 1, _Manifest) ->
    ok;
close_files(Level, Manifest) ->
    LevelList = get_item(Level, Manifest, []),
    lists:foreach(fun(F) ->
                        ok = leveled_sft:sft_close(F#manifest_entry.owner) end,
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

print_manifest(Manifest) ->
    lists:foreach(fun(L) ->
                        leveled_log:log("P0022", [L]),
                        Level = get_item(L, Manifest, []),
                        lists:foreach(fun print_manifest_entry/1, Level)
                        end,
                    lists:seq(0, ?MAX_LEVELS - 1)),
    ok.

print_manifest_entry(Entry) ->
    {S1, S2, S3} = leveled_codec:print_key(Entry#manifest_entry.start_key),
    {E1, E2, E3} = leveled_codec:print_key(Entry#manifest_entry.end_key),
    leveled_log:log("P0023",
                    [S1, S2, S3, E1, E2, E3, Entry#manifest_entry.filename]).

initiate_rangequery_frommanifest(StartKey, EndKey, Manifest) ->
    CompareFun = fun(M) ->
                    C1 = StartKey > M#manifest_entry.end_key,
                    C2 = leveled_codec:endkey_passed(EndKey,
                                                        M#manifest_entry.start_key),
                    not (C1 or C2) end,
    lists:foldl(fun(L, AccL) ->
                    Level = get_item(L, Manifest, []),
                    FL = lists:foldl(fun(M, Acc) ->
                                            case CompareFun(M) of
                                                true ->
                                                    Acc ++ [{next_file, M}];
                                                false ->
                                                    Acc
                                            end end,
                                        [],
                                        Level),
                    case FL of
                        [] -> AccL;
                        FL -> AccL ++ [{L, FL}]
                    end
                    end,
                [],
                lists:seq(0, ?MAX_LEVELS - 1)).

%% Looks to find the best choice for the next key across the levels (other
%% than in-memory table)
%% In finding the best choice, the next key in a given level may be a next
%% block or next file pointer which will need to be expanded

find_nextkey(QueryArray, StartKey, EndKey) ->
    find_nextkey(QueryArray,
                    0,
                    {null, null},
                    {fun leveled_sft:sft_getkvrange/4, StartKey, EndKey, 1}).

find_nextkey(_QueryArray, LCnt, {null, null}, _QueryFunT)
                                            when LCnt > ?MAX_LEVELS ->
    % The array has been scanned wihtout finding a best key - must be
    % exhausted - respond to indicate no more keys to be found by the
    % iterator
    no_more_keys;
find_nextkey(QueryArray, LCnt, {BKL, BestKV}, _QueryFunT)
                                            when LCnt > ?MAX_LEVELS ->
    % All levels have been scanned, so need to remove the best result from
    % the array, and return that array along with the best key/sqn/status
    % combination
    {BKL, [BestKV|Tail]} = lists:keyfind(BKL, 1, QueryArray),
    {lists:keyreplace(BKL, 1, QueryArray, {BKL, Tail}), BestKV};
find_nextkey(QueryArray, LCnt, {BestKeyLevel, BestKV}, QueryFunT) ->
    % Get the next key at this level
    {NextKey, RestOfKeys} = case lists:keyfind(LCnt, 1, QueryArray) of
                                    false ->
                                        {null, null};
                                    {LCnt, []} ->
                                        {null, null};
                                    {LCnt, [NK|ROfKs]} ->
                                        {NK, ROfKs}
                                end,
    % Compare the next key at this level with the best key
    case {NextKey, BestKeyLevel, BestKV} of
        {null, BKL, BKV} ->
            % There is no key at this level - go to the next level
            find_nextkey(QueryArray, LCnt + 1, {BKL, BKV}, QueryFunT);
        {{next_file, ManifestEntry}, BKL, BKV} ->
            % The first key at this level is pointer to a file - need to query
            % the file to expand this level out before proceeding
            Owner = ManifestEntry#manifest_entry.owner,
            {QueryFun, StartKey, EndKey, ScanSize} = QueryFunT,
            QueryResult = QueryFun(Owner, StartKey, EndKey, ScanSize),
            NewEntry = {LCnt, QueryResult ++ RestOfKeys},
            % Need to loop around at this level (LCnt) as we have not yet
            % examined a real key at this level
            find_nextkey(lists:keyreplace(LCnt, 1, QueryArray, NewEntry),
                            LCnt,
                            {BKL, BKV},
                            QueryFunT);
        {{next, SFTpid, NewStartKey}, BKL, BKV} ->
            % The first key at this level is pointer within a file  - need to
            % query the file to expand this level out before proceeding
            {QueryFun, _StartKey, EndKey, ScanSize} = QueryFunT,
            QueryResult = QueryFun(SFTpid, NewStartKey, EndKey, ScanSize),
            NewEntry = {LCnt, QueryResult ++ RestOfKeys},
            % Need to loop around at this level (LCnt) as we have not yet
            % examined a real key at this level
            find_nextkey(lists:keyreplace(LCnt, 1, QueryArray, NewEntry),
                            LCnt,
                            {BKL, BKV},
                            QueryFunT);
        {{Key, Val}, null, null} ->
            % No best key set - so can assume that this key is the best key,
            % and check the lower levels
            find_nextkey(QueryArray,
                            LCnt + 1,
                            {LCnt, {Key, Val}},
                            QueryFunT);
        {{Key, Val}, _BKL, {BestKey, _BestVal}} when Key < BestKey ->
            % There is a real key and a best key to compare, and the real key
            % at this level is before the best key, and so is now the new best
            % key
            % The QueryArray is not modified until we have checked all levels
            find_nextkey(QueryArray,
                            LCnt + 1,
                            {LCnt, {Key, Val}},
                            QueryFunT);
        {{Key, Val}, BKL, {BestKey, BestVal}} when Key == BestKey ->
            SQN = leveled_codec:strip_to_seqonly({Key, Val}),
            BestSQN = leveled_codec:strip_to_seqonly({BestKey, BestVal}),
            if
                SQN =< BestSQN ->
                    % This is a dominated key, so we need to skip over it
                    NewEntry = {LCnt, RestOfKeys},
                    find_nextkey(lists:keyreplace(LCnt, 1, QueryArray, NewEntry),
                                    LCnt + 1,
                                    {BKL, {BestKey, BestVal}},
                                    QueryFunT);
                SQN > BestSQN ->
                    % There is a real key at the front of this level and it has
                    % a higher SQN than the best key, so we should use this as
                    % the best key
                    % But we also need to remove the dominated key from the
                    % lower level in the query array
                    OldBestEntry = lists:keyfind(BKL, 1, QueryArray),
                    {BKL, [{BestKey, BestVal}|BestTail]} = OldBestEntry,
                    find_nextkey(lists:keyreplace(BKL,
                                                    1,
                                                    QueryArray,
                                                    {BKL, BestTail}),
                                    LCnt + 1,
                                    {LCnt, {Key, Val}},
                                    QueryFunT)
            end;
        {_, BKL, BKV} ->
            % This is not the best key
            find_nextkey(QueryArray, LCnt + 1, {BKL, BKV}, QueryFunT)
    end.


keyfolder(IMMiter, SFTiter, StartKey, EndKey, {AccFun, Acc}) ->
    keyfolder({IMMiter, SFTiter}, {StartKey, EndKey}, {AccFun, Acc}, -1).

keyfolder(_Iterators, _KeyRange, {_AccFun, Acc}, MaxKeys) when MaxKeys == 0 ->
    Acc;
keyfolder({[], SFTiter}, KeyRange, {AccFun, Acc}, MaxKeys) ->
    {StartKey, EndKey} = KeyRange,
    case find_nextkey(SFTiter, StartKey, EndKey) of
        no_more_keys ->
            Acc;
        {NxSFTiter, {SFTKey, SFTVal}} ->
            Acc1 = AccFun(SFTKey, SFTVal, Acc),
            keyfolder({[], NxSFTiter}, KeyRange, {AccFun, Acc1}, MaxKeys - 1)
    end;
keyfolder({[{IMMKey, IMMVal}|NxIMMiterator], SFTiterator}, KeyRange,
                                                    {AccFun, Acc}, MaxKeys) ->
    {StartKey, EndKey} = KeyRange,
    case {IMMKey < StartKey, leveled_codec:endkey_passed(EndKey, IMMKey)} of
        {true, _} ->
    
            % Normally everything is pre-filterd, but the IMM iterator can
            % be re-used and so may be behind the StartKey if the StartKey has
            % advanced from the previous use
            keyfolder({NxIMMiterator, SFTiterator},
                        KeyRange,
                        {AccFun, Acc},
                        MaxKeys);
        {false, true} ->
            % There are no more keys in-range in the in-memory
            % iterator, so take action as if this iterator is empty
            % (see above)
            keyfolder({[], SFTiterator},
                        KeyRange,
                        {AccFun, Acc},
                        MaxKeys);
        {false, false} ->
            case find_nextkey(SFTiterator, StartKey, EndKey) of
                no_more_keys ->
                    % No more keys in range in the persisted store, so use the
                    % in-memory KV as the next
                    Acc1 = AccFun(IMMKey, IMMVal, Acc),
                    keyfolder({NxIMMiterator, SFTiterator},
                                KeyRange,
                                {AccFun, Acc1},
                                MaxKeys - 1);
                {NxSFTiterator, {SFTKey, SFTVal}} ->
                    % There is a next key, so need to know which is the
                    % next key between the two (and handle two keys
                    % with different sequence numbers).  
                    case leveled_codec:key_dominates({IMMKey,
                                                            IMMVal},
                                                        {SFTKey,
                                                            SFTVal}) of
                        left_hand_first ->
                            Acc1 = AccFun(IMMKey, IMMVal, Acc),
                            keyfolder({NxIMMiterator, SFTiterator},
                                        KeyRange,
                                        {AccFun, Acc1},
                                        MaxKeys - 1);
                        right_hand_first ->
                            Acc1 = AccFun(SFTKey, SFTVal, Acc),
                            keyfolder({[{IMMKey, IMMVal}|NxIMMiterator],
                                            NxSFTiterator},
                                        KeyRange,
                                        {AccFun, Acc1},
                                        MaxKeys - 1);
                        left_hand_dominant ->
                            Acc1 = AccFun(IMMKey, IMMVal, Acc),
                            keyfolder({NxIMMiterator, NxSFTiterator},
                                        KeyRange,
                                        {AccFun, Acc1},
                                        MaxKeys - 1)
                    end
            end
    end.    


assess_workqueue(WorkQ, ?MAX_LEVELS - 1, _Man, BasementLevel) ->
    {WorkQ, BasementLevel};
assess_workqueue(WorkQ, LevelToAssess, Man, BasementLevel) ->
    MaxFiles = get_item(LevelToAssess, ?LEVEL_SCALEFACTOR, 0),
    case length(get_item(LevelToAssess, Man, [])) of
        FileCount when FileCount > 0 ->
            NewWQ = maybe_append_work(WorkQ,
                                        LevelToAssess,
                                        Man,
                                        MaxFiles,
                                        FileCount),
            assess_workqueue(NewWQ, LevelToAssess + 1, Man, LevelToAssess);
        0 ->
            assess_workqueue(WorkQ, LevelToAssess + 1, Man, BasementLevel)
    end.


maybe_append_work(WorkQ, Level, Manifest,
                    MaxFiles, FileCount)
                        when FileCount > MaxFiles ->
    Overhead = FileCount - MaxFiles,
    leveled_log:log("P0024", [Overhead, Level]),
    lists:append(WorkQ, [{Level, Manifest, Overhead}]);
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
    
    if
        NewMSN == SentWorkItem#penciller_work.next_sqn ->
            WISrcLevel = SentWorkItem#penciller_work.src_level,
            leveled_log:log_timer("P0025",
                                    [SentWorkItem#penciller_work.next_sqn,
                                        WISrcLevel],
                                    SentWorkItem#penciller_work.start_time),
            ok = rename_manifest_files(RootPath, NewMSN),
            FilesToDelete = ReturnedWorkItem#penciller_work.unreferenced_files,
            UnreferencedFilesUpd = update_deletions(FilesToDelete,
                                                        NewMSN,
                                                        UnreferencedFiles),
            leveled_log:log("P0026", [NewMSN]),
            NewManifest = ReturnedWorkItem#penciller_work.new_manifest,
            
            CurrL0 = get_item(0, State#state.manifest, []),
            % If the work isn't L0 work, then we may have an uncommitted
            % manifest change at L0 - so add this back into the Manifest loop
            % state
            RevisedManifest = case {WISrcLevel, CurrL0} of
                                    {0, _} ->
                                        NewManifest;
                                    {_, []} ->
                                        NewManifest;
                                    {_, [L0ManEntry]} ->
                                        lists:keystore(0,
                                                        1,
                                                        NewManifest,
                                                        {0, [L0ManEntry]})
                                end,
            {ok, State#state{ongoing_work=[],
                                manifest_sqn=NewMSN,
                                manifest=RevisedManifest,
                                unreferenced_files=UnreferencedFilesUpd}}
    end.


rename_manifest_files(RootPath, NewMSN) ->
    OldFN = filepath(RootPath, NewMSN, pending_manifest),
    NewFN = filepath(RootPath, NewMSN, current_manifest),
    leveled_log:log("P0027", [OldFN, filelib:is_file(OldFN),
                                NewFN, filelib:is_file(NewFN)]),
    ok = file:rename(OldFN,NewFN).

filepath(RootPath, manifest) ->
    RootPath ++ "/" ++ ?MANIFEST_FP;
filepath(RootPath, files) ->
    RootPath ++ "/" ++ ?FILES_FP.

filepath(RootPath, NewMSN, pending_manifest) ->
    filepath(RootPath, manifest) ++ "/" ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?PENDING_FILEX;
filepath(RootPath, NewMSN, current_manifest) ->
    filepath(RootPath, manifest) ++ "/" ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?CURRENT_FILEX;
filepath(RootPath, NewMSN, new_merge_files) ->
    filepath(RootPath, files) ++ "/" ++ integer_to_list(NewMSN).
 
update_deletions([], _NewMSN, UnreferencedFiles) ->
    UnreferencedFiles;
update_deletions([ClearedFile|Tail], MSN, UnreferencedFiles) ->
    leveled_log:log("P0028", [ClearedFile#manifest_entry.filename]),
    update_deletions(Tail,
                        MSN,
                        lists:append(UnreferencedFiles,
                            [{ClearedFile#manifest_entry.filename,
                                ClearedFile#manifest_entry.owner,
                                MSN}])).

confirm_delete(Filename, UnreferencedFiles, RegisteredSnapshots) ->
    case lists:keyfind(Filename, 1, UnreferencedFiles) of
        {Filename, Pid, MSN} ->
            LowSQN = lists:foldl(fun({_, SQN}, MinSQN) -> min(SQN, MinSQN) end,
                                    infinity,
                                    RegisteredSnapshots),
            if
                MSN >= LowSQN ->
                    false;
                true ->
                    {true, Pid}
            end
    end.



%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

clean_testdir(RootPath) ->
    clean_subdir(filepath(RootPath, manifest)),
    clean_subdir(filepath(RootPath, files)).

clean_subdir(DirPath) ->
    case filelib:is_dir(DirPath) of
        true ->
            {ok, Files} = file:list_dir(DirPath),
            lists:foreach(fun(FN) ->
                                File = filename:join(DirPath, FN),
                                ok = file:delete(File),
                                io:format("Success deleting ~s~n", [File])
                                end,
                            Files);
        false ->
            ok
    end.


compaction_work_assessment_test() ->
    L0 = [{{o, "B1", "K1", null}, {o, "B3", "K3", null}, dummy_pid}],
    L1 = [{{o, "B1", "K1", null}, {o, "B2", "K2", null}, dummy_pid},
            {{o, "B2", "K3", null}, {o, "B4", "K4", null}, dummy_pid}],
    Manifest = [{0, L0}, {1, L1}],
    {WorkQ1, 1} = assess_workqueue([], 0, Manifest, 0),
    ?assertMatch([{0, Manifest, 1}], WorkQ1),
    L1Alt = lists:append(L1,
                        [{{o, "B5", "K0001", null}, {o, "B5", "K9999", null},
                            dummy_pid},
                        {{o, "B6", "K0001", null}, {o, "B6", "K9999", null},
                            dummy_pid},
                        {{o, "B7", "K0001", null}, {o, "B7", "K9999", null},
                            dummy_pid},
                        {{o, "B8", "K0001", null}, {o, "B8", "K9999", null},
                            dummy_pid},
                        {{o, "B9", "K0001", null}, {o, "B9", "K9999", null},
                            dummy_pid},
                        {{o, "BA", "K0001", null}, {o, "BA", "K9999", null},
                            dummy_pid},
                        {{o, "BB", "K0001", null}, {o, "BB", "K9999", null},
                            dummy_pid}]),
    Manifest3 = [{0, []}, {1, L1Alt}],
    {WorkQ3, 1} = assess_workqueue([], 0, Manifest3, 0),
    ?assertMatch([{1, Manifest3, 1}], WorkQ3).

confirm_delete_test() ->
    Filename = 'test.sft',
    UnreferencedFiles = [{'other.sft', dummy_owner, 15},
                            {Filename, dummy_owner, 10}],
    RegisteredIterators1 = [{dummy_pid, 16}, {dummy_pid, 12}],
    R1 = confirm_delete(Filename, UnreferencedFiles, RegisteredIterators1),
    ?assertMatch(R1, {true, dummy_owner}),
    RegisteredIterators2 = [{dummy_pid, 10}, {dummy_pid, 12}],
    R2 = confirm_delete(Filename, UnreferencedFiles, RegisteredIterators2),
    ?assertMatch(R2, false),
    RegisteredIterators3 = [{dummy_pid, 9}, {dummy_pid, 12}],
    R3 = confirm_delete(Filename, UnreferencedFiles, RegisteredIterators3),
    ?assertMatch(R3, false).


maybe_pause_push(PCL, KL) ->
    T0 = leveled_skiplist:empty(true),
    T1 = lists:foldl(fun({K, V}, {AccSL, MinSQN, MaxSQN}) ->
                            SL = leveled_skiplist:enter(K, V, AccSL),
                            SQN = leveled_codec:strip_to_seqonly({K, V}),
                            {SL, min(SQN, MinSQN), max(SQN, MaxSQN)}
                            end,
                        {T0, infinity, 0},
                        KL),
    case pcl_pushmem(PCL, T1) of
        returned ->
            timer:sleep(50),
            maybe_pause_push(PCL, KL);
        ok ->
            ok
    end.

%% old test data doesn't have the magic hash
add_missing_hash({K, {SQN, ST, MD}}) ->
    {K, {SQN, ST, leveled_codec:magic_hash(K), MD}}.


simple_server_test() ->
    RootPath = "../test/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    Key1_Pre = {{o,"Bucket0001", "Key0001", null},
                    {1, {active, infinity}, null}},
    Key1 = add_missing_hash(Key1_Pre),
    KL1 = leveled_sft:generate_randomkeys({1000, 2}),
    Key2_Pre = {{o,"Bucket0002", "Key0002", null},
                {1002, {active, infinity}, null}},
    Key2 = add_missing_hash(Key2_Pre),
    KL2 = leveled_sft:generate_randomkeys({900, 1003}),
    % Keep below the max table size by having 900 not 1000
    Key3_Pre = {{o,"Bucket0003", "Key0003", null},
                {2003, {active, infinity}, null}},
    Key3 = add_missing_hash(Key3_Pre),
    KL3 = leveled_sft:generate_randomkeys({1000, 2004}), 
    Key4_Pre = {{o,"Bucket0004", "Key0004", null},
                {3004, {active, infinity}, null}},
    Key4 = add_missing_hash(Key4_Pre),
    KL4 = leveled_sft:generate_randomkeys({1000, 3005}),
    ok = maybe_pause_push(PCL, [Key1]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, KL1),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, [Key2]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    
    ok = maybe_pause_push(PCL, KL2),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ok = maybe_pause_push(PCL, [Key3]),
    
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCL, {o,"Bucket0003", "Key0003", null})),
    timer:sleep(200),
    % This sleep should make sure that the merge to L1 has occurred
    % This will free up the L0 slot for the remainder to be written in shutdown
    ok = pcl_close(PCL),
    
    {ok, PCLr} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    ?assertMatch(2003, pcl_getstartupsequencenumber(PCLr)),
    % ok = maybe_pause_push(PCLr, [Key2] ++ KL2 ++ [Key3]),
    
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCLr, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCLr, {o,"Bucket0003", "Key0003", null})),
    ok = maybe_pause_push(PCLr, KL3),
    ok = maybe_pause_push(PCLr, [Key4]),
    ok = maybe_pause_push(PCLr, KL4),
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCLr, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCLr, {o,"Bucket0003", "Key0003", null})),
    ?assertMatch(Key4, pcl_fetch(PCLr, {o,"Bucket0004", "Key0004", null})),
    SnapOpts = #penciller_options{start_snapshot = true,
                                    source_penciller = PCLr},
    {ok, PclSnap} = pcl_start(SnapOpts),
    leveled_bookie:load_snapshot(PclSnap,
                                    leveled_bookie:empty_ledgercache()),
    ?assertMatch(Key1, pcl_fetch(PclSnap, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PclSnap, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PclSnap, {o,"Bucket0003", "Key0003", null})),
    ?assertMatch(Key4, pcl_fetch(PclSnap, {o,"Bucket0004", "Key0004", null})),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0001",
                                                    "Key0001",
                                                    null},
                                                1)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0002",
                                                    "Key0002",
                                                    null},
                                                1002)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0003",
                                                    "Key0003",
                                                    null},
                                                2003)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0004",
                                                    "Key0004",
                                                    null},
                                                3004)),
    % Add some more keys and confirm that check sequence number still
    % sees the old version in the previous snapshot, but will see the new version
    % in a new snapshot
    Key1A_Pre = {{o,"Bucket0001", "Key0001", null},
                    {4005, {active, infinity}, null}},
    Key1A = add_missing_hash(Key1A_Pre),
    KL1A = leveled_sft:generate_randomkeys({2000, 4006}),
    ok = maybe_pause_push(PCLr, [Key1A]),
    ok = maybe_pause_push(PCLr, KL1A),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0001",
                                                    "Key0001",
                                                    null},
                                                1)),
    ok = pcl_close(PclSnap),
    
    % Ignore a fake pending mnaifest on startup
    ok = file:write_file(RootPath ++ "/" ++ ?MANIFEST_FP ++ "nonzero_99.pnd",
                            term_to_binary("Hello")),
    
    {ok, PclSnap2} = pcl_start(SnapOpts),
    leveled_bookie:load_snapshot(PclSnap2, leveled_bookie:empty_ledgercache()),
    ?assertMatch(false, pcl_checksequencenumber(PclSnap2,
                                                {o,
                                                    "Bucket0001",
                                                    "Key0001",
                                                    null},
                                                1)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap2,
                                                {o,
                                                    "Bucket0001",
                                                    "Key0001",
                                                    null},
                                                4005)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap2,
                                                {o,
                                                    "Bucket0002",
                                                    "Key0002",
                                                    null},
                                                1002)),
    ok = pcl_close(PclSnap2),
    ok = pcl_close(PCLr),
    clean_testdir(RootPath).


rangequery_manifest_test() ->
    {E1,
        E2,
        E3} = {#manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                                end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
                #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                                end_key={o, "Bucket1", "K71", null},
                                filename="Z2"},
                #manifest_entry{start_key={o, "Bucket1", "K75", null},
                                end_key={o, "Bucket1", "K993", null},
                                filename="Z3"}},
    {E4,
        E5,
        E6} = {#manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                                end_key={i, "Bucket1", {"Idx1", "Fld7"}, "K93"},
                                filename="Z4"},
                #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld7"}, "K97"},
                                end_key={o, "Bucket1", "K78", null},
                                filename="Z5"},
                #manifest_entry{start_key={o, "Bucket1", "K81", null},
                                end_key={o, "Bucket1", "K996", null},
                                filename="Z6"}},
    Man = [{1, [E1, E2, E3]}, {2, [E4, E5, E6]}],
    R1 = initiate_rangequery_frommanifest({o, "Bucket1", "K711", null},
                                            {o, "Bucket1", "K999", null},
                                            Man),
    ?assertMatch([{1, [{next_file, E3}]},
                        {2, [{next_file, E5}, {next_file, E6}]}],
                    R1),
    R2 = initiate_rangequery_frommanifest({i, "Bucket1", {"Idx1", "Fld8"}, null},
                                            {i, "Bucket1", {"Idx1", "Fld8"}, null},
                                            Man),
    ?assertMatch([{1, [{next_file, E1}]}, {2, [{next_file, E5}]}],
                    R2),
    R3 = initiate_rangequery_frommanifest({i, "Bucket1", {"Idx0", "Fld8"}, null},
                                            {i, "Bucket1", {"Idx0", "Fld9"}, null},
                                            Man),
    ?assertMatch([], R3).

print_manifest_test() ->
    M1 = #manifest_entry{start_key={i, "Bucket1", {<<"Idx1">>, "Fld1"}, "K8"},
                                end_key={i, 4565, {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
    M2 = #manifest_entry{start_key={i, self(), {null, "Fld1"}, "K8"},
                                end_key={i, <<200:32/integer>>, {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
    M3 = #manifest_entry{start_key={?STD_TAG, self(), {null, "Fld1"}, "K8"},
                                end_key={?RIAK_TAG, <<200:32/integer>>, {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
    print_manifest([{1, [M1, M2, M3]}]).

simple_findnextkey_test() ->
    QueryArray = [
    {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}},
            {{o, "Bucket1", "Key5"}, {4, {active, infinity}, null}}]},
    {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}]},
    {5, [{{o, "Bucket1", "Key2"}, {2, {active, infinity}, null}}]}
    ],
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}}, KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key2"}, {2, {active, infinity}, null}}, KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}, KV3),
    {Array5, KV4} = find_nextkey(Array4,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key5"}, {4, {active, infinity}, null}}, KV4),
    ER = find_nextkey(Array5,
                        {o, "Bucket1", "Key0"},
                        {o, "Bucket1", "Key5"}),
    ?assertMatch(no_more_keys, ER).

sqnoverlap_findnextkey_test() ->
    QueryArray = [
    {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, 0, null}},
            {{o, "Bucket1", "Key5"}, {4, {active, infinity}, 0, null}}]},
    {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, 0, null}}]},
    {5, [{{o, "Bucket1", "Key5"}, {2, {active, infinity}, 0, null}}]}
    ],
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key1"}, {5, {active, infinity}, 0, null}},
                    KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key3"}, {3, {active, infinity}, 0, null}},
                    KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key5"}, {4, {active, infinity}, 0, null}},
                    KV3),
    ER = find_nextkey(Array4,
                        {o, "Bucket1", "Key0"},
                        {o, "Bucket1", "Key5"}),
    ?assertMatch(no_more_keys, ER).

sqnoverlap_otherway_findnextkey_test() ->
    QueryArray = [
    {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, 0, null}},
            {{o, "Bucket1", "Key5"}, {1, {active, infinity}, 0, null}}]},
    {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, 0, null}}]},
    {5, [{{o, "Bucket1", "Key5"}, {2, {active, infinity}, 0, null}}]}
    ],
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key1"}, {5, {active, infinity}, 0, null}},
                    KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key3"}, {3, {active, infinity}, 0, null}},
                    KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key5"}, {2, {active, infinity}, 0, null}},
                    KV3),
    ER = find_nextkey(Array4,
                        {o, "Bucket1", "Key0"},
                        {o, "Bucket1", "Key5"}),
    ?assertMatch(no_more_keys, ER).

foldwithimm_simple_test() ->
    QueryArray = [
        {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, 0, null}},
                {{o, "Bucket1", "Key5"}, {1, {active, infinity}, 0, null}}]},
        {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, 0, null}}]},
        {5, [{{o, "Bucket1", "Key5"}, {2, {active, infinity}, 0, null}}]}
    ],
    IMM0 = leveled_skiplist:enter({o, "Bucket1", "Key6"},
                                        {7, {active, infinity}, 0, null},
                                    leveled_skiplist:empty()),
    IMM1 = leveled_skiplist:enter({o, "Bucket1", "Key1"},
                                        {8, {active, infinity}, 0, null},
                                    IMM0),
    IMM2 = leveled_skiplist:enter({o, "Bucket1", "Key8"},
                                        {9, {active, infinity}, 0, null},
                                    IMM1),
    IMMiter = leveled_skiplist:to_range(IMM2, {o, "Bucket1", "Key1"}),
    AccFun = fun(K, V, Acc) -> SQN = leveled_codec:strip_to_seqonly({K, V}),
                                Acc ++ [{K, SQN}] end,
    Acc = keyfolder(IMMiter,
                    QueryArray,
                    {o, "Bucket1", "Key1"}, {o, "Bucket1", "Key6"},
                    {AccFun, []}),
    ?assertMatch([{{o, "Bucket1", "Key1"}, 8},
                    {{o, "Bucket1", "Key3"}, 3},
                    {{o, "Bucket1", "Key5"}, 2},
                    {{o, "Bucket1", "Key6"}, 7}], Acc),
    
    IMM1A = leveled_skiplist:enter({o, "Bucket1", "Key1"},
                                        {8, {active, infinity}, 0, null},
                                    leveled_skiplist:empty()),
    IMMiterA = leveled_skiplist:to_range(IMM1A, {o, "Bucket1", "Key1"}),
    AccA = keyfolder(IMMiterA,
                    QueryArray,
                    {o, "Bucket1", "Key1"}, {o, "Bucket1", "Key6"},
                    {AccFun, []}),
    ?assertMatch([{{o, "Bucket1", "Key1"}, 8},
                    {{o, "Bucket1", "Key3"}, 3},
                    {{o, "Bucket1", "Key5"}, 2}], AccA),
    
    IMM3 = leveled_skiplist:enter({o, "Bucket1", "Key4"},
                                     {10, {active, infinity}, 0, null},
                                    IMM2),
    IMMiterB = leveled_skiplist:to_range(IMM3, {o, "Bucket1", "Key1"}),
    AccB = keyfolder(IMMiterB,
                    QueryArray,
                    {o, "Bucket1", "Key1"}, {o, "Bucket1", "Key6"},
                    {AccFun, []}),
    ?assertMatch([{{o, "Bucket1", "Key1"}, 8},
                    {{o, "Bucket1", "Key3"}, 3},
                    {{o, "Bucket1", "Key4"}, 10},
                    {{o, "Bucket1", "Key5"}, 2},
                    {{o, "Bucket1", "Key6"}, 7}], AccB).

create_file_test() ->
    Filename = "../test/new_file.sft",
    ok = file:write_file(Filename, term_to_binary("hello")),
    KVL = lists:usort(leveled_sft:generate_randomkeys(10000)),
    Tree = leveled_skiplist:from_list(KVL),
    FetchFun = fun(Slot) -> lists:nth(Slot, [Tree]) end,
    {ok,
        SP,
        noreply} = leveled_sft:sft_newfroml0cache(Filename,
                                                    1,
                                                    FetchFun,
                                                    #sft_options{wait=false}),
    lists:foreach(fun(X) ->
                        case checkready(SP) of
                            timeout ->
                                timer:sleep(X);
                            _ ->
                                ok
                        end end,
                    [50, 50, 50, 50, 50]),
    {ok, SrcFN, StartKey, EndKey} = checkready(SP),
    io:format("StartKey ~w EndKey ~w~n", [StartKey, EndKey]),
    ?assertMatch({o, _, _, _}, StartKey),
    ?assertMatch({o, _, _, _}, EndKey),
    ?assertMatch("../test/new_file.sft", SrcFN),
    ok = leveled_sft:sft_clear(SP),
    {ok, Bin} = file:read_file("../test/new_file.sft.discarded"),
    ?assertMatch("hello", binary_to_term(Bin)).

commit_manifest_test() ->
    Sent_WI = #penciller_work{next_sqn=1,
                                src_level=0,
                                start_time=os:timestamp()},
    Resp_WI = #penciller_work{next_sqn=1,
                                src_level=0},
    State = #state{ongoing_work = [Sent_WI],
                    root_path = "test",
                    manifest_sqn = 0},
    ManifestFP = "test" ++ "/" ++ ?MANIFEST_FP ++ "/",
    ok = filelib:ensure_dir(ManifestFP),
    ok = file:write_file(ManifestFP ++ "nonzero_1.pnd",
                            term_to_binary("dummy data")),
    
    L1_0 = [{1, [#manifest_entry{filename="1.sft"}]}],
    Resp_WI0 = Resp_WI#penciller_work{new_manifest=L1_0,
                                        unreferenced_files=[]},
    {ok, State0} = commit_manifest_change(Resp_WI0, State),
    ?assertMatch(1, State0#state.manifest_sqn),
    ?assertMatch([], get_item(0, State0#state.manifest, [])),
    
    L0Entry = [#manifest_entry{filename="0.sft"}],
    ManifestPlus = [{0, L0Entry}|State0#state.manifest],
    
    NxtSent_WI = #penciller_work{next_sqn=2,
                                    src_level=1,
                                    start_time=os:timestamp()},
    NxtResp_WI = #penciller_work{next_sqn=2,
                                 src_level=1},
    State1 = State0#state{ongoing_work=[NxtSent_WI],
                            manifest = ManifestPlus},
    
    ok = file:write_file(ManifestFP ++ "nonzero_2.pnd",
                            term_to_binary("dummy data")),
    
    L2_0 = [#manifest_entry{filename="2.sft"}],
    NxtResp_WI0 = NxtResp_WI#penciller_work{new_manifest=[{2, L2_0}],
                                           unreferenced_files=[]},
    {ok, State2} = commit_manifest_change(NxtResp_WI0, State1),
    
    ?assertMatch(1, State1#state.manifest_sqn),
    ?assertMatch(2, State2#state.manifest_sqn),
    ?assertMatch(L0Entry, get_item(0, State2#state.manifest, [])),
    ?assertMatch(L2_0, get_item(2, State2#state.manifest, [])),
    
    clean_testdir(State#state.root_path).


badmanifest_test() ->
    RootPath = "../test/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    Key1_pre = {{o,"Bucket0001", "Key0001", null},
                {1001, {active, infinity}, null}},
    Key1 = add_missing_hash(Key1_pre),
    KL1 = leveled_sft:generate_randomkeys({1000, 1}),
    
    ok = maybe_pause_push(PCL, KL1 ++ [Key1]),
    %% Added together, as split apart there will be a race between the close
    %% call to the penciller and the second fetch of the cache entry
    ?assertMatch(Key1, pcl_fetch(PCL, {o, "Bucket0001", "Key0001", null})),
    
    timer:sleep(100), % Avoids confusion if L0 file not written before close
    ok = pcl_close(PCL),
    
    ManifestFP = filepath(RootPath, manifest),
    ok = file:write_file(filename:join(ManifestFP, "yeszero_123.man"),
                            term_to_binary("hello")),
    {ok, PCLr} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ok = pcl_close(PCLr),
    clean_testdir(RootPath).

checkready(Pid) ->
    try
        leveled_sft:sft_checkready(Pid)
    catch
        exit:{timeout, _} ->
            timeout
    end.

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).

-endif.
