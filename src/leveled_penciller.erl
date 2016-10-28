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
%5 the journal.
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
%% - L Minus 1: Used to cache the last ledger cache push for use in queries
%% whilst the Penciller awaits a callback from the roll_clerk with the new
%% merged L0 file containing the L-1 updates.
%%
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
%% When the clerk has completed the work it cna request that the manifest
%% change be committed by the Penciller.  The commit is made through changing
%% the filename of the new manifest - so the Penciller is not held up by the
%% process of wiritng a file, just altering file system metadata.
%%
%% The manifest is locked by a clerk taking work, or by there being a need to
%% write a file to Level 0.  If the manifest is locked, then new keys can still
%% be added in memory - however, the response to that push will be to "pause",
%% that is to say the Penciller will ask the Bookie to slowdown.
%%
%% ---------- PUSH ----------
%%
%% The Penciller must support the PUSH of a dump of keys from the Bookie.  The
%% call to PUSH should be immediately acknowledged, and then work should be
%% completed to merge the tree into the L0 tree (with the tree being cached as
%% a Level -1 tree so as not to block reads whilst it waits.
%%
%% The Penciller MUST NOT accept a new PUSH if the Clerk has commenced the
%% conversion of the current ETS table into a SFT file, but not completed this
%% change.  The Penciller in this case returns the push, and the Bookie should
%% continue to gorw the cache before trying again.
%%
%% ---------- FETCH ----------
%%
%% On request to fetch a key the Penciller should look first in the L0 ETS 
%% table, and then look in the SFT files Level by Level, consulting the
%% Manifest to determine which file should be checked at each level.
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
%% has reached capacity.  When there is a next push into memory the Penciller
%% calls to check that the file is now active (which may pause if the write is
%% ongoing the acceptence of the push), and if so it can clear the L0 tree
%% and build a new tree from an empty tree and the keys from the latest push.
%%
%% Only a single L0 file may exist at any one moment in time.  If pushes are
%% received when memory is over the maximum size, the pushes must be kept into
%% memory.
%%
%% 1 - A L0 file is prompted to be created at ManifestSQN n
%% 2 - The next push to memory will be stalled until the L0 write is reported
%% as completed (as the memory needs to be flushed)
%% 3 - The completion of the L0 file will cause a prompt to be cast to the
%% clerk for them to look for work
%% 4 - On completion of the merge (of the L0 file into L1, as this will be the
%% highest priority work), the clerk will create a new manifest file at
%% manifest SQN n+1
%% 5 - The clerk will prompt the penciller about the change, and the Penciller
%% will then commit the change (by renaming the manifest file to be active, and
%% advancing the in-memory state of the manifest and manifest SQN)
%% 6 - The Penciller having committed the change will cast back to the Clerk
%% to inform the Clerk that the chnage has been committed, and so it can carry
%% on requetsing new work
%% 7 - If the Penciller now receives a Push to over the max size, a new L0 file
%% can now be created with the ManifestSQN of n+1
%%
%% ---------- NOTES ON THE (NON) USE OF ETS ----------
%%
%% Insertion into ETS is very fast, and so using ETS does not slow the PUT
%% path.  However, an ETS table is mutable, so it does complicate the
%% snapshotting of the Ledger.
%%
%% Originally the solution had used an ETS tbale for insertion speed as the L0
%% cache.  Insertion speed was an order or magnitude faster than gb_trees.  To
%% resolving issues of trying to have fast start-up snapshots though led to
%% keeping a seperate set of trees alongside the ETS table to be used by
%% snapshots.
%%
%% The current strategy is to perform the expensive operation (merging the
%% Ledger cache into the Level0 cache), within a dedicated Penciller's clerk,
%% known as the roll_clerk.  This may take 30-40ms, but during this period
%% the Penciller will keep a Level -1 cache of the unmerged elements which
%% it will wipe once the roll_clerk returns with an updated L0 cache.
%%
%% This means that the in-memory cache is now using immutable objects, and
%% so can be shared with snapshots.


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
        pcl_fetch/2,
        pcl_fetchkeys/5,
        pcl_checksequencenumber/3,
        pcl_workforclerk/1,
        pcl_promptmanifestchange/2,
        pcl_confirmdelete/2,
        pcl_close/1,
        pcl_registersnapshot/2,
        pcl_releasesnapshot/2,
        pcl_updatelevelzero/4,
        pcl_loadsnapshot/2,
        pcl_getstartupsequencenumber/1,
        roll_new_tree/3,
        roll_into_list/1,
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
-define(MAX_TABLESIZE, 32000).
-define(PROMPT_WAIT_ONL0, 5).


-record(state, {manifest = [] :: list(),
				manifest_sqn = 0 :: integer(),
                ledger_sqn = 0 :: integer(),
                registered_snapshots = [] :: list(),
                unreferenced_files = [] :: list(),
                root_path = "../test" :: string(),
                
                merge_clerk :: pid(),
                roll_clerk :: pid(),
                
                levelzero_pending = false :: boolean(),
                levelzero_constructor :: pid(),
                levelzero_cache = gb_trees:empty() :: gb_trees:tree(),
                levelzero_cachesize = 0 :: integer(),
                levelzero_maxcachesize :: integer(),
                
                levelminus1_active = false :: boolean(),
                levelminus1_cache = gb_trees:empty() :: gb_trees:tree(),
                
                is_snapshot = false :: boolean(),
                snapshot_fully_loaded = false :: boolean(),
                source_penciller :: pid(),
                
                ongoing_work = [] :: list()}).


%%%============================================================================
%%% API
%%%============================================================================

 
pcl_start(PCLopts) ->
    gen_server:start(?MODULE, [PCLopts], []).

pcl_pushmem(Pid, DumpList) ->
    %% Bookie to dump memory onto penciller
    gen_server:call(Pid, {push_mem, DumpList}, infinity).
    
pcl_fetch(Pid, Key) ->
    gen_server:call(Pid, {fetch, Key}, infinity).

pcl_fetchkeys(Pid, StartKey, EndKey, AccFun, InitAcc) ->
    gen_server:call(Pid,
                    {fetch_keys, StartKey, EndKey, AccFun, InitAcc},
                    infinity).

pcl_checksequencenumber(Pid, Key, SQN) ->
    gen_server:call(Pid, {check_sqn, Key, SQN}, infinity).

pcl_workforclerk(Pid) ->
    gen_server:call(Pid, work_for_clerk, infinity).

pcl_promptmanifestchange(Pid, WI) ->
    gen_server:cast(Pid, {manifest_change, WI}).

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

pcl_updatelevelzero(Pid, L0Cache, L0Size, L0SQN) ->
    gen_server:cast(Pid, {load_levelzero, L0Cache, L0Size, L0SQN}).

pcl_close(Pid) ->
    gen_server:call(Pid, close, 60000).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([PCLopts]) ->
    case {PCLopts#penciller_options.root_path,
            PCLopts#penciller_options.start_snapshot} of
        {undefined, true} ->
            SrcPenciller = PCLopts#penciller_options.source_penciller,
            io:format("Registering ledger snapshot~n"),
            {ok, State} = pcl_registersnapshot(SrcPenciller, self()),
            io:format("Lesger snapshot registered~n"),
            {ok, State#state{is_snapshot=true, source_penciller=SrcPenciller}};
            %% Need to do something about timeout
        {_RootPath, false} ->
            start_from_file(PCLopts)
    end.    
    

handle_call({push_mem, PushedTree}, From, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->
    % The push_mem process is as follows:
    %
    % 1 - Receive a gb_tree containing the latest Key/Value pairs (note that
    % we mean value from the perspective of the Ledger, not the full value
    % stored in the Inker)
    %
    % 2 - Check to see if the levelminus1 cache is active, if so the update
    % cannot yet be accepted and must be returned (so the Bookie will maintain
    % the cache and try again later).
    %
    % 3 - Check to see if there is a levelzero file pending.  If so check if
    % the levelzero file is complete.  If it is complete, the levelzero tree
    % can be flushed, the in-memory manifest updated, and the new tree can
    % be accepted as the new levelzero cache.  If not, the update must be
    % returned.
    %
    % 3 - Make the new tree the levelminus1 cache, and mark this as active
    %
    % 4 - The Penciller can now reply to the Bookie to show that the push has
    % been accepted
    %
    % 5 - A background worker clerk can now be triggered to produce a new
    % levelzero cache (tree) containing the level minus 1 tree.  When this
    % completes it will cast back the updated tree, and on receipt of this
    % the Penciller may:
    % a) Clear down the levelminus1 cache
    % b) Determine if the cache is full and it is necessary to build a new
    % persisted levelzero file

    SW = os:timestamp(),
    
    S = case {State#state.levelzero_pending,
                State#state.levelminus1_active} of
            {_, true} ->
                log_pushmem_reply(From, {returned, "L-1 Active"}, SW),
                State;
            {true, _} ->
                L0Pid = State#state.levelzero_constructor,
                case checkready(L0Pid) of
                    timeout ->
                        log_pushmem_reply(From,
                                            {returned,
                                                "L-0 persist pending"},
                                            SW),
                        State;
                    {ok, SrcFN, StartKey, EndKey} ->
                        log_pushmem_reply(From, ok, SW),
                        ManEntry = #manifest_entry{start_key=StartKey,
                                                    end_key=EndKey,
                                                    owner=L0Pid,
                                                    filename=SrcFN},
                        % Prompt clerk to ask about work - do this for
                        % every L0 roll
                        ok = leveled_pclerk:mergeclerk_prompt(State#state.merge_clerk),
                        UpdMan = lists:keystore(0,
                                                1,
                                                State#state.manifest,
                                                {0, [ManEntry]}),
                        {MinSQN, MaxSQN, Size, _L} = assess_sqn(PushedTree),
                        if
                            MinSQN > State#state.ledger_sqn ->
                                State#state{manifest=UpdMan,
                                                levelzero_cache=PushedTree,
                                                levelzero_cachesize=Size,
                                                levelzero_pending=false,
                                                ledger_sqn=MaxSQN}
                        end
                end;
            {false, false} ->
                log_pushmem_reply(From, ok, SW),
                ok = leveled_pclerk:rollclerk_levelzero(State#state.roll_clerk,
                                                        State#state.levelzero_cache,
                                                        PushedTree,
                                                        State#state.ledger_sqn,
                                                        self()),
                State#state{levelminus1_active=true,
                                levelminus1_cache=PushedTree}
        end,
    io:format("Handling of push completed in ~w microseconds with "
                    ++ "L0 cache size now ~w~n",
                [timer:now_diff(os:timestamp(), SW),
                    S#state.levelzero_cachesize]),       
    {noreply, S};
handle_call({fetch, Key}, _From, State) ->
    {reply,
        fetch(Key,
                State#state.manifest,
                State#state.levelminus1_active,
                State#state.levelminus1_cache,
                State#state.levelzero_cache),
        State};
handle_call({check_sqn, Key, SQN}, _From, State) ->
    {reply,
        compare_to_sqn(fetch(Key,
                                State#state.manifest,
                                State#state.levelminus1_active,
                                State#state.levelminus1_cache,
                                State#state.levelzero_cache),
                        SQN),
        State};
handle_call({fetch_keys, StartKey, EndKey, AccFun, InitAcc},
                _From,
                State=#state{snapshot_fully_loaded=Ready})
                                                        when Ready == true ->
    L0iter = gb_trees:iterator_from(StartKey, State#state.levelzero_cache),
    SFTiter = initiate_rangequery_frommanifest(StartKey,
                                                EndKey,
                                                State#state.manifest),
    Acc = keyfolder(L0iter, SFTiter, StartKey, EndKey, {AccFun, InitAcc}),
    {reply, Acc, State};
handle_call(work_for_clerk, From, State) ->
    {UpdState, Work} = return_work(State, From),
    {reply, Work, UpdState};
handle_call(get_startup_sqn, _From, State) ->
    {reply, State#state.ledger_sqn, State};
handle_call({register_snapshot, Snapshot}, _From, State) ->
    Rs = [{Snapshot, State#state.manifest_sqn}|State#state.registered_snapshots],
    {reply, {ok, State}, State#state{registered_snapshots = Rs}};
handle_call({load_snapshot, BookieIncrTree}, _From, State) ->
    {L0T0, _, L0SQN0} = case State#state.levelminus1_active of
                            true ->
                                roll_new_tree(State#state.levelzero_cache,
                                                State#state.levelminus1_cache,
                                                State#state.ledger_sqn);
                            false ->
                                {State#state.levelzero_cache,
                                    0,
                                    State#state.ledger_sqn}
                        end,
    {L0T1, _, L0SQN1} = roll_new_tree(L0T0,
                                        BookieIncrTree,
                                        L0SQN0),
    io:format("Ledger snapshot loaded with increments to start at SQN=~w~n",
                [L0SQN1]),
    {reply, ok, State#state{levelzero_cache=L0T1,
                            ledger_sqn=L0SQN1,
                            levelminus1_active=false,
                            snapshot_fully_loaded=true}};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({manifest_change, WI}, State) ->
    {ok, UpdState} = commit_manifest_change(WI, State),
    ok = leveled_pclerk:mergeclerk_manifestchange(State#state.merge_clerk,
                                                    confirm,
                                                    false),
    {noreply, UpdState};
handle_cast({release_snapshot, Snapshot}, State) ->
    Rs = lists:keydelete(Snapshot, 1, State#state.registered_snapshots),
    io:format("Ledger snapshot ~w released~n", [Snapshot]),
    io:format("Remaining ledger snapshots are ~w~n", [Rs]),
    {noreply, State#state{registered_snapshots=Rs}};
handle_cast({confirm_delete, FileName}, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->    
    Reply = confirm_delete(FileName,
                            State#state.unreferenced_files,
                            State#state.registered_snapshots),
    case Reply of
        {true, Pid} ->
            UF1 = lists:keydelete(FileName, 1, State#state.unreferenced_files),
            io:format("Filename ~s removed from unreferenced files as delete "
                            ++ "is confirmed - file should now close~n",
                        [FileName]),
            ok = leveled_sft:sft_deleteconfirmed(Pid),
            {noreply, State#state{unreferenced_files=UF1}};
        _ ->
            {noreply, State}
    end;
handle_cast({load_levelzero, L0Cache, L0Size, L0SQN}, State) ->
    if
        L0SQN >= State#state.ledger_sqn ->
            CacheTooBig = L0Size > State#state.levelzero_maxcachesize,
            Level0Free = length(get_item(0, State#state.manifest, [])) == 0,
            case {CacheTooBig, Level0Free} of
                {true, true}  ->
                    L0Constructor = roll_memory(State, L0Cache),        
                    {noreply,
                        State#state{levelminus1_active=false,
                                    levelminus1_cache=gb_trees:empty(),
                                    levelzero_cache=L0Cache,
                                    levelzero_cachesize=L0Size,
                                    levelzero_pending=true,
                                    levelzero_constructor=L0Constructor,
                                    ledger_sqn=L0SQN}};
                _ ->
                    {noreply,
                        State#state{levelminus1_active=false,
                                    levelminus1_cache=gb_trees:empty(),
                                    levelzero_cache=L0Cache,
                                    levelzero_cachesize=L0Size,
                                    ledger_sqn=L0SQN}}
            end;
        L0Size == 0 ->
            {noreply,
                    State#state{levelminus1_active=false,
                                levelminus1_cache=gb_trees:empty(),
                                levelzero_cache=L0Cache,
                                levelzero_cachesize=L0Size}}
    end.

handle_info({_Ref, {ok, SrcFN, _StartKey, _EndKey}}, State) ->
    io:format("Orphaned reply after timeout on L0 file write ~s~n", [SrcFN]),
    {noreply, State}.

terminate(Reason, State=#state{is_snapshot=Snap}) when Snap == true ->
    ok = pcl_releasesnapshot(State#state.source_penciller, self()),
    io:format("Sent release message for cloned Penciller following close for "
                ++ "reason ~w~n", [Reason]),   
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
    io:format("Penciller closing for reason - ~w~n", [Reason]),
    MC = leveled_pclerk:mergeclerk_manifestchange(State#state.merge_clerk,
                                                    return,
                                                    true),
    UpdState = case MC of
                    {ok, WI} ->
                        {ok, NewState} = commit_manifest_change(WI, State),
                        Clerk = State#state.merge_clerk,
                        ok = leveled_pclerk:mergeclerk_manifestchange(Clerk,
                                                                        confirm,
                                                                        true),
                        NewState;
                    no_change ->
                        State
                end,
    case {UpdState#state.levelzero_pending,
            get_item(0, State#state.manifest, []),
                gb_trees:size(State#state.levelzero_cache)} of
        {true, [], _} ->
            ok = leveled_sft:sft_close(State#state.levelzero_constructor);
        {false, [], 0} ->
            io:format("Level 0 cache empty at close of Penciller~n");
        {false, [], _N} ->
            KL = roll_into_list(State#state.levelzero_cache),
            L0Pid = roll_memory(UpdState, KL, true),
            ok = leveled_sft:sft_close(L0Pid);
        _ ->
            io:format("No level zero action on close of Penciller~n")
    end,
    
    leveled_pclerk:rollclerk_close(State#state.roll_clerk),
    
    % Tidy shutdown of individual files
    ok = close_files(0, UpdState#state.manifest),
    lists:foreach(fun({_FN, Pid, _SN}) ->
                            ok = leveled_sft:sft_close(Pid) end,
                    UpdState#state.unreferenced_files),
    io:format("Shutdown complete for Penciller~n"),
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
    
    {ok, MergeClerk} = leveled_pclerk:clerk_new(self(), merge),
    {ok, RollClerk} = leveled_pclerk:clerk_new(self(), roll),
    InitState = #state{merge_clerk=MergeClerk,
                        roll_clerk=RollClerk,
                        root_path=RootPath,
                        levelzero_maxcachesize=MaxTableSize},
    
    %% Open manifest
    ManifestPath = InitState#state.root_path ++ "/" ++ ?MANIFEST_FP ++ "/",
    {ok, Filenames} = case filelib:is_dir(ManifestPath) of
                            true ->
                                file:list_dir(ManifestPath);
                            false ->
                                {ok, []}
                        end,
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
    ManUpdate = case TopManSQN of
                    0 ->
                        io:format("Seqence number of 0 indicates no valid " ++
                                    "manifest~n"),
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
    io:format("Maximum sequence number of ~w found in nonzero levels~n",
                [MaxSQN]),
            
    %% Find any L0 files
    L0FN = filepath(RootPath, TopManSQN, new_merge_files) ++ "_0_0.sft",
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
    end.


log_pushmem_reply(From, Reply, SW) ->
    io:format("Respone to push_mem of ~w took ~w microseconds~n",
        [Reply, timer:now_diff(os:timestamp(), SW)]),
    gen_server:reply(From, Reply).

checkready(Pid) ->
    try
        leveled_sft:sft_checkready(Pid)
    catch
        exit:{timeout, _} ->
            timeout
    end.

roll_memory(State, L0Tree) ->
    roll_memory(State, L0Tree, false).

roll_memory(State, L0Tree, Wait) ->
    MSN = State#state.manifest_sqn,
    FileName = State#state.root_path
                ++ "/" ++ ?FILES_FP ++ "/"
                ++ integer_to_list(MSN) ++ "_0_0",
    Opts = #sft_options{wait=Wait},
    {ok, Constructor, _} = leveled_sft:sft_new(FileName, L0Tree, [], 0, Opts),
    Constructor.

% Merge the Level Minus 1 tree to the Level 0 tree, incrementing the
% SQN, and ensuring all entries do increment the SQN

roll_new_tree(L0Cache, LMinus1Cache, LedgerSQN) ->
    {MinSQN, MaxSQN, Size, LMinus1List} = assess_sqn(LMinus1Cache),
    if
        MinSQN >= LedgerSQN ->
            UpdTree = lists:foldl(fun({Kx, Vx}, TreeAcc) ->
                                        gb_trees:enter(Kx, Vx, TreeAcc)
                                        end,
                                    L0Cache,
                                    LMinus1List),
            {UpdTree, gb_trees:size(UpdTree), MaxSQN};
        Size == 0 ->
            {L0Cache, gb_trees:size(L0Cache), LedgerSQN}
    end.

%% This takes the three parts of a memtable copy - the increments, the tree
%% and the SQN at which the tree was formed, and outputs a sorted list
roll_into_list(Tree) ->
    gb_trees:to_list(Tree).

assess_sqn(Tree) ->
    L = roll_into_list(Tree),
    FoldFun = fun(KV, {AccMinSQN, AccMaxSQN, AccSize}) ->
                    SQN = leveled_codec:strip_to_seqonly(KV),
                    {min(SQN, AccMinSQN), max(SQN, AccMaxSQN), AccSize + 1}
                    end,
    {MinSQN, MaxSQN, Size} = lists:foldl(FoldFun, {infinity, 0, 0}, L),
    {MinSQN, MaxSQN, Size, L}.


fetch(Key, Manifest, LM1Active, LM1Tree, L0Tree) ->
    case LM1Active of
        true ->
            case gb_trees:lookup(Key, LM1Tree) of
                none ->
                    case gb_trees:lookup(Key, L0Tree) of
                        none ->
                            fetch(Key, Manifest, 0, fun leveled_sft:sft_get/2);
                        {value, Value} ->
                            {Key, Value}
                    end;
                {value, Value} ->
                    {Key, Value}
            end;
        false ->
            case gb_trees:lookup(Key, L0Tree) of
                none ->
                    fetch(Key, Manifest, 0, fun leveled_sft:sft_get/2);
                {value, Value} ->
                    {Key, Value}
            end
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
            [{SrcLevel, Manifest}|OtherWork] = WorkQ,
            Backlog = length(OtherWork),
            io:format("Work at Level ~w to be scheduled for ~w with ~w " ++
                        "queue items outstanding~n",
                        [SrcLevel, From, Backlog]),
            IsBasement = if
                                SrcLevel + 1 == BasementL ->
                                    true;
                                true ->
                                    false
                            end,
            case State#state.levelzero_pending of
                true ->
                    % Once the L0 file is completed there will be more work
                    % - so don't be busy doing other work now
                    io:format("Allocation of work blocked as L0 pending~n"),
                    {State, none};
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
                    {State#state{ongoing_work=[WI]}, WI}
            end;
        _ ->
            {State, none}
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
                        io:format("Manifest at Level ~w~n", [L]),
                        Level = get_item(L, Manifest, []),
                        lists:foreach(fun print_manifest_entry/1, Level)
                        end,
                    lists:seq(0, ?MAX_LEVELS - 1)),
    ok.

print_manifest_entry(Entry) ->
    {S1, S2, S3,
        FS2, FS3} = leveled_codec:print_key(Entry#manifest_entry.start_key),
    {E1, E2, E3,
        FE2, FE3} = leveled_codec:print_key(Entry#manifest_entry.end_key),
    io:format("Manifest entry of " ++ 
                "startkey ~s " ++ FS2 ++ " " ++ FS3 ++
                " endkey ~s " ++ FE2 ++ " " ++ FE3 ++
                " filename=~s~n",
        [S1, S2, S3, E1, E2, E3,
            Entry#manifest_entry.filename]).

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
                    io:format("Key at level ~w with SQN ~w is better than " ++
                                    "key at lower level ~w with SQN ~w~n",
                                [LCnt, SQN, BKL, BestSQN]),   
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


keyfolder(null, SFTiterator, StartKey, EndKey, {AccFun, Acc}) ->
    case find_nextkey(SFTiterator, StartKey, EndKey) of
        no_more_keys ->
            Acc;
        {NxtSFTiterator, {SFTKey, SFTVal}} ->
            Acc1 = AccFun(SFTKey, SFTVal, Acc),
            keyfolder(null, NxtSFTiterator, StartKey, EndKey, {AccFun, Acc1})
    end;
keyfolder(IMMiterator, SFTiterator, StartKey, EndKey, {AccFun, Acc}) ->
    case gb_trees:next(IMMiterator) of
        none ->
            % There are no more keys in the in-memory iterator, so now
            % iterate only over the remaining keys in the SFT iterator
            keyfolder(null, SFTiterator, StartKey, EndKey, {AccFun, Acc});
        {IMMKey, IMMVal, NxtIMMiterator} ->
            case leveled_codec:endkey_passed(EndKey, IMMKey) of
                true ->
                    % There are no more keys in-range in the in-memory
                    % iterator, so take action as if this iterator is empty
                    % (see above)
                    keyfolder(null, SFTiterator,
                                    StartKey, EndKey, {AccFun, Acc});
                false ->
                    case find_nextkey(SFTiterator, StartKey, EndKey) of
                        no_more_keys ->
                            % No more keys in range in the persisted store, so use the
                            % in-memory KV as the next
                            Acc1 = AccFun(IMMKey, IMMVal, Acc),
                            keyfolder(NxtIMMiterator, SFTiterator,
                                            StartKey, EndKey, {AccFun, Acc1});
                        {NxtSFTiterator, {SFTKey, SFTVal}} ->
                            % There is a next key, so need to know which is the
                            % next key between the two (and handle two keys
                            % with different sequence numbers).  
                            case leveled_codec:key_dominates({IMMKey,
                                                                    IMMVal},
                                                                {SFTKey,
                                                                    SFTVal}) of
                                left_hand_first ->
                                    Acc1 = AccFun(IMMKey, IMMVal, Acc),
                                    keyfolder(NxtIMMiterator, SFTiterator,
                                                    StartKey, EndKey,
                                                    {AccFun, Acc1});
                                right_hand_first ->
                                    Acc1 = AccFun(SFTKey, SFTVal, Acc),
                                    keyfolder(IMMiterator, NxtSFTiterator,
                                                    StartKey, EndKey,
                                                    {AccFun, Acc1});
                                left_hand_dominant ->
                                    Acc1 = AccFun(IMMKey, IMMVal, Acc),
                                    keyfolder(NxtIMMiterator, NxtSFTiterator,
                                                    StartKey, EndKey,
                                                    {AccFun, Acc1})
                            end
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
            WISrcLevel = SentWorkItem#penciller_work.src_level,
            io:format("Merge to sqn ~w completed in ~w microseconds " ++
                        "from Level ~w~n",
                        [SentWorkItem#penciller_work.next_sqn,
                            MTime,
                            WISrcLevel]),
            ok = rename_manifest_files(RootPath, NewMSN),
            FilesToDelete = ReturnedWorkItem#penciller_work.unreferenced_files,
            UnreferencedFilesUpd = update_deletions(FilesToDelete,
                                                        NewMSN,
                                                        UnreferencedFiles),
            io:format("Merge has been commmitted at sequence number ~w~n",
                        [NewMSN]),
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
                                unreferenced_files=UnreferencedFilesUpd}};
        {MaybeWrongMSN, From} ->
            io:format("Merge commit at sqn ~w not matched to expected" ++
                        " sqn ~w from Clerk ~w~n",
                        [NewMSN, MaybeWrongMSN, From]),
            {error, State}
    end.


rename_manifest_files(RootPath, NewMSN) ->
    OldFN = filepath(RootPath, NewMSN, pending_manifest),
    NewFN = filepath(RootPath, NewMSN, current_manifest),
    io:format("Rename of manifest from ~s ~w to ~s ~w~n",
                [OldFN,
                    filelib:is_file(OldFN),
                    NewFN,
                    filelib:is_file(NewFN)]),
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
    io:format("Adding cleared file ~s to deletion list ~n",
                [ClearedFile#manifest_entry.filename]),
    update_deletions(Tail,
                        MSN,
                        lists:append(UnreferencedFiles,
                            [{ClearedFile#manifest_entry.filename,
                                ClearedFile#manifest_entry.owner,
                                MSN}])).

confirm_delete(Filename, UnreferencedFiles, RegisteredSnapshots) ->
    case lists:keyfind(Filename, 1, UnreferencedFiles) of
        false ->
            false;
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
                                case file:delete(File) of
                                    ok -> io:format("Success deleting ~s~n", [File]);
                                    _ -> io:format("Error deleting ~s~n", [File])
                                end end,
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
    ?assertMatch(WorkQ1, [{0, Manifest}]),
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
    ?assertMatch(WorkQ3, [{1, Manifest3}]).

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
    T0 = gb_trees:empty(),
    T1 = lists:foldl(fun({K, V}, Acc) -> gb_trees:enter(K, V, Acc) end,
                        T0,
                        KL),
    case pcl_pushmem(PCL, T1) of
        {returned, _Reason} ->
            timer:sleep(50),
            maybe_pause_push(PCL, KL);
        ok ->
            ok
    end.

simple_server_test() ->
    RootPath = "../test/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    Key1 = {{o,"Bucket0001", "Key0001", null}, {1, {active, infinity}, null}},
    KL1 = leveled_sft:generate_randomkeys({1000, 2}),
    Key2 = {{o,"Bucket0002", "Key0002", null}, {1002, {active, infinity}, null}},
    KL2 = leveled_sft:generate_randomkeys({1000, 1003}),
    Key3 = {{o,"Bucket0003", "Key0003", null}, {2003, {active, infinity}, null}},
    KL3 = leveled_sft:generate_randomkeys({1000, 2004}),
    Key4 = {{o,"Bucket0004", "Key0004", null}, {3004, {active, infinity}, null}},
    KL4 = leveled_sft:generate_randomkeys({1000, 3005}),
    ok = maybe_pause_push(PCL, [Key1]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, KL1),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, [Key2]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    
    ok = maybe_pause_push(PCL, KL2),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ok = maybe_pause_push(PCL, [Key3]),
    
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCL, {o,"Bucket0003", "Key0003", null})),
    ok = pcl_close(PCL),
    
    {ok, PCLr} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    ?assertMatch(1001, pcl_getstartupsequencenumber(PCLr)),
    ok = maybe_pause_push(PCLr, [Key2] ++ KL2 ++ [Key3]),
    io:format("Back to starting position with lost data recovered~n"),
    
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
    ok = pcl_loadsnapshot(PclSnap, gb_trees:empty()),
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
    Key1A = {{o,"Bucket0001", "Key0001", null}, {4005, {active, infinity}, null}},
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
    
    {ok, PclSnap2} = pcl_start(SnapOpts),
    ok = pcl_loadsnapshot(PclSnap2, gb_trees:empty()),
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
    {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}},
            {{o, "Bucket1", "Key5"}, {4, {active, infinity}, null}}]},
    {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}]},
    {5, [{{o, "Bucket1", "Key5"}, {2, {active, infinity}, null}}]}
    ],
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}}, KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}, KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key5"}, {4, {active, infinity}, null}}, KV3),
    ER = find_nextkey(Array4,
                        {o, "Bucket1", "Key0"},
                        {o, "Bucket1", "Key5"}),
    ?assertMatch(no_more_keys, ER).

sqnoverlap_otherway_findnextkey_test() ->
    QueryArray = [
    {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}},
            {{o, "Bucket1", "Key5"}, {1, {active, infinity}, null}}]},
    {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}]},
    {5, [{{o, "Bucket1", "Key5"}, {2, {active, infinity}, null}}]}
    ],
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}}, KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}, KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0"},
                                    {o, "Bucket1", "Key5"}),
    ?assertMatch({{o, "Bucket1", "Key5"}, {2, {active, infinity}, null}}, KV3),
    ER = find_nextkey(Array4,
                        {o, "Bucket1", "Key0"},
                        {o, "Bucket1", "Key5"}),
    ?assertMatch(no_more_keys, ER).

foldwithimm_simple_test() ->
    QueryArray = [
        {2, [{{o, "Bucket1", "Key1"}, {5, {active, infinity}, null}},
                {{o, "Bucket1", "Key5"}, {1, {active, infinity}, null}}]},
        {3, [{{o, "Bucket1", "Key3"}, {3, {active, infinity}, null}}]},
        {5, [{{o, "Bucket1", "Key5"}, {2, {active, infinity}, null}}]}
    ],
    IMM0 = gb_trees:enter({o, "Bucket1", "Key6"},
                                {7, {active, infinity}, null},
                            gb_trees:empty()),
    IMM1 = gb_trees:enter({o, "Bucket1", "Key1"},
                                {8, {active, infinity}, null},
                            IMM0),
    IMM2 = gb_trees:enter({o, "Bucket1", "Key8"},
                                {9, {active, infinity}, null},
                            IMM1),
    IMMiter = gb_trees:iterator_from({o, "Bucket1", "Key1"}, IMM2),
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
    
    IMM1A = gb_trees:enter({o, "Bucket1", "Key1"},
                                {8, {active, infinity}, null},
                            gb_trees:empty()),
    IMMiterA = gb_trees:iterator_from({o, "Bucket1", "Key1"}, IMM1A),
    AccA = keyfolder(IMMiterA,
                    QueryArray,
                    {o, "Bucket1", "Key1"}, {o, "Bucket1", "Key6"},
                    {AccFun, []}),
    ?assertMatch([{{o, "Bucket1", "Key1"}, 8},
                    {{o, "Bucket1", "Key3"}, 3},
                    {{o, "Bucket1", "Key5"}, 2}], AccA),
    
    IMM3 = gb_trees:enter({o, "Bucket1", "Key4"},
                                {10, {active, infinity}, null},
                            IMM2),
    IMMiterB = gb_trees:iterator_from({o, "Bucket1", "Key1"}, IMM3),
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
    {KL1, KL2} = {lists:sort(leveled_sft:generate_randomkeys(10000)), []},
    {ok, SP, noreply} = leveled_sft:sft_new(Filename,
                                                KL1,
                                                KL2,
                                                0,
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

coverage_test() ->
    RootPath = "../test/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    Key1 = {{o,"Bucket0001", "Key0001", null}, {1, {active, infinity}, null}},
    KL1 = leveled_sft:generate_randomkeys({1000, 2}),
    ok = maybe_pause_push(PCL, [Key1]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, KL1),
    ok = pcl_close(PCL),
    ManifestFP = filepath(RootPath, manifest),
    file:write_file(ManifestFP ++ "/yeszero_123.man", term_to_binary("hello")),
    {ok, PCLr} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ok = pcl_close(PCLr),
    clean_testdir(RootPath).

-endif.