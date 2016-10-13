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
%% - The accepts new dumps (in the form of lists of keys) from the Bookie, and
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
%% - L0: New keys are received from the Bookie and merged into a single ETS
%% table, until that table is the size of a SFT file, and it is then persisted
%% as a SFT file at this level.  L0 SFT files can be larger than the normal 
%% maximum size - so we don't have to consider problems of either having more
%% than one L0 file (and handling what happens on a crash between writing the
%% files when the second may have overlapping sequence numbers), or having a
%% remainder with overlapping in sequence numbers in memory after the file is
%% written.   Once the persistence is completed, the ETS table can be erased.
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
%% Iterators may request a snapshot of the database.  A snapshot is a cloned
%% Penciller seeded not from disk, but by the in-memory ETS table and the
%% in-memory manifest.

%% To provide a snapshot the Penciller must snapshot the ETS table.  The
%% snapshot of the ETS table is managed by the Penciller storing a list of the
%% batches of Keys which have been pushed to the Penciller, and it is expected
%% that this will be converted by the clone into a gb_tree.  The clone may
%% then update the master Penciller with the gb_tree to be cached and used by
%% other cloned processes. 
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
%% ---------- NOTES ON THE USE OF ETS ----------
%%
%% Insertion into ETS is very fast, and so using ETS does not slow the PUT
%% path.  However, an ETS table is mutable, so it does complicate the
%% snapshotting of the Ledger.
%%
%% Some alternatives have been considered:
%%
%% A1 - Use gb_trees not ETS table
%% * Speed of inserts are too slow especially as the Bookie is blocked until
%% the insert is complete.  Inserting 32K very simple keys takes 250ms.  Only
%% the naive commands can be used, as Keys may be present - so not easy to
%% optimise.  There is a lack of bulk operations
%%
%% A2 - Use some other structure other than gb_trees or ETS tables
%% * There is nothing else that will support iterators, so snapshots would
%% either need to do a conversion when they request the snapshot if
%% they need to iterate, or iterate through map functions scanning all the
%% keys. The conversion may not be expensive, as we know loading into an ETS
%% table is fast - but there may be some hidden overheads with creating and
%5 destroying many ETS tables.
%%
%% A3 - keep a parallel list of lists of things that have gone in the ETS
%% table in the format they arrived in
%% * There is doubling up of memory, and the snapshot must do some work to
%% make use of these lists.  This combines the continued use of fast ETS
%% with the solution of A2 at a memory cost.
%%
%% A4 - Try and cache the conversion to be shared between snapshots registered
%% at the same Ledger SQN
%% * This is a rif on A2/A3, but if generally there is o(10) or o(100) seconds
%% between memory pushes, but much more frequent snapshots this may be more
%% efficient
%%
%% A5 - Produce a specific snapshot of the ETS table via an iterator on demand
%% for each snapshot
%% * So if a snapshot was required for na iterator, the Penciller would block
%% whilst it iterated over the ETS table first to produce a snapshot-specific
%% immutbale view.  If the snapshot was required for a long-lived complete view
%% of the database the Penciller would block for a tab2list.
%%
%% A6 - Have snapshots incrementally create and share immutable trees, from a
%% parallel cache of changes
%% * This is a variance on A3.  As changes are pushed to the Penciller in the
%% form of lists the Penciller updates a cache of the lists that are contained
%% in the current ETS table.  These lists are returned to the snapshot when
%% the snapshot is registered.  All snapshots it is assumed will convert these
%% lists into a gb_tree to use, but following that conversion they can cast
%% to the Penciller to refine the cache, so that the cache will become a
%% gb_tree up the ledger SQN at which the snapshot is registered, and now only
%% store new lists for subsequent updates.  Future snapshot requests (before
%% the ets table is flushed) will now receive the array (if no changes have)
%% been made, or the array and only the lists needed to incrementally change
%% the array.  If changes are infrequent, each snapshot request will pay the
%% full 20ms to 250ms cost of producing the array (although perhaps the clerk
%% could also update periodiclaly to avoid this).  If changes are frequent,
%% the snapshot will generally not require to do a conversion, or will only
%% be required to do a small conversion
%%
%% A6 is the preferred option


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
        pcl_fetchkeys/5,
        pcl_checksequencenumber/3,
        pcl_workforclerk/1,
        pcl_promptmanifestchange/2,
        pcl_confirmdelete/2,
        pcl_close/1,
        pcl_registersnapshot/2,
        pcl_releasesnapshot/2,
        pcl_updatesnapshotcache/3,
        pcl_loadsnapshot/2,
        pcl_getstartupsequencenumber/1,
        roll_new_tree/3,
        clean_testdir/1]).

-include_lib("eunit/include/eunit.hrl").

-define(LEVEL_SCALEFACTOR, [{0, 0}, {1, 8}, {2, 64}, {3, 512},
                            {4, 4096}, {5, 32768}, {6, 262144}, {7, infinity}]).
-define(MAX_LEVELS, 8).
-define(MAX_WORK_WAIT, 300).
-define(MANIFEST_FP, "ledger_manifest").
-define(FILES_FP, "ledger_files").
-define(CURRENT_FILEX, "crr").
-define(PENDING_FILEX, "pnd").
-define(MEMTABLE, mem).
-define(MAX_TABLESIZE, 32000).
-define(PROMPT_WAIT_ONL0, 5).
-define(L0PEND_RESET, {false, null, null}).

-record(l0snapshot, {increments = [] :: list(),
                        tree = gb_trees:empty() :: gb_trees:tree(),
                        ledger_sqn = 0 :: integer()}).                 

-record(state, {manifest = [] :: list(),
                ongoing_work = [] :: list(),
				manifest_sqn = 0 :: integer(),
                ledger_sqn = 0 :: integer(),
                registered_snapshots = [] :: list(),
                unreferenced_files = [] :: list(),
                root_path = "../test" :: string(),
                table_size = 0 :: integer(),
                clerk :: pid(),
                levelzero_pending = ?L0PEND_RESET :: tuple(),
                memtable_copy = #l0snapshot{} :: #l0snapshot{},
                levelzero_snapshot = gb_trees:empty() :: gb_trees:tree(),
                memtable,
                backlog = false :: boolean(),
                memtable_maxsize :: integer(),
                is_snapshot = false :: boolean(),
                snapshot_fully_loaded = false :: boolean(),
                source_penciller :: pid()}).


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
    gen_server:call(Pid, {confirm_delete, FileName}, infinity).

pcl_getstartupsequencenumber(Pid) ->
    gen_server:call(Pid, get_startup_sqn, infinity).

pcl_registersnapshot(Pid, Snapshot) ->
    gen_server:call(Pid, {register_snapshot, Snapshot}, infinity).

pcl_releasesnapshot(Pid, Snapshot) ->
    gen_server:cast(Pid, {release_snapshot, Snapshot}).

pcl_updatesnapshotcache(Pid, Tree, SQN) ->
    gen_server:cast(Pid, {update_snapshotcache, Tree, SQN}).

pcl_loadsnapshot(Pid, Increment) ->
    gen_server:call(Pid, {load_snapshot, Increment}, infinity).

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
            {ok,
                LedgerSQN,
                Manifest,
                MemTableCopy} = pcl_registersnapshot(SrcPenciller, self()),
            
            {ok, #state{memtable_copy=MemTableCopy,
                            is_snapshot=true,
                            source_penciller=SrcPenciller,
                            manifest=Manifest,
                            ledger_sqn=LedgerSQN}};
            %% Need to do something about timeout
        {_RootPath, false} ->
            start_from_file(PCLopts)
    end.    
    

handle_call({push_mem, DumpList}, From, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->
    % The process for pushing to memory is as follows
    % - Check that the inbound list does not contain any Keys with a lower
    % sequence number than any existing keys (assess_sqn/1)
    % - Check that any file that had been sent to be written to L0 previously
    % is now completed.  If it is wipe out the in-memory view as this is now
    % safely persisted.  This will block waiting for this to complete if it
    % hasn't (checkready_pushmem/1).
    % - Quick check to see if there is a need to write a L0 file
    % (quickcheck_pushmem/3).  If there clearly isn't, then we can reply, and
    % then add to memory in the background before updating the loop state
    % - Push the update into memory (do_pushtomem/3)
    % - If we haven't got through quickcheck now need to check if there is a
    % definite need to write a new L0 file (roll_memory/2).  If all clear this
    % will write the file in the background and allow a response to the user.
    % If not the change has still been made but the the L0 file will not have
    % been prompted - so the reply does not indicate failure but returns the
    % atom 'pause' to signal a loose desire for back-pressure to be applied.
    % The only reason in this case why there should be a pause is if the
    % manifest is locked pending completion of a manifest change - so reacting
    % to the pause signal may not be sensible
    StartWatch = os:timestamp(),
    case assess_sqn(DumpList) of
        {MinSQN, MaxSQN} when MaxSQN >= MinSQN,
                                MinSQN >= State#state.ledger_sqn ->
            MaxTableSize = State#state.memtable_maxsize,
            {TableSize0, State1} = checkready_pushtomem(State),
            case quickcheck_pushtomem(DumpList,
                                        TableSize0,
                                        MaxTableSize) of
                {twist, TableSize1} ->
                    gen_server:reply(From, ok),
                    io:format("Reply made on push in ~w microseconds~n",
                                [timer:now_diff(os:timestamp(), StartWatch)]),
                    L0Snap = do_pushtomem(DumpList,
                                            State1#state.memtable,
                                            State1#state.memtable_copy,
                                            MaxSQN),
                    io:format("Push completed in ~w microseconds~n",
                                [timer:now_diff(os:timestamp(), StartWatch)]),
                    {noreply,
                        State1#state{memtable_copy=L0Snap,
                                        table_size=TableSize1,
                                        ledger_sqn=MaxSQN}};
                {maybe_roll, TableSize1} ->
                    L0Snap = do_pushtomem(DumpList,
                                            State1#state.memtable,
                                            State1#state.memtable_copy,
                                            MaxSQN),

                    case roll_memory(State1, MaxTableSize) of
                        {ok, L0Pend, ManSN, TableSize2} ->
                            io:format("Push completed in ~w microseconds~n",
                                [timer:now_diff(os:timestamp(), StartWatch)]),
                            {reply,
                                ok,
                                State1#state{levelzero_pending=L0Pend,
                                                table_size=TableSize2,
                                                manifest_sqn=ManSN,
                                                memtable_copy=L0Snap,
                                                ledger_sqn=MaxSQN,
                                                backlog=false}};
                        {pause, Reason, Details} ->
                            io:format("Excess work due to - " ++ Reason,
                                        Details),
                            {reply,
                                pause,
                                State1#state{backlog=true,
                                                memtable_copy=L0Snap,
                                                table_size=TableSize1,
                                                ledger_sqn=MaxSQN}}
                    end
            end;
        {MinSQN, MaxSQN} ->
            io:format("Mismatch of sequence number expectations with push "
                        ++ "having sequence numbers between ~w and ~w "
                        ++ "but current sequence number is ~w~n",
                        [MinSQN, MaxSQN, State#state.ledger_sqn]),
            {reply, refused, State};
        empty ->
            io:format("Empty request pushed to Penciller~n"),
            {reply, ok, State}
    end;
handle_call({confirm_delete, FileName}, _From, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->    
    Reply = confirm_delete(FileName,
                            State#state.unreferenced_files,
                            State#state.registered_snapshots),
    case Reply of
        true ->
            UF1 = lists:keydelete(FileName, 1, State#state.unreferenced_files),
            {reply, true, State#state{unreferenced_files=UF1}};
        _ ->
            {reply, Reply, State}
    end;
handle_call({fetch, Key}, _From, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->
    {reply,
        fetch(Key,
                State#state.manifest,
                State#state.memtable),
        State};
handle_call({fetch, Key},
                _From,
                State=#state{snapshot_fully_loaded=Ready})
                                                        when Ready == true ->
    {reply,
        fetch_snap(Key,
                    State#state.manifest,
                    State#state.levelzero_snapshot),
        State};
handle_call({check_sqn, Key, SQN},
                _From,
                State=#state{snapshot_fully_loaded=Ready})
                                                        when Ready == true ->
    {reply,
        compare_to_sqn(fetch_snap(Key,
                                    State#state.manifest,
                                    State#state.levelzero_snapshot),
                        SQN),
        State};
handle_call({fetch_keys, StartKey, EndKey, AccFun, InitAcc},
                _From,
                State=#state{snapshot_fully_loaded=Ready})
                                                        when Ready == true ->
    L0iter = gb_trees:iterator_from(StartKey, State#state.levelzero_snapshot),
    SFTiter = initiate_rangequery_frommanifest(StartKey,
                                                EndKey,
                                                State#state.manifest),
    io:format("Manifest for iterator of:~n"),
    print_manifest(SFTiter),
    Acc = keyfolder(L0iter, SFTiter, StartKey, EndKey, {AccFun, InitAcc}),
    {reply, Acc, State};
handle_call(work_for_clerk, From, State) ->
    {UpdState, Work} = return_work(State, From),
    {reply, {Work, UpdState#state.backlog}, UpdState};
handle_call(get_startup_sqn, _From, State) ->
    {reply, State#state.ledger_sqn, State};
handle_call({register_snapshot, Snapshot}, _From, State) ->
    Rs = [{Snapshot, State#state.ledger_sqn}|State#state.registered_snapshots],
    {reply,
        {ok,
            State#state.ledger_sqn,
            State#state.manifest,
            State#state.memtable_copy},
        State#state{registered_snapshots = Rs}};
handle_call({load_snapshot, Increment}, _From, State) ->
    MemTableCopy = State#state.memtable_copy,
    {Tree0, TreeSQN0} = roll_new_tree(MemTableCopy#l0snapshot.tree,
                                        MemTableCopy#l0snapshot.increments,
                                        MemTableCopy#l0snapshot.ledger_sqn),
    if
        TreeSQN0 > MemTableCopy#l0snapshot.ledger_sqn ->
            pcl_updatesnapshotcache(State#state.source_penciller,
                                    Tree0,
                                    TreeSQN0);
        true ->
            io:format("No update required to snapshot cache~n"),
            ok
    end,
    {Tree1, TreeSQN1} = roll_new_tree(Tree0, [Increment], TreeSQN0),
    io:format("Snapshot loaded with increments to start at SQN=~w~n",
                [TreeSQN1]),
    {reply, ok, State#state{levelzero_snapshot=Tree1,
                            ledger_sqn=TreeSQN1,
                            snapshot_fully_loaded=true}};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({update_snapshotcache, Tree, SQN}, State) ->
    MemTableC = cache_tree_in_memcopy(State#state.memtable_copy, Tree, SQN),
    {noreply, State#state{memtable_copy=MemTableC}};
handle_cast({manifest_change, WI}, State) ->
    {ok, UpdState} = commit_manifest_change(WI, State),
    ok = leveled_pclerk:clerk_manifestchange(State#state.clerk,
                                                confirm,
                                                false),
    {noreply, UpdState};
handle_cast({release_snapshot, Snapshot}, State) ->
    Rs = lists:keydelete(Snapshot, 1, State#state.registered_snapshots),
    io:format("Penciller snapshot ~w released~n", [Snapshot]),
    {noreply, State#state{registered_snapshots=Rs}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State=#state{is_snapshot=Snap}) when Snap == true ->
    ok = pcl_releasesnapshot(State#state.source_penciller, self()),
    io:format("Sent release message for snapshot following close for "
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
    Dump = ets:tab2list(UpdState#state.memtable),
    case {UpdState#state.levelzero_pending,
            get_item(0, UpdState#state.manifest, []), length(Dump)} of
        {?L0PEND_RESET, [], L} when L > 0 ->
            MSN = UpdState#state.manifest_sqn + 1,
            FileName = UpdState#state.root_path
                        ++ "/" ++ ?FILES_FP ++ "/"
                        ++ integer_to_list(MSN) ++ "_0_0",
            NewSFT = leveled_sft:sft_new(FileName ++ ".pnd",
                                            Dump,
                                            [],
                                            0),
            {ok, L0Pid, {{[], []}, _SK, _HK}} = NewSFT,
            io:format("Dump of memory on close to filename ~s~n",
                        [FileName]),
            leveled_sft:sft_close(L0Pid),
            file:rename(FileName ++ ".pnd", FileName ++ ".sft");
        {?L0PEND_RESET, [], L} when L == 0 ->
            io:format("No keys to dump from memory when closing~n");
        {{true, L0Pid, _TS}, _, _} ->
            leveled_sft:sft_close(L0Pid),
            io:format("No opportunity to persist memory before closing"
                        ++ " with ~w keys discarded~n",
                        [length(Dump)]);
        _ ->
            io:format("No opportunity to persist memory before closing"
                        ++ " with ~w keys discarded~n",
                        [length(Dump)])
    end,
    ok = close_files(0, UpdState#state.manifest),
    lists:foreach(fun({_FN, Pid, _SN}) ->
                            leveled_sft:sft_close(Pid) end,
                    UpdState#state.unreferenced_files),
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
    TID = ets:new(?MEMTABLE, [ordered_set]),
    {ok, Clerk} = leveled_pclerk:clerk_new(self()),
    InitState = #state{memtable=TID,
                        clerk=Clerk,
                        root_path=RootPath,
                        memtable_maxsize=MaxTableSize},
    
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
            
            %% Find any L0 files
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


checkready_pushtomem(State) ->
    {TableSize, UpdState} = case State#state.levelzero_pending of
        {true, Pid, _TS} ->
            %% Need to handle error scenarios?
            %% N.B. Sync call - so will be ready
            {ok, SrcFN, StartKey, EndKey} = leveled_sft:sft_checkready(Pid),
            true = ets:delete_all_objects(State#state.memtable),
            ManifestEntry = #manifest_entry{start_key=StartKey,
                                                end_key=EndKey,
                                                owner=Pid,
                                                filename=SrcFN},
            % Prompt clerk to ask about work - do this for every L0 roll
            ok = leveled_pclerk:clerk_prompt(State#state.clerk),
            {0,
                State#state{manifest=lists:keystore(0,
                                                    1,
                                                    State#state.manifest,
                                                    {0, [ManifestEntry]}),
                            levelzero_pending=?L0PEND_RESET,
                            memtable_copy=#l0snapshot{}}};
        ?L0PEND_RESET ->
            {State#state.table_size, State}
    end,
    {TableSize, UpdState}.

quickcheck_pushtomem(DumpList, TableSize, MaxSize) ->
    case TableSize + length(DumpList) of
        ApproxTableSize when ApproxTableSize > MaxSize ->
            {maybe_roll, ApproxTableSize};
        ApproxTableSize ->
            io:format("Table size is approximately ~w~n", [ApproxTableSize]),
            {twist, ApproxTableSize}
    end.

do_pushtomem(DumpList, MemTable, Snapshot, MaxSQN) ->
    SW = os:timestamp(),
    UpdSnapshot = add_increment_to_memcopy(Snapshot, MaxSQN, DumpList),
    ets:insert(MemTable, DumpList),
    io:format("Push into memory timed at ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW)]),
    UpdSnapshot.

roll_memory(State, MaxSize) ->
    case ets:info(State#state.memtable, size) of
        Size when Size > MaxSize ->
            L0 = get_item(0, State#state.manifest, []),
            case {L0, manifest_locked(State)} of
                {[], false} ->
                    MSN = State#state.manifest_sqn + 1,
                    FileName = State#state.root_path
                                ++ "/" ++ ?FILES_FP ++ "/"
                                ++ integer_to_list(MSN) ++ "_0_0",
                    Opts = #sft_options{wait=false},
                    {ok, L0Pid} = leveled_sft:sft_new(FileName,
                                                        State#state.memtable,
                                                        [],
                                                        0,
                                                        Opts),
                    {ok, {true, L0Pid, os:timestamp()}, MSN, Size};
                {[], true} ->
                    {pause,
                        "L0 file write blocked by change at sqn=~w~n",
                        [State#state.manifest_sqn]};
                _ ->
                    {pause,
                        "L0 file write blocked by L0 file in manifest~n",
                        []}
            end;
        Size ->
            {ok, ?L0PEND_RESET, State#state.manifest_sqn, Size}
    end.


fetch_snap(Key, Manifest, Tree) ->
    case gb_trees:lookup(Key, Tree) of
        {value, Value} ->
            {Key, Value};
        none ->
            fetch(Key, Manifest, 0, fun leveled_sft:sft_get/2)
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


%% Manifest lock - don't have two changes to the manifest happening
%% concurrently
% TODO: Is this necessary now?

manifest_locked(State) ->
    if
        length(State#state.ongoing_work) > 0 ->
            true;
        true ->
            case State#state.levelzero_pending of
                {true, _Pid, _TS} ->
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
%%
%% The full queue is calculated for logging purposes only

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


%% This takes the three parts of a memtable copy - the increments, the tree
%% and the SQN at which the tree was formed, and outputs a new tree

roll_new_tree(Tree, [], HighSQN) ->
    {Tree, HighSQN};
roll_new_tree(Tree, [{SQN, KVList}|TailIncs], HighSQN) when SQN >= HighSQN ->
    R = lists:foldl(fun({Kx, Vx}, {TreeAcc, MaxSQN}) ->
                        UpdTree = gb_trees:enter(Kx, Vx, TreeAcc),
                        SQNx = leveled_codec:strip_to_seqonly({Kx, Vx}),
                        {UpdTree, max(SQNx, MaxSQN)}
                        end,
                    {Tree, HighSQN},
                    KVList),
    {UpdTree, UpdSQN} = R,
    roll_new_tree(UpdTree, TailIncs, UpdSQN);
roll_new_tree(Tree, [_H|TailIncs], HighSQN) ->
    roll_new_tree(Tree, TailIncs, HighSQN).

%% Update the memtable copy if the tree created advances the SQN
cache_tree_in_memcopy(MemCopy, Tree, SQN) ->
    case MemCopy#l0snapshot.ledger_sqn of
        CurrentSQN when SQN > CurrentSQN ->
            % Discard any merged increments
            io:format("Updating cache with new tree at SQN=~w~n", [SQN]),
            Incs = lists:foldl(fun({PushSQN, PushL}, Acc) ->
                                    if
                                        SQN >= PushSQN ->
                                            Acc;
                                        true ->
                                            Acc ++ [{PushSQN, PushL}]
                                    end end,
                                [],
                                MemCopy#l0snapshot.increments),
            #l0snapshot{ledger_sqn = SQN,
                        increments = Incs,
                        tree = Tree};           
        _CurrentSQN ->
            MemCopy
    end.

add_increment_to_memcopy(MemCopy, SQN, KVList) ->
    Incs = MemCopy#l0snapshot.increments ++ [{SQN, KVList}],
    MemCopy#l0snapshot{increments=Incs}.

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

print_manifest(Manifest) ->
    lists:foreach(fun(L) ->
                        io:format("Manifest at Level ~w~n", [L]),
                        Level = get_item(L, Manifest, []),
                        lists:foreach(fun(M) ->
                                            R = is_record(M, manifest_entry),
                                            case R of
                                                true ->
                                                    print_manifest_entry(M);
                                                false ->
                                                    {_, M1} = M,
                                                    print_manifest_entry(M1)
                                            end end,
                                        Level)
                        end,
                    lists:seq(0, ?MAX_LEVELS - 1)).

print_manifest_entry(Entry) ->
    {S1, S2, S3} = leveled_codec:print_key(Entry#manifest_entry.start_key),
    {E1, E2, E3} = leveled_codec:print_key(Entry#manifest_entry.end_key),
    io:format("Manifest entry of " ++ 
                "startkey ~s ~s ~s " ++
                "endkey ~s ~s ~s " ++
                "filename=~s~n",
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
            % and check the higher levels
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
            print_manifest(NewManifest),
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
    OldFN = filepath(RootPath, NewMSN, pending_manifest),
    NewFN = filepath(RootPath, NewMSN, current_manifest),
    io:format("Rename of manifest from ~s ~w to ~s ~w~n",
                [OldFN,
                    filelib:is_file(OldFN),
                    NewFN,
                    filelib:is_file(NewFN)]),
    file:rename(OldFN,NewFN).

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
        {Filename, _Pid, MSN} ->
            LowSQN = lists:foldl(fun({_, SQN}, MinSQN) -> min(SQN, MinSQN) end,
                                    infinity,
                                    RegisteredSnapshots),
            if
                MSN >= LowSQN ->
                    false;
                true ->
                    true
            end
    end.



assess_sqn([]) ->
    empty;
assess_sqn(DumpList) ->
    assess_sqn(DumpList, infinity, 0).

assess_sqn([], MinSQN, MaxSQN) ->
    {MinSQN, MaxSQN};
assess_sqn([HeadKey|Tail], MinSQN, MaxSQN) ->
    {_K, SQN} = leveled_codec:strip_to_keyseqonly(HeadKey),
    assess_sqn(Tail, min(MinSQN, SQN), max(MaxSQN, SQN)).


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
    WorkQ1 = assess_workqueue([], 0, Manifest),
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


maybe_pause_push(R) ->
    if
        R == pause ->
            io:format("Pausing push~n"),
            timer:sleep(1000);
        true ->
            ok
    end.

simple_server_test() ->
    RootPath = "../test/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    Key1 = {{o,"Bucket0001", "Key0001", null}, {1, {active, infinity}, null}},
    KL1 = lists:sort(leveled_sft:generate_randomkeys({1000, 2})),
    Key2 = {{o,"Bucket0002", "Key0002", null}, {1002, {active, infinity}, null}},
    KL2 = lists:sort(leveled_sft:generate_randomkeys({1000, 1002})),
    Key3 = {{o,"Bucket0003", "Key0003", null}, {2002, {active, infinity}, null}},
    KL3 = lists:sort(leveled_sft:generate_randomkeys({1000, 2002})),
    Key4 = {{o,"Bucket0004", "Key0004", null}, {3002, {active, infinity}, null}},
    KL4 = lists:sort(leveled_sft:generate_randomkeys({1000, 3002})),
    ok = pcl_pushmem(PCL, [Key1]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = pcl_pushmem(PCL, KL1),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    maybe_pause_push(pcl_pushmem(PCL, [Key2])),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    maybe_pause_push(pcl_pushmem(PCL, KL2)),
    maybe_pause_push(pcl_pushmem(PCL, [Key3])),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCL, {o,"Bucket0003", "Key0003", null})),
    ok = pcl_close(PCL),
    {ok, PCLr} = pcl_start(#penciller_options{root_path=RootPath,
                                                max_inmemory_tablesize=1000}),
    TopSQN = pcl_getstartupsequencenumber(PCLr),
    Check = case TopSQN of
                2001 ->
                    %% Last push not persisted
                    S3a = pcl_pushmem(PCL, [Key3]),
                    if S3a == pause -> timer:sleep(1000); true -> ok end,
                    ok;
                2002 ->
                    %% everything got persisted
                    ok;
                _ ->
                    io:format("Unexpected sequence number on restart ~w~n",
                                [TopSQN]),
                    error
            end,
    ?assertMatch(ok, Check),
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCLr, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCLr, {o,"Bucket0003", "Key0003", null})),
    maybe_pause_push(pcl_pushmem(PCLr, KL3)),
    maybe_pause_push(pcl_pushmem(PCLr, [Key4])),
    maybe_pause_push(pcl_pushmem(PCLr, KL4)),
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCLr, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCLr, {o,"Bucket0003", "Key0003", null})),
    ?assertMatch(Key4, pcl_fetch(PCLr, {o,"Bucket0004", "Key0004", null})),
    SnapOpts = #penciller_options{start_snapshot = true,
                                    source_penciller = PCLr},
    {ok, PclSnap} = pcl_start(SnapOpts),
    ok = pcl_loadsnapshot(PclSnap, []),
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
                                                2002)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0004",
                                                    "Key0004",
                                                    null},
                                                3002)),
    % Add some more keys and confirm that chekc sequence number still
    % sees the old version in the previous snapshot, but will see the new version
    % in a new snapshot
    Key1A = {{o,"Bucket0001", "Key0001", null}, {4002, {active, infinity}, null}},
    KL1A = lists:sort(leveled_sft:generate_randomkeys({4002, 2})),
    maybe_pause_push(pcl_pushmem(PCLr, [Key1A])),
    maybe_pause_push(pcl_pushmem(PCLr, KL1A)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap,
                                                {o,
                                                    "Bucket0001",
                                                    "Key0001",
                                                    null},
                                                1)),
    ok = pcl_close(PclSnap),
    {ok, PclSnap2} = pcl_start(SnapOpts),
    ok = pcl_loadsnapshot(PclSnap2, []),
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
                                                4002)),
    ?assertMatch(true, pcl_checksequencenumber(PclSnap2,
                                                {o,
                                                    "Bucket0002",
                                                    "Key0002",
                                                    null},
                                                1002)),
    ok = pcl_close(PclSnap2),
    ok = pcl_close(PCLr),
    clean_testdir(RootPath).

memcopy_updatecache1_test() ->
    KVL1 = lists:map(fun(X) -> {"Key" ++ integer_to_list(X),
                                {X, null, "Val" ++ integer_to_list(X) ++ "A"}}
                                end,
                lists:seq(1, 1000)),
    KVL2 = lists:map(fun(X) -> {"Key" ++ integer_to_list(X),
                                {X, null, "Val" ++ integer_to_list(X) ++ "B"}}
                                end,
                lists:seq(1001, 2000)),
    KVL3 = lists:map(fun(X) -> {"Key" ++ integer_to_list(X),
                                {X, null, "Val" ++ integer_to_list(X) ++ "C"}}
                                end,
                lists:seq(2001, 3000)),
    MemCopy0 = #l0snapshot{},
    MemCopy1 = add_increment_to_memcopy(MemCopy0, 1000, KVL1),
    MemCopy2 = add_increment_to_memcopy(MemCopy1, 2000, KVL2),
    MemCopy3 = add_increment_to_memcopy(MemCopy2, 3000, KVL3),
    ?assertMatch(0, MemCopy3#l0snapshot.ledger_sqn),
    {Tree1, HighSQN1} = roll_new_tree(gb_trees:empty(), MemCopy3#l0snapshot.increments, 0),
    MemCopy4 = cache_tree_in_memcopy(MemCopy3, Tree1, HighSQN1),
    ?assertMatch(0, length(MemCopy4#l0snapshot.increments)),
    Size2 = gb_trees:size(MemCopy4#l0snapshot.tree),
    ?assertMatch(3000, Size2),
    ?assertMatch(3000, MemCopy4#l0snapshot.ledger_sqn).

memcopy_updatecache2_test() ->
    KVL1 = lists:map(fun(X) -> {"Key" ++ integer_to_list(X),
                                {X, null, "Val" ++ integer_to_list(X) ++ "A"}}
                                end,
                lists:seq(1, 1000)),
    KVL2 = lists:map(fun(X) -> {"Key" ++ integer_to_list(X),
                                {X, null, "Val" ++ integer_to_list(X) ++ "B"}}
                                end,
                lists:seq(1001, 2000)),
    KVL3 = lists:map(fun(X) -> {"Key" ++ integer_to_list(X),
                                {X, null, "Val" ++ integer_to_list(X) ++ "C"}}
                                end,
                lists:seq(1, 1000)),
    MemCopy0 = #l0snapshot{},
    MemCopy1 = add_increment_to_memcopy(MemCopy0, 1000, KVL1),
    MemCopy2 = add_increment_to_memcopy(MemCopy1, 2000, KVL2),
    MemCopy3 = add_increment_to_memcopy(MemCopy2, 3000, KVL3),
    ?assertMatch(0, MemCopy3#l0snapshot.ledger_sqn),
    {Tree1, HighSQN1} = roll_new_tree(gb_trees:empty(), MemCopy3#l0snapshot.increments, 0),
    MemCopy4 = cache_tree_in_memcopy(MemCopy3, Tree1, HighSQN1),
    ?assertMatch(1, length(MemCopy4#l0snapshot.increments)),
    Size2 = gb_trees:size(MemCopy4#l0snapshot.tree),
    ?assertMatch(2000, Size2),
    ?assertMatch(2000, MemCopy4#l0snapshot.ledger_sqn).

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

-endif.