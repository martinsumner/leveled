%% -------- Inker ---------
%% 
%% The Inker is responsible for managing access and updates to the Journal. 
%%
%% The Inker maintains a manifest of what files make up the Journal, and which
%% file is the current append-only nursery log to accept new PUTs into the
%% Journal.  The Inker also marshals GET requests to the appropriate database
%% file within the Journal (routed by sequence number).  The Inker is also
%% responsible for scheduling compaction work to be carried out by the Inker's
%% clerk.
%%
%% -------- Journal Files ---------
%%
%% The Journal is a series of files originally named as <SQN>_<GUID>
%% where the sequence number is the first object sequence number (key) within
%% the given database file.  The files will be named *.cdb at the point they
%% have been made immutable (through a rename operation).  Prior to this, they
%% will originally start out as a *.pnd file.
%%
%% At some stage in the future compacted versions of old journal cdb files may
%% be produced.  These files will be named <SQN>-<NewGUID>.cdb, and once
%% the manifest is updated the original <SQN>_<GUID>.cdb (or
%% <SQN>_<previousGUID>.cdb) files they replace will be erased.
%%
%% The current Journal is made up of a set of files referenced in the manifest.
%% No PUTs are made to files which are not in the manifest.
%% 
%% The Journal is ordered by sequence number from front to back both within
%% and across files.  
%%
%% On startup the Inker should open the manifest with the highest sequence
%% number, and this will contain the list of filenames that make up the
%% non-recent part of the Journal.  All the filenames should then be opened.
%% How they are opened depends on the file extension:
%%
%% - If the file extension is *.cdb the file is opened read only
%% - If the file extension is *.pnd and the file is not the most recent in the
%% manifest, then the file should be completed bfore being opened read-only
%% - If the file extension is *.pnd the file is opened for writing
%%
%% -------- Manifest Files ---------
%%
%% The manifest is just saved as a straight term_to_binary blob, with a
%% filename ordered by the Manifest SQN.  The Manifest is first saved with a
%% *.pnd extension, and then renamed to one with a *.man extension.
%%
%% On startup the *.man manifest file with the highest manifest sequence
%% number should be used.
%%
%% -------- Objects ---------
%%
%% From the perspective of the Inker, objects to store are made up of:
%%  - An Inker Key formed from
%%      - A sequence number (assigned by the Inker)
%%      - An Inker key type (stnd, tomb or keyd)
%%      - A Ledger Key (as an Erlang term)
%%  - A value formed from
%%      - An object (an Erlang term) which should be null for tomb types, and
%%      maybe null for keyd types
%%      - A set of Key Deltas associated with the change (which may be an
%%      empty list )    
%%
%% Note that only the Inker key type of stnd is directly fetchable, other
%% key types are to be found only in scans and so can be added without being
%% entered into the hashtree
%%
%% -------- Compaction ---------
%%
%% Compaction is a process whereby an Inker's clerk will:
%% - Request a view of the current Inker manifest and a snaphot of the Ledger
%% - Test all files within the Journal to find the approximate comapction
%% potential percentage (the volume of the Journal that has been replaced)
%% - Attempts to find the optimal "run" of files to compact
%% - Compacts those files in the run, by rolling over the files re-writing
%% to a new Journal if and only if the Key is still present in the Ledger (or
%% the sequence number of the Key is higher than the SQN of the snapshot)
%% - Requests the Inker update the manifest with the new changes
%% - Instructs the files to destroy themselves when they are next closed
%%
%% TODO: how to instruct the files to close is tbd
%%


-module(leveled_inker).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        ink_start/1,
        ink_put/4,
        ink_get/3,
        ink_fetch/3,
        ink_keycheck/3,
        ink_loadpcl/4,
        ink_registersnapshot/2,
        ink_confirmdelete/2,
        ink_compactjournal/3,
        ink_compactioncomplete/1,
        ink_compactionpending/1,
        ink_getmanifest/1,
        ink_updatemanifest/3,
        ink_printmanifest/1,
        ink_close/1,
        ink_doom/1,
        build_dummy_journal/0,
        clean_testdir/1,
        filepath/2,
        filepath/3]).

-include_lib("eunit/include/eunit.hrl").

-define(MANIFEST_FP, "journal_manifest").
-define(FILES_FP, "journal_files").
-define(COMPACT_FP, "post_compact").
-define(WASTE_FP, "waste").
-define(JOURNAL_FILEX, "cdb").
-define(PENDING_FILEX, "pnd").
-define(LOADING_PAUSE, 1000).
-define(LOADING_BATCH, 1000).

-record(state, {manifest = [] :: list(),
				manifest_sqn = 0 :: integer(),
                journal_sqn = 0 :: integer(),
                active_journaldb :: pid() | undefined,
                pending_removals = [] :: list(),
                registered_snapshots = [] :: list(),
                root_path :: string() | undefined,
                cdb_options :: #cdb_options{} | undefined,
                clerk :: pid() | undefined,
                compaction_pending = false :: boolean(),
                is_snapshot = false :: boolean(),
                source_inker :: pid() | undefined}).


-type inker_options() :: #inker_options{}.


%%%============================================================================
%%% API
%%%============================================================================

-spec ink_start(inker_options()) -> {ok, pid()}.
%% @doc 
%% Startup an inker process - passing in options.
%%
%% The first options are root_path and start_snapshot - if the inker is to be a
%% snapshot clone of another inker then start_snapshot should be true,
%% otherwise the root_path sill be used to find a file structure to provide a
%% starting point of state for the inker.
%%
%% The inker will need ot read and write CDB files (if it is not a snapshot),
%% and so a cdb_options record should be passed in as an inker_option to be
%% used when opening such files.
%%
%% The inker will need to know what the reload strategy is, to inform the
%% clerk about the rules to enforce during compaction.
ink_start(InkerOpts) ->
    gen_server:start(?MODULE, [InkerOpts], []).

-spec ink_put(pid(),
                {atom(), any(), any(), any()}|string(),
                any(),
                {list(), integer()|infinity}) ->
                                   {ok, integer(), integer()}.
%% @doc
%% PUT an object into the journal, returning the sequence number for the PUT
%% as well as the size of the object (information required by the ledger).
%%
%% The primary key is expected to be a tuple of the form 
%% {Tag, Bucket, Key, null}, but unit tests support pure string Keys and so
%% these types are also supported.
%%
%% KeyChanges is a tuple of {KeyChanges, TTL} where the TTL is an
%% expiry time (or infinity).
ink_put(Pid, PrimaryKey, Object, KeyChanges) ->
    gen_server:call(Pid, {put, PrimaryKey, Object, KeyChanges}, infinity).

-spec ink_get(pid(),
                {atom(), any(), any(), any()}|string(),
                integer()) ->
                         {{integer(), any()}, {any(), any()}}.
%% @doc
%% Fetch the object as stored in the Journal.  Will not mask errors, should be
%% used only in tests
ink_get(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {get, PrimaryKey, SQN}, infinity).

-spec ink_fetch(pid(),
                {atom(), any(), any(), any()}|string(),
                integer()) ->
                         any().
%% @doc
%% Fetch the value that was stored for a given Key at a particular SQN (i.e.
%% this must be a SQN of the write for this key).  the full object is returned
%% or the atome not_present if there is no such Key stored at that SQN, or if
%% fetching the Key prompted some anticipated error (e.g. CRC check failed)
ink_fetch(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {fetch, PrimaryKey, SQN}, infinity).

-spec ink_keycheck(pid(), 
                    {atom(), any(), any(), any()}|string(),
                    integer()) ->
                            probably|missing.
%% @doc
%% Quick check to determine if key is probably present.  Positive results have
%% a very small false positive rate, as can be triggered through a hash
%% collision.
ink_keycheck(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {key_check, PrimaryKey, SQN}, infinity).

-spec ink_registersnapshot(pid(), pid()) -> {list(), pid()}.
%% @doc
%% Register a snapshot clone for the process, returning the Manifest and the
%% pid of the active journal.
ink_registersnapshot(Pid, Requestor) ->
    gen_server:call(Pid, {register_snapshot, Requestor}, infinity).

-spec ink_releasesnapshot(pid(), pid()) -> ok.
%% @doc
%% Release a registered snapshot as it is no longer in use.  This should be
%% called by all terminating snapshots - otherwise space may not be cleared
%% following compaction.
ink_releasesnapshot(Pid, Snapshot) ->
    gen_server:cast(Pid, {release_snapshot, Snapshot}).

-spec ink_confirmdelete(pid(), integer()) -> boolean().
%% @doc
%% Confirm if a Journal CDB file can be deleted, as it has been set to delete
%% and is no longer in use by any snapshots
ink_confirmdelete(Pid, ManSQN) ->
    gen_server:call(Pid, {confirm_delete, ManSQN}).

-spec ink_close(pid()) -> ok.
%% @doc
%% Close the inker, prompting all the Journal file processes to be called.
ink_close(Pid) ->
    gen_server:call(Pid, close, infinity).

-spec ink_doom(pid()) -> {ok, [{string(), string(), string(), string()}]}.
%% @doc
%% Test function used to close a file, and return all file paths (potentially
%% to erase all persisted existence)
ink_doom(Pid) ->
    gen_server:call(Pid, doom, 60000).

-spec ink_loadpcl(pid(), integer(), fun(), pid()) -> ok.
%% @doc
%% Function to prompt load of the Ledger at startup.  the Penciller should
%% have determined the lowest SQN not present in the Ledger, and the inker
%% should fold over the Journal from that point, using the function to load
%% penciller with the results.
%%
%% The load fun should be a five arity function like:
%% load_fun(KeyInJournal, ValueInJournal, _Position, Acc0, ExtractFun)
ink_loadpcl(Pid, MinSQN, FilterFun, Penciller) ->
    gen_server:call(Pid, {load_pcl, MinSQN, FilterFun, Penciller}, infinity).

-spec ink_compactjournal(pid(), pid(), integer()) -> ok.
%% @doc
%% Trigger a compaction event.  the compaction event will use a sqn check
%% against the Ledger to see if a value can be compacted - if the penciller
%% believes it to be superseded that it can be compacted.
%%
%% The inker will get the maximum persisted sequence number from the
%% initiate_penciller_snapshot/1 function - and use that as a pre-filter so
%% that any value that was written more recently than the last flush to disk
%% of the Ledger will not be considered for compaction (as this may be
%% required to reload the Ledger on startup).
ink_compactjournal(Pid, Bookie, Timeout) ->
    CheckerInitiateFun = fun initiate_penciller_snapshot/1,
    CheckerCloseFun = fun leveled_penciller:pcl_close/1,
    CheckerFilterFun = fun leveled_penciller:pcl_checksequencenumber/3,
    gen_server:call(Pid,
                        {compact,
                            Bookie,
                            CheckerInitiateFun,
                            CheckerCloseFun,
                            CheckerFilterFun,
                            Timeout},
                        infinity).

%% Allows the Checker to be overriden in test, use something other than a
%% penciller
ink_compactjournal(Pid, Checker, InitiateFun, CloseFun, FilterFun, Timeout) ->
    gen_server:call(Pid,
                        {compact,
                            Checker,
                            InitiateFun,
                            CloseFun,
                            FilterFun,
                            Timeout},
                        infinity).
-spec ink_compactioncomplete(pid()) -> ok.
%% @doc
%% Used by a clerk to state that a compaction process is over, only change
%% is to unlock the Inker for further compactions.
ink_compactioncomplete(Pid) ->
    gen_server:call(Pid, compaction_complete, infinity).

-spec ink_compactionpending(pid()) -> boolean().
%% @doc
%% Is there ongoing compaction work?  No compaction work should be initiated
%5 if there is already some compaction work ongoing.
ink_compactionpending(Pid) ->
    gen_server:call(Pid, compaction_pending, infinity).

-spec ink_getmanifest(pid()) -> list().
%% @doc
%% Allows the clerk to fetch the manifest at the point it starts a compaction
%% job
ink_getmanifest(Pid) ->
    gen_server:call(Pid, get_manifest, infinity).

-spec ink_updatemanifest(pid(), list(), list()) -> {ok, integer()}.
%% @doc
%% Add a section of new entries into the manifest, and drop a bunch of deleted
%% files out of the manifest.  Used to update the manifest after a compaction
%% job.
%%
%% Returns {ok, ManSQN} with the ManSQN being the sequence number of the
%% updated manifest
ink_updatemanifest(Pid, ManifestSnippet, DeletedFiles) ->
    gen_server:call(Pid,
                        {update_manifest,
                            ManifestSnippet,
                            DeletedFiles},
                        infinity).

-spec ink_printmanifest(pid()) -> ok.
%% @doc 
%% Used in tests to print out the manifest
ink_printmanifest(Pid) ->
    gen_server:call(Pid, print_manifest, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([InkerOpts]) ->
    leveled_rand:seed(),
    case {InkerOpts#inker_options.root_path,
            InkerOpts#inker_options.start_snapshot} of
        {undefined, true} ->
            SrcInker = InkerOpts#inker_options.source_inker,
            {Manifest,
                ActiveJournalDB} = ink_registersnapshot(SrcInker, self()),
            {ok, #state{manifest=Manifest,
                            active_journaldb=ActiveJournalDB,
                            source_inker=SrcInker,
                            is_snapshot=true}};
            %% Need to do something about timeout
        {_RootPath, false} ->
            start_from_file(InkerOpts)
    end.


handle_call({put, Key, Object, KeyChanges}, _From, State) ->
    case put_object(Key, Object, KeyChanges, State) of
        {_, UpdState, ObjSize} ->
            {reply, {ok, UpdState#state.journal_sqn, ObjSize}, UpdState}
    end;
handle_call({fetch, Key, SQN}, _From, State) ->
    case get_object(Key, SQN, State#state.manifest, true) of
        {{SQN, Key}, {Value, _IndexSpecs}} ->
            {reply, {ok, Value}, State};
        Other ->
            leveled_log:log("I0001", [Key, SQN, Other]),
            {reply, not_present, State}
    end;
handle_call({get, Key, SQN}, _From, State) ->
    {reply, get_object(Key, SQN, State#state.manifest), State};
handle_call({key_check, Key, SQN}, _From, State) ->
    {reply, key_check(Key, SQN, State#state.manifest), State};
handle_call({load_pcl, StartSQN, FilterFun, Penciller}, _From, State) ->
    Manifest = lists:reverse(leveled_imanifest:to_list(State#state.manifest)),
    Reply = load_from_sequence(StartSQN, FilterFun, Penciller, Manifest),
    {reply, Reply, State};
handle_call({register_snapshot, Requestor}, _From , State) ->
    Rs = [{Requestor,
            State#state.manifest_sqn}|State#state.registered_snapshots],
    leveled_log:log("I0002", [Requestor, State#state.manifest_sqn]),
    {reply, {State#state.manifest,
                State#state.active_journaldb},
                State#state{registered_snapshots=Rs}};
handle_call({confirm_delete, ManSQN}, _From, State) ->
    Reply = lists:foldl(fun({_R, SnapSQN}, Bool) ->
                                case SnapSQN >= ManSQN of
                                    true ->
                                        Bool;
                                    false ->
                                        false
                                end end,
                            true,
                            State#state.registered_snapshots),
    {reply, Reply, State};
handle_call(get_manifest, _From, State) ->
    {reply, leveled_imanifest:to_list(State#state.manifest), State};
handle_call({update_manifest,
                ManifestSnippet,
                DeletedFiles}, _From, State) ->
    DropFun =
        fun(E, Acc) ->
            leveled_imanifest:remove_entry(Acc, E)
        end,
    Man0 = lists:foldl(DropFun, State#state.manifest, DeletedFiles),                    
    AddFun =
        fun(E, Acc) ->
            leveled_imanifest:add_entry(Acc, E, false)
        end,
    Man1 = lists:foldl(AddFun, Man0, ManifestSnippet),
    NewManifestSQN = State#state.manifest_sqn + 1,
    leveled_imanifest:printer(Man1),
    leveled_imanifest:writer(Man1, NewManifestSQN, State#state.root_path),
    {reply,
        {ok, NewManifestSQN},
        State#state{manifest=Man1,
                        manifest_sqn=NewManifestSQN,
                        pending_removals=DeletedFiles}};
handle_call(print_manifest, _From, State) ->
    leveled_imanifest:printer(State#state.manifest),
    {reply, ok, State};
handle_call({compact,
                Checker,
                InitiateFun,
                CloseFun,
                FilterFun,
                Timeout},
                    _From, State) ->
    leveled_iclerk:clerk_compact(State#state.clerk,
                                    Checker,
                                    InitiateFun,
                                    CloseFun,
                                    FilterFun,
                                    self(),
                                    Timeout),
    {reply, ok, State#state{compaction_pending=true}};
handle_call(compaction_complete, _From, State) ->
    {reply, ok, State#state{compaction_pending=false}};
handle_call(compaction_pending, _From, State) ->
    {reply, State#state.compaction_pending, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(doom, _From, State) ->
    FPs = [filepath(State#state.root_path, journal_dir),
            filepath(State#state.root_path, manifest_dir),
            filepath(State#state.root_path, journal_compact_dir),
            filepath(State#state.root_path, journal_waste_dir)],
    leveled_log:log("I0018", []),
    {stop, normal, {ok, FPs}, State}.

handle_cast({release_snapshot, Snapshot}, State) ->
    Rs = lists:keydelete(Snapshot, 1, State#state.registered_snapshots),
    leveled_log:log("I0003", [Snapshot]),
    leveled_log:log("I0004", [length(Rs)]),
    {noreply, State#state{registered_snapshots=Rs}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    case State#state.is_snapshot of
        true ->
            ok = ink_releasesnapshot(State#state.source_inker, self());
        false ->    
            leveled_log:log("I0005", [Reason]),
            leveled_log:log("I0006", [State#state.journal_sqn,
                                        State#state.manifest_sqn]),
            leveled_iclerk:clerk_stop(State#state.clerk),
            lists:foreach(fun({Snap, _SQN}) -> ok = ink_close(Snap) end,
                            State#state.registered_snapshots),
            leveled_log:log("I0007", []),
            leveled_imanifest:printer(State#state.manifest),
            ManAsList = leveled_imanifest:to_list(State#state.manifest),
            ok = close_allmanifest(ManAsList)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

start_from_file(InkOpts) ->
    RootPath = InkOpts#inker_options.root_path,
    CDBopts = InkOpts#inker_options.cdb_options,
    
    JournalFP = filepath(RootPath, journal_dir),
    filelib:ensure_dir(JournalFP),
    CompactFP = filepath(RootPath, journal_compact_dir),
    filelib:ensure_dir(CompactFP),
    WasteFP = filepath(RootPath, journal_waste_dir),
    filelib:ensure_dir(WasteFP),
    ManifestFP = filepath(RootPath, manifest_dir),
    ok = filelib:ensure_dir(ManifestFP),
    
    {ok, ManifestFilenames} = file:list_dir(ManifestFP),
    
    IClerkCDBOpts = CDBopts#cdb_options{file_path = CompactFP,
                                        waste_path = WasteFP},
    ReloadStrategy = InkOpts#inker_options.reload_strategy,
    MRL = InkOpts#inker_options.max_run_length,
    WRP = InkOpts#inker_options.waste_retention_period,
    IClerkOpts = #iclerk_options{inker = self(),
                                    cdb_options=IClerkCDBOpts,
                                    waste_retention_period = WRP,
                                    reload_strategy = ReloadStrategy,
                                    max_run_length = MRL},
    {ok, Clerk} = leveled_iclerk:clerk_new(IClerkOpts),
    
    {Manifest,
        ManifestSQN,
        JournalSQN,
        ActiveJournal} = build_manifest(ManifestFilenames,
                                        RootPath,
                                        CDBopts),
    {ok, #state{manifest = Manifest,
                    manifest_sqn = ManifestSQN,
                    journal_sqn = JournalSQN,
                    active_journaldb = ActiveJournal,
                    root_path = RootPath,
                    cdb_options = CDBopts#cdb_options{waste_path=WasteFP},
                    clerk = Clerk}}.


put_object(LedgerKey, Object, KeyChanges, State) ->
    NewSQN = State#state.journal_sqn + 1,
    ActiveJournal = State#state.active_journaldb,
    {JournalKey, JournalBin} = leveled_codec:to_inkerkv(LedgerKey,
                                                            NewSQN,
                                                            Object,
                                                            KeyChanges),
    case leveled_cdb:cdb_put(ActiveJournal,
                                JournalKey,
                                JournalBin) of
        ok ->
            {ok,
                State#state{journal_sqn=NewSQN},
                byte_size(JournalBin)};
        roll ->
            SWroll = os:timestamp(),
            LastKey = leveled_cdb:cdb_lastkey(ActiveJournal),
            ok = leveled_cdb:cdb_roll(ActiveJournal),
            Manifest0 = leveled_imanifest:append_lastkey(State#state.manifest, 
                                                            ActiveJournal,
                                                            LastKey),
            CDBopts = State#state.cdb_options,
            ManEntry = start_new_activejournal(NewSQN,
                                                State#state.root_path,
                                                CDBopts),
            {_, _, NewJournalP, _} = ManEntry,
            Manifest1 = leveled_imanifest:add_entry(Manifest0, ManEntry, true),
            ok = leveled_imanifest:writer(Manifest1,
                                            State#state.manifest_sqn + 1,
                                            State#state.root_path),
            ok = leveled_cdb:cdb_put(NewJournalP,
                                        JournalKey,
                                        JournalBin),
            leveled_log:log_timer("I0008", [], SWroll),
            {rolling,
                State#state{journal_sqn=NewSQN,
                                manifest=Manifest1,
                                manifest_sqn = State#state.manifest_sqn + 1,
                                active_journaldb=NewJournalP},
                byte_size(JournalBin)}
    end.


get_object(LedgerKey, SQN, Manifest) ->
    get_object(LedgerKey, SQN, Manifest, false).

get_object(LedgerKey, SQN, Manifest, ToIgnoreKeyChanges) ->
    JournalP = leveled_imanifest:find_entry(SQN, Manifest),
    {InkerKey, _V, true} = leveled_codec:to_inkerkv(LedgerKey,
                                                    SQN,
                                                    to_fetch,
                                                    null),
    Obj = leveled_cdb:cdb_get(JournalP, InkerKey),
    leveled_codec:from_inkerkv(Obj, ToIgnoreKeyChanges).

key_check(LedgerKey, SQN, Manifest) ->
    JournalP = leveled_imanifest:find_entry(SQN, Manifest),
    {InkerKey, _V, true} = leveled_codec:to_inkerkv(LedgerKey,
                                                    SQN,
                                                    to_fetch,
                                                    null),
    leveled_cdb:cdb_keycheck(JournalP, InkerKey).

build_manifest(ManifestFilenames,
                RootPath,
                CDBopts) ->
    % Find the manifest with a highest Manifest sequence number
    % Open it and read it to get the current Confirmed Manifest
    ManifestRegex = "(?<MSQN>[0-9]+)\\." ++ leveled_imanifest:complete_filex(),
    ValidManSQNs = sequencenumbers_fromfilenames(ManifestFilenames,
                                                    ManifestRegex,
                                                    'MSQN'),
    {Manifest,
        ManifestSQN} = case length(ValidManSQNs) of
                            0 ->
                                {[], 1};
                            _ ->
                                PersistedManSQN = lists:max(ValidManSQNs),
                                M1 = leveled_imanifest:reader(PersistedManSQN,
                                                                RootPath),
                                {M1, PersistedManSQN}
                        end,
    
    % Open the manifest files, completing if necessary and ensure there is
    % a valid active journal at the head of the manifest
    OpenManifest = open_all_manifest(Manifest, RootPath, CDBopts),
    
    {ActiveLowSQN,
        _FN,
        ActiveJournal,
        _LK} = leveled_imanifest:head_entry(OpenManifest),
    JournalSQN = case leveled_cdb:cdb_lastkey(ActiveJournal) of
                        empty ->
                            ActiveLowSQN;
                        {JSQN, _Type, _LastKey} ->
                            JSQN
                    end,
    
    % Update the manifest if it has been changed by the process of laoding 
    % the manifest (must also increment the manifest SQN).
    UpdManifestSQN =
        if
            length(OpenManifest) > length(Manifest)  ->
                leveled_log:log("I0009", []),
                leveled_imanifest:printer(OpenManifest),
                NextSQN = ManifestSQN + 1,
                leveled_imanifest:writer(OpenManifest, NextSQN, RootPath),
                NextSQN;
            true ->
                leveled_log:log("I0010", []),
                leveled_imanifest:printer(OpenManifest),
                ManifestSQN
        end,
    {OpenManifest, UpdManifestSQN, JournalSQN, ActiveJournal}.


close_allmanifest([]) ->
    ok;
close_allmanifest([H|ManifestT]) ->
    {_, _, Pid, _} = H,
    ok = leveled_cdb:cdb_close(Pid),
    close_allmanifest(ManifestT).


open_all_manifest([], RootPath, CDBOpts) ->
    leveled_log:log("I0011", []),
    leveled_imanifest:add_entry([],
                                start_new_activejournal(1, RootPath, CDBOpts),
                                true);
open_all_manifest(Man0, RootPath, CDBOpts) ->
    Man1 = leveled_imanifest:to_list(Man0),
    [{HeadSQN, HeadFN, _IgnorePid, HeadLK}|ManifestTail] = Man1,
    OpenJournalFun =
        fun(ManEntry) ->
            {LowSQN, FN, _, LK_RO} = ManEntry,
            CFN = FN ++ "." ++ ?JOURNAL_FILEX,
            PFN = FN ++ "." ++ ?PENDING_FILEX,
            case filelib:is_file(CFN) of
                true ->
                    {ok, Pid} = leveled_cdb:cdb_reopen_reader(CFN,
                                                                LK_RO),
                    {LowSQN, FN, Pid, LK_RO};
                false ->
                    W = leveled_cdb:cdb_open_writer(PFN, CDBOpts),
                    {ok, Pid} = W,
                    ok = leveled_cdb:cdb_roll(Pid),
                    LK_WR = leveled_cdb:cdb_lastkey(Pid),
                    {LowSQN, FN, Pid, LK_WR}
            end
        end,
    OpenedTailAsList = lists:map(OpenJournalFun, ManifestTail),
    OpenedTail = leveled_imanifest:from_list(OpenedTailAsList),
    CompleteHeadFN = HeadFN ++ "." ++ ?JOURNAL_FILEX,
    PendingHeadFN = HeadFN ++ "." ++ ?PENDING_FILEX,
    case filelib:is_file(CompleteHeadFN) of
        true ->
            leveled_log:log("I0012", [HeadFN]),
            {ok, HeadR} = leveled_cdb:cdb_open_reader(CompleteHeadFN),
            LastKey = leveled_cdb:cdb_lastkey(HeadR),
            LastSQN = element(1, LastKey),
            ManToHead = leveled_imanifest:add_entry(OpenedTail,
                                                    {HeadSQN,
                                                        HeadFN,
                                                        HeadR,
                                                        LastKey},
                                                    true),
            NewManEntry = start_new_activejournal(LastSQN + 1,
                                                        RootPath,
                                                        CDBOpts),
            leveled_imanifest:add_entry(ManToHead,
                                        NewManEntry,
                                        true);
        false ->
            {ok, HeadW} = leveled_cdb:cdb_open_writer(PendingHeadFN,
                                                        CDBOpts),
            leveled_imanifest:add_entry(OpenedTail,
                                            {HeadSQN, HeadFN, HeadW, HeadLK},
                                            true)
    end.


start_new_activejournal(SQN, RootPath, CDBOpts) ->
    Filename = filepath(RootPath, SQN, new_journal),
    {ok, PidW} = leveled_cdb:cdb_open_writer(Filename, CDBOpts),
    {SQN, Filename, PidW, empty}.


%% Scan between sequence numbers applying FilterFun to each entry where
%% FilterFun{K, V, Acc} -> Penciller Key List
%% Load the output for the CDB file into the Penciller.

load_from_sequence(_MinSQN, _FilterFun, _PCL, []) ->
    ok;
load_from_sequence(MinSQN, FilterFun, PCL, [{LowSQN, FN, Pid, _LK}|Rest])
                                        when LowSQN >= MinSQN ->
    load_between_sequence(MinSQN,
                            MinSQN + ?LOADING_BATCH,
                            FilterFun,
                            PCL,
                            Pid,
                            undefined,
                            FN,
                            Rest);
load_from_sequence(MinSQN, FilterFun, PCL, [{_LowSQN, FN, Pid, _LK}|Rest]) ->
    case Rest of
        [] ->
            load_between_sequence(MinSQN,
                                    MinSQN + ?LOADING_BATCH,
                                    FilterFun,
                                    PCL,
                                    Pid,
                                    undefined,
                                    FN,
                                    Rest);
        [{NextSQN, _NxtFN, _NxtPid, _NxtLK}|_Rest] when NextSQN > MinSQN ->
            load_between_sequence(MinSQN,
                                    MinSQN + ?LOADING_BATCH,
                                    FilterFun,
                                    PCL,
                                    Pid,
                                    undefined,
                                    FN,
                                    Rest);
        _ ->
            load_from_sequence(MinSQN, FilterFun, PCL, Rest)
    end.



load_between_sequence(MinSQN, MaxSQN, FilterFun, Penciller,
                                CDBpid, StartPos, FN, Rest) ->
    leveled_log:log("I0014", [FN, MinSQN]),
    InitAcc = {MinSQN, MaxSQN, leveled_bookie:empty_ledgercache()},
    Res = case leveled_cdb:cdb_scan(CDBpid, FilterFun, InitAcc, StartPos) of
                {eof, {AccMinSQN, _AccMaxSQN, AccLC}} ->
                    ok = push_to_penciller(Penciller, AccLC),
                    {ok, AccMinSQN};
                {LastPosition, {_AccMinSQN, _AccMaxSQN, AccLC}} ->
                    ok = push_to_penciller(Penciller, AccLC),
                    NextSQN = MaxSQN + 1,
                    load_between_sequence(NextSQN,
                                            NextSQN + ?LOADING_BATCH,
                                            FilterFun,
                                            Penciller,
                                            CDBpid,
                                            LastPosition,
                                            FN,
                                            Rest)
            end,
    case Res of
        {ok, LMSQN} ->
            load_from_sequence(LMSQN, FilterFun, Penciller, Rest);
        ok ->
            ok
    end.

push_to_penciller(Penciller, LedgerCache) ->
    % The push to penciller must start as a tree to correctly de-duplicate
    % the list by order before becoming a de-duplicated list for loading
    LC0 = leveled_bookie:loadqueue_ledgercache(LedgerCache),
    push_to_penciller_loop(Penciller, LC0).

push_to_penciller_loop(Penciller, LedgerCache) ->
    case leveled_bookie:push_ledgercache(Penciller, LedgerCache) of
        returned ->
            timer:sleep(?LOADING_PAUSE),
            push_to_penciller_loop(Penciller, LedgerCache);
        ok ->
            ok
    end.
            

sequencenumbers_fromfilenames(Filenames, Regex, IntName) ->
    lists:foldl(fun(FN, Acc) ->
                            case re:run(FN,
                                        Regex,
                                        [{capture, [IntName], list}]) of
                                nomatch ->
                                    Acc;
                                {match, [Int]} when is_list(Int) ->
                                    Acc ++ [list_to_integer(Int)]
                            end end,
                            [],
                            Filenames).


filepath(RootPath, journal_dir) ->
    RootPath ++ "/" ++ ?FILES_FP ++ "/";
filepath(RootPath, manifest_dir) ->
    RootPath ++ "/" ++ ?MANIFEST_FP ++ "/";
filepath(RootPath, journal_compact_dir) ->
    filepath(RootPath, journal_dir) ++ "/" ++ ?COMPACT_FP ++ "/";
filepath(RootPath, journal_waste_dir) ->
    filepath(RootPath, journal_dir) ++ "/" ++ ?WASTE_FP ++ "/".

filepath(RootPath, NewSQN, new_journal) ->
    filename:join(filepath(RootPath, journal_dir),
                    integer_to_list(NewSQN) ++ "_"
                        ++ leveled_codec:generate_uuid()
                        ++ "." ++ ?PENDING_FILEX);
filepath(CompactFilePath, NewSQN, compact_journal) ->
    filename:join(CompactFilePath,
                    integer_to_list(NewSQN) ++ "_"
                        ++ leveled_codec:generate_uuid()
                        ++ "." ++ ?PENDING_FILEX).


initiate_penciller_snapshot(Bookie) ->
    {ok, LedgerSnap, _} = leveled_bookie:book_snapshotledger(Bookie, self(), undefined),
    MaxSQN = leveled_penciller:pcl_getstartupsequencenumber(LedgerSnap),
    {LedgerSnap, MaxSQN}.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

create_value_for_journal(Obj, Comp) ->
    leveled_codec:create_value_for_journal(Obj, Comp).

build_dummy_journal() ->
    F = fun(X) -> X end,
    build_dummy_journal(F).

build_dummy_journal(KeyConvertF) ->
    RootPath = "../test/journal",
    clean_testdir(RootPath),
    JournalFP = filepath(RootPath, journal_dir),
    ManifestFP = filepath(RootPath, manifest_dir),
    ok = filelib:ensure_dir(RootPath),
    ok = filelib:ensure_dir(JournalFP),
    ok = filelib:ensure_dir(ManifestFP),
    F1 = filename:join(JournalFP, "nursery_1.pnd"),
    {ok, J1} = leveled_cdb:cdb_open_writer(F1),
    {K1, V1} = {KeyConvertF("Key1"), "TestValue1"},
    {K2, V2} = {KeyConvertF("Key2"), "TestValue2"},
    ok = leveled_cdb:cdb_put(J1,
                                {1, stnd, K1},
                                create_value_for_journal({V1, []}, false)),
    ok = leveled_cdb:cdb_put(J1,
                                {2, stnd, K2},
                                create_value_for_journal({V2, []}, false)),
    ok = leveled_cdb:cdb_roll(J1),
    LK1 = leveled_cdb:cdb_lastkey(J1),
    lists:foldl(fun(X, Closed) ->
                        case Closed of
                            true -> true;
                            false ->
                                case leveled_cdb:cdb_checkhashtable(J1) of
                                    true -> leveled_cdb:cdb_close(J1), true;
                                    false -> timer:sleep(X), false
                                end
                        end
                        end,
                    false,
                    lists:seq(1, 5)),
    F2 = filename:join(JournalFP, "nursery_3.pnd"),
    {ok, J2} = leveled_cdb:cdb_open_writer(F2),
    {K1, V3} = {KeyConvertF("Key1"), "TestValue3"},
    {K4, V4} = {KeyConvertF("Key4"), "TestValue4"},
    ok = leveled_cdb:cdb_put(J2,
                                {3, stnd, K1},
                                create_value_for_journal({V3, []}, false)),
    ok = leveled_cdb:cdb_put(J2,
                                {4, stnd, K4},
                                create_value_for_journal({V4, []}, false)),
    LK2 = leveled_cdb:cdb_lastkey(J2),
    ok = leveled_cdb:cdb_close(J2),
    Manifest = [{1, "../test/journal/journal_files/nursery_1", "pid1", LK1},
                    {3, "../test/journal/journal_files/nursery_3", "pid2", LK2}],
    ManifestBin = term_to_binary(Manifest),
    {ok, MF1} = file:open(filename:join(ManifestFP, "1.man"),
                            [binary, raw, read, write]),
    ok = file:write(MF1, ManifestBin),
    ok = file:close(MF1).


clean_testdir(RootPath) ->
    clean_subdir(filepath(RootPath, journal_dir)),
    clean_subdir(filepath(RootPath, journal_compact_dir)),
    clean_subdir(filepath(RootPath, manifest_dir)).

clean_subdir(DirPath) ->
    ok = filelib:ensure_dir(DirPath),
    {ok, Files} = file:list_dir(DirPath),
    lists:foreach(fun(FN) ->
                        File = filename:join(DirPath, FN),
                        case file:delete(File) of
                            ok -> io:format("Success deleting ~s~n", [File]);
                            _ -> io:format("Error deleting ~s~n", [File])
                        end
                        end,
                    Files).


simple_inker_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000, binary_mode=true},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    Obj1 = ink_get(Ink1, "Key1", 1),
    ?assertMatch({{1, "Key1"}, {"TestValue1", []}}, Obj1),
    Obj3 = ink_get(Ink1, "Key1", 3),
    ?assertMatch({{3, "Key1"}, {"TestValue3", []}}, Obj3),
    Obj4 = ink_get(Ink1, "Key4", 4),
    ?assertMatch({{4, "Key4"}, {"TestValue4", []}}, Obj4),
    ink_close(Ink1),
    clean_testdir(RootPath).

simple_inker_completeactivejournal_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000, binary_mode=true},
    JournalFP = filepath(RootPath, journal_dir),
    F2 = filename:join(JournalFP, "nursery_3.pnd"),
    {ok, PidW} = leveled_cdb:cdb_open_writer(F2),
    {ok, _F2} = leveled_cdb:cdb_complete(PidW),
    F1 = filename:join(JournalFP, "nursery_1.cdb"),
    F1r = filename:join(JournalFP, "nursery_1.pnd"),
    ok = file:rename(F1, F1r),
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    Obj1 = ink_get(Ink1, "Key1", 1),
    ?assertMatch({{1, "Key1"}, {"TestValue1", []}}, Obj1),
    Obj2 = ink_get(Ink1, "Key4", 4),
    ?assertMatch({{4, "Key4"}, {"TestValue4", []}}, Obj2),
    ink_close(Ink1),
    clean_testdir(RootPath).
    
test_ledgerkey(Key) ->
    {o, "Bucket", Key, null}.

compact_journal_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(fun test_ledgerkey/1),
    CDBopts = #cdb_options{max_size=300000},
    RStrategy = [{?STD_TAG, recovr}],
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts,
                                            reload_strategy=RStrategy}),
    {ok, NewSQN1, _ObjSize} = ink_put(Ink1,
                                        test_ledgerkey("KeyAA"),
                                        "TestValueAA",
                                        {[], infinity}),
    ?assertMatch(NewSQN1, 5),
    ok = ink_printmanifest(Ink1),
    R0 = ink_get(Ink1, test_ledgerkey("KeyAA"), 5),
    ?assertMatch(R0,
                    {{5, test_ledgerkey("KeyAA")},
                        {"TestValueAA", {[], infinity}}}),
    FunnyLoop = lists:seq(1, 48),
    Checker = lists:map(fun(X) ->
                            PK = "KeyZ" ++ integer_to_list(X),
                            {ok, SQN, _} = ink_put(Ink1,
                                                    test_ledgerkey(PK),
                                                    leveled_rand:rand_bytes(10000),
                                                    {[], infinity}),
                            {SQN, test_ledgerkey(PK)}
                            end,
                        FunnyLoop),
    {ok, NewSQN2, _ObjSize} = ink_put(Ink1,
                                        test_ledgerkey("KeyBB"),
                                        "TestValueBB",
                                        {[], infinity}),
    ?assertMatch(NewSQN2, 54),
    ActualManifest = ink_getmanifest(Ink1),
    ok = ink_printmanifest(Ink1),
    ?assertMatch(3, length(ActualManifest)),
    ok = ink_compactjournal(Ink1,
                            Checker,
                            fun(X) -> {X, 55} end,
                            fun(_F) -> ok end,
                            fun(L, K, SQN) -> lists:member({SQN, K}, L) end,
                            5000),
    timer:sleep(1000),
    CompactedManifest1 = ink_getmanifest(Ink1),
    ?assertMatch(2, length(CompactedManifest1)),
    Checker2 = lists:sublist(Checker, 16),
    ok = ink_compactjournal(Ink1,
                            Checker2,
                            fun(X) -> {X, 55} end,
                            fun(_F) -> ok end,
                            fun(L, K, SQN) -> lists:member({SQN, K}, L) end,
                            5000),
    timer:sleep(1000),
    CompactedManifest2 = ink_getmanifest(Ink1),
    R = lists:foldl(fun({_SQN, FN, _P, _LK}, Acc) ->
                                case string:str(FN, "post_compact") of
                                    N when N > 0 ->
                                        true;
                                    0 ->
                                        Acc
                                end end,
                            false,
                            CompactedManifest2),
    ?assertMatch(false, R),
    ?assertMatch(2, length(CompactedManifest2)),
    ink_close(Ink1),
    clean_testdir(RootPath).

empty_manifest_test() ->
    RootPath = "../test/journal",
    clean_testdir(RootPath),
    CDBopts = #cdb_options{max_size=300000},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    ?assertMatch(not_present, ink_fetch(Ink1, "Key1", 1)),
    
    CheckFun = fun(L, K, SQN) -> lists:member({SQN, K}, L) end,
    ?assertMatch(false, CheckFun([], "key", 1)),
    ok = ink_compactjournal(Ink1,
                            [],
                            fun(X) -> {X, 55} end,
                            fun(_F) -> ok end,
                            CheckFun,
                            5000),
    timer:sleep(1000),
    ?assertMatch(1, length(ink_getmanifest(Ink1))),
    ok = ink_close(Ink1),
    
    % Add pending manifest to be ignored
    FN = filepath(RootPath, manifest_dir) ++ "999.pnd",
    ok = file:write_file(FN, term_to_binary("Hello")),
    
    {ok, Ink2} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    ?assertMatch(not_present, ink_fetch(Ink2, "Key1", 1)),
    {ok, SQN, Size} = ink_put(Ink2, "Key1", "Value1", {[], infinity}),
    ?assertMatch(2, SQN),
    ?assertMatch(true, Size > 0),
    {ok, V} = ink_fetch(Ink2, "Key1", 2),
    ?assertMatch("Value1", V),
    ink_close(Ink2),
    clean_testdir(RootPath).

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).

-endif.
