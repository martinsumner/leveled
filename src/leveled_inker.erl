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
        ink_snapstart/1,
        ink_put/4,
        ink_mput/3,
        ink_get/3,
        ink_fetch/3,
        ink_keycheck/3,
        ink_fold/4,
        ink_loadpcl/4,
        ink_registersnapshot/2,
        ink_confirmdelete/2,
        ink_compactjournal/3,
        ink_clerkcomplete/3,
        ink_compactionpending/1,
        ink_trim/2,
        ink_getmanifest/1,
        ink_printmanifest/1,
        ink_close/1,
        ink_doom/1,
        ink_roll/1,
        ink_backup/2,
        ink_checksqn/2,
        ink_loglevel/2,
        ink_addlogs/2,
        ink_removelogs/2,
        ink_getjournalsqn/1]).

-export([build_dummy_journal/0,
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
-define(TEST_KC, {[], infinity}).

-record(state, {manifest = [] :: list(),
				manifest_sqn = 0 :: integer(),
                journal_sqn = 0 :: integer(),
                active_journaldb :: pid() | undefined,
                pending_removals = [] :: list(),
                registered_snapshots = [] :: list(registered_snapshot()),
                root_path :: string() | undefined,
                cdb_options :: #cdb_options{} | undefined,
                clerk :: pid() | undefined,
                compaction_pending = false :: boolean(),
                bookie_monref :: reference() | undefined,
                is_snapshot = false :: boolean(),
                compression_method = native :: lz4|native,
                compress_on_receipt = false :: boolean(),
                snap_timeout :: pos_integer() | undefined, % in seconds
                source_inker :: pid() | undefined}).


-type inker_options() :: #inker_options{}.
-type ink_state() :: #state{}.
-type registered_snapshot() :: {pid(), os:timestamp(), integer()}.

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
    gen_server:start_link(?MODULE, [leveled_log:get_opts(), InkerOpts], []).

-spec ink_snapstart(inker_options()) -> {ok, pid()}.
%% @doc
%% Don't link on startup as snapshot
ink_snapstart(InkerOpts) ->
    gen_server:start(?MODULE, [leveled_log:get_opts(), InkerOpts], []).

-spec ink_put(pid(),
                leveled_codec:ledger_key(),
                any(),
                leveled_codec:journal_keychanges()) ->
                                   {ok, integer(), integer()}.
%% @doc
%% PUT an object into the journal, returning the sequence number for the PUT
%% as well as the size of the object (information required by the ledger).
%%
%% KeyChanges is a tuple of {KeyChanges, TTL} where the TTL is an
%% expiry time (or infinity).
ink_put(Pid, PrimaryKey, Object, KeyChanges) ->
    gen_server:call(Pid, {put, PrimaryKey, Object, KeyChanges}, infinity).


-spec ink_mput(pid(), any(), {list(), integer()|infinity}) -> {ok, integer()}.
%% @doc
%% MPUT as series of object specifications, which will be converted into 
%% objects in the Ledger.  This should only be used when the Bookie is 
%% running in head_only mode.  The journal entries arekept only for handling
%% consistency on startup
ink_mput(Pid, PrimaryKey, ObjectChanges) ->
    gen_server:call(Pid, {mput, PrimaryKey, ObjectChanges}, infinity).

-spec ink_get(pid(),
                leveled_codec:ledger_key(),
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
                    leveled_codec:ledger_key(),
                    integer()) ->
                            probably|missing.
%% @doc
%% Quick check to determine if key is probably present.  Positive results have
%% a very small false positive rate, as can be triggered through a hash
%% collision.
ink_keycheck(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {key_check, PrimaryKey, SQN}, infinity).

-spec ink_registersnapshot(pid(), pid()) -> {list(), pid(), integer()}.
%% @doc
%% Register a snapshot clone for the process, returning the Manifest and the
%% pid of the active journal, as well as the JournalSQN.
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
    gen_server:call(Pid, doom, infinity).

-spec ink_fold(pid(), integer(), {fun(), fun(), fun()}, any()) -> fun().
%% @doc
%% Fold over the journal from a starting sequence number (MinSQN), passing 
%% in three functions and a snapshot of the penciller.  The Fold functions
%% should be 
%% - a FilterFun to accumulate the objects and decided when to stop or loop
%% - a InitAccFun to re-initialise for the fold over the accumulator
%% - a FoldFun to actually perform the fold
%%
%% The inker fold works in batches, so the FilterFun determines what should
%% go into a batch and when the batch is complete.  The FoldFun completes the
%% actual desired outcome by being applied on the batch.
%%
%% The FilterFun should be a five arity function which takes as inputs:
%% KeyInJournal 
%% ValueInJournal
%% Position - the actual position within the CDB file of the object
%% Acc - the bathc accumulator
%% ExtractFun - a single arity function which can be applied to ValueInJournal
%% to extract the actual object, and the size of the object,
%%
%% The FilterFun should return either:
%% {loop, {MinSQN, MaxSQN, UpdAcc}} or
%% {stop, {MinSQN, MaxSQN, UpdAcc}}
%% The FilterFun is required to call stop when MaxSQN is reached
%%
%% The InitAccFun should return an initial batch accumulator for each subfold.
%% It is a 2-arity function that takes a filename and a MinSQN as an input 
%% potentially to be used in logging 
%%
%% The BatchFun is a two arity function that should take as inputs:
%% An overall accumulator
%% The batch accumulator built over the sub-fold
%%
%% The output of ink_fold is a folder, that may actually run the fold.  The
%% type of the output of the function when called will depend on the type of
%% the accumulator
ink_fold(Pid, MinSQN, FoldFuns, Acc) ->
    gen_server:call(Pid,
                    {fold, MinSQN, FoldFuns, Acc, by_runner},
                    infinity).

-spec ink_loadpcl(pid(), integer(), fun(), pid()) -> ok.
%%
%% Function to prompt load of the Ledger at startup.  the Penciller should
%% have determined the lowest SQN not present in the Ledger, and the inker
%% should fold over the Journal from that point, using the function to load
%% penciller with the results.
%%
%% The load fun should be a five arity function like:
%% load_fun(KeyInJournal, ValueInJournal, _Position, Acc0, ExtractFun)
ink_loadpcl(Pid, MinSQN, FilterFun, Penciller) ->
    BatchFun = 
        fun(BatchAcc, _Acc) ->
            push_to_penciller(Penciller, BatchAcc)
        end,
    InitAccFun =
        fun(FN, CurrentMinSQN) ->
            leveled_log:log("I0014", [FN, CurrentMinSQN]),
            leveled_bookie:empty_ledgercache()
        end,
    gen_server:call(Pid, 
                    {fold, 
                        MinSQN, 
                        {FilterFun, InitAccFun, BatchFun}, 
                        ok,
                        as_ink},
                    infinity).

-spec ink_compactjournal(pid(), pid(), integer()) -> {ok|busy, pid()}.
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
ink_compactjournal(Pid, Bookie, _Timeout) ->
    CheckerInitiateFun = fun initiate_penciller_snapshot/1,
    CheckerCloseFun = fun leveled_penciller:pcl_close/1,
    CheckerFilterFun =
        wrap_checkfilterfun(fun leveled_penciller:pcl_checksequencenumber/3),
    gen_server:call(Pid,
                        {compact,
                            Bookie,
                            CheckerInitiateFun,
                            CheckerCloseFun,
                            CheckerFilterFun},
                        infinity).

%% Allows the Checker to be overriden in test, use something other than a
%% penciller
ink_compactjournal(Pid, Checker, InitiateFun, CloseFun, FilterFun, _Timeout) ->
    gen_server:call(Pid,
                        {compact,
                            Checker,
                            InitiateFun,
                            CloseFun,
                            FilterFun},
                        infinity).

-spec ink_clerkcomplete(pid(), list(), list()) -> ok.
%% @doc
%% Used by a clerk to state that a compaction process is over, only change
%% is to unlock the Inker for further compactions.
ink_clerkcomplete(Pid, ManifestSnippet, FilesToDelete) ->
    gen_server:cast(Pid, {clerk_complete, ManifestSnippet, FilesToDelete}).

-spec ink_compactionpending(pid()) -> boolean().
%% @doc
%% Is there ongoing compaction work?  No compaction work should be initiated
%% if there is already some compaction work ongoing.
ink_compactionpending(Pid) ->
    gen_server:call(Pid, compaction_pending, infinity).

-spec ink_trim(pid(), integer()) -> ok.
%% @doc
%% Trim the Journal to just those files that contain entries since the 
%% Penciller's persisted SQN
ink_trim(Pid, PersistedSQN) ->
    gen_server:call(Pid, {trim, PersistedSQN}, infinity).

-spec ink_roll(pid()) -> ok.
%% @doc
%% Roll the active journal
ink_roll(Pid) ->
    gen_server:call(Pid, roll, infinity).

-spec ink_backup(pid(), string()) -> ok.
%% @doc
%% Backup the journal to the specified path
ink_backup(Pid, BackupPath) ->
    gen_server:call(Pid, {backup, BackupPath}).

-spec ink_getmanifest(pid()) -> list().
%% @doc
%% Allows the clerk to fetch the manifest at the point it starts a compaction
%% job
ink_getmanifest(Pid) ->
    gen_server:call(Pid, get_manifest, infinity).

-spec ink_printmanifest(pid()) -> ok.
%% @doc 
%% Used in tests to print out the manifest
ink_printmanifest(Pid) ->
    gen_server:call(Pid, print_manifest, infinity).

-spec ink_checksqn(pid(), integer()) -> ok.
%% @doc
%% Check that the Inker doesn't have a SQN behind that of the Ledger
ink_checksqn(Pid, LedgerSQN) ->
    gen_server:call(Pid, {check_sqn, LedgerSQN}).

-spec ink_loglevel(pid(), leveled_log:log_level()) -> ok.
%% @doc
%% Change the log level of the Journal
ink_loglevel(Pid, LogLevel) ->
    gen_server:cast(Pid, {log_level, LogLevel}).

-spec ink_addlogs(pid(), list(string())) -> ok.
%% @doc
%% Add to the list of forced logs, a list of more forced logs
ink_addlogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {add_logs, ForcedLogs}).

-spec ink_removelogs(pid(), list(string())) -> ok.
%% @doc
%% Remove from the list of forced logs, a list of forced logs
ink_removelogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {remove_logs, ForcedLogs}).

-spec ink_getjournalsqn(pid()) -> {ok, pos_integer()}.
%% @doc
%% Return the current Journal SQN, which may be in the actual past if the Inker
%% is in fact a snapshot
ink_getjournalsqn(Pid) ->
    gen_server:call(Pid, get_journalsqn).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([LogOpts, InkerOpts]) ->
    leveled_log:save(LogOpts),
    leveled_rand:seed(),
    case {InkerOpts#inker_options.root_path,
            InkerOpts#inker_options.start_snapshot} of
        {undefined, true} ->
            %% monitor the bookie, and close the snapshot when bookie
            %% exits
	        BookieMonitor = erlang:monitor(process, InkerOpts#inker_options.bookies_pid),
            SrcInker = InkerOpts#inker_options.source_inker,
            {Manifest,
                ActiveJournalDB,
                JournalSQN} = ink_registersnapshot(SrcInker, self()),
            {ok, #state{manifest = Manifest,
                            active_journaldb = ActiveJournalDB,
                            source_inker = SrcInker,
                            journal_sqn = JournalSQN,
                            bookie_monref = BookieMonitor,
                            is_snapshot = true}};
            %% Need to do something about timeout
        {_RootPath, false} ->
            start_from_file(InkerOpts)
    end.


handle_call({put, Key, Object, KeyChanges}, _From,
                State=#state{is_snapshot=Snap}) when Snap == false ->
    case put_object(Key, Object, KeyChanges, State) of
        {_, UpdState, ObjSize} ->
            {reply, {ok, UpdState#state.journal_sqn, ObjSize}, UpdState}
    end;
handle_call({mput, Key, ObjChanges}, _From,
                State=#state{is_snapshot=Snap}) when Snap == false ->
    case put_object(Key, head_only, ObjChanges, State) of
        {_, UpdState, _ObjSize} ->
            {reply, {ok, UpdState#state.journal_sqn}, UpdState}
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
handle_call({fold, 
                StartSQN, {FilterFun, InitAccFun, FoldFun}, Acc, By},
                _From, State) ->
    Manifest = lists:reverse(leveled_imanifest:to_list(State#state.manifest)),
    Folder = 
        fun() ->
            fold_from_sequence(StartSQN, 
                                {FilterFun, InitAccFun, FoldFun}, 
                                Acc, 
                                Manifest)
        end,
    case By of
        as_ink ->
            {reply, Folder(), State};
        by_runner ->
            {reply, Folder, State}
    end;
handle_call({register_snapshot, Requestor},
            _From , State=#state{is_snapshot=Snap}) when Snap == false ->
    Rs = [{Requestor,
            os:timestamp(),
            State#state.manifest_sqn}|State#state.registered_snapshots],
    leveled_log:log("I0002", [Requestor, State#state.manifest_sqn]),
    {reply, {State#state.manifest,
                State#state.active_journaldb,
                State#state.journal_sqn},
                State#state{registered_snapshots=Rs}};
handle_call({confirm_delete, ManSQN}, _From, State) ->
    % Check there are no snapshots that may be aware of the file process that
    % is waiting to delete itself.
    CheckSQNFun = 
        fun({_R, _TS, SnapSQN}, Bool) ->
            % If the Snapshot SQN was at the same point the file was set to
            % delete (or after), then the snapshot would not have been told
            % of the file, and the snapshot should not hold up its deletion 
            (SnapSQN >= ManSQN) and Bool
        end,
    CheckSnapshotExpiryFun =
        fun({_R, TS, _SnapSQN}) ->
            Expiry = leveled_util:integer_time(TS) + State#state.snap_timeout,
                % If Expiry has passed this will be false, and the snapshot
                % will be removed from the list of registered snapshots and
                % so will not longer block deletes
            leveled_util:integer_now() < Expiry
        end,
    RegisteredSnapshots0 =
        lists:filter(CheckSnapshotExpiryFun, State#state.registered_snapshots),
    {reply, 
        lists:foldl(CheckSQNFun, true, RegisteredSnapshots0), 
        State#state{registered_snapshots = RegisteredSnapshots0}};
handle_call(get_manifest, _From, State) ->
    {reply, leveled_imanifest:to_list(State#state.manifest), State};
handle_call(print_manifest, _From, State) ->
    leveled_imanifest:printer(State#state.manifest),
    {reply, ok, State};
handle_call({compact,
                Checker,
                InitiateFun,
                CloseFun,
                FilterFun},
                _From, State=#state{is_snapshot=Snap}) when Snap == false ->
    Clerk = State#state.clerk,
    Manifest = leveled_imanifest:to_list(State#state.manifest),
    leveled_iclerk:clerk_compact(State#state.clerk,
                                    Checker,
                                    InitiateFun,
                                    CloseFun,
                                    FilterFun,
                                    Manifest),
    {reply, {ok, Clerk}, State#state{compaction_pending=true}};
handle_call(compaction_pending, _From, State) ->
    {reply, State#state.compaction_pending, State};
handle_call({trim, PersistedSQN}, _From, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->
    Manifest = leveled_imanifest:to_list(State#state.manifest),
    ok = leveled_iclerk:clerk_trim(State#state.clerk, PersistedSQN, Manifest),
    {reply, ok, State};
handle_call(roll, _From, State=#state{is_snapshot=Snap}) when Snap == false ->
    case leveled_cdb:cdb_lastkey(State#state.active_journaldb) of
        empty ->
            {reply, ok, State};
        _ ->
            NewSQN = State#state.journal_sqn + 1,
            SWroll = os:timestamp(),
            {NewJournalP, Manifest1, NewManSQN} = 
                roll_active(State#state.active_journaldb, 
                            State#state.manifest, 
                            NewSQN,
                            State#state.cdb_options,
                            State#state.root_path,
                            State#state.manifest_sqn),
            leveled_log:log_timer("I0024", [NewSQN], SWroll),
            {reply, ok, State#state{journal_sqn = NewSQN,
                                        manifest = Manifest1,
                                        manifest_sqn = NewManSQN,
                                        active_journaldb = NewJournalP}}
    end;
handle_call({backup, BackupPath}, _from, State) 
                                        when State#state.is_snapshot == true ->
    SW = os:timestamp(),
    BackupJFP = filepath(filename:join(BackupPath, ?JOURNAL_FP), journal_dir),
    ok = filelib:ensure_dir(BackupJFP),
    {ok, CurrentFNs} = file:list_dir(BackupJFP),
    leveled_log:log("I0023", [length(CurrentFNs)]),
    BackupFun =
        fun({SQN, FN, PidR, LastKey}, {ManAcc, FTRAcc}) ->
            case SQN < State#state.journal_sqn of
                true ->
                    BaseFN = filename:basename(FN),
                    ExtendedBaseFN = BaseFN ++ "." ++ ?JOURNAL_FILEX,
                    BackupName = filename:join(BackupJFP, BaseFN),
                    true = leveled_cdb:finished_rolling(PidR),
                    case file:make_link(FN ++ "." ++ ?JOURNAL_FILEX, 
                                            BackupName ++ "." ++ ?JOURNAL_FILEX) of
                        ok ->
                            ok;
                        {error, eexist} ->
                            ok
                    end,
                    {[{SQN, BackupName, PidR, LastKey}|ManAcc],
                        [ExtendedBaseFN|FTRAcc]};
                false ->
                    leveled_log:log("I0021", [FN, SQN, State#state.journal_sqn]),
                    {ManAcc, FTRAcc}
            end
        end,
    {BackupManifest, FilesToRetain} = 
        lists:foldr(BackupFun, 
                    {[], []}, 
                    leveled_imanifest:to_list(State#state.manifest)),    
    
    FilesToRemove = lists:subtract(CurrentFNs, FilesToRetain),
    RemoveFun = 
        fun(RFN) -> 
            leveled_log:log("I0022", [RFN]),
            RemoveFile = filename:join(BackupJFP, RFN),
            case filelib:is_file(RemoveFile) 
                    and not filelib:is_dir(RemoveFile) of 
                true ->
                    ok = file:delete(RemoveFile);
                false ->
                    ok
            end
        end,
    lists:foreach(RemoveFun, FilesToRemove),
    leveled_imanifest:writer(leveled_imanifest:from_list(BackupManifest),
                                State#state.manifest_sqn, 
                                filename:join(BackupPath, ?JOURNAL_FP)),
    leveled_log:log_timer("I0020", 
                            [filename:join(BackupPath, ?JOURNAL_FP), 
                                length(BackupManifest)], 
                            SW),
    {reply, ok, State};
handle_call({check_sqn, LedgerSQN}, _From, State) ->
    case State#state.journal_sqn of
        JSQN when JSQN < LedgerSQN ->
            leveled_log:log("I0025", [JSQN, LedgerSQN]),
            {reply, ok, State#state{journal_sqn = LedgerSQN}};
        _JSQN ->
            {reply, ok, State}
    end;
handle_call(get_journalsqn, _From, State) ->
    {reply, {ok, State#state.journal_sqn}, State};
handle_call(close, _From, State) ->
    case State#state.is_snapshot of
        true ->
            ok = ink_releasesnapshot(State#state.source_inker, self());
        false ->    
            leveled_log:log("I0005", [close]),
            leveled_log:log("I0006", [State#state.journal_sqn,
                                        State#state.manifest_sqn]),
            ok = leveled_iclerk:clerk_stop(State#state.clerk),
            shutdown_snapshots(State#state.registered_snapshots),
            shutdown_manifest(State#state.manifest)
    end,
    {stop, normal, ok, State};
handle_call(doom, _From, State) ->
    FPs = [filepath(State#state.root_path, journal_dir),
            filepath(State#state.root_path, manifest_dir),
            filepath(State#state.root_path, journal_compact_dir),
            filepath(State#state.root_path, journal_waste_dir)],
    leveled_log:log("I0018", []),

    leveled_log:log("I0005", [doom]),
    leveled_log:log("I0006", [State#state.journal_sqn,
                                State#state.manifest_sqn]),
    ok = leveled_iclerk:clerk_stop(State#state.clerk),
    shutdown_snapshots(State#state.registered_snapshots),
    shutdown_manifest(State#state.manifest),
    {stop, normal, {ok, FPs}, State}.


handle_cast({clerk_complete, ManifestSnippet, FilesToDelete}, State) ->
    CDBOpts = State#state.cdb_options,
    DropFun =
        fun(E, Acc) ->
            leveled_imanifest:remove_entry(Acc, E)
        end,
    Man0 = lists:foldl(DropFun, State#state.manifest, FilesToDelete),                    
    AddFun =
        fun(ManEntry, Acc) ->
            {LowSQN, FN, _, LK_RO} = ManEntry,
                % At this stage the FN has a .cdb extension, which will be
                % stripped during add_entry - so need to add the .cdb here
            {ok, Pid} = leveled_cdb:cdb_reopen_reader(FN, LK_RO, CDBOpts),
            UpdEntry = {LowSQN, FN, Pid, LK_RO},
            leveled_imanifest:add_entry(Acc, UpdEntry, false)
        end,
    Man1 = lists:foldl(AddFun, Man0, ManifestSnippet),
    NewManifestSQN = State#state.manifest_sqn + 1,
    leveled_imanifest:printer(Man1),
    leveled_imanifest:writer(Man1, NewManifestSQN, State#state.root_path),
    ok = leveled_iclerk:clerk_promptdeletions(State#state.clerk,
                                                NewManifestSQN,
                                                FilesToDelete),
    {noreply, State#state{manifest=Man1,
                            manifest_sqn=NewManifestSQN,
                            pending_removals=FilesToDelete,
                            compaction_pending=false}};
handle_cast({release_snapshot, Snapshot}, State) ->
    leveled_log:log("I0003", [Snapshot]),
    case lists:keydelete(Snapshot, 1, State#state.registered_snapshots) of
        [] ->
            {noreply, State#state{registered_snapshots=[]}};
        Rs ->
            leveled_log:log("I0004", [length(Rs)]),
            {noreply, State#state{registered_snapshots=Rs}}
    end;
handle_cast({log_level, LogLevel}, State) ->
    INC = State#state.clerk,
    ok = leveled_iclerk:clerk_loglevel(INC, LogLevel),
    ok = leveled_log:set_loglevel(LogLevel),
    CDBopts = State#state.cdb_options,
    CDBopts0 = CDBopts#cdb_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{cdb_options = CDBopts0}};
handle_cast({add_logs, ForcedLogs}, State) ->
    INC = State#state.clerk,
    ok = leveled_iclerk:clerk_addlogs(INC, ForcedLogs),
    ok = leveled_log:add_forcedlogs(ForcedLogs),
    CDBopts = State#state.cdb_options,
    CDBopts0 = CDBopts#cdb_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{cdb_options = CDBopts0}};
handle_cast({remove_logs, ForcedLogs}, State) ->
    INC = State#state.clerk,
    ok = leveled_iclerk:clerk_removelogs(INC, ForcedLogs),
    ok = leveled_log:remove_forcedlogs(ForcedLogs),
    CDBopts = State#state.cdb_options,
    CDBopts0 = CDBopts#cdb_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{cdb_options = CDBopts0}}.

%% handle the bookie stopping and stop this snapshot
handle_info({'DOWN', BookieMonRef, process, _BookiePid, _Info},
	    State=#state{bookie_monref = BookieMonRef}) ->
    %% Monitor only registered on snapshots
    ok = ink_releasesnapshot(State#state.source_inker, self()),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec start_from_file(inker_options()) -> {ok, ink_state()}.
%% @doc
%% Start an Inker from the state on disk (i.e. not a snapshot).  
start_from_file(InkOpts) ->
    % Setting the correct CDB options is important when starting the inker, in
    % particular for waste retention which is determined by the CDB options 
    % with which the file was last opened
    CDBopts = get_cdbopts(InkOpts),
    
    % Determine filepaths
    RootPath = InkOpts#inker_options.root_path,
    JournalFP = filepath(RootPath, journal_dir),
    ok = filelib:ensure_dir(JournalFP),
    CompactFP = filepath(RootPath, journal_compact_dir),
    ok = filelib:ensure_dir(CompactFP),
    ManifestFP = filepath(RootPath, manifest_dir),
    ok = filelib:ensure_dir(ManifestFP),
    % The IClerk must start files with the compaction file path so that they
    % will be stored correctly in this folder
    IClerkCDBOpts = CDBopts#cdb_options{file_path = CompactFP},
    
    WRP = InkOpts#inker_options.waste_retention_period,
    ReloadStrategy = InkOpts#inker_options.reload_strategy,
    MRL = InkOpts#inker_options.max_run_length,
    SFL_CompactPerc = InkOpts#inker_options.singlefile_compactionperc,
    MRL_CompactPerc = InkOpts#inker_options.maxrunlength_compactionperc,
    PressMethod = InkOpts#inker_options.compression_method,
    PressOnReceipt = InkOpts#inker_options.compress_on_receipt,
    SnapTimeout = InkOpts#inker_options.snaptimeout_long,

    IClerkOpts = 
        #iclerk_options{inker = self(),
                            cdb_options=IClerkCDBOpts,
                            waste_retention_period = WRP,
                            reload_strategy = ReloadStrategy,
                            compression_method = PressMethod,
                            max_run_length = MRL,
                            singlefile_compactionperc = SFL_CompactPerc,
                            maxrunlength_compactionperc = MRL_CompactPerc},
    
    {ok, Clerk} = leveled_iclerk:clerk_new(IClerkOpts),
    
    % The building of the manifest will load all the CDB files, starting a 
    % new leveled_cdb process for each file
    {ok, ManifestFilenames} = file:list_dir(ManifestFP),
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
                    cdb_options = CDBopts,
                    compression_method = PressMethod,
                    compress_on_receipt = PressOnReceipt,
                    snap_timeout = SnapTimeout,
                    clerk = Clerk}}.


-spec shutdown_snapshots(list(registered_snapshot())) -> ok.
%% @doc
%% Shutdown any snapshots before closing the store
shutdown_snapshots(Snapshots) ->
    lists:foreach(fun({Snap, _TS, _SQN}) -> ok = ink_close(Snap) end,
                    Snapshots).

-spec shutdown_manifest(leveled_imanifest:manifest()) -> ok.
%% @doc
%% Shutdown all files in the manifest
shutdown_manifest(Manifest) ->
    leveled_log:log("I0007", []),
    leveled_imanifest:printer(Manifest),
    ManAsList = leveled_imanifest:to_list(Manifest),
    close_allmanifest(ManAsList).

-spec get_cdbopts(inker_options()) -> #cdb_options{}.
%% @doc
%% Extract the options for the indibvidal Journal files from the Inker options
get_cdbopts(InkOpts)->
    CDBopts = InkOpts#inker_options.cdb_options,
    WasteFP = 
        case InkOpts#inker_options.waste_retention_period of 
            undefined ->
                % If the waste retention period is undefined, there will
                % be no retention of waste.  This is triggered by making
                % the waste path undefined
                undefined;
            _WRP ->
                WFP = filepath(InkOpts#inker_options.root_path, 
                                journal_waste_dir),
                filelib:ensure_dir(WFP),
                WFP
        end,
    CDBopts#cdb_options{waste_path = WasteFP}.


-spec put_object(leveled_codec:ledger_key(), 
                    any(), 
                    leveled_codec:journal_keychanges(), 
                    ink_state()) 
                                    -> {ok|rolling, ink_state(), integer()}.
%% @doc
%% Add the object to the current journal if it fits.  If it doesn't fit, a new 
%% journal must be started, and the old journal is set to "roll" into a read
%% only Journal. 
%% The reply contains the byte_size of the object, using the size calculated
%% to store the object.
put_object(LedgerKey, Object, KeyChanges, State) ->
    NewSQN = State#state.journal_sqn + 1,
    ActiveJournal = State#state.active_journaldb,
    {JournalKey, JournalBin} = 
        leveled_codec:to_inkerkv(LedgerKey,
                                    NewSQN,
                                    Object,
                                    KeyChanges,
                                    State#state.compression_method,
                                    State#state.compress_on_receipt),
    case leveled_cdb:cdb_put(ActiveJournal,
                                JournalKey,
                                JournalBin) of
        ok ->
            {ok,
                State#state{journal_sqn=NewSQN},
                byte_size(JournalBin)};
        roll ->
            SWroll = os:timestamp(),
            {NewJournalP, Manifest1, NewManSQN} = 
                roll_active(ActiveJournal, 
                            State#state.manifest, 
                            NewSQN,
                            State#state.cdb_options,
                            State#state.root_path,
                            State#state.manifest_sqn),
            leveled_log:log_timer("I0008", [], SWroll),
            ok = leveled_cdb:cdb_put(NewJournalP,
                                        JournalKey,
                                        JournalBin),
            {rolling,
                State#state{journal_sqn=NewSQN,
                                manifest=Manifest1,
                                manifest_sqn = NewManSQN,
                                active_journaldb=NewJournalP},
                byte_size(JournalBin)}
    end.


-spec get_object(leveled_codec:ledger_key(), 
                    integer(), 
                    leveled_imanifest:manifest()) -> any().
%% @doc
%% Find the SQN in the manifest and then fetch the object from the Journal, 
%% in the manifest.  If the fetch is in response to a user GET request then
%% the KeyChanges are irrelevant, so no need to process them.  In this case
%% the KeyChanges are processed (as ToIgnoreKeyChanges will be set to false).
get_object(LedgerKey, SQN, Manifest) ->
    get_object(LedgerKey, SQN, Manifest, false).

get_object(LedgerKey, SQN, Manifest, ToIgnoreKeyChanges) ->
    JournalP = leveled_imanifest:find_entry(SQN, Manifest),
    InkerKey = leveled_codec:to_inkerkey(LedgerKey, SQN),
    Obj = leveled_cdb:cdb_get(JournalP, InkerKey),
    leveled_codec:from_inkerkv(Obj, ToIgnoreKeyChanges).


-spec roll_active(pid(), leveled_imanifest:manifest(), 
                    integer(), #cdb_options{}, string(), integer()) ->
                            {pid(), leveled_imanifest:manifest(), integer()}.
%% @doc
%% Roll the active journal, and start a new active journal, updating the 
%% manifest
roll_active(ActiveJournal, Manifest, NewSQN, CDBopts, RootPath, ManifestSQN) ->
    LastKey = leveled_cdb:cdb_lastkey(ActiveJournal),
    ok = leveled_cdb:cdb_roll(ActiveJournal),
    Manifest0 = 
        leveled_imanifest:append_lastkey(Manifest, ActiveJournal, LastKey),
    ManEntry = 
        start_new_activejournal(NewSQN, RootPath, CDBopts),
    {_, _, NewJournalP, _} = ManEntry,
    Manifest1 = leveled_imanifest:add_entry(Manifest0, ManEntry, true),
    ok = leveled_imanifest:writer(Manifest1, ManifestSQN + 1, RootPath),
    
    {NewJournalP, Manifest1, ManifestSQN + 1}.

-spec key_check(leveled_codec:ledger_key(), 
                    integer(), 
                    leveled_imanifest:manifest()) -> missing|probably.
%% @doc
%% Checks for the presence of the key at that SQN withing the journal, 
%% avoiding the cost of actually reading the object from disk.
%% a KeyCheck is not absolute proof of the existence of the object - there 
%% could be a hash collision, or the on-disk object could be corrupted.  So
%% the positive answer is 'probably' not 'true'
key_check(LedgerKey, SQN, Manifest) ->
    JournalP = leveled_imanifest:find_entry(SQN, Manifest),
    InkerKey = leveled_codec:to_inkerkey(LedgerKey, SQN),
    leveled_cdb:cdb_keycheck(JournalP, InkerKey).


-spec build_manifest(list(), list(), #cdb_options{}) -> 
                {leveled_imanifest:manifest(), integer(), integer(), pid()}.
%% @doc
%% Selectes the correct manifets to open, and the starts a process for each 
%% file in the manifest, storing the PID for that process within the manifest.
%% Opens an active journal if one is not present.
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


-spec close_allmanifest(list()) -> ok.
%% @doc
%% Close every file in the manifest.  Will cause deletion of any delete_pending
%% files.
close_allmanifest([]) ->
    ok;
close_allmanifest([H|ManifestT]) ->
    {_, _, Pid, _} = H,
    ok = leveled_cdb:cdb_close(Pid),
    close_allmanifest(ManifestT).


-spec open_all_manifest(leveled_imanifest:manifest(), list(), #cdb_options{})
                                            -> leveled_imanifest:manifest().
%% @doc
%% Open all the files in the manifets, and updating the manifest with the PIDs
%% of the opened files
open_all_manifest([], RootPath, CDBOpts) ->
    leveled_log:log("I0011", []),
    leveled_imanifest:add_entry([],
                                start_new_activejournal(0, RootPath, CDBOpts),
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
                    {ok, Pid} = 
                        leveled_cdb:cdb_reopen_reader(CFN, LK_RO, CDBOpts),
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
            leveled_imanifest:add_entry(ManToHead, NewManEntry, true);
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



-spec fold_from_sequence(integer(), {fun(), fun(), fun()}, any(), list()) 
                                                                    -> any().
%% @doc
%%
%% Scan from the starting sequence number to the end of the Journal.  Apply
%% the FilterFun as it scans over the CDB file to build up a Batch of relevant
%% objects - and then apply the FoldFun to the batch once the batch is 
%% complete
%%
%% Inputs - MinSQN, FoldFuns, OverallAccumulator, Inker's Manifest
%%
%% The fold loops over all the CDB files in the Manifest.  Each file is looped
%% over in batches using foldfile_between_sequence/7.  The batch is a range of
%% sequence numbers (so the batch size may be << ?LOADING_BATCH) in compacted 
%% files
fold_from_sequence(_MinSQN, _FoldFuns, Acc, []) ->
    Acc;
fold_from_sequence(MinSQN, FoldFuns, Acc, [{LowSQN, FN, Pid, _LK}|Rest])
                                                    when LowSQN >= MinSQN ->    
    Acc0 = foldfile_between_sequence(MinSQN,
                                        MinSQN + ?LOADING_BATCH,
                                        FoldFuns,
                                        Acc,
                                        Pid,
                                        undefined,
                                        FN),
    fold_from_sequence(MinSQN, FoldFuns, Acc0, Rest);
fold_from_sequence(MinSQN, FoldFuns, Acc, [{_LowSQN, FN, Pid, _LK}|Rest]) ->
    % If this file has a LowSQN less than the minimum, we can skip it if the 
    % next file also has a LowSQN below the minimum
    Acc0 = 
        case Rest of
            [] ->
                foldfile_between_sequence(MinSQN,
                                            MinSQN + ?LOADING_BATCH,
                                            FoldFuns,
                                            Acc,
                                            Pid,
                                            undefined,
                                            FN);
            [{NextSQN, _NxtFN, _NxtPid, _NxtLK}|_Rest] when NextSQN > MinSQN ->
                foldfile_between_sequence(MinSQN,
                                            MinSQN + ?LOADING_BATCH,
                                            FoldFuns,
                                            Acc,
                                            Pid,
                                            undefined,
                                            FN);
            _ ->
                Acc    
        end,
    fold_from_sequence(MinSQN, FoldFuns, Acc0, Rest).

foldfile_between_sequence(MinSQN, MaxSQN, FoldFuns, 
                                                Acc, CDBpid, StartPos, FN) ->
    {FilterFun, InitAccFun, FoldFun} = FoldFuns,
    InitBatchAcc = {MinSQN, MaxSQN, InitAccFun(FN, MinSQN)},
    
    case leveled_cdb:cdb_scan(CDBpid, FilterFun, InitBatchAcc, StartPos) of
        {eof, {_AccMinSQN, _AccMaxSQN, BatchAcc}} ->
            FoldFun(BatchAcc, Acc);
        {LastPosition, {_AccMinSQN, _AccMaxSQN, BatchAcc}} ->
            UpdAcc = FoldFun(BatchAcc, Acc),
            NextSQN = MaxSQN + 1,
            foldfile_between_sequence(NextSQN,
                                        NextSQN + ?LOADING_BATCH,
                                        FoldFuns,
                                        UpdAcc,
                                        CDBpid,
                                        LastPosition,
                                        FN)
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
                        ++ leveled_util:generate_uuid()
                        ++ "." ++ ?PENDING_FILEX);
filepath(CompactFilePath, NewSQN, compact_journal) ->
    filename:join(CompactFilePath,
                    integer_to_list(NewSQN) ++ "_"
                        ++ leveled_util:generate_uuid()
                        ++ "." ++ ?PENDING_FILEX).


initiate_penciller_snapshot(LedgerSnap) ->
    MaxSQN = leveled_penciller:pcl_getstartupsequencenumber(LedgerSnap),
    {LedgerSnap, MaxSQN}.


-spec wrap_checkfilterfun(fun()) -> fun().
%% @doc
%% Make a check of the validity of the key being passed into the CheckFilterFun
wrap_checkfilterfun(CheckFilterFun) ->
    fun(Pcl, LK, SQN) ->
        case leveled_codec:isvalid_ledgerkey(LK) of
            true ->
                CheckFilterFun(Pcl, LK, SQN);
            false ->
                false
        end
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

create_value_for_journal(Obj, Comp) ->
    leveled_codec:create_value_for_journal(Obj, Comp, native).

key_converter(K) ->
    {o, <<"B">>, K, null}.

build_dummy_journal() ->
    build_dummy_journal(fun key_converter/1).

build_dummy_journal(KeyConvertF) ->
    RootPath = "test/test_area/journal",
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
    ok = 
        leveled_cdb:cdb_put(J1,
                            {1, stnd, K1},
                            create_value_for_journal({V1, ?TEST_KC}, false)),
    ok = 
        leveled_cdb:cdb_put(J1,
                            {2, stnd, K2},
                            create_value_for_journal({V2, ?TEST_KC}, false)),
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
    ok = 
        leveled_cdb:cdb_put(J2,
                            {3, stnd, K1},
                            create_value_for_journal({V3, ?TEST_KC}, false)),
    ok = 
        leveled_cdb:cdb_put(J2,
                            {4, stnd, K4},
                            create_value_for_journal({V4, ?TEST_KC}, false)),
    LK2 = leveled_cdb:cdb_lastkey(J2),
    ok = leveled_cdb:cdb_close(J2),
    Manifest = [{1, "test/test_area/journal/journal_files/nursery_1", "pid1", LK1},
                    {3, "test/test_area/journal/journal_files/nursery_3", "pid2", LK2}],
    ManifestBin = term_to_binary(Manifest),
    {ok, MF1} = file:open(filename:join(ManifestFP, "1.man"),
                            [binary, raw, read, write]),
    ok = file:write(MF1, ManifestBin),
    ok = file:close(MF1).


clean_testdir(RootPath) ->
    clean_subdir(filepath(RootPath, journal_dir)),
    clean_subdir(filepath(RootPath, journal_compact_dir)),
    clean_subdir(filepath(RootPath, journal_waste_dir)),
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
    RootPath = "test/test_area/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000, binary_mode=true},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts,
                                            compression_method=native,
                                            compress_on_receipt=true}),
    Obj1 = ink_get(Ink1, key_converter("Key1"), 1),
    ?assertMatch(Obj1, {{1, key_converter("Key1")}, {"TestValue1", ?TEST_KC}}),
    Obj3 = ink_get(Ink1, key_converter("Key1"), 3),
    ?assertMatch(Obj3, {{3, key_converter("Key1")}, {"TestValue3", ?TEST_KC}}),
    Obj4 = ink_get(Ink1, key_converter("Key4"), 4),
    ?assertMatch(Obj4, {{4, key_converter("Key4")}, {"TestValue4", ?TEST_KC}}),
    ink_close(Ink1),
    clean_testdir(RootPath).

simple_inker_completeactivejournal_test() ->
    RootPath = "test/test_area/journal",
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
                                            cdb_options=CDBopts,
                                            compression_method=native,
                                            compress_on_receipt=true}),
    Obj1 = ink_get(Ink1, key_converter("Key1"), 1),
    ?assertMatch(Obj1, {{1, key_converter("Key1")}, {"TestValue1", ?TEST_KC}}),
    Obj2 = ink_get(Ink1, key_converter("Key4"), 4),
    ?assertMatch(Obj2, {{4, key_converter("Key4")}, {"TestValue4", ?TEST_KC}}),
    ink_close(Ink1),
    clean_testdir(RootPath).
    
test_ledgerkey(Key) ->
    {o, "Bucket", Key, null}.

compact_journal_wasteretained_test_() ->
    {timeout, 60, fun() -> compact_journal_testto(300, true) end}.

compact_journal_wastediscarded_test_() ->
    {timeout, 60, fun() -> compact_journal_testto(undefined, false) end}.

compact_journal_testto(WRP, ExpectedFiles) ->
    RootPath = "test/test_area/journal",
    CDBopts = #cdb_options{max_size=300000},
    RStrategy = [{?STD_TAG, recovr}],
    InkOpts = #inker_options{root_path=RootPath,
                                cdb_options=CDBopts,
                                reload_strategy=RStrategy,
                                waste_retention_period=WRP,
                                singlefile_compactionperc=40.0,
                                maxrunlength_compactionperc=70.0,
                                compression_method=native,
                                compress_on_receipt=false},
    
    build_dummy_journal(fun test_ledgerkey/1),
    {ok, Ink1} = ink_start(InkOpts),
    
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
    {ok, _ICL1} = ink_compactjournal(Ink1,
                                    Checker,
                                    fun(X) -> {X, 55} end,
                                    fun(_F) -> ok end,
                                    fun(L, K, SQN) ->
                                        lists:member({SQN, K}, L)
                                    end,
                                    5000),
    timer:sleep(1000),
    CompactedManifest1 = ink_getmanifest(Ink1),
    ?assertMatch(2, length(CompactedManifest1)),
    Checker2 = lists:sublist(Checker, 16),
    {ok, _ICL2} = ink_compactjournal(Ink1,
                                        Checker2,
                                        fun(X) -> {X, 55} end,
                                        fun(_F) -> ok end,
                                        fun(L, K, SQN) ->
                                            lists:member({SQN, K}, L)
                                        end,
                                        5000),
    timer:sleep(1000),
    CompactedManifest2 = ink_getmanifest(Ink1),
    {ok, PrefixTest} = re:compile(?COMPACT_FP),
    lists:foreach(fun({_SQN, FN, _P, _LK}) ->
                            nomatch = re:run(FN, PrefixTest)
                        end,
                    CompactedManifest2),
    ?assertMatch(2, length(CompactedManifest2)),
    ink_close(Ink1),
    % Need to wait for delete_pending files to timeout
    timer:sleep(12000),
    % Are there files in the waste folder after compaction
    {ok, WasteFNs} = file:list_dir(filepath(RootPath, journal_waste_dir)),
    ?assertMatch(ExpectedFiles, length(WasteFNs) > 0),
    clean_testdir(RootPath).

empty_manifest_test() ->
    RootPath = "test/test_area/journal",
    clean_testdir(RootPath),
    CDBopts = #cdb_options{max_size=300000},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts,
                                            compression_method=native,
                                            compress_on_receipt=true}),
    ?assertMatch(not_present, ink_fetch(Ink1, key_converter("Key1"), 1)),
    
    CheckFun = fun(L, K, SQN) -> lists:member({SQN, key_converter(K)}, L) end,
    ?assertMatch(false, CheckFun([], "key", 1)),
    {ok, _ICL1} = ink_compactjournal(Ink1,
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
                                            cdb_options=CDBopts,
                                            compression_method=native,
                                            compress_on_receipt=false}),
    ?assertMatch(not_present, ink_fetch(Ink2, key_converter("Key1"), 1)),
    {ok, SQN, Size} = 
        ink_put(Ink2, key_converter("Key1"), "Value1", {[], infinity}),
    ?assertMatch(1, SQN), % This is the first key - so should have SQN of 1
    ?assertMatch(true, Size > 0),
    {ok, V} = ink_fetch(Ink2, key_converter("Key1"), 1),
    ?assertMatch("Value1", V),
    ink_close(Ink2),
    clean_testdir(RootPath).


wrapper_test() ->
    KeyNotTuple = [?STD_TAG, <<"B">>, <<"K">>, null],
    TagNotAtom = {"tag", <<"B">>, <<"K">>, null},
    CheckFilterFun = fun(_Pcl, _LK, _SQN) -> true end,
    WrappedFun = wrap_checkfilterfun(CheckFilterFun),
    ?assertMatch(false, WrappedFun(null, KeyNotTuple, 1)),
    ?assertMatch(false, WrappedFun(null, TagNotAtom, 1)).
    

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).

handle_down_test() ->
    RootPath = "test/test_area/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000, binary_mode=true},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                          cdb_options=CDBopts,
                                          compression_method=native,
                                          compress_on_receipt=true}),

    FakeBookie = spawn(fun loop/0),

    Mon = erlang:monitor(process, FakeBookie),

    SnapOpts = #inker_options{start_snapshot=true,
                              bookies_pid = FakeBookie,
                              source_inker=Ink1},

    {ok, Snap1} = ink_snapstart(SnapOpts),

    FakeBookie ! stop,

    receive
        {'DOWN', Mon, process, FakeBookie, normal} ->
            %% Now we know that inker should have received this too!
            %% (better than timer:sleep/1)
            ok
    end,

    ?assertEqual(undefined, erlang:process_info(Snap1)),

    ink_close(Ink1),
    clean_testdir(RootPath).

loop() ->
    receive
        stop ->
            ok
    end.

-endif.
