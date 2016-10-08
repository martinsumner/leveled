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
%% - A Primary Key (as an Erlang term)
%% - A sequence number (assigned by the Inker)
%% - An object (an Erlang term)
%% - A set of Key Deltas associated with the change
%%
%% -------- Compaction ---------
%%
%% Compaction is a process whereby an Inker's clerk will:
%% - Request a view of the current Inker manifest and a snaphot of the Ledger
%% - Test all files within the Journal to find th eapproximate comapction
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

-include("../include/leveled.hrl").

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
        ink_loadpcl/4,
        ink_registersnapshot/2,
        ink_compactjournal/3,
        ink_compactioncomplete/1,
        ink_getmanifest/1,
        ink_updatemanifest/3,
        ink_print_manifest/1,
        ink_close/1,
        ink_forceclose/1,
        build_dummy_journal/0,
        simple_manifest_reader/2,
        clean_testdir/1,
        filepath/2,
        filepath/3]).

-include_lib("eunit/include/eunit.hrl").

-define(MANIFEST_FP, "journal_manifest").
-define(FILES_FP, "journal_files").
-define(COMPACT_FP, "post_compact").
-define(JOURNAL_FILEX, "cdb").
-define(MANIFEST_FILEX, "man").
-define(PENDING_FILEX, "pnd").
-define(LOADING_PAUSE, 5000).
-define(LOADING_BATCH, 1000).

-record(state, {manifest = [] :: list(),
				manifest_sqn = 0 :: integer(),
                journal_sqn = 0 :: integer(),
                active_journaldb :: pid(),
                pending_removals = [] :: list(),
                registered_snapshots = [] :: list(),
                root_path :: string(),
                cdb_options :: #cdb_options{},
                clerk :: pid(),
                compaction_pending = false :: boolean(),
                is_snapshot = false :: boolean(),
                source_inker :: pid()}).


%%%============================================================================
%%% API
%%%============================================================================
 
ink_start(InkerOpts) ->
    gen_server:start(?MODULE, [InkerOpts], []).

ink_put(Pid, PrimaryKey, Object, KeyChanges) ->
    gen_server:call(Pid, {put, PrimaryKey, Object, KeyChanges}, infinity).

ink_get(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {get, PrimaryKey, SQN}, infinity).

ink_fetch(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {fetch, PrimaryKey, SQN}, infinity).

ink_registersnapshot(Pid, Requestor) ->
    gen_server:call(Pid, {register_snapshot, Requestor}, infinity).

ink_releasesnapshot(Pid, Snapshot) ->
    gen_server:call(Pid, {release_snapshot, Snapshot}, infinity).

ink_close(Pid) ->
    gen_server:call(Pid, {close, false}, infinity).

ink_forceclose(Pid) ->
    gen_server:call(Pid, {close, true}, infinity).

ink_loadpcl(Pid, MinSQN, FilterFun, Penciller) ->
    gen_server:call(Pid, {load_pcl, MinSQN, FilterFun, Penciller}, infinity).

ink_compactjournal(Pid, Bookie, Timeout) ->
    CheckerInitiateFun = fun initiate_penciller_snapshot/1,
    CheckerFilterFun = fun leveled_penciller:pcl_checksequencenumber/3,
    gen_server:call(Pid,
                        {compact,
                            Bookie,
                            CheckerInitiateFun,
                            CheckerFilterFun,
                            Timeout},
                        infinity).

%% Allows the Checker to be overriden in test, use something other than a
%% penciller
ink_compactjournal(Pid, Checker, InitiateFun, FilterFun, Timeout) ->
    gen_server:call(Pid,
                        {compact,
                            Checker,
                            InitiateFun,
                            FilterFun,
                            Timeout},
                        infinity).

ink_compactioncomplete(Pid) ->
    gen_server:call(Pid, compaction_complete, infinity).

ink_getmanifest(Pid) ->
    gen_server:call(Pid, get_manifest, infinity).

ink_updatemanifest(Pid, ManifestSnippet, DeletedFiles) ->
    gen_server:call(Pid,
                        {update_manifest,
                            ManifestSnippet,
                            DeletedFiles},
                        infinity).

ink_print_manifest(Pid) ->
    gen_server:call(Pid, print_manifest, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([InkerOpts]) ->
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
        {ok, UpdState, ObjSize} ->
            {reply, {ok, UpdState#state.journal_sqn, ObjSize}, UpdState};
        {rolling, UpdState, ObjSize} ->
            ok = leveled_cdb:cdb_roll(State#state.active_journaldb),
            {reply, {ok, UpdState#state.journal_sqn, ObjSize}, UpdState}
    end;
handle_call({fetch, Key, SQN}, _From, State) ->
    case get_object(Key, SQN, State#state.manifest) of
        {{SQN, Key}, {Value, _IndexSpecs}} ->
            {reply, {ok, Value}, State};
        Other ->
            io:format("Unexpected failure to fetch value for" ++
                        "Key=~w SQN=~w with reason ~w", [Key, SQN, Other]),
            {reply, not_present, State}
    end;
handle_call({get, Key, SQN}, _From, State) ->
    {reply, get_object(Key, SQN, State#state.manifest), State};
handle_call({load_pcl, StartSQN, FilterFun, Penciller}, _From, State) ->
    Manifest = lists:reverse(State#state.manifest),
    Reply = load_from_sequence(StartSQN, FilterFun, Penciller, Manifest),
    {reply, Reply, State};
handle_call({register_snapshot, Requestor}, _From , State) ->
    Rs = [{Requestor,
            State#state.manifest_sqn}|State#state.registered_snapshots],
    io:format("Inker snapshot ~w registered at SQN ~w~n",
                    [Requestor, State#state.manifest_sqn]),
    {reply, {State#state.manifest,
                State#state.active_journaldb},
                State#state{registered_snapshots=Rs}};
handle_call({release_snapshot, Snapshot}, _From , State) ->
    Rs = lists:keydelete(Snapshot, 1, State#state.registered_snapshots),
    io:format("Snapshot ~w released~n", [Snapshot]),
    io:format("Remaining snapshots are ~w~n", [Rs]),
    {reply, ok, State#state{registered_snapshots=Rs}};
handle_call(get_manifest, _From, State) ->
    {reply, State#state.manifest, State};
handle_call({update_manifest,
                ManifestSnippet,
                DeletedFiles}, _From, State) ->
    Man0 = lists:foldl(fun(ManEntry, AccMan) ->
                            remove_from_manifest(AccMan, ManEntry)
                            end,
                        State#state.manifest,
                        DeletedFiles),                    
    Man1 = lists:foldl(fun(ManEntry, AccMan) ->
                            add_to_manifest(AccMan, ManEntry) end,
                        Man0,
                        ManifestSnippet),
    NewManifestSQN = State#state.manifest_sqn + 1,
    manifest_printer(Man1),
    ok = simple_manifest_writer(Man1, NewManifestSQN, State#state.root_path),
    PendingRemovals = [{NewManifestSQN, DeletedFiles}],
    {reply, ok, State#state{manifest=Man1,
                                manifest_sqn=NewManifestSQN,
                                pending_removals=PendingRemovals}};
handle_call(print_manifest, _From, State) ->
    manifest_printer(State#state.manifest),
    {reply, ok, State};
handle_call({compact,
                Checker,
                InitiateFun,
                FilterFun,
                Timeout},
                    _From, State) ->
    leveled_iclerk:clerk_compact(State#state.clerk,
                                    Checker,
                                    InitiateFun,
                                    FilterFun,
                                    self(),
                                    Timeout),
    {reply, ok, State#state{compaction_pending=true}};
handle_call(compaction_complete, _From, State) ->
    {reply, ok, State#state{compaction_pending=false}};
handle_call({close, Force}, _From, State) ->
    case {State#state.compaction_pending, Force} of
        {true, false} ->
            {reply, pause, State};
        _ ->
            {stop, normal, ok, State}
    end;
handle_call(Msg, _From, State) ->
    io:format("Unexpected message ~w~n", [Msg]),
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    case State#state.is_snapshot of
        true ->
            ok = ink_releasesnapshot(State#state.source_inker, self());
        false ->    
            io:format("Inker closing journal for reason ~w~n", [Reason]),
            io:format("Close triggered with journal_sqn=~w and manifest_sqn=~w~n",
                            [State#state.journal_sqn, State#state.manifest_sqn]),
            io:format("Manifest when closing is: ~n"),
            leveled_iclerk:clerk_stop(State#state.clerk),
            lists:foreach(fun({Snap, _SQN}) -> ok = ink_close(Snap) end,
                            State#state.registered_snapshots),
            manifest_printer(State#state.manifest),
            ok = close_allmanifest(State#state.manifest),
            ok = close_allremovals(State#state.pending_removals)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

start_from_file(InkerOpts) ->
    RootPath = InkerOpts#inker_options.root_path,
    CDBopts = InkerOpts#inker_options.cdb_options,
    JournalFP = filepath(RootPath, journal_dir),
    filelib:ensure_dir(JournalFP),
    CompactFP = filepath(RootPath, journal_compact_dir),
    filelib:ensure_dir(CompactFP),
    
    ManifestFP = filepath(RootPath, manifest_dir),
    {ok, ManifestFilenames} = case filelib:is_dir(ManifestFP) of
        true ->
            file:list_dir(ManifestFP);
        false ->
            filelib:ensure_dir(ManifestFP),
            {ok, []}
    end,
    
    IClerkCDBOpts = CDBopts#cdb_options{file_path = CompactFP},
    IClerkOpts = #iclerk_options{inker = self(),
                                    cdb_options=IClerkCDBOpts},
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
                    cdb_options = CDBopts,
                    clerk = Clerk}}.


put_object(PrimaryKey, Object, KeyChanges, State) ->
    NewSQN = State#state.journal_sqn + 1,
    %% TODO: The term goes through a double binary_to_term conversion
    %% as the CDB will also do the same conversion
    %% Perhaps have CDB started up in apure binary mode, when it doesn't
    %5 receive terms?
    Bin1 = term_to_binary({Object, KeyChanges}, [compressed]),
    ObjSize = byte_size(Bin1),
    case leveled_cdb:cdb_put(State#state.active_journaldb,
                                {NewSQN, PrimaryKey},
                                Bin1) of
        ok ->
            {ok, State#state{journal_sqn=NewSQN}, ObjSize};
        roll ->
            SW = os:timestamp(),
            CDBopts = State#state.cdb_options,
            ManEntry = start_new_activejournal(NewSQN,
                                                State#state.root_path,
                                                CDBopts),
            {_, _, NewJournalP} = ManEntry,
            NewManifest = add_to_manifest(State#state.manifest, ManEntry),
            ok = simple_manifest_writer(NewManifest,
                                        State#state.manifest_sqn + 1,
                                        State#state.root_path),
            ok = leveled_cdb:cdb_put(NewJournalP, {NewSQN, PrimaryKey}, Bin1),
            io:format("Put to new active journal " ++
                            "with manifest write took ~w microseconds~n",
                        [timer:now_diff(os:timestamp(),SW)]),
            {rolling,
                State#state{journal_sqn=NewSQN,
                                manifest=NewManifest,
                                manifest_sqn = State#state.manifest_sqn + 1,
                                active_journaldb=NewJournalP},
                ObjSize}
    end.


get_object(PrimaryKey, SQN, Manifest) ->
    JournalP = find_in_manifest(SQN, Manifest),
    Obj = leveled_cdb:cdb_get(JournalP, {SQN, PrimaryKey}),
    case Obj of
        {{SQN, PK}, Bin} when is_binary(Bin) ->
            {{SQN, PK}, binary_to_term(Bin)};
        {{SQN, PK}, Term} ->
            {{SQN, PK}, Term};
        _ ->
            Obj
    end.


build_manifest(ManifestFilenames,
                RootPath,
                CDBopts) ->
    % Find the manifest with a highest Manifest sequence number
    % Open it and read it to get the current Confirmed Manifest
    ManifestRegex = "(?<MSQN>[0-9]+)\\." ++ ?MANIFEST_FILEX,
    ValidManSQNs = sequencenumbers_fromfilenames(ManifestFilenames,
                                                    ManifestRegex,
                                                    'MSQN'),
    {Manifest,
        ManifestSQN} = case length(ValidManSQNs) of
                            0 ->
                                {[], 1};
                            _ ->
                                PersistedManSQN = lists:max(ValidManSQNs),
                                M1 = simple_manifest_reader(PersistedManSQN,
                                                                RootPath),
                                {M1, PersistedManSQN}
                        end,
    
    % Open the manifest files, completing if necessary and ensure there is
    % a valid active journal at the head of the manifest
    OpenManifest = open_all_manifest(Manifest, RootPath, CDBopts),
    {ActiveLowSQN, _FN, ActiveJournal} = lists:nth(1, OpenManifest),
    JournalSQN = case leveled_cdb:cdb_lastkey(ActiveJournal) of
                        empty ->
                            ActiveLowSQN;
                        {JSQN, _LastKey} ->
                            JSQN
                    end,
    
    % Update the manifest if it has been changed by the process of laoding 
    % the manifest (must also increment the manifest SQN).
    UpdManifestSQN = if
                            length(OpenManifest) > length(Manifest)  ->
                                io:format("Updated manifest on startup: ~n"),
                                manifest_printer(OpenManifest),
                                simple_manifest_writer(OpenManifest,
                                                        ManifestSQN + 1,
                                                        RootPath),
                                ManifestSQN + 1;
                            true ->
                                io:format("Unchanged manifest on startup: ~n"),
                                manifest_printer(OpenManifest),
                                ManifestSQN
                        end,
    {OpenManifest, UpdManifestSQN, JournalSQN, ActiveJournal}.


close_allmanifest([]) ->
    ok;
close_allmanifest([H|ManifestT]) ->
    {_, _, Pid} = H,
    ok = leveled_cdb:cdb_close(Pid),
    close_allmanifest(ManifestT).


open_all_manifest([], RootPath, CDBOpts) ->
    io:format("Manifest is empty, starting from manifest SQN 1~n"),
    add_to_manifest([], start_new_activejournal(1, RootPath, CDBOpts));
open_all_manifest(Man0, RootPath, CDBOpts) ->
    Man1 = lists:reverse(lists:sort(Man0)),
    [{HeadSQN, HeadFN}|ManifestTail] = Man1,
    CompleteHeadFN = HeadFN ++ "." ++ ?JOURNAL_FILEX,
    PendingHeadFN = HeadFN ++ "." ++ ?PENDING_FILEX,
    Man2 = case filelib:is_file(CompleteHeadFN) of
                true ->
                    io:format("Head manifest entry ~s is complete~n",
                                [HeadFN]),
                    {ok, HeadR} = leveled_cdb:cdb_open_reader(CompleteHeadFN),
                    {LastSQN, _LastPK} = leveled_cdb:cdb_lastkey(HeadR),
                    add_to_manifest(add_to_manifest(ManifestTail,
                                                    {HeadSQN, HeadFN, HeadR}),
                                        start_new_activejournal(LastSQN + 1,
                                                                RootPath,
                                                                CDBOpts));
                false ->
                    {ok, HeadW} = leveled_cdb:cdb_open_writer(PendingHeadFN,
                                                                CDBOpts),
                    add_to_manifest(ManifestTail, {HeadSQN, HeadFN, HeadW})
            end,
    lists:map(fun(ManEntry) ->
                case ManEntry of
                    {LowSQN, FN} ->
                        CFN = FN ++ "." ++ ?JOURNAL_FILEX,
                        PFN = FN ++ "." ++ ?PENDING_FILEX,
                        case filelib:is_file(CFN) of
                            true ->
                                {ok,
                                    Pid} = leveled_cdb:cdb_open_reader(CFN),
                                {LowSQN, FN, Pid};
                            false ->
                                {ok,
                                    Pid} = leveled_cdb:cdb_open_reader(PFN),
                                {LowSQN, FN, Pid}
                        end;
                    _ ->
                        ManEntry
                end end,
                Man2).


start_new_activejournal(SQN, RootPath, CDBOpts) ->
    Filename = filepath(RootPath, SQN, new_journal),
    {ok, PidW} = leveled_cdb:cdb_open_writer(Filename, CDBOpts),
    {SQN, Filename, PidW}.

add_to_manifest(Manifest, Entry) ->
    {SQN, FN, PidR} = Entry,
    StrippedName = filename:rootname(FN),
    lists:reverse(lists:sort([{SQN, StrippedName, PidR}|Manifest])).

remove_from_manifest(Manifest, Entry) ->
    {SQN, FN, _PidR} = Entry,
    io:format("File ~s to be removed from manifest~n", [FN]),
    lists:keydelete(SQN, 1, Manifest).

find_in_manifest(_SQN, []) ->
    error;
find_in_manifest(SQN, [{LowSQN, _FN, Pid}|_Tail]) when SQN >= LowSQN ->
    Pid;
find_in_manifest(SQN, [_Head|Tail]) ->
    find_in_manifest(SQN, Tail).


close_allremovals([]) ->
    ok;
close_allremovals([{ManifestSQN, Removals}|Tail]) ->
    io:format("Closing removals at ManifestSQN=~w~n", [ManifestSQN]),
    lists:foreach(fun({LowSQN, FN, Handle}) ->
                        io:format("Closing removed file with LowSQN=~w" ++
                                        " and filename ~s~n",
                                    [LowSQN, FN]),
                        if
                            is_pid(Handle) == true ->
                                ok = leveled_cdb:cdb_close(Handle);
                            true ->
                                io:format("Non pid in removal ~w - test~n",
                                            [Handle])
                        end
                        end,
                    Removals),
    close_allremovals(Tail).


%% Scan between sequence numbers applying FilterFun to each entry where
%% FilterFun{K, V, Acc} -> Penciller Key List
%% Load the output for the CDB file into the Penciller.

load_from_sequence(_MinSQN, _FilterFun, _Penciller, []) ->
    ok;
load_from_sequence(MinSQN, FilterFun, Penciller, [{LowSQN, FN, Pid}|Rest])
                                        when LowSQN >= MinSQN ->
    load_between_sequence(MinSQN,
                            MinSQN + ?LOADING_BATCH,
                            FilterFun,
                            Penciller,
                            Pid,
                            undefined,
                            FN,
                            Rest);
load_from_sequence(MinSQN, FilterFun, Penciller, [{_LowSQN, FN, Pid}|Rest]) ->
    case Rest of
        [] ->
            load_between_sequence(MinSQN,
                                    MinSQN + ?LOADING_BATCH,
                                    FilterFun,
                                    Penciller,
                                    Pid,
                                    undefined,
                                    FN,
                                    Rest);
        [{NextSQN, _FN, Pid}|_Rest] when NextSQN > MinSQN ->
            load_between_sequence(MinSQN,
                                    MinSQN + ?LOADING_BATCH,
                                    FilterFun,
                                    Penciller,
                                    Pid,
                                    undefined,
                                    FN,
                                    Rest);
        _ ->
            load_from_sequence(MinSQN, FilterFun, Penciller, Rest)
    end.



load_between_sequence(MinSQN, MaxSQN, FilterFun, Penciller,
                                CDBpid, StartPos, FN, Rest) ->
    io:format("Loading from filename ~s from SQN ~w~n", [FN, MinSQN]),
    InitAcc = {MinSQN, MaxSQN, []},
    Res = case leveled_cdb:cdb_scan(CDBpid, FilterFun, InitAcc, StartPos) of
                {eof, {AccMinSQN, _AccMaxSQN, AccKL}} ->
                    ok = push_to_penciller(Penciller, AccKL),
                    {ok, AccMinSQN};
                {LastPosition, {_AccMinSQN, _AccMaxSQN, AccKL}} ->
                    ok = push_to_penciller(Penciller, AccKL),
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

push_to_penciller(Penciller, KeyList) ->
    R = leveled_penciller:pcl_pushmem(Penciller, KeyList),
    if
        R == pause ->
            timer:sleep(?LOADING_PAUSE);
        true ->
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
                                    Acc ++ [list_to_integer(Int)];       
                                _ ->
                                    Acc
                            end end,
                            [],
                            Filenames).


filepath(RootPath, journal_dir) ->
    RootPath ++ "/" ++ ?FILES_FP ++ "/";
filepath(RootPath, manifest_dir) ->
    RootPath ++ "/" ++ ?MANIFEST_FP ++ "/";
filepath(RootPath, journal_compact_dir) ->
    filepath(RootPath, journal_dir) ++ "/" ++ ?COMPACT_FP ++ "/".

filepath(RootPath, NewSQN, new_journal) ->
    filename:join(filepath(RootPath, journal_dir),
                    integer_to_list(NewSQN) ++ "_"
                        ++ generate_uuid()
                        ++ "." ++ ?PENDING_FILEX);
filepath(CompactFilePath, NewSQN, compact_journal) ->
    filename:join(CompactFilePath,
                    integer_to_list(NewSQN) ++ "_"
                        ++ generate_uuid()
                        ++ "." ++ ?PENDING_FILEX).

%% Credit to
%% https://github.com/afiskon/erlang-uuid-v4/blob/master/src/uuid.erl
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:rand_bytes(16),
    io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b", 
                        [A, B, C band 16#0fff, D band 16#3fff bor 16#8000, E]).

simple_manifest_reader(SQN, RootPath) ->
    ManifestPath = filepath(RootPath, manifest_dir),
    io:format("Opening manifest file at ~s with SQN ~w~n",
                [ManifestPath, SQN]),
    {ok, MBin} = file:read_file(filename:join(ManifestPath,
                                                integer_to_list(SQN)
                                                ++ ".man")),
    binary_to_term(MBin).
    

simple_manifest_writer(Manifest, ManSQN, RootPath) ->
    ManPath = filepath(RootPath, manifest_dir),
    NewFN = filename:join(ManPath,
                            integer_to_list(ManSQN) ++ "." ++ ?MANIFEST_FILEX),
    TmpFN = filename:join(ManPath,
                            integer_to_list(ManSQN) ++ "." ++ ?PENDING_FILEX),
    MBin = term_to_binary(lists:map(fun({SQN, FN, _PID}) -> {SQN, FN} end,
                                        Manifest)),
    case filelib:is_file(NewFN) of
        true ->
            io:format("Error - trying to write manifest for"
                        ++ " ManifestSQN=~w which already exists~n", [ManSQN]),
            error;
        false ->
            io:format("Writing new version of manifest for "
                        ++ " manifestSQN=~w~n", [ManSQN]),
            ok = file:write_file(TmpFN, MBin),
            ok = file:rename(TmpFN, NewFN),
            ok
    end.
    
manifest_printer(Manifest) ->
    lists:foreach(fun(X) ->
                        {SQN, FN} = case X of
                                        {A, B, _PID} ->
                                            {A, B};
                                        {A, B} ->
                                            {A, B}
                                    end,
                            io:format("At SQN=~w journal has filename ~s~n",
                                            [SQN, FN]) end,
                    Manifest).


initiate_penciller_snapshot(Bookie) ->
    {ok,
        {LedgerSnap, LedgerCache},
        _} = leveled_bookie:book_snapshotledger(Bookie, self(), undefined),
    ok = leveled_penciller:pcl_loadsnapshot(LedgerSnap,
                                                gb_trees:to_list(LedgerCache)),
    MaxSQN = leveled_penciller:pcl_getstartupsequencenumber(LedgerSnap),
    {LedgerSnap, MaxSQN}.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

build_dummy_journal() ->
    RootPath = "../test/journal",
    clean_testdir(RootPath),
    JournalFP = filepath(RootPath, journal_dir),
    ManifestFP = filepath(RootPath, manifest_dir),
    ok = filelib:ensure_dir(RootPath),
    ok = filelib:ensure_dir(JournalFP),
    ok = filelib:ensure_dir(ManifestFP),
    F1 = filename:join(JournalFP, "nursery_1.pnd"),
    {ok, J1} = leveled_cdb:cdb_open_writer(F1),
    {K1, V1} = {"Key1", "TestValue1"},
    {K2, V2} = {"Key2", "TestValue2"},
    ok = leveled_cdb:cdb_put(J1, {1, K1}, term_to_binary({V1, []})),
    ok = leveled_cdb:cdb_put(J1, {2, K2}, term_to_binary({V2, []})),
    ok = leveled_cdb:cdb_roll(J1),
    _LK = leveled_cdb:cdb_lastkey(J1),
    ok = leveled_cdb:cdb_close(J1),
    F2 = filename:join(JournalFP, "nursery_3.pnd"),
    {ok, J2} = leveled_cdb:cdb_open_writer(F2),
    {K1, V3} = {"Key1", "TestValue3"},
    {K4, V4} = {"Key4", "TestValue4"},
    ok = leveled_cdb:cdb_put(J2, {3, K1}, term_to_binary({V3, []})),
    ok = leveled_cdb:cdb_put(J2, {4, K4}, term_to_binary({V4, []})),
    ok = leveled_cdb:cdb_close(J2),
    Manifest = [{1, "../test/journal/journal_files/nursery_1"},
                    {3, "../test/journal/journal_files/nursery_3"}],
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
    CDBopts = #cdb_options{max_size=300000},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    Obj1 = ink_get(Ink1, "Key1", 1),
    ?assertMatch({{1, "Key1"}, {"TestValue1", []}}, Obj1),
    Obj2 = ink_get(Ink1, "Key4", 4),
    ?assertMatch({{4, "Key4"}, {"TestValue4", []}}, Obj2),
    ink_close(Ink1),
    clean_testdir(RootPath).

simple_inker_completeactivejournal_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000},
    {ok, PidW} = leveled_cdb:cdb_open_writer(filepath(RootPath,
                                                        3,
                                                        new_journal)),
    {ok, _FN} = leveled_cdb:cdb_complete(PidW),
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    Obj1 = ink_get(Ink1, "Key1", 1),
    ?assertMatch({{1, "Key1"}, {"TestValue1", []}}, Obj1),
    Obj2 = ink_get(Ink1, "Key4", 4),
    ?assertMatch({{4, "Key4"}, {"TestValue4", []}}, Obj2),
    ink_close(Ink1),
    clean_testdir(RootPath).
    
    
compact_journal_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    {ok, NewSQN1, _ObjSize} = ink_put(Ink1, "KeyAA", "TestValueAA", []),
    ?assertMatch(NewSQN1, 5),
    ok = ink_print_manifest(Ink1),
    R0 = ink_get(Ink1, "KeyAA", 5),
    ?assertMatch(R0, {{5, "KeyAA"}, {"TestValueAA", []}}),
    FunnyLoop = lists:seq(1, 48),
    Checker = lists:map(fun(X) ->
                            PK = "KeyZ" ++ integer_to_list(X),
                            {ok, SQN, _} = ink_put(Ink1,
                                                    PK,
                                                    crypto:rand_bytes(10000),
                                                    []),
                            {SQN, PK}
                            end,
                        FunnyLoop),
    {ok, NewSQN2, _ObjSize} = ink_put(Ink1, "KeyBB", "TestValueBB", []),
    ?assertMatch(NewSQN2, 54),
    ActualManifest = ink_getmanifest(Ink1),
    ok = ink_print_manifest(Ink1),
    ?assertMatch(3, length(ActualManifest)),
    ok = ink_compactjournal(Ink1,
                            Checker,
                            fun(X) -> {X, 55} end,
                            fun(L, K, SQN) -> lists:member({SQN, K}, L) end,
                            5000),
    timer:sleep(1000),
    CompactedManifest1 = ink_getmanifest(Ink1),
    ?assertMatch(2, length(CompactedManifest1)),
    Checker2 = lists:sublist(Checker, 16),
    ok = ink_compactjournal(Ink1,
                            Checker2,
                            fun(X) -> {X, 55} end,
                            fun(L, K, SQN) -> lists:member({SQN, K}, L) end,
                            5000),
    timer:sleep(1000),
    CompactedManifest2 = ink_getmanifest(Ink1),
    R = lists:foldl(fun({_SQN, FN, _P}, Acc) ->
                                case string:str(FN, "post_compact") of
                                    N when N > 0 ->
                                        true;
                                    0 ->
                                        Acc
                                end end,
                            false,
                            CompactedManifest2),
    ?assertMatch(true, R),
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
    ok = ink_compactjournal(Ink1,
                            [],
                            fun(X) -> {X, 55} end,
                            fun(L, K, SQN) -> lists:member({SQN, K}, L) end,
                            5000),
    timer:sleep(1000),
    ?assertMatch(1, length(ink_getmanifest(Ink1))),
    ok = ink_close(Ink1),
    {ok, Ink2} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    ?assertMatch(not_present, ink_fetch(Ink2, "Key1", 1)),
    {ok, SQN, Size} = ink_put(Ink2, "Key1", "Value1", []),
    ?assertMatch(2, SQN),
    ?assertMatch(true, Size > 0),
    {ok, V} = ink_fetch(Ink2, "Key1", 2),
    ?assertMatch("Value1", V),
    ink_close(Ink2),
    clean_testdir(RootPath).
    

-endif.