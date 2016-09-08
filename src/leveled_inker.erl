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
%% -------- Journal ---------
%%
%% The Journal is a series of files originally named as <SQN>_nursery.cdb
%% where the sequence number is the first object sequence number (key) within
%% the given database file.  The files will be named *.cdb at the point they
%% have been made immutable (through a rename operation).  Prior to this, they
%% will originally start out as a *.pnd file.
%%
%% At some stage in the future compacted versions of old journal cdb files may
%% be produced.  These files will be named <SQN>-<CompactionID>.cdb, and once
%% the manifest is updated the original <SQN>_nursery.cdb (or
%% <SQN>_<previous CompactionID>.cdb) files they replace will be erased.
%%
%% The current Journal is made up of a set of files referenced in the manifest,
%% combined with a set of files of the form <SQN>_nursery.[cdb|pnd] with
%% a higher Sequence Number compared to the files in the manifest.
%% 
%% The Journal is ordered by sequence number from front to back both within
%% and across files.  
%%
%% On startup the Inker should open the manifest with the highest sequence
%% number, and this will contain the list of filenames that make up the
%% non-recent part of the Journal.  The Manifest is completed by opening these
%% files plus any other files with a higher sequence number.  The file with
%% the highest sequence number is assumed to to be the active writer.  Any file
%% with a lower sequence number and a *.pnd extension should be re-rolled into
%% a *.cdb file. 
%%
%% -------- Objects ---------
%%
%% From the perspective of the Inker, objects to store are made up of:
%% - A Primary Key (as an Erlang term)
%% - A sequence number (assigned by the Inker)
%% - An object (an Erlang term)
%% - A set of Key Deltas associated with the change
%%
%% -------- Manifest ---------
%%
%% The Journal has a manifest which is the current record of which cdb files
%% are currently active in the Journal (i.e. following compaction). The
%% manifest holds this information through two lists - a list of files which
%% are definitely in the current manifest, and a list of files which have been
%% removed, but may still be present on disk.  The use of two lists is to 
%% avoid any circumsatnces where a compaction event has led to the deletion of 
%% a Journal file with a higher sequence number than any in the remaining
%% manifest.
%%
%% A new manifest file is saved for every compaction event.  The manifest files
%% are saved using the filename <ManifestSQN>.man once saved.  The ManifestSQN
%% is incremented once for every compaction event.
%%
%% -------- Compaction ---------
%%
%% Compaction is a process whereby an Inker's clerk will:
%% - Request a snapshot of the Ledger, as well as the lowest sequence number
%% that is currently registerd by another snapshot owner
%% - Picks a Journal database file at random (not including the current
%% nursery log)
%% - Performs a random walk on keys and sequence numbers in the chosen CDB
%% file to extract a subset of 100 key and sequence number combinations from
%% the database
%% - Looks up the current sequence number for those keys in the Ledger
%% - If more than <n>% (default n=20) of the keys are now at a higher sequence
%% number, then the database file is a candidate for compaction.  In this case
%% each of the next 8 files in sequence should be checked until all those 8
%% files have been checked or one of the files has been found to be below the
%% threshold.
%% - If a set of below-the-threshold files is found, the files are re-written
%% without any superceded values
%%- The clerk should then request that the Inker commit the manifest change
%%
%% -------- Inker's Clerk ---------
%%
%%
%%
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
        ink_snap/1,
        ink_close/1,
        ink_print_manifest/1,
        build_dummy_journal/0,
        simple_manifest_reader/2]).

-include_lib("eunit/include/eunit.hrl").

-define(MANIFEST_FP, "journal_manifest").
-define(FILES_FP, "journal_files").
-define(JOURNAL_FILEX, "cdb").
-define(MANIFEST_FILEX, "man").
-define(PENDING_FILEX, "pnd").


-record(state, {manifest = [] :: list(),
				manifest_sqn = 0 :: integer(),
                journal_sqn = 0 :: integer(),
                active_journaldb :: pid(),
                active_journaldb_sqn :: integer(),
                removed_journaldbs = [] :: list(),
                root_path :: string(),
                cdb_options :: #cdb_options{}}).


%%%============================================================================
%%% API
%%%============================================================================
 
ink_start(InkerOpts) ->
    gen_server:start(?MODULE, [InkerOpts], []).

ink_put(Pid, PrimaryKey, Object, KeyChanges) ->
    gen_server:call(Pid, {put, PrimaryKey, Object, KeyChanges}, infinity).

ink_get(Pid, PrimaryKey, SQN) ->
    gen_server:call(Pid, {get, PrimaryKey, SQN}, infinity).

ink_snap(Pid) ->
    gen_server:call(Pid, snapshot, infinity).

ink_close(Pid) ->
    gen_server:call(Pid, close, infinity).

ink_print_manifest(Pid) ->
    gen_server:call(Pid, print_manifest, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([InkerOpts]) ->
    RootPath = InkerOpts#inker_options.root_path,
    CDBopts = InkerOpts#inker_options.cdb_options,
    JournalFP = filepath(RootPath, journal_dir),
    {ok, JournalFilenames} = case filelib:is_dir(JournalFP) of
        true ->
            file:list_dir(JournalFP);
        false ->
            filelib:ensure_dir(JournalFP),
            {ok, []}
    end,
    ManifestFP = filepath(RootPath, manifest_dir),
    {ok, ManifestFilenames} = case filelib:is_dir(ManifestFP) of
        true ->
            file:list_dir(ManifestFP);
        false ->
            filelib:ensure_dir(ManifestFP),
            {ok, []}
    end,
    {Manifest,
        {ActiveJournal, LowActiveSQN},
        JournalSQN,
        ManifestSQN} = build_manifest(ManifestFilenames,
                                        JournalFilenames,
                                        fun simple_manifest_reader/2,
                                        RootPath,
                                        CDBopts),
    {ok, #state{manifest = Manifest,
                    manifest_sqn = ManifestSQN,
                    journal_sqn = JournalSQN,
                    active_journaldb = ActiveJournal,
                    active_journaldb_sqn = LowActiveSQN,
                    root_path = RootPath,
                    cdb_options = CDBopts}}.


handle_call({put, Key, Object, KeyChanges}, From, State) ->
    case put_object(Key, Object, KeyChanges, State) of
        {ok, UpdState} ->
            {reply, {ok, UpdState#state.journal_sqn}, UpdState};
        {rolling, UpdState} ->
            gen_server:reply(From, {ok, UpdState#state.journal_sqn}),
            {NewManifest,
                NewManifestSQN} = roll_active_file(State#state.active_journaldb,
                                                    State#state.manifest,
                                                    State#state.manifest_sqn,
                                                    State#state.root_path),
            {noreply, UpdState#state{manifest=NewManifest,
                                        manifest_sqn=NewManifestSQN}};
        {blocked, UpdState} ->
            {reply, blocked, UpdState}
    end;
handle_call({get, Key, SQN}, _From, State) ->
    {reply, get_object(Key,
                        SQN,
                        State#state.manifest,
                        State#state.active_journaldb,
                        State#state.active_journaldb_sqn), State};
handle_call(snapshot, _From , State) ->
    %% TODO: Not yet implemented registration of snapshot
    %% Should return manifest and register the snapshot
    {reply, {State#state.manifest,
                State#state.active_journaldb,
                State#state.active_journaldb_sqn},
                State};
handle_call(print_manifest, _From, State) ->
    manifest_printer(State#state.manifest),
    {reply, ok, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    io:format("Inker closing journal for reason ~w~n", [Reason]),
    io:format("Close triggered with journal_sqn=~w and manifest_sqn=~w~n",
                    [State#state.journal_sqn, State#state.manifest_sqn]),
    io:format("Manifest when closing is: ~n"),
    manifest_printer(State#state.manifest),
    close_allmanifest(State#state.manifest, State#state.active_journaldb).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

put_object(PrimaryKey, Object, KeyChanges, State) ->
    NewSQN = State#state.journal_sqn + 1,
    Bin1 = term_to_binary({Object, KeyChanges}, [compressed]),
    case leveled_cdb:cdb_put(State#state.active_journaldb,
                                {NewSQN, PrimaryKey},
                                Bin1) of
        ok ->
            {ok, State#state{journal_sqn=NewSQN}};
        roll ->
            FileName = filepath(State#state.root_path, NewSQN, new_journal),
            CDBopts = State#state.cdb_options,
            {ok, NewJournalP} = leveled_cdb:cdb_open_writer(FileName, CDBopts),
            case leveled_cdb:cdb_put(NewJournalP,
                                        {NewSQN, PrimaryKey},
                                        Bin1) of
                ok ->
                    {rolling, State#state{journal_sqn=NewSQN,
                                            active_journaldb=NewJournalP,
                                            active_journaldb_sqn=NewSQN}};
                roll ->
                    {blocked, State#state{journal_sqn=NewSQN,
                                            active_journaldb=NewJournalP,
                                            active_journaldb_sqn=NewSQN}}
            end
    end.

roll_active_file(OldActiveJournal, Manifest, ManifestSQN, RootPath) ->
    io:format("Rolling old journal ~w~n", [OldActiveJournal]),
    {ok, NewFilename} = leveled_cdb:cdb_complete(OldActiveJournal),
    {ok, PidR} = leveled_cdb:cdb_open_reader(NewFilename),
    JournalRegex2 = "nursery_(?<SQN>[0-9]+)\\." ++ ?JOURNAL_FILEX,
    [JournalSQN] = sequencenumbers_fromfilenames([NewFilename],
                                                    JournalRegex2,
                                                    'SQN'),
    NewManifest = add_to_manifest(Manifest, {JournalSQN, NewFilename, PidR}),
    NewManifestSQN = ManifestSQN + 1,
    ok = simple_manifest_writer(NewManifest, NewManifestSQN, RootPath),
    {NewManifest, NewManifestSQN}.

get_object(PrimaryKey, SQN, Manifest, ActiveJournal, ActiveJournalSQN) ->
    Obj = if
        SQN < ActiveJournalSQN ->
            JournalP = find_in_manifest(SQN, Manifest),
            if 
                JournalP == error ->
                    io:format("Unable to find SQN~w in Manifest~w~n",
                                [SQN, Manifest]),
                    error;
                true ->
                    leveled_cdb:cdb_get(JournalP, {SQN, PrimaryKey})
            end;
        true ->
            leveled_cdb:cdb_get(ActiveJournal, {SQN, PrimaryKey})
    end,
    case Obj of
        {{SQN, PK}, Bin} ->
            {{SQN, PK}, binary_to_term(Bin)};
        _ ->
            Obj
    end.


build_manifest(ManifestFilenames,
                JournalFilenames,
                ManifestRdrFun,
                RootPath) ->
    build_manifest(ManifestFilenames,
                    JournalFilenames,
                    ManifestRdrFun,
                    RootPath,
                    #cdb_options{}).

build_manifest(ManifestFilenames,
                JournalFilenames,
                ManifestRdrFun,
                RootPath,
                CDBopts) ->
    %% Setup root paths
    JournalFP = filepath(RootPath, journal_dir),
    %% Find the manifest with a highest Manifest sequence number
    %% Open it and read it to get the current Confirmed Manifest
    ManifestRegex = "(?<MSQN>[0-9]+)\\." ++ ?MANIFEST_FILEX,
    ValidManSQNs = sequencenumbers_fromfilenames(ManifestFilenames,
                                                    ManifestRegex,
                                                    'MSQN'),
    {JournalSQN1,
        ConfirmedManifest,
        Removed,
        ManifestSQN} = case length(ValidManSQNs) of
                            0 ->
                                {0, [], [], 0};
                            _ ->
                                PersistedManSQN = lists:max(ValidManSQNs),
                                {J1, M1, R1} = ManifestRdrFun(PersistedManSQN,
                                                                RootPath),
                                {J1, M1, R1, PersistedManSQN}
                        end,
    
    %% Find any more recent immutable files that have a higher sequence number
    %% - the immutable files have already been rolled, and so have a completed
    %% hashtree lookup
    JournalRegex1 = "nursery_(?<SQN>[0-9]+)\\." ++ ?JOURNAL_FILEX,
    UnremovedJournalFiles = lists:foldl(fun(FN, Acc) ->
                                            case lists:member(FN, Removed) of
                                                true ->
                                                    Acc;
                                                false ->
                                                    Acc ++ [FN]
                                            end end,
                                        [],
                                        JournalFilenames),
    OtherSQNs_imm = sequencenumbers_fromfilenames(UnremovedJournalFiles,
                                                    JournalRegex1,
                                                    'SQN'),
    Manifest1 = lists:foldl(fun(X, Acc) ->
                                        if
                                            X > JournalSQN1
                                                ->
                                                    FN = "nursery_" ++
                                                            integer_to_list(X)
                                                            ++ "." ++
                                                            ?JOURNAL_FILEX,
                                                    add_to_manifest(Acc, {X, FN});
                                            true
                                                -> Acc
                                        end end,
                                    ConfirmedManifest,
                                    lists:sort(OtherSQNs_imm)),
    
    %% Enrich the manifest so it contains the Pid of any of the immutable 
    %% entries
    io:format("Manifest on startup is: ~n"),
    manifest_printer(Manifest1),
    Manifest2 = lists:map(fun({LowSQN, FN}) ->
                                FP = filename:join(JournalFP, FN),
                                {ok, Pid} = leveled_cdb:cdb_open_reader(FP),
                                {LowSQN, FN, Pid} end,
                            Manifest1),
    
    %% Find any more recent mutable files that have a higher sequence number
    %% Roll any mutable files which do not have the highest sequence number
    %% to create the hashtree and complete the header entries
    JournalRegex2 = "nursery_(?<SQN>[0-9]+)\\." ++ ?PENDING_FILEX,
    OtherSQNs_pnd = sequencenumbers_fromfilenames(JournalFilenames,
                                                    JournalRegex2,
                                                    'SQN'),
    
    case length(OtherSQNs_pnd) of
        0 ->
            %% Need to create a new active writer, but also find the highest
            %% SQN from within the confirmed manifest
            TopSQNInManifest =
                case length(Manifest2) of
                    0 ->
                        %% Manifest is empty and no active writers
                        %% can be found so database is empty
                        0;
                    _ ->
                        TM = lists:last(lists:keysort(1,Manifest2)),
                        {_SQN, _FN, TMPid} = TM,
                        {HighSQN, _HighKey} = leveled_cdb:cdb_lastkey(TMPid),
                        HighSQN
                end,
            LowActiveSQN = TopSQNInManifest + 1,
            ActiveFN = filepath(RootPath, LowActiveSQN, new_journal),
            {ok, ActiveJournal} = leveled_cdb:cdb_open_writer(ActiveFN,
                                                                CDBopts),
            {Manifest2,
                {ActiveJournal, LowActiveSQN},
                TopSQNInManifest,
                ManifestSQN};
        _ ->
            {ActiveJournalSQN,
                Manifest3} = roll_pending_journals(lists:sort(OtherSQNs_pnd),
                                                    Manifest2,
                                                    RootPath),
            %% Need to work out highest sequence number in tail file to feed 
            %% into opening of pending journal
            ActiveFN = filepath(RootPath, ActiveJournalSQN, new_journal),
            {ok, ActiveJournal} = leveled_cdb:cdb_open_writer(ActiveFN,
                                                                CDBopts),
            {HighestSQN, _HighestKey} = leveled_cdb:cdb_lastkey(ActiveJournal),
            {Manifest3,
                {ActiveJournal, ActiveJournalSQN},
                HighestSQN,
                ManifestSQN}
    end.

close_allmanifest([], ActiveJournal) ->
    leveled_cdb:cdb_close(ActiveJournal);
close_allmanifest([H|ManifestT], ActiveJournal) ->
    {_, _, Pid} = H,
    ok = leveled_cdb:cdb_close(Pid),
    close_allmanifest(ManifestT, ActiveJournal).


roll_pending_journals([TopJournalSQN], Manifest, _RootPath)
                        when is_integer(TopJournalSQN) ->
    {TopJournalSQN, Manifest};
roll_pending_journals([JournalSQN|T], Manifest, RootPath) ->
    Filename = filepath(RootPath, JournalSQN, new_journal),
    {ok, PidW} = leveled_cdb:cdb_open_writer(Filename),
    {ok, NewFilename} = leveled_cdb:cdb_complete(PidW),
    {ok, PidR} = leveled_cdb:cdb_open_reader(NewFilename),
    roll_pending_journals(T,
                            add_to_manifest(Manifest,
                                            {JournalSQN, NewFilename, PidR}),
                            RootPath).



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

add_to_manifest(Manifest, Entry) ->
    lists:reverse(lists:sort([Entry|Manifest])).

find_in_manifest(_SQN, []) ->
    error;
find_in_manifest(SQN, [{LowSQN, _FN, Pid}|_Tail]) when SQN >= LowSQN ->
    Pid;
find_in_manifest(SQN, [_Head|Tail]) ->
    find_in_manifest(SQN, Tail).

filepath(RootPath, journal_dir) ->
    RootPath ++ "/" ++ ?FILES_FP ++ "/";
filepath(RootPath, manifest_dir) ->
    RootPath ++ "/" ++ ?MANIFEST_FP ++ "/".


filepath(RootPath, NewSQN, new_journal) ->
    filename:join(filepath(RootPath, journal_dir),
                    "nursery_"
                        ++ integer_to_list(NewSQN)
                        ++ "." ++ ?PENDING_FILEX).


simple_manifest_reader(SQN, RootPath) ->
    ManifestPath = filepath(RootPath, manifest_dir),
    {ok, MBin} = file:read_file(filename:join(ManifestPath,
                                                integer_to_list(SQN)
                                                ++ ".man")),
    binary_to_term(MBin).
    

simple_manifest_writer(Manifest, ManSQN, RootPath) ->
    ManPath = filepath(RootPath, manifest_dir),
    NewFN = filename:join(ManPath, integer_to_list(ManSQN) ++ ?MANIFEST_FILEX),
    TmpFN = filename:join(ManPath, integer_to_list(ManSQN) ++ ?PENDING_FILEX),
    MBin = term_to_binary(Manifest),
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

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

build_dummy_journal() ->
    RootPath = "../test/journal",
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
    {ok, _} = leveled_cdb:cdb_complete(J1),
    F2 = filename:join(JournalFP, "nursery_3.pnd"),
    {ok, J2} = leveled_cdb:cdb_open_writer(F2),
    {K1, V3} = {"Key1", "TestValue3"},
    {K4, V4} = {"Key4", "TestValue4"},
    ok = leveled_cdb:cdb_put(J2, {3, K1}, term_to_binary({V3, []})),
    ok = leveled_cdb:cdb_put(J2, {4, K4}, term_to_binary({V4, []})),
    ok = leveled_cdb:cdb_close(J2),
    Manifest = {2, [{1, "nursery_1.cdb"}], []},
    ManifestBin = term_to_binary(Manifest),
    {ok, MF1} = file:open(filename:join(ManifestFP, "1.man"),
                            [binary, raw, read, write]),
    ok = file:write(MF1, ManifestBin),
    ok = file:close(MF1).


clean_testdir(RootPath) ->
    clean_subdir(filepath(RootPath, journal_dir)),
    clean_subdir(filepath(RootPath, manifest_dir)).

clean_subdir(DirPath) ->
    {ok, Files} = file:list_dir(DirPath),
    lists:foreach(fun(FN) -> file:delete(filename:join(DirPath, FN)) end,
                    Files).

simple_buildmanifest_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(),
    Res = build_manifest(["1.man"],
                            ["nursery_1.cdb", "nursery_3.pnd"],
                            fun simple_manifest_reader/2,
                            RootPath),
    io:format("Build manifest output is ~w~n", [Res]),
    {Man, {ActJournal, ActJournalSQN}, HighSQN, ManSQN} = Res,
    ?assertMatch(HighSQN, 4),
    ?assertMatch(ManSQN, 1),
    ?assertMatch([{1, "nursery_1.cdb", _}], Man),
    {ActSQN, _ActK} = leveled_cdb:cdb_lastkey(ActJournal),
    ?assertMatch(ActSQN, 4),
    ?assertMatch(ActJournalSQN, 3),
    close_allmanifest(Man, ActJournal),
    clean_testdir(RootPath).

another_buildmanifest_test() ->
    %% There is a rolled jounral file which is not yet in the manifest
    RootPath = "../test/journal",
    build_dummy_journal(),
    FN = filepath(RootPath, 3, new_journal),
    {ok, FileToRoll} = leveled_cdb:cdb_open_writer(FN),
    {ok, _} = leveled_cdb:cdb_complete(FileToRoll),
    FN2 = filepath(RootPath, 5, new_journal),
    {ok, NewActiveJN} = leveled_cdb:cdb_open_writer(FN2),
    {K5, V5} = {"Key5", "TestValue5"},
    {K6, V6} = {"Key6", "TestValue6"},
    ok = leveled_cdb:cdb_put(NewActiveJN, {5, K5}, term_to_binary({V5, []})),
    ok = leveled_cdb:cdb_put(NewActiveJN, {6, K6}, term_to_binary({V6, []})),
    ok = leveled_cdb:cdb_close(NewActiveJN),
    %% Test setup - now build manifest
    Res = build_manifest(["1.man"],
                            ["nursery_1.cdb",
                                "nursery_3.cdb",
                                "nursery_5.pnd"],
                            fun simple_manifest_reader/2,
                            RootPath),
    io:format("Build manifest output is ~w~n", [Res]),
    {Man, {ActJournal, ActJournalSQN}, HighSQN, ManSQN} = Res,
    ?assertMatch(HighSQN, 6),
    ?assertMatch(ManSQN, 1),
    ?assertMatch([{3, "nursery_3.cdb", _}, {1, "nursery_1.cdb", _}], Man),
    {ActSQN, _ActK} = leveled_cdb:cdb_lastkey(ActJournal),
    ?assertMatch(ActSQN, 6),
    ?assertMatch(ActJournalSQN, 5),
    close_allmanifest(Man, ActJournal),
    clean_testdir(RootPath).


empty_buildmanifest_test() ->
    RootPath = "../test/journal",
    Res = build_manifest([],
                            [],
                            fun simple_manifest_reader/2,
                            RootPath),
    io:format("Build manifest output is ~w~n", [Res]),
    {Man, {ActJournal, ActJournalSQN}, HighSQN, ManSQN} = Res,
    ?assertMatch(Man, []),
    ?assertMatch(ManSQN, 0),
    ?assertMatch(HighSQN, 0),
    ?assertMatch(ActJournalSQN, 1),
    empty = leveled_cdb:cdb_lastkey(ActJournal),
    FN = leveled_cdb:cdb_filename(ActJournal),
    %% The filename should be based on the next journal SQN (1) not 0
    ?assertMatch(FN, filepath(RootPath, 1, new_journal)),
    close_allmanifest(Man, ActJournal),
    clean_testdir(RootPath).

simplejournal_test() ->
    %% build up a database, and then open it through the gen_server wrap
    %% Get and Put some keys
    RootPath = "../test/journal",
    build_dummy_journal(),
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=#cdb_options{}}),
    R1 = ink_get(Ink1, "Key1", 1),
    ?assertMatch(R1, {{1, "Key1"}, {"TestValue1", []}}),
    R2 = ink_get(Ink1, "Key1", 3),
    ?assertMatch(R2, {{3, "Key1"}, {"TestValue3", []}}),
    {ok, NewSQN1} = ink_put(Ink1, "Key99", "TestValue99", []),
    ?assertMatch(NewSQN1, 5),
    R3 = ink_get(Ink1, "Key99", 5),
    io:format("Result 3 is ~w~n", [R3]),
    ?assertMatch(R3, {{5, "Key99"}, {"TestValue99", []}}),
    ink_close(Ink1),
    clean_testdir(RootPath).

rollafile_simplejournal_test() ->
    RootPath = "../test/journal",
    build_dummy_journal(),
    CDBopts = #cdb_options{max_size=300000},
    {ok, Ink1} = ink_start(#inker_options{root_path=RootPath,
                                            cdb_options=CDBopts}),
    FunnyLoop = lists:seq(1, 48),
    {ok, NewSQN1} = ink_put(Ink1, "KeyAA", "TestValueAA", []),
    ?assertMatch(NewSQN1, 5),
    ok = ink_print_manifest(Ink1),
    R0 = ink_get(Ink1, "KeyAA", 5),
    ?assertMatch(R0, {{5, "KeyAA"}, {"TestValueAA", []}}),
    lists:foreach(fun(X) ->
                        {ok, _} = ink_put(Ink1,
                                            "KeyZ" ++ integer_to_list(X),
                                            crypto:rand_bytes(10000),
                                            []) end,
                    FunnyLoop),
    {ok, NewSQN2} = ink_put(Ink1, "KeyBB", "TestValueBB", []),
    ?assertMatch(NewSQN2, 54),
    ok = ink_print_manifest(Ink1),
    R1 = ink_get(Ink1, "KeyAA", 5),
    ?assertMatch(R1, {{5, "KeyAA"}, {"TestValueAA", []}}),
    R2 = ink_get(Ink1, "KeyBB", 54),
    ?assertMatch(R2, {{54, "KeyBB"}, {"TestValueBB", []}}),
    ink_close(Ink1),
    clean_testdir(RootPath).
    

-endif.