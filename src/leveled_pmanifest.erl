%% -------- PENCILLER MANIFEST ---------
%%
%% The manifest is an ordered set of files for each level to be used to find
%% which file is relevant for a given key or range lookup at a given level.
%%
%% This implementation is incomplete, in that it just uses a plain list at 
%% each level.  This is fine for short-lived volume tests, but as the deeper
%% levels are used there will be an exponential penalty.
%%
%% The originial intention was to swap out this implementation for a
%% multi-version ETS table - but that became complex.  So one of two changes
%% are pending:
%% - Use a single version ES cache for lower levels (and not allow snapshots to
%% access the cache)
%% - Use a skiplist like enhanced list at lower levels.


-module(leveled_pmanifest).

-include("include/leveled.hrl").

-export([
        new_manifest/0,
        open_manifest/1,
        copy_manifest/1,
        load_manifest/3,
        close_manifest/2,
        save_manifest/2,
        get_manifest_sqn/1,
        key_lookup/3,
        range_lookup/4,
        merge_lookup/4,
        insert_manifest_entry/4,
        remove_manifest_entry/4,
        replace_manifest_entry/5,
        switch_manifest_entry/4,
        mergefile_selector/2,
        add_snapshot/3,
        release_snapshot/2,
        merge_snapshot/2,
        ready_to_delete/2,
        check_for_work/2,
        is_basement/2,
        levelzero_present/1,
        check_bloom/3
        ]).      

-export([
        filepath/2
        ]).

-include_lib("eunit/include/eunit.hrl").

-define(MANIFEST_FILEX, "man").
-define(MANIFEST_FP, "ledger_manifest").
-define(MAX_LEVELS, 8).
-define(TREE_TYPE, idxt).
-define(TREE_WIDTH, 8).
-define(PHANTOM_PID, r2d_fail).
-define(MANIFESTS_TO_RETAIN, 5).

-record(manifest, {levels,
                        % an array of lists or trees representing the manifest
                    manifest_sqn = 0 :: integer(),
                        % The current manifest SQN
                    snapshots :: list() | undefined,
                        % A list of snaphots (i.e. clones)
                    min_snapshot_sqn = 0 :: integer(),
                        % The smallest snapshot manifest SQN in the snapshot
                        % list
                    pending_deletes, % OTP16 does not like defining type
                        % a dictionary mapping keys (filenames) to SQN when the
                        % deletion was made, and the original Manifest Entry
                    basement :: integer(),
                        % Currently the lowest level (the largest number)
                    blooms :: any() % actually a dict but OTP 16 compatability
                        % A dictionary mapping PIDs to bloom filters
                    }).      

-type manifest() :: #manifest{}.
-type manifest_entry() :: #manifest_entry{}.
-type manifest_owner() :: pid()|list().

-export_type([manifest/0, manifest_entry/0, manifest_owner/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec new_manifest() -> manifest().
%% @doc
%% The manifest in this case is a manifest of the ledger.  This contains
%% information on the layout of the files, but also information of snapshots
%% that may have an influence on the manifest as they require files to remain
%% after the primary penciller is happy for them to be removed.
new_manifest() ->
    LevelArray0 = array:new([{size, ?MAX_LEVELS + 1}, {default, []}]),
    SetLowerLevelFun =
        fun(IDX, Acc) ->
            array:set(IDX, leveled_tree:empty(?TREE_TYPE), Acc)
        end,
    LevelArray1 = lists:foldl(SetLowerLevelFun,
                                LevelArray0,
                                lists:seq(2, ?MAX_LEVELS)),
    #manifest{
        levels = LevelArray1, 
        manifest_sqn = 0, 
        snapshots = [],
        pending_deletes = dict:new(),
        basement = 0,
        blooms = dict:new()
    }.    

-spec open_manifest(string()) -> manifest().
%% @doc
%% Open a manifest in the appropriate sub-directory of the RootPath, and will
%% return an empty manifest if no such manifest is present.
open_manifest(RootPath) ->
    % Open the manifest in the file path which has the highest SQN, and will
    % open without error
    ManifestPath = filepath(RootPath, manifest),
    {ok, Filenames} = file:list_dir(ManifestPath),
    CurrRegex = "nonzero_(?<MSN>[0-9]+)\\." ++ ?MANIFEST_FILEX,
    ExtractSQNFun =
        fun(FN, Acc) ->
            case re:run(FN, CurrRegex, [{capture, ['MSN'], list}]) of
                nomatch ->
                    Acc;
                {match, [Int]} when is_list(Int) ->
                    Acc ++ [list_to_integer(Int)]
            end
        end,
    ValidManSQNs = lists:reverse(lists:sort(lists:foldl(ExtractSQNFun,
                                                        [],
                                                        Filenames))),
    open_manifestfile(RootPath, ValidManSQNs).

-spec copy_manifest(manifest()) -> manifest().
%% @doc
%% Used to pass the manifest to a snapshot, removing information not required
%% by a snapshot
copy_manifest(Manifest) ->
    % Copy the manifest ensuring anything only the master process should care
    % about is switched to undefined
    Manifest#manifest{snapshots = undefined, pending_deletes = undefined}.

-spec load_manifest(manifest(), fun(), fun()) -> 
                                        {integer(), manifest(), list()}.
%% @doc
%% Roll over the manifest starting a process to manage each file in the
%% manifest.  The PidFun should be able to return the Pid of a file process
%% (having started one).  The SQNFun will return the max sequence number
%% of that file, if passed the Pid that owns it.
load_manifest(Manifest, LoadFun, SQNFun) ->
    UpdateLevelFun =
        fun(LevelIdx, {AccMaxSQN, AccMan, AccFL}) ->
            L0 = array:get(LevelIdx, AccMan#manifest.levels),
            {L1, SQN1, FileList, LvlBloom} = 
                load_level(LevelIdx, L0, LoadFun, SQNFun),
            UpdLevels = array:set(LevelIdx, L1, AccMan#manifest.levels),
            FoldBloomFun = 
                fun({P, B}, BAcc) -> 
                    dict:store(P, B, BAcc) 
                end,
            UpdBlooms = 
                lists:foldl(FoldBloomFun, AccMan#manifest.blooms, LvlBloom),
            {max(AccMaxSQN, SQN1), 
                AccMan#manifest{levels = UpdLevels, blooms = UpdBlooms},
                AccFL ++ FileList}
        end,
    lists:foldl(UpdateLevelFun, 
                {0, Manifest, []}, 
                lists:seq(0, Manifest#manifest.basement)).

-spec close_manifest(manifest(), fun()) -> ok.
%% @doc
%% Close all the files in the manifest (using CloseEntryFun to call close on
%% a file).  Firts all the files in the active manifest are called, and then
%% any files which were pending deletion.
close_manifest(Manifest, CloseEntryFun) ->
    CloseLevelFun =
        fun(LevelIdx) ->
            Level = array:get(LevelIdx, Manifest#manifest.levels),
            close_level(LevelIdx, Level, CloseEntryFun)
        end,
    lists:foreach(CloseLevelFun, lists:seq(0, Manifest#manifest.basement)),
    
    ClosePDFun =
        fun({_FN, {_SQN, ME}}) ->
            CloseEntryFun(ME)
        end,
    lists:foreach(ClosePDFun, dict:to_list(Manifest#manifest.pending_deletes)).

-spec save_manifest(manifest(), string()) -> ok.
%% @doc
%% Save the manifest to file (with a checksum)
save_manifest(Manifest, RootPath) ->
    FP = filepath(RootPath, Manifest#manifest.manifest_sqn, current_manifest),
    ManBin = term_to_binary(Manifest#manifest{snapshots = [],
                                                pending_deletes = dict:new(),
                                                min_snapshot_sqn = 0,
                                                blooms = dict:new()}),
    CRC = erlang:crc32(ManBin),
    ok = file:write_file(FP, <<CRC:32/integer, ManBin/binary>>),
    {ok, <<CRC:32/integer, ManBin/binary>>} = file:read_file(FP),
    GC_SQN = Manifest#manifest.manifest_sqn - ?MANIFESTS_TO_RETAIN,
        % If a manifest is corrupted the previous one will be tried, so don't
        % delete the previous one straight away.  Retain until enough have been
        % kept to make the probability of all being independently corrupted 
        % through separate events negligible
    ok = remove_manifest(RootPath, GC_SQN),
        % Sometimes we skip a SQN, so to GC all may need to clear up previous
        % as well
    ok = remove_manifest(RootPath, GC_SQN - 1).

-spec remove_manifest(string(), integer()) -> ok.
remove_manifest(RootPath, GC_SQN) ->
    LFP = filepath(RootPath, GC_SQN, current_manifest),
    ok = 
        case filelib:is_file(LFP) of
            true ->
                file:delete(LFP);
            _ ->
                ok
        end.


-spec replace_manifest_entry(manifest(), integer(), integer(),
                                    list()|manifest_entry(),
                                    list()|manifest_entry()) -> manifest().
%% @doc
%% Replace a list of manifest entries in the manifest with a new set of entries
%% Pass in the new manifest SQN to be used for this manifest.  The list of
%% entries can just be a single entry
%%
%% This is generally called on the level being merged down into.
replace_manifest_entry(Manifest, ManSQN, LevelIdx, Removals, Additions) ->
    Levels = Manifest#manifest.levels,
    Level = array:get(LevelIdx, Levels),
    {UpdBlooms, StrippedAdditions} = 
        update_blooms(Removals, Additions, Manifest#manifest.blooms),
    UpdLevel = replace_entry(LevelIdx, Level, Removals, StrippedAdditions),
    leveled_log:log("PC019", ["insert", LevelIdx, UpdLevel]),
    PendingDeletes = 
        update_pendingdeletes(ManSQN, 
                                Removals, 
                                Manifest#manifest.pending_deletes),
    UpdLevels = array:set(LevelIdx, UpdLevel, Levels),
    case is_empty(LevelIdx, UpdLevel) of
        true ->
            Manifest#manifest{levels = UpdLevels,
                                basement = get_basement(UpdLevels),
                                manifest_sqn = ManSQN,
                                pending_deletes = PendingDeletes,
                                blooms = UpdBlooms};
        false ->
            Basement = max(LevelIdx, Manifest#manifest.basement),
            Manifest#manifest{levels = UpdLevels,
                                basement = Basement,
                                manifest_sqn = ManSQN,
                                pending_deletes = PendingDeletes,
                                blooms = UpdBlooms}
    end.

-spec insert_manifest_entry(manifest(), integer(), integer(),
                                    list()|manifest_entry()) -> manifest().
%% @doc
%% Place a single new manifest entry into a level of the manifest, at a given
%% level and manifest sequence number
insert_manifest_entry(Manifest, ManSQN, LevelIdx, Entry) ->
    Levels = Manifest#manifest.levels,
    Level = array:get(LevelIdx, Levels),
    {UpdBlooms, UpdEntry} = 
        update_blooms([], Entry, Manifest#manifest.blooms),
    UpdLevel = add_entry(LevelIdx, Level, UpdEntry),
    leveled_log:log("PC019", ["insert", LevelIdx, UpdLevel]),
    Basement = max(LevelIdx, Manifest#manifest.basement),
    Manifest#manifest{levels = array:set(LevelIdx, UpdLevel, Levels),
                        basement = Basement,
                        manifest_sqn = ManSQN,
                        blooms = UpdBlooms}.

-spec remove_manifest_entry(manifest(), integer(), integer(),
                                   list()|manifest_entry()) -> manifest().
%% @doc
%% Remove a manifest entry (as it has been merged into the level below)
remove_manifest_entry(Manifest, ManSQN, LevelIdx, Entry) ->
    Levels = Manifest#manifest.levels,
    Level = array:get(LevelIdx, Levels),
    {UpdBlooms, []} = 
        update_blooms(Entry, [], Manifest#manifest.blooms),
    UpdLevel = remove_entry(LevelIdx, Level, Entry),
    leveled_log:log("PC019", ["remove", LevelIdx, UpdLevel]),
    PendingDeletes = update_pendingdeletes(ManSQN,
                                            Entry,
                                            Manifest#manifest.pending_deletes),
    UpdLevels = array:set(LevelIdx, UpdLevel, Levels),
    case is_empty(LevelIdx, UpdLevel) of
        true ->
            Manifest#manifest{levels = UpdLevels,
                                basement = get_basement(UpdLevels),
                                manifest_sqn = ManSQN,
                                pending_deletes = PendingDeletes,
                                blooms = UpdBlooms};
        false ->
            Manifest#manifest{levels = UpdLevels,
                                manifest_sqn = ManSQN,
                                pending_deletes = PendingDeletes,
                                blooms = UpdBlooms}
    end.

-spec switch_manifest_entry(manifest(), integer(), integer(),
                                    list()|manifest_entry()) -> manifest().
%% @doc
%% Switch a manifest etry from this level to the level below (i.e when there
%% are no overlapping manifest entries in the level below)
switch_manifest_entry(Manifest, ManSQN, SrcLevel, Entry) ->
    % Move to level below - so needs to be removed but not marked as a
    % pending deletion
    Levels = Manifest#manifest.levels,
    Level = array:get(SrcLevel, Levels),
    UpdLevel = remove_entry(SrcLevel, Level, Entry),
    UpdLevels = array:set(SrcLevel, UpdLevel, Levels),
    insert_manifest_entry(Manifest#manifest{levels = UpdLevels},
                            ManSQN,
                            SrcLevel + 1,
                            Entry).

-spec get_manifest_sqn(manifest()) -> integer().
%% @doc
%% Return the manifest SQN for this manifest
get_manifest_sqn(Manifest) ->
    Manifest#manifest.manifest_sqn.

-spec key_lookup(manifest(), integer(), leveled_codec:ledger_key()) 
                                                    -> false|manifest_owner().
%% @doc
%% For a given key find which manifest entry covers that key at that level,
%% returning false if there is no covering manifest entry at that level.
key_lookup(Manifest, LevelIdx, Key) ->
    case LevelIdx > Manifest#manifest.basement of
        true ->
            false;
        false ->
            key_lookup_level(LevelIdx,
                                array:get(LevelIdx, Manifest#manifest.levels),
                                Key)
    end.

-spec range_lookup(manifest(), 
                    integer(), 
                    leveled_codec:ledger_key(), 
                    leveled_codec:ledger_key()) -> list().
%% @doc
%% Return a list of manifest_entry pointers at this level which cover the
%% key query range.
range_lookup(Manifest, LevelIdx, StartKey, EndKey) ->
    MakePointerFun =
        fun(M) ->
            {next, M, StartKey}
        end,
    range_lookup_int(Manifest, LevelIdx, StartKey, EndKey, MakePointerFun).

-spec merge_lookup(manifest(), 
                    integer(), 
                    leveled_codec:ledger_key(), 
                    leveled_codec:ledger_key()) -> list().
%% @doc
%% Return a list of manifest_entry pointers at this level which cover the
%% key query range, only all keys in the files should be included in the
%% pointers, not just the queries in the range.
merge_lookup(Manifest, LevelIdx, StartKey, EndKey) ->
    MakePointerFun =
        fun(M) ->
            {next, M, all}
        end,
    range_lookup_int(Manifest, LevelIdx, StartKey, EndKey, MakePointerFun).


-spec mergefile_selector(manifest(), integer()) -> manifest_entry().
%% @doc
%% An algorithm for discovering which files to merge ....
%% We can find the most optimal file:
%% - The one with the most overlapping data below?
%% - The one that overlaps with the fewest files below?
%% - The smallest file?
%% We could try and be fair in some way (merge oldest first)
%% Ultimately, there is a lack of certainty that being fair or optimal is
%% genuinely better - eventually every file has to be compacted.
%%
%% Hence, the initial implementation is to select files to merge at random
mergefile_selector(Manifest, LevelIdx) when LevelIdx =< 1 ->
    Level = array:get(LevelIdx, Manifest#manifest.levels),
    lists:nth(leveled_rand:uniform(length(Level)), Level);
mergefile_selector(Manifest, LevelIdx) ->
    Level = leveled_tree:to_list(array:get(LevelIdx,
                                            Manifest#manifest.levels)),
    {_SK, ME} = lists:nth(leveled_rand:uniform(length(Level)), Level),
    ME.

-spec merge_snapshot(manifest(), manifest()) -> manifest().
%% @doc
%% When the clerk returns an updated manifest to the penciller, the penciller
%% should restore its view of the snapshots to that manifest.  Snapshots can
%% be received in parallel to the manifest ebing updated, so the updated
%% manifest must not trample over any accrued state in the manifest.
merge_snapshot(PencillerManifest, ClerkManifest) ->
    ClerkManifest#manifest{snapshots =
                                PencillerManifest#manifest.snapshots,
                            min_snapshot_sqn =
                                PencillerManifest#manifest.min_snapshot_sqn}.

-spec add_snapshot(manifest(), pid()|atom(), integer()) -> manifest().
%% @doc
%% Add a snapshot reference to the manifest, withe rusing the pid or an atom
%% known to reference a special process.  The timeout should be in seconds, and
%% the snapshot will assume to have expired at timeout (and so at that stage
%% files which depended on the snapshot will potentially expire, and if the
%% clone is still active it may crash)
add_snapshot(Manifest, Pid, Timeout) ->
    SnapEntry = {Pid, Manifest#manifest.manifest_sqn, seconds_now(), Timeout},
    SnapList0 = [SnapEntry|Manifest#manifest.snapshots],
    ManSQN = Manifest#manifest.manifest_sqn,
    case Manifest#manifest.min_snapshot_sqn of
        0 ->
            Manifest#manifest{snapshots = SnapList0,
                                min_snapshot_sqn = ManSQN};
        N ->
            N0 = min(N, ManSQN),
            Manifest#manifest{snapshots = SnapList0,
                                min_snapshot_sqn = N0}
    end.

-spec release_snapshot(manifest(), pid()|atom()) -> manifest().
%% @doc
%% When a clone is complete the release should be notified to the manifest.
release_snapshot(Manifest, Pid) ->
    FilterFun =
        fun({P, SQN, ST, TO}, {Acc, MinSQN, Found}) ->
            case P of
                Pid ->
                    {Acc, MinSQN, true};
                _ ->
                    case seconds_now() > (ST + TO) of 
                        true ->
                            leveled_log:log("P0038", [P, SQN,  ST, TO]),
                            {Acc, MinSQN, Found};
                        false ->
                            {[{P, SQN, ST, TO}|Acc], min(SQN, MinSQN), Found}
                    end
            end
        end,
    {SnapList0, MinSnapSQN, Hit} = lists:foldl(FilterFun,
                                                {[], infinity, false},
                                                Manifest#manifest.snapshots),
    case Hit of 
        false ->
            leveled_log:log("P0039", [Pid, length(SnapList0), MinSnapSQN]);
        true ->
            ok 
    end,
    case SnapList0 of
        [] ->
            Manifest#manifest{snapshots = SnapList0,
                                min_snapshot_sqn = 0};
        _  ->
            leveled_log:log("P0004", [SnapList0]),
            Manifest#manifest{snapshots = SnapList0,
                                min_snapshot_sqn = MinSnapSQN}
    end.

-spec ready_to_delete(manifest(), string()) -> {boolean(), manifest()}.
%% @doc
%% A SST file which is in the delete_pending state can check to see if it is
%% ready to delete against the manifest.
ready_to_delete(Manifest, Filename) ->
    {ChangeSQN, _ME} = dict:fetch(Filename, Manifest#manifest.pending_deletes),
    case Manifest#manifest.min_snapshot_sqn of
        0 ->
            % no shapshots
            PDs = dict:erase(Filename, Manifest#manifest.pending_deletes),
            {true, Manifest#manifest{pending_deletes = PDs}};
        N when N >= ChangeSQN ->
            % Every snapshot is looking at a version of history after this
            % was removed
            PDs = dict:erase(Filename, Manifest#manifest.pending_deletes),
            {true, Manifest#manifest{pending_deletes = PDs}};
        _N ->
            % If failed to delete then we should release a phantom pid
            % in case this is necessary to timeout any snapshots
            % This wll also trigger a log  
            {false, release_snapshot(Manifest, ?PHANTOM_PID)}
    end.

-spec check_for_work(manifest(), list()) -> {list(), integer()}.
%% @doc
%% Check for compaction work in the manifest - look at levels which contain
%% more files in the threshold.
%%
%% File count determines size in leveled (unlike leveldb which works on the
%% total data volume).  Files are fixed size in terms of keys, and the size of
%% metadata is assumed to be contianed and regular and so uninteresting for
%% level sizing.
%%
%% Return a list of levels which are over-sized as well as the total items
%% across the manifest which are beyond the size (the total work outstanding).
check_for_work(Manifest, Thresholds) ->
    CheckLevelFun =
        fun({LevelIdx, MaxCount}, {AccL, AccC}) ->
            case LevelIdx > Manifest#manifest.basement of
                true ->
                    {AccL, AccC};
                false ->
                    Level = array:get(LevelIdx, Manifest#manifest.levels),
                    S = size(LevelIdx, Level),
                    case S > MaxCount of
                        true ->
                            {[LevelIdx|AccL], AccC + S - MaxCount};
                        false ->
                            {AccL, AccC}
                    end
            end
        end,
    lists:foldr(CheckLevelFun, {[], 0}, Thresholds).    

-spec is_basement(manifest(), integer()) -> boolean().
%% @doc
%% Is this level the lowest in the manifest which contains active files. When
%% merging down to the basement level special rules may apply (for example to
%% reap tombstones)
is_basement(Manifest, Level) ->
    Level >= Manifest#manifest.basement.

-spec levelzero_present(manifest()) -> boolean().
%% @doc
%% Is there a file in level zero (as only one file only can be in level zero).
levelzero_present(Manifest) ->
    not is_empty(0, array:get(0, Manifest#manifest.levels)).


-spec check_bloom(manifest(), pid(), {integer(), integer()}) -> boolean().
%% @doc
%% Check to see if a hash is present in a manifest entry by using the exported
%% bloom filter
check_bloom(Manifest, FP, Hash) ->
    case dict:find(FP, Manifest#manifest.blooms) of 
        {ok, Bloom} when is_binary(Bloom) ->
            leveled_ebloom:check_hash(Hash, Bloom);
        _ ->
            true
    end.


%%%============================================================================
%%% Internal Functions
%%%============================================================================


%% All these internal functions that work on a level are also passed LeveIdx
%% even if this is not presently relevant.  Currnetly levels are lists, but
%% future branches may make lower levels trees or skiplists to improve fetch
%% efficiency

load_level(LevelIdx, Level, LoadFun, SQNFun) ->
    HigherLevelLoadFun =
        fun(ME, {L_Out, L_MaxSQN, FileList, BloomL}) ->
            FN = ME#manifest_entry.filename,
            {P, Bloom} = LoadFun(FN, LevelIdx),
            SQN = SQNFun(P),
            {[ME#manifest_entry{owner=P}|L_Out], 
                max(SQN, L_MaxSQN),
                [FN|FileList],
                [{P, Bloom}|BloomL]}
        end,
    LowerLevelLoadFun =
        fun({EK, ME}, {L_Out, L_MaxSQN, FileList, BloomL}) ->
            FN = ME#manifest_entry.filename,
            {P, Bloom} = LoadFun(FN, LevelIdx),
            SQN = SQNFun(P),
            {[{EK, ME#manifest_entry{owner=P}}|L_Out], 
                max(SQN, L_MaxSQN),
                [FN|FileList],
                [{P, Bloom}|BloomL]}
        end,
    case LevelIdx =< 1 of
        true ->
            lists:foldr(HigherLevelLoadFun, {[], 0, [], []}, Level);
        false ->
            {L0, MaxSQN, Flist, UpdBloomL} = 
                lists:foldr(LowerLevelLoadFun, 
                            {[], 0, [], []}, 
                            leveled_tree:to_list(Level)),
            {leveled_tree:from_orderedlist(L0, ?TREE_TYPE, ?TREE_WIDTH), 
                MaxSQN, 
                Flist,
                UpdBloomL}
    end.

close_level(LevelIdx, Level, CloseEntryFun) when LevelIdx =< 1 ->
    lists:foreach(CloseEntryFun, Level);
close_level(_LevelIdx, Level, CloseEntryFun) ->
    lists:foreach(CloseEntryFun, leveled_tree:to_list(Level)).

is_empty(_LevelIdx, []) ->
    true;
is_empty(LevelIdx, _Level) when LevelIdx =< 1 ->
    false;
is_empty(_LevelIdx, Level) ->
    leveled_tree:tsize(Level) == 0.

size(LevelIdx, Level) when LevelIdx =< 1 ->
    length(Level);
size(_LevelIdx, Level) ->
    leveled_tree:tsize(Level).

pred_fun(LevelIdx, StartKey, _EndKey) when LevelIdx =< 1 ->
    fun(ME) ->
        ME#manifest_entry.start_key < StartKey
    end;
pred_fun(_LevelIdx, _StartKey, EndKey) ->
    fun({EK, _ME}) ->
        EK < EndKey
    end.

add_entry(_LevelIdx, Level, []) ->
    Level;
add_entry(LevelIdx, Level, Entries) when is_list(Entries) ->
    FirstEntry = lists:nth(1, Entries),
    PredFun = pred_fun(LevelIdx,
                        FirstEntry#manifest_entry.start_key,
                        FirstEntry#manifest_entry.end_key),
    case LevelIdx =< 1 of
        true ->
            {LHS, RHS} = lists:splitwith(PredFun, Level),
            lists:append([LHS, Entries, RHS]);
        false ->
            {LHS, RHS} = lists:splitwith(PredFun, leveled_tree:to_list(Level)),
            MapFun =
                fun(ME) ->
                    {ME#manifest_entry.end_key, ME}
                end,
            Entries0 = lists:map(MapFun, Entries),
            leveled_tree:from_orderedlist(lists:append([LHS, Entries0, RHS]),
                                            ?TREE_TYPE,
                                            ?TREE_WIDTH)
    end.

remove_entry(LevelIdx, Level, Entries) ->
    % We're assuming we're removing a sorted sublist
    {RemLength, FirstRemoval} = measure_removals(Entries),
    remove_section(LevelIdx, Level, FirstRemoval, RemLength).

measure_removals(Removals) ->
    case is_list(Removals) of
        true ->
            {length(Removals), lists:nth(1, Removals)};
        false ->
            {1, Removals}
    end.

remove_section(LevelIdx, Level, FirstEntry, SectionLength) ->
    PredFun = pred_fun(LevelIdx,
                        FirstEntry#manifest_entry.start_key,
                        FirstEntry#manifest_entry.end_key),
    case LevelIdx =< 1 of
        true ->
            {LHS, RHS} = lists:splitwith(PredFun, Level),
            Post = lists:nthtail(SectionLength, RHS),
            lists:append([LHS, Post]);
        false ->
            {LHS, RHS} = lists:splitwith(PredFun, leveled_tree:to_list(Level)),
            Post = lists:nthtail(SectionLength, RHS),
            leveled_tree:from_orderedlist(lists:append([LHS, Post]),
                                            ?TREE_TYPE,
                                            ?TREE_WIDTH)
    end.

replace_entry(LevelIdx, Level, Removals, Additions) when LevelIdx =< 1 ->
    {SectionLength, FirstEntry} = measure_removals(Removals),
    PredFun = pred_fun(LevelIdx,
                        FirstEntry#manifest_entry.start_key,
                        FirstEntry#manifest_entry.end_key),
    {LHS, RHS} = lists:splitwith(PredFun, Level),
    Post = lists:nthtail(SectionLength, RHS),
    lists:append([LHS, Additions, Post]);
replace_entry(LevelIdx, Level, Removals, Additions) ->
    {SectionLength, FirstEntry} = measure_removals(Removals),
    PredFun = pred_fun(LevelIdx,
                        FirstEntry#manifest_entry.start_key,
                        FirstEntry#manifest_entry.end_key),
    {LHS, RHS} = lists:splitwith(PredFun, leveled_tree:to_list(Level)),
    Post =
        case RHS of
            [] ->
                [];
            _ ->
                lists:nthtail(SectionLength, RHS)
        end,
    MapFun =
        fun(ME) ->
            {ME#manifest_entry.end_key, ME}
        end,
    UpdList = lists:append([LHS, lists:map(MapFun, Additions), Post]),
    leveled_tree:from_orderedlist(UpdList, ?TREE_TYPE, ?TREE_WIDTH).
    

update_pendingdeletes(ManSQN, Removals, PendingDeletes) ->
    DelFun =
        fun(E, Acc) ->
            dict:store(E#manifest_entry.filename,
                        {ManSQN, E},
                        Acc)
        end,
    Entries = 
        case is_list(Removals) of
            true ->
                Removals;
            false ->
                [Removals]
        end,
    lists:foldl(DelFun, PendingDeletes, Entries).

-spec update_blooms(list()|manifest_entry(), 
                    list()|manifest_entry(), 
                    any()) 
                                                -> {any(), list()}.
%% @doc
%%
%% The manifest is a Pid-> Bloom mappping for every Pid, and this needs to 
%% be updated to represent the changes.  However, the bloom would bloat out 
%% the stored manifest, so the bloom must be stripped from the manifest entry
%% as part of this process
update_blooms(Removals, Additions, Blooms) ->
    Additions0 =
        case is_list(Additions) of 
            true -> Additions;
            false -> [Additions]
        end,
    Removals0 = 
        case is_list(Removals) of   
            true -> Removals;
            false -> [Removals]
        end,

    RemFun = 
        fun(R, BloomD) ->
            dict:erase(R#manifest_entry.owner, BloomD)
        end,
    AddFun =
        fun(A, BloomD) ->
            dict:store(A#manifest_entry.owner, A#manifest_entry.bloom, BloomD)
        end,
    StripFun =
        fun(A) ->
            A#manifest_entry{bloom = none}
        end,
    
    Blooms0 = lists:foldl(RemFun, Blooms, Removals0),
    Blooms1 = lists:foldl(AddFun, Blooms0, Additions0),
    {Blooms1, lists:map(StripFun, Additions0)}.


key_lookup_level(LevelIdx, [], _Key) when LevelIdx =< 1 ->
    false;
key_lookup_level(LevelIdx, [Entry|Rest], Key) when LevelIdx =< 1 ->
    case Entry#manifest_entry.end_key >= Key of
        true ->
            case Key >= Entry#manifest_entry.start_key of
                true ->
                    Entry#manifest_entry.owner;
                false ->
                    false
            end;
        false ->
            key_lookup_level(LevelIdx, Rest, Key)
    end;
key_lookup_level(_LevelIdx, Level, Key) ->
    StartKeyFun =
        fun(ME) ->
            ME#manifest_entry.start_key
        end,
    case leveled_tree:search(Key, Level, StartKeyFun) of
        none ->
            false;
        {_EK, ME} ->
            ME#manifest_entry.owner
    end.


range_lookup_int(Manifest, LevelIdx, StartKey, EndKey, MakePointerFun) ->
    Range = 
        case LevelIdx > Manifest#manifest.basement of
            true ->
                [];
            false ->
                range_lookup_level(LevelIdx,
                                    array:get(LevelIdx,
                                                Manifest#manifest.levels),
                                    StartKey,
                                    EndKey)
        end,
    lists:map(MakePointerFun, Range).
    
range_lookup_level(LevelIdx, Level, QStartKey, QEndKey) when LevelIdx =< 1 ->
    BeforeFun =
        fun(M) ->
            QStartKey > M#manifest_entry.end_key
        end,
    NotAfterFun =
        fun(M) ->
            not leveled_codec:endkey_passed(QEndKey,
                                            M#manifest_entry.start_key)
        end,
    {_Before, MaybeIn} = lists:splitwith(BeforeFun, Level),
    {In, _After} = lists:splitwith(NotAfterFun, MaybeIn),
    In;
range_lookup_level(_LevelIdx, Level, QStartKey, QEndKey) ->
    StartKeyFun =
        fun(ME) ->
            ME#manifest_entry.start_key
        end,
    Range = leveled_tree:search_range(QStartKey, QEndKey, Level, StartKeyFun),
    MapFun =
        fun({_EK, ME}) ->
            ME
        end,
    lists:map(MapFun, Range).
    

get_basement(Levels) ->
    GetBaseFun =
        fun(L, Acc) ->
            case is_empty(L, array:get(L, Levels)) of
                false ->
                    max(L, Acc);
                true ->
                    Acc
            end
        end,
    lists:foldl(GetBaseFun, 0, lists:seq(0, ?MAX_LEVELS)).


filepath(RootPath, manifest) ->
    MFP = RootPath ++ "/" ++ ?MANIFEST_FP ++ "/",
    filelib:ensure_dir(MFP),
    MFP.

filepath(RootPath, NewMSN, current_manifest) ->
    filepath(RootPath, manifest)  ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?MANIFEST_FILEX.


open_manifestfile(_RootPath, L) when L == [] orelse L == [0] ->
    leveled_log:log("P0013", []),
    new_manifest();
open_manifestfile(RootPath, [TopManSQN|Rest]) ->
    CurrManFile = filepath(RootPath, TopManSQN, current_manifest),
    {ok, FileBin} = file:read_file(CurrManFile),
    <<CRC:32/integer, BinaryOfTerm/binary>> = FileBin,
    case erlang:crc32(BinaryOfTerm) of
        CRC ->
            leveled_log:log("P0012", [TopManSQN]),
            binary_to_term(BinaryOfTerm);
        _ ->
            leveled_log:log("P0033", [CurrManFile, "crc wonky"]),
            open_manifestfile(RootPath, Rest)
    end.

seconds_now() ->
    {MegaNow, SecNow, _} = os:timestamp(),
    MegaNow * 1000000 + SecNow.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

initial_setup() -> 
    initial_setup(single_change).

initial_setup(Changes) ->
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1",
                            owner="pid_z1",
                            bloom=none},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                            end_key={o, "Bucket1", "K71", null},
                            filename="Z2",
                            owner="pid_z2",
                            bloom=none},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3",
                            owner="pid_z3",
                            bloom=none},
    E4 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld7"}, "K93"},
                            filename="Z4",
                            owner="pid_z4",
                            bloom=none},
    E5 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld7"}, "K97"},
                            end_key={o, "Bucket1", "K78", null},
                            filename="Z5",
                            owner="pid_z5",
                            bloom=none},
    E6 = #manifest_entry{start_key={o, "Bucket1", "K81", null},
                            end_key={o, "Bucket1", "K996", null},
                            filename="Z6",
                            owner="pid_z6",
                            bloom=none},
    initial_setup(Changes, E1, E2, E3, E4, E5, E6).    


initial_setup(single_change, E1, E2, E3, E4, E5, E6) ->
    Man0 = new_manifest(),
    
    Man1 = insert_manifest_entry(Man0, 1, 1, E1),
    Man2 = insert_manifest_entry(Man1, 1, 1, E2),
    Man3 = insert_manifest_entry(Man2, 1, 1, E3),
    Man4 = insert_manifest_entry(Man3, 1, 2, E4),
    Man5 = insert_manifest_entry(Man4, 1, 2, E5),
    Man6 = insert_manifest_entry(Man5, 1, 2, E6),
    ?assertMatch(Man6, insert_manifest_entry(Man6, 1, 2, [])),
    {Man0, Man1, Man2, Man3, Man4, Man5, Man6};
initial_setup(multi_change, E1, E2, E3, E4, E5, E6) ->
    Man0 = new_manifest(),
    
    Man1 = insert_manifest_entry(Man0, 1, 1, E1),
    Man2 = insert_manifest_entry(Man1, 2, 1, E2),
    Man3 = insert_manifest_entry(Man2, 3, 1, E3),
    Man4 = insert_manifest_entry(Man3, 4, 2, E4),
    Man5 = insert_manifest_entry(Man4, 5, 2, E5),
    Man6 = insert_manifest_entry(Man5, 6, 2, E6),
    ?assertMatch(Man6, insert_manifest_entry(Man6, 6, 2, [])),
    {Man0, Man1, Man2, Man3, Man4, Man5, Man6}.


changeup_setup(Man6) ->
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1",
                            owner="pid_z1",
                            bloom=none},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                            end_key={o, "Bucket1", "K71", null},
                            filename="Z2",
                            owner="pid_z2",
                            bloom=none},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3",
                            owner="pid_z3",
                            bloom=none},
                            
    E1_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld4"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K62"},
                            owner="pid_y1",
                            filename="Y1",
                            bloom=none},
    E2_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K67"},
                            end_key={o, "Bucket1", "K45", null},
                            owner="pid_y2",
                            filename="Y2",
                            bloom=none},
    E3_2 = #manifest_entry{start_key={o, "Bucket1", "K47", null},
                            end_key={o, "Bucket1", "K812", null},
                            owner="pid_y3",
                            filename="Y3",
                            bloom=none},
    E4_2 = #manifest_entry{start_key={o, "Bucket1", "K815", null},
                            end_key={o, "Bucket1", "K998", null},
                            owner="pid_y4",
                            filename="Y4",
                            bloom=none},
    
    Man7 = remove_manifest_entry(Man6, 2, 1, E1),
    Man8 = remove_manifest_entry(Man7, 2, 1, E2),
    Man9 = remove_manifest_entry(Man8, 2, 1, E3),
    
    Man10 = insert_manifest_entry(Man9, 2, 1, E1_2),
    Man11 = insert_manifest_entry(Man10, 2, 1, E2_2),
    Man12 = insert_manifest_entry(Man11, 2, 1, E3_2),
    Man13 = insert_manifest_entry(Man12, 2, 1, E4_2),
    % remove_manifest_entry(Manifest, ManSQN, Level, Entry)
    
    {Man7, Man8, Man9, Man10, Man11, Man12, Man13}.

random_select_test() ->
    ManTuple = initial_setup(),
    LastManifest = element(7, ManTuple),
    L1File = mergefile_selector(LastManifest, 1),
    % This blows up if the function is not prepared for the different format
    % https://github.com/martinsumner/leveled/issues/43
    _L2File = mergefile_selector(LastManifest, 2),
    Level1 = array:get(1, LastManifest#manifest.levels),
    ?assertMatch(true, lists:member(L1File, Level1)).

manifest_gc_test() ->
    RP = "test/test_area/",
    ok = filelib:ensure_dir(RP),
    ok = leveled_penciller:clean_testdir(RP),
    ManifestT = initial_setup(multi_change),
    ManifestL = tuple_to_list(ManifestT),
    lists:foreach(fun(M) -> save_manifest(M, RP) end, ManifestL),
    {ok, FNs} = file:list_dir(filepath(RP, manifest)),
    io:format("FNs ~w~n", [FNs]),
    ?assertMatch(true, length(ManifestL) > ?MANIFESTS_TO_RETAIN),
    ?assertMatch(?MANIFESTS_TO_RETAIN, length(FNs)).


keylookup_manifest_test() ->
    {Man0, Man1, Man2, Man3, _Man4, _Man5, Man6} = initial_setup(),
    LK1_1 = {o, "Bucket1", "K711", null},
    LK1_2 = {o, "Bucket1", "K70", null},
    LK1_3 = {o, "Bucket1", "K71", null},
    LK1_4 = {o, "Bucket1", "K75", null},
    LK1_5 = {o, "Bucket1", "K76", null},
    
    ?assertMatch(false, key_lookup(Man0, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man1, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man2, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man3, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man6, 1, LK1_1)),
    
    ?assertMatch("pid_z2", key_lookup(Man6, 1, LK1_2)),
    ?assertMatch("pid_z2", key_lookup(Man6, 1, LK1_3)),
    ?assertMatch("pid_z3", key_lookup(Man6, 1, LK1_4)),
    ?assertMatch("pid_z3", key_lookup(Man6, 1, LK1_5)),
    
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_2)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_3)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_4)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_5)),
    
    {_Man7, _Man8, _Man9, _Man10, _Man11, _Man12,
        Man13} = changeup_setup(Man6),
    
    ?assertMatch(false, key_lookup(Man0, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man1, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man2, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man3, 1, LK1_1)),
    ?assertMatch(false, key_lookup(Man6, 1, LK1_1)),
    
    ?assertMatch("pid_z2", key_lookup(Man6, 1, LK1_2)),
    ?assertMatch("pid_z2", key_lookup(Man6, 1, LK1_3)),
    ?assertMatch("pid_z3", key_lookup(Man6, 1, LK1_4)),
    ?assertMatch("pid_z3", key_lookup(Man6, 1, LK1_5)),
    
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_2)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_3)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_4)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_5)),
    
    ?assertMatch("pid_y3", key_lookup(Man13, 1, LK1_4)),
    ?assertMatch("pid_z5", key_lookup(Man13, 2, LK1_4)).

ext_keylookup_manifest_test() ->
    RP = "test/test_area",
    ok = leveled_penciller:clean_testdir(RP),
    {_Man0, _Man1, _Man2, _Man3, _Man4, _Man5, Man6} = initial_setup(),
    save_manifest(Man6, RP),
    
    E7 = #manifest_entry{start_key={o, "Bucket1", "K997", null},
                            end_key={o, "Bucket1", "K999", null},
                            filename="Z7",
                            owner="pid_z7"},
    Man7 = insert_manifest_entry(Man6, 2, 2, E7),
    save_manifest(Man7, RP),
    ManOpen1 = open_manifest(RP),
    ?assertMatch(2, get_manifest_sqn(ManOpen1)),
    
    Man7FN = filepath(RP, 2, current_manifest),
    Man7FNAlt = filename:rootname(Man7FN) ++ ".pnd",
    {ok, BytesCopied} = file:copy(Man7FN, Man7FNAlt),
    {ok, Bin} = file:read_file(Man7FN),
    ?assertMatch(BytesCopied, byte_size(Bin)),
    RandPos = leveled_rand:uniform(bit_size(Bin) - 1),
    <<Pre:RandPos/bitstring, BitToFlip:1/integer, Rest/bitstring>> = Bin,
    Flipped = BitToFlip bxor 1,
    ok  = file:write_file(Man7FN,
                            <<Pre:RandPos/bitstring,
                                Flipped:1/integer,
                                Rest/bitstring>>),
    
    ?assertMatch(2, get_manifest_sqn(Man7)),
    
    ManOpen2 = open_manifest(RP),
    ?assertMatch(1, get_manifest_sqn(ManOpen2)),
    
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1",
                            owner="pid_z1",
                            bloom=none},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                            end_key={o, "Bucket1", "K71", null},
                            filename="Z2",
                            owner="pid_z2",
                            bloom=none},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3",
                            owner="pid_z3",
                            bloom=none},
    
    E1_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld4"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K62"},
                            owner="pid_y1",
                            filename="Y1",
                            bloom=none},
    E2_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K67"},
                            end_key={o, "Bucket1", "K45", null},
                            owner="pid_y2",
                            filename="Y2",
                            bloom=none},
    E3_2 = #manifest_entry{start_key={o, "Bucket1", "K47", null},
                            end_key={o, "Bucket1", "K812", null},
                            owner="pid_y3",
                            filename="Y3",
                            bloom=none},
    E4_2 = #manifest_entry{start_key={o, "Bucket1", "K815", null},
                            end_key={o, "Bucket1", "K998", null},
                            owner="pid_y4",
                            filename="Y4",
                            bloom=none},
    
    Man8 = replace_manifest_entry(ManOpen2, 2, 1, E1, E1_2),
    Man9 = remove_manifest_entry(Man8, 2, 1, [E2, E3]),
    Man10 = insert_manifest_entry(Man9, 2, 1, [E2_2, E3_2, E4_2]),
    ?assertMatch(2, get_manifest_sqn(Man10)),
    
    LK1_4 = {o, "Bucket1", "K75", null},
    ?assertMatch("pid_y3", key_lookup(Man10, 1, LK1_4)),
    ?assertMatch("pid_z5", key_lookup(Man10, 2, LK1_4)),
    
    E5 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld7"}, "K97"},
                            end_key={o, "Bucket1", "K78", null},
                            filename="Z5",
                            owner="pid_z5",
                            bloom=none},
    E6 = #manifest_entry{start_key={o, "Bucket1", "K81", null},
                            end_key={o, "Bucket1", "K996", null},
                            filename="Z6",
                            owner="pid_z6",
                            bloom=none},
    
    Man11 = remove_manifest_entry(Man10, 3, 2, [E5, E6]),
    ?assertMatch(3, get_manifest_sqn(Man11)),
    ?assertMatch(false, key_lookup(Man11, 2, LK1_4)),
    
    Man12 = replace_manifest_entry(Man11, 4, 2, E2_2, E5),
    ?assertMatch(4, get_manifest_sqn(Man12)),
    ?assertMatch("pid_z5", key_lookup(Man12, 2, LK1_4)).

rangequery_manifest_test() ->
    {_Man0, _Man1, _Man2, _Man3, _Man4, _Man5, Man6} = initial_setup(),
    
    PidMapFun =
        fun(Pointer) ->
            {next, ME, _SK} = Pointer,
            ME#manifest_entry.owner
        end,
    
    SK1 = {o, "Bucket1", "K711", null},
    EK1 = {o, "Bucket1", "K999", null},
    RL1_1 = lists:map(PidMapFun, range_lookup(Man6, 1, SK1, EK1)),
    ?assertMatch(["pid_z3"], RL1_1),
    RL1_2 = lists:map(PidMapFun, range_lookup(Man6, 2, SK1, EK1)),
    ?assertMatch(["pid_z5", "pid_z6"], RL1_2),
    SK2 = {i, "Bucket1", {"Idx1", "Fld8"}, null},
    EK2 = {i, "Bucket1", {"Idx1", "Fld8"}, null},
    RL2_1 = lists:map(PidMapFun, range_lookup(Man6, 1, SK2, EK2)),
    ?assertMatch(["pid_z1"], RL2_1),
    RL2_2 = lists:map(PidMapFun, range_lookup(Man6, 2, SK2, EK2)),
    ?assertMatch(["pid_z5"], RL2_2),
    
    SK3 = {o, "Bucket1", "K994", null},
    EK3 = {o, "Bucket1", "K995", null},
    RL3_1 = lists:map(PidMapFun, range_lookup(Man6, 1, SK3, EK3)),
    ?assertMatch([], RL3_1),
    RL3_2 = lists:map(PidMapFun, range_lookup(Man6, 2, SK3, EK3)),
    ?assertMatch(["pid_z6"], RL3_2),
    
    {_Man7, _Man8, _Man9, _Man10, _Man11, _Man12,
        Man13} = changeup_setup(Man6),
    
    RL1_1A = lists:map(PidMapFun, range_lookup(Man6, 1, SK1, EK1)),
    ?assertMatch(["pid_z3"], RL1_1A),
    RL2_1A = lists:map(PidMapFun, range_lookup(Man6, 1, SK2, EK2)),
    ?assertMatch(["pid_z1"], RL2_1A),
    RL3_1A = lists:map(PidMapFun, range_lookup(Man6, 1, SK3, EK3)),
    ?assertMatch([], RL3_1A),
     
    RL1_1B = lists:map(PidMapFun, range_lookup(Man13, 1, SK1, EK1)),
    ?assertMatch(["pid_y3", "pid_y4"], RL1_1B),
    RL2_1B = lists:map(PidMapFun, range_lookup(Man13, 1, SK2, EK2)),
    ?assertMatch(["pid_y1"], RL2_1B),
    RL3_1B = lists:map(PidMapFun, range_lookup(Man13, 1, SK3, EK3)),
    ?assertMatch(["pid_y4"], RL3_1B).

levelzero_present_test() ->
    E0 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={o, "Bucket1", "Key996", null},
                            filename="Z0",
                            owner="pid_z0",
                            bloom=none},
     
    Man0 = new_manifest(),
    ?assertMatch(false, levelzero_present(Man0)),
    % insert_manifest_entry(Manifest, ManSQN, Level, Entry)
    Man1 = insert_manifest_entry(Man0, 1, 0, E0),
    ?assertMatch(true, levelzero_present(Man1)).

snapshot_release_test() ->
    Man6 = element(7, initial_setup()),
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1",
                            owner="pid_z1",
                            bloom=none},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                            end_key={o, "Bucket1", "K71", null},
                            filename="Z2",
                            owner="pid_z2",
                            bloom=none},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3",
                            owner="pid_z3",
                            bloom=none},
    
    Man7 = add_snapshot(Man6, pid_a1, 3600),
    Man8 = remove_manifest_entry(Man7, 2, 1, E1),
    Man9 = add_snapshot(Man8, pid_a2, 3600),
    Man10 = remove_manifest_entry(Man9, 3, 1, E2),
    Man11 = add_snapshot(Man10, pid_a3, 3600),
    Man12 = remove_manifest_entry(Man11, 4, 1, E3),
    Man13 = add_snapshot(Man12, pid_a4, 3600),
    
    ?assertMatch(false, element(1, ready_to_delete(Man8, "Z1"))),
    ?assertMatch(false, element(1, ready_to_delete(Man10, "Z2"))),
    ?assertMatch(false, element(1, ready_to_delete(Man12, "Z3"))),
    
    Man14 = release_snapshot(Man13, pid_a1),
    ?assertMatch(false, element(1, ready_to_delete(Man14, "Z2"))),
    ?assertMatch(false, element(1, ready_to_delete(Man14, "Z3"))),
    {Bool14, Man15} = ready_to_delete(Man14, "Z1"),
    ?assertMatch(true, Bool14),
    
    %This doesn't change anything - released snaphsot not the min
    Man16 = release_snapshot(Man15, pid_a4),
    ?assertMatch(false, element(1, ready_to_delete(Man16, "Z2"))),
    ?assertMatch(false, element(1, ready_to_delete(Man16, "Z3"))),
    
    Man17 = release_snapshot(Man16, pid_a2),
    ?assertMatch(false, element(1, ready_to_delete(Man17, "Z3"))),
    {Bool17, Man18} = ready_to_delete(Man17, "Z2"),
    ?assertMatch(true, Bool17),
    
    Man19 = release_snapshot(Man18, pid_a3),
    
    io:format("MinSnapSQN ~w~n", [Man19#manifest.min_snapshot_sqn]),
    
    {Bool19, _Man20} = ready_to_delete(Man19, "Z3"),
    ?assertMatch(true, Bool19).
    

snapshot_timeout_test() ->
    Man6 = element(7, initial_setup()),
    Man7 = add_snapshot(Man6, pid_a1, 3600),
    ?assertMatch(1, length(Man7#manifest.snapshots)),
    Man8 = release_snapshot(Man7, pid_a1),
    ?assertMatch(0, length(Man8#manifest.snapshots)),
    Man9 = add_snapshot(Man8, pid_a1, 0),
    timer:sleep(2001),
    ?assertMatch(1, length(Man9#manifest.snapshots)),
    Man10 = release_snapshot(Man9, ?PHANTOM_PID),
    ?assertMatch(0, length(Man10#manifest.snapshots)).

potential_issue_test() ->
    Manifest = 
        {manifest,{array,9,0,[],
                 {[],
                  [{manifest_entry,{o_rkv,"Bucket","Key10",null},
                                   {o_rkv,"Bucket","Key12949",null},
                                   "<0.313.0>","./16_1_0.sst", none},
                   {manifest_entry,{o_rkv,"Bucket","Key129490",null},
                                   {o_rkv,"Bucket","Key158981",null},
                                   "<0.315.0>","./16_1_1.sst", none},
                   {manifest_entry,{o_rkv,"Bucket","Key158982",null},
                                   {o_rkv,"Bucket","Key188472",null},
                                   "<0.316.0>","./16_1_2.sst", none}],
                  {idxt,1,
                        {{[{{o_rkv,"Bucket1","Key1",null},
                            {manifest_entry,{o_rkv,"Bucket","Key9083",null},
                                            {o_rkv,"Bucket1","Key1",null},
                                            "<0.320.0>","./16_1_6.sst", none}}]},
                         {1,{{o_rkv,"Bucket1","Key1",null},1,nil,nil}}}},
                  {idxt,0,{{},{0,nil}}},
                  {idxt,0,{{},{0,nil}}},
                  {idxt,0,{{},{0,nil}}},
                  {idxt,0,{{},{0,nil}}},
                  {idxt,0,{{},{0,nil}}},
                  {idxt,0,{{},{0,nil}}},
                  []}},
          19,[],0,
          {dict,0,16,16,8,80,48,
                {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}},
          2,
          dict:new()},
    Range1 = range_lookup(Manifest, 
                            1, 
                            {o_rkv, "Bucket", null, null}, 
                            {o_rkv, "Bucket", null, null}),
    Range2 = range_lookup(Manifest, 
                            2, 
                            {o_rkv, "Bucket", null, null}, 
                            {o_rkv, "Bucket", null, null}),
    io:format("Range in Level 1 ~w~n", [Range1]),
    io:format("Range in Level 2 ~w~n", [Range2]),
    ?assertMatch(3, length(Range1)),
    ?assertMatch(1, length(Range2)).

    
-endif.
