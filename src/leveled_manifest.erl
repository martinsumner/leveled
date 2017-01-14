%% -------- PENCILLER MANIFEST ---------
%%
%% The manifest is an ordered set of files for each level to be used to find
%% which file is relevant for a given key or range lookup at a given level.
%%
%% It is implemented as an ETS table, primarily to optimise lookup speed, but
%% also for the convenience of the tab2file format to support the persisting
%% of the manifest.  However, it needs to be a multi-version table, as the 
%% table may be accessed by both the real actor but also cloned actors too,
%% and they need to see state at different points in time.
%%
%% The ets table is an ordered set.  The Key is a tuple of:
%%
%% {Level, LastKey, Filename}
%%
%% for the file.  The manifest entry will have a value of:
%%
%% {FirstKey, {active, aSQN}, {tomb, tSQN}}
%%
%% When an item is added to the manifest it is added with aSQN set to the
%% manifets SQN which is intended to make this change current, and a tSQN
%% of infinity.  When an item is removed the element is first altered so
%% that the tSQN is set to the next ManifestSQN.  When the active
%% (non-cloned) actor reads items in the manifest it should also reap any
%% tombstone elements that have passed the lowest manifest SQN of any of
%% the registered clones.

-module(leveled_manifest).

-include("include/leveled.hrl").

-export([
        new_manifest/0,
        open_manifest/1,
        copy_manifest/1,
        load_manifest/3,
        save_manifest/2,
        get_manifest_sqn/1,
        key_lookup/3,
        range_lookup/4,
        merge_lookup/4,
        insert_manifest_entry/4,
        remove_manifest_entry/4,
        mergefile_selector/2,
        add_snapshot/3,
        release_snapshot/2,
        ready_to_delete/2,
        delete_confirmed/2,
        check_for_work/2,
        is_basement/2,
        dump_pidmap/1,
        levelzero_present/1,
        pointer_convert/2
        ]).      

-export([
        filepath/2
        ]).

-include_lib("eunit/include/eunit.hrl").

-define(MANIFEST_FILEX, "man").
-define(MANIFEST_FP, "ledger_manifest").
-define(MAX_LEVELS, 8).

-record(manifest, {table,
                        % A Multi-Version ETS table for lookup
                    pidmap,
                        % A dictionary to map filenames to {Pid, DeleteSQN}
                    manifest_sqn = 0 :: integer(),
                        % The current manifest SQN
                    is_clone = false :: boolean(),
                        % Is this manifest held by a clone (i.e. snapshot)
                    level_counts,
                        % An array of level counts to speed up compation work assessment
                    snapshots :: list(),
                        % A list of snaphots (i.e. clones)
                    delete_sqn :: integer()
                        % The lowest SQN of any clone
                    }).      

%%%============================================================================
%%% API
%%%============================================================================

new_manifest() ->
    Table = ets:new(manifest, [ordered_set, public]),
    new_manifest(Table).

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
    {ManSQN, Manifest} = open_manifestfile(RootPath, ValidManSQNs),
    Manifest#manifest{manifest_sqn = ManSQN, delete_sqn = ManSQN}.
    
copy_manifest(Manifest) ->
    % Copy the manifest ensuring anything only the master process should care
    % about is switched to undefined
    #manifest{is_clone = true,
                table = Manifest#manifest.table,
                manifest_sqn = Manifest#manifest.manifest_sqn,
                pidmap = Manifest#manifest.pidmap}.

load_manifest(Manifest, PidFun, SQNFun) ->
    FlatManifest = ets:tab2list(Manifest#manifest.table),
    InitiateFun =
        fun({{L, _EK, FN}, {_SK, ActSt, DelSt}}, {MaxSQN, AccMan}) ->
            case {ActSt, DelSt} of
                {{active, _ActSQN}, {tomb, infinity}} ->
                    Pid = PidFun(FN),
                    PidMap0 = dict:store(FN,
                                            {Pid, infinity},
                                            AccMan#manifest.pidmap),
                    LC = array:get(L, AccMan#manifest.level_counts),
                    LC0 = array:set(L, LC + 1, AccMan#manifest.level_counts),
                    AccMan0 = AccMan#manifest{pidmap = PidMap0,
                                                level_counts = LC0},
                    SQN = SQNFun(Pid),
                    MaxSQN0 = max(MaxSQN, SQN),
                    {MaxSQN0, AccMan0};
                {_, {tomb, _TombSQN}} ->
                    {MaxSQN, AccMan}
            end
        end,
    lists:foldl(InitiateFun, {1, Manifest}, FlatManifest).

save_manifest(Manifest, RootPath) ->
    FP = filepath(RootPath, Manifest#manifest.manifest_sqn, current_manifest),
    ets:tab2file(Manifest#manifest.table,
                    FP,
                    [{extended_info, [md5sum]}, {sync, true}]).


insert_manifest_entry(Manifest, ManSQN, Level, Entry) ->
    Key = {Level, Entry#manifest_entry.end_key, Entry#manifest_entry.filename},
    Pid = Entry#manifest_entry.owner,
    Value = {Entry#manifest_entry.start_key,
                {active, ManSQN},
                {tomb, infinity}},
    true = ets:insert_new(Manifest#manifest.table, {Key, Value}),
    PidMap0 = dict:store(Entry#manifest_entry.filename,
                            {Pid, infinity},
                            Manifest#manifest.pidmap),
    LC = array:get(Level, Manifest#manifest.level_counts),
    LCArray0 = array:set(Level, LC + 1, Manifest#manifest.level_counts),
    MaxManSQN = max(ManSQN, Manifest#manifest.manifest_sqn),
    Manifest#manifest{pidmap = PidMap0,
                        level_counts = LCArray0,
                        manifest_sqn = MaxManSQN}.

remove_manifest_entry(Manifest, ManSQN, Level, Entry) ->
    Key = {Level, Entry#manifest_entry.end_key, Entry#manifest_entry.filename},
    [{Key, Value0}] = ets:lookup(Manifest#manifest.table, Key),
    {StartKey, {active, ActiveSQN}, {tomb, infinity}} = Value0,
    Value1 = {StartKey, {active, ActiveSQN}, {tomb, ManSQN}},
    true = ets:insert(Manifest#manifest.table, {Key, Value1}),
    {Pid, infinity} = dict:fetch(Entry#manifest_entry.filename,
                                    Manifest#manifest.pidmap),
    PidMap0 = dict:store(Entry#manifest_entry.filename,
                            {Pid, ManSQN},
                            Manifest#manifest.pidmap),
    LC = array:get(Level, Manifest#manifest.level_counts),
    LCArray0 = array:set(Level, LC - 1, Manifest#manifest.level_counts),
    MaxManSQN = max(ManSQN, Manifest#manifest.manifest_sqn),
    Manifest#manifest{pidmap = PidMap0,
                        level_counts = LCArray0,
                        manifest_sqn = MaxManSQN}.

get_manifest_sqn(Manifest) ->
    Manifest#manifest.manifest_sqn.

key_lookup(Manifest, Level, Key) ->
    GC =
        case Manifest#manifest.is_clone of
            true ->
                false;
            false ->
                {true, Manifest#manifest.delete_sqn}
        end,
    FN = key_lookup(Manifest#manifest.table,
                        Level,
                        Key,
                        Manifest#manifest.manifest_sqn,
                        GC),
    case FN of
        false ->
            false;
        _ ->
            {Pid, _TombSQN} = dict:fetch(FN, Manifest#manifest.pidmap),
            Pid
    end.
    
range_lookup(Manifest, Level, StartKey, EndKey) ->
    MapFun =
        fun({{_Level, _LastKey, FN}, FirstKey}) ->
            {Pid, _SQN} = dict:fetch(FN, Manifest#manifest.pidmap),
            case FirstKey < StartKey of
                true ->
                    {next, Pid, StartKey};
                false ->
                    {next, Pid, FirstKey}
            end
        end,
    range_lookup(Manifest, Level, StartKey, EndKey, MapFun).

merge_lookup(Manifest, Level, StartKey, EndKey) ->
    MapFun =
        fun({{_Level, LastKey, FN}, FirstKey}) ->
            {Owner, _DelSQN} = dict:fetch(FN, Manifest#manifest.pidmap),
            #manifest_entry{filename = FN,
                                owner = Owner,
                                start_key = FirstKey,
                                end_key = LastKey}
        end,
    range_lookup(Manifest, Level, StartKey, EndKey, MapFun).

pointer_convert(Manifest, EntryList) ->
    MapFun =
        fun(Entry) ->
            {Pid, _DelSQN} = dict:fetch(Entry#manifest_entry.filename,
                                            Manifest#manifest.pidmap),
            {next, Pid, all}
        end,
    lists:map(MapFun, EntryList).

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
mergefile_selector(Manifest, Level) ->
    KL = range_lookup(Manifest#manifest.table,
                        Level,
                        {all, 0},
                        all,
                        all,
                        [],
                        Manifest#manifest.manifest_sqn),
    {{Level, LastKey, FN},
        FirstKey} = lists:nth(random:uniform(length(KL)), KL),
    {Owner, infinity} = dict:fetch(FN, Manifest#manifest.pidmap),
    #manifest_entry{filename = FN,
                        owner = Owner,
                        start_key = FirstKey,
                        end_key = LastKey}.

add_snapshot(Manifest, Pid, Timeout) ->
    SnapEntry = {Pid, Manifest#manifest.manifest_sqn, Timeout},
    SnapList0 = [SnapEntry|Manifest#manifest.snapshots],
    MinDelSQN = min(Manifest#manifest.delete_sqn, Manifest#manifest.manifest_sqn),
    Manifest#manifest{snapshots = SnapList0, delete_sqn = MinDelSQN}.

release_snapshot(Manifest, Pid) ->
    FilterFun =
        fun({P, SQN, TS}, {Acc, MinSQN}) ->
            case P of
                Pid ->
                    {Acc, MinSQN};
                _ ->
                    {[{P, SQN, TS}|Acc], min(SQN, MinSQN)}
            end
        end,
    {SnapList0,
        DeleteSQN} = lists:foldl(FilterFun,
                                    {[], Manifest#manifest.manifest_sqn},
                                    Manifest#manifest.snapshots),
    leveled_log:log("P0004", [SnapList0]),
    Manifest#manifest{snapshots = SnapList0, delete_sqn = DeleteSQN}.

ready_to_delete(Manifest, Filename) ->
    case dict:fetch(Filename, Manifest#manifest.pidmap) of
        {P, infinity} ->
            {false, P};
        {P, DeleteSQN} ->
            {ready_to_delete(Manifest#manifest.snapshots,
                                DeleteSQN,
                                os:timestamp()),
                P}
    end.

delete_confirmed(Manifest, Filename) ->
    PidMap = dict:erase(Filename, Manifest#manifest.pidmap),
    % Would be better to clear ETS at this point rather than on lookup?
    Manifest#manifest{pidmap = PidMap}.


check_for_work(Manifest, Thresholds) ->
    CheckLevelFun =
        fun({Level, MaxCount}, {AccL, AccC}) ->
            case array:get(Level, Manifest#manifest.level_counts) of
                LC when LC > MaxCount ->
                    {[Level|AccL], AccC + LC - MaxCount};
                _ ->
                    {AccL, AccC}
            end
        end,
    lists:foldl(CheckLevelFun, {[], 0}, Thresholds).    

is_basement(Manifest, Level) ->
    CheckFun =
        fun(L, Acc) ->
            case array:get(L, Manifest#manifest.level_counts) of
                0 ->
                    Acc;
                _N ->
                    false
            end
        end,
    lists:foldl(CheckFun, true, lists:seq(Level + 1, ?MAX_LEVELS)).

dump_pidmap(Manifest) ->
    dict:to_list(Manifest#manifest.pidmap).

levelzero_present(Manifest) ->
    case key_lookup(Manifest, 0, all) of
        false ->
            false;
        _ ->
            true
    end.

%%%============================================================================
%%% Internal Functions
%%%============================================================================


new_manifest(Table) ->
    #manifest{
        table = Table, 
        pidmap = dict:new(), 
        level_counts = array:new([{size, ?MAX_LEVELS + 1}, {default, 0}]),
        snapshots = [],
        delete_sqn = 0
    }.

range_lookup(Manifest, Level, StartKey, EndKey, MapFun) ->
    KL = range_lookup(Manifest#manifest.table,
                        Level,
                        {StartKey, 0},
                        StartKey,
                        EndKey,
                        [],
                        Manifest#manifest.manifest_sqn),
    lists:map(MapFun, KL).

range_lookup(Manifest, Level, {LastKey, LastFN}, SK, EK, Acc, ManSQN) ->
    case ets:next(Manifest, {Level, LastKey, LastFN}) of
        '$end_of_table' ->
            Acc;
        {Level, NextKey, NextFN} ->
            [{K, V}] = ets:lookup(Manifest, {Level, NextKey, NextFN}),
            {FirstKey, {active, ActiveSQN}, {tomb, TombSQN}} = V,
            Active = (ManSQN >= ActiveSQN) and (ManSQN < TombSQN),
            case Active of
                true ->
                    PostEnd = leveled_codec:endkey_passed(EK, FirstKey),
                    case PostEnd of
                        true ->
                            Acc;
                        false ->
                            range_lookup(Manifest,
                                            Level,
                                            {NextKey, NextFN},
                                            SK,
                                            EK,
                                            Acc ++ [{K, FirstKey}],
                                            ManSQN)
                    end;
                false ->
                    range_lookup(Manifest,
                                    Level,
                                    {NextKey, NextFN},
                                    SK,
                                    EK,
                                    Acc,
                                    ManSQN)
            end;
        {OtherLevel, _, _} when OtherLevel > Level ->
            Acc
    end.

ready_to_delete(SnapList, FileDeleteSQN, Now) ->
    FilterFun =
        fun({P, SnapSQN, ExpiryTS}, Acc) ->
            case Acc of
                false ->
                    false;
                true ->
                    case FileDeleteSQN < SnapSQN of
                        true ->
                            % Snapshot taken after the file deletion
                            true;
                        false ->
                            case Now > ExpiryTS of
                                true ->
                                    leveled_log:log("P0034", [P]),
                                    true;
                                false ->
                                    false
                            end
                    end
            end
        end,
    lists:foldl(FilterFun, true, SnapList).

filepath(RootPath, manifest) ->
    MFP = RootPath ++ "/" ++ ?MANIFEST_FP ++ "/",
    filelib:ensure_dir(MFP),
    MFP.

filepath(RootPath, NewMSN, current_manifest) ->
    filepath(RootPath, manifest)  ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?MANIFEST_FILEX.


open_manifestfile(_RootPath, []) ->
    leveled_log:log("P0013", []),
    {0, new_manifest()};
open_manifestfile(_RootPath, [0]) ->
    leveled_log:log("P0013", []),
    {0, new_manifest()};
open_manifestfile(RootPath, [TopManSQN|Rest]) ->
    CurrManFile = filepath(RootPath, TopManSQN, current_manifest),
    case ets:file2tab(CurrManFile, [{verify,true}]) of
        {error, Reason} ->
            leveled_log:log("P0033", [CurrManFile, Reason]),
            open_manifestfile(RootPath, Rest);
        {ok, Table} ->
            leveled_log:log("P0012", [TopManSQN]),
            {TopManSQN, new_manifest(Table)}
    end.

key_lookup(Manifest, Level, KeyToFind, ManSQN, GC) ->
    key_lookup(Manifest, Level, {KeyToFind, any}, KeyToFind, ManSQN, GC).

key_lookup(Manifest, Level, {LastKey, LastFN}, KeyToFind, ManSQN, GC) ->
    case ets:next(Manifest, {Level, LastKey, LastFN}) of
        '$end_of_table' ->
            false;
        {Level, NextKey, NextFN} ->
            [{K, V}] = ets:lookup(Manifest, {Level, NextKey, NextFN}),
            {FirstKey, {active, ActiveSQN}, {tomb, TombSQN}} = V,
            Active = (ManSQN >= ActiveSQN) and (ManSQN < TombSQN),
            case Active of
                true ->
                    InRange = (KeyToFind >= FirstKey) or (KeyToFind == all),
                    case InRange of
                        true ->
                            NextFN;
                        false ->
                            false
                    end;
                false ->
                    case GC of
                        false ->
                            ok;
                        {true, GC_SQN} ->
                            case TombSQN < GC_SQN of
                                true ->
                                    ets:delete(Manifest, K);
                                false ->
                                    ok
                            end
                    end,
                    key_lookup(Manifest,
                                Level,
                                {NextKey, NextFN},
                                KeyToFind,
                                ManSQN,
                                GC)
            end;
        {OtherLevel, _, _} when OtherLevel > Level ->
            false
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

initial_setup() -> 
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1",
                            owner="pid_z1"},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                                end_key={o, "Bucket1", "K71", null},
                                filename="Z2",
                                owner="pid_z2"},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3",
                            owner="pid_z3"},
    E4 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld7"}, "K93"},
                            filename="Z4",
                            owner="pid_z4"},
    E5 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld7"}, "K97"},
                            end_key={o, "Bucket1", "K78", null},
                            filename="Z5",
                            owner="pid_z5"},
    E6 = #manifest_entry{start_key={o, "Bucket1", "K81", null},
                            end_key={o, "Bucket1", "K996", null},
                            filename="Z6",
                            owner="pid_z6"},
    
    Man0 = new_manifest(),
    % insert_manifest_entry(Manifest, ManSQN, Level, Entry)
    Man1 = insert_manifest_entry(Man0, 1, 1, E1),
    Man2 = insert_manifest_entry(Man1, 1, 1, E2),
    Man3 = insert_manifest_entry(Man2, 1, 1, E3),
    Man4 = insert_manifest_entry(Man3, 1, 2, E4),
    Man5 = insert_manifest_entry(Man4, 1, 2, E5),
    Man6 = insert_manifest_entry(Man5, 1, 2, E6),
    {Man0, Man1, Man2, Man3, Man4, Man5, Man6}.

changeup_setup(Man6) ->
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1",
                            owner="pid_z1"},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                                end_key={o, "Bucket1", "K71", null},
                                filename="Z2",
                                owner="pid_z2"},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3",
                            owner="pid_z3"},
                            
    E1_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld4"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K62"},
                            owner="pid_y1",
                            filename="Y1"},
    E2_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K67"},
                            end_key={o, "Bucket1", "K45", null},
                            owner="pid_y2",
                            filename="Y2"},
    E3_2 = #manifest_entry{start_key={o, "Bucket1", "K47", null},
                            end_key={o, "Bucket1", "K812", null},
                            owner="pid_y3",
                            filename="Y3"},
    E4_2 = #manifest_entry{start_key={o, "Bucket1", "K815", null},
                            end_key={o, "Bucket1", "K998", null},
                            owner="pid_y4",
                            filename="Y4"},
    
    Man7 = insert_manifest_entry(Man6, 2, 1, E1_2),
    Man8 = insert_manifest_entry(Man7, 2, 1, E2_2),
    Man9 = insert_manifest_entry(Man8, 2, 1, E3_2),
    Man10 = insert_manifest_entry(Man9, 2, 1, E4_2),
    % remove_manifest_entry(Manifest, ManSQN, Level, Entry)
    Man11 = remove_manifest_entry(Man10, 2, 1, E1),
    Man12 = remove_manifest_entry(Man11, 2, 1, E2),
    Man13 = remove_manifest_entry(Man12, 2, 1, E3),
    {Man7, Man8, Man9, Man10, Man11, Man12, Man13}.

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
    io:format("Commencing failing test:~n"),
    ?assertMatch("pid_z3", key_lookup(Man6, 1, LK1_4)),
    ?assertMatch("pid_z3", key_lookup(Man6, 1, LK1_5)),
    
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_2)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_3)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_4)),
    ?assertMatch("pid_z5", key_lookup(Man6, 2, LK1_5)),
    
    ?assertMatch("pid_y3", key_lookup(Man13, 1, LK1_4)),
    ?assertMatch("pid_z5", key_lookup(Man13, 2, LK1_4)).


rangequery_manifest_test() ->
    {_Man0, _Man1, _Man2, _Man3, _Man4, _Man5, Man6} = initial_setup(),
    
    SK1 = {o, "Bucket1", "K711", null},
    EK1 = {o, "Bucket1", "K999", null},
    RL1_1 = range_lookup(Man6, 1, SK1, EK1),
    ?assertMatch([{next, "pid_z3", {o, "Bucket1", "K75", null}}], RL1_1),
    RL1_2 = range_lookup(Man6, 2, SK1, EK1),
    ?assertMatch([{next, "pid_z5", {o, "Bucket1", "K711", null}},
                    {next, "pid_z6", {o, "Bucket1", "K81", null}}],
                    RL1_2),
    SK2 = {i, "Bucket1", {"Idx1", "Fld8"}, null},
    EK2 = {i, "Bucket1", {"Idx1", "Fld8"}, null},
    RL2_1 = range_lookup(Man6, 1, SK2, EK2),
    ?assertMatch([{next, "pid_z1", {i, "Bucket1", {"Idx1", "Fld8"}, null}}],
                    RL2_1),
    RL2_2 = range_lookup(Man6, 2, SK2, EK2),
    ?assertMatch([{next, "pid_z5", {i, "Bucket1", {"Idx1", "Fld8"}, null}}],
                    RL2_2),
    
    SK3 = {o, "Bucket1", "K994", null},
    EK3 = {o, "Bucket1", "K995", null},
    RL3_1 = range_lookup(Man6, 1, SK3, EK3),
    ?assertMatch([], RL3_1),
    RL3_2 = range_lookup(Man6, 2, SK3, EK3),
    ?assertMatch([{next, "pid_z6", {o, "Bucket1", "K994", null}}], RL3_2),
    
    {_Man7, _Man8, _Man9, _Man10, _Man11, _Man12,
        Man13} = changeup_setup(Man6),
    
    % Results unchanged despiter ES table change if using old manifest
    RL1_1A = range_lookup(Man6, 1, SK1, EK1),
    ?assertMatch([{next, "pid_z3", {o, "Bucket1", "K75", null}}], RL1_1A),
    RL2_1A = range_lookup(Man6, 1, SK2, EK2),
    ?assertMatch([{next, "pid_z1", {i, "Bucket1", {"Idx1", "Fld8"}, null}}],
                    RL2_1A),
    RL3_1A = range_lookup(Man6, 1, SK3, EK3),
    ?assertMatch([], RL3_1A),
     
    RL1_1B = range_lookup(Man13, 1, SK1, EK1),
    ?assertMatch([{next, "pid_y3", {o, "Bucket1", "K711", null}},
                    {next, "pid_y4", {o, "Bucket1", "K815", null}}], RL1_1B),
    RL2_1B = range_lookup(Man13, 1, SK2, EK2),
    ?assertMatch([{next, "pid_y1", {i, "Bucket1", {"Idx1", "Fld8"}, null}}],
                    RL2_1B),
    RL3_1B = range_lookup(Man13, 1, SK3, EK3),
    ?assertMatch([{next, "pid_y4", {o, "Bucket1", "K994", null}}], RL3_1B).

levelzero_present_test() ->
    E0 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={o, "Bucket1", "Key996", null},
                            filename="Z0",
                            owner="pid_z0"},
     
    Man0 = new_manifest(),
    ?assertMatch(false, levelzero_present(Man0)),
    % insert_manifest_entry(Manifest, ManSQN, Level, Entry)
    Man1 = insert_manifest_entry(Man0, 1, 0, E0),
    ?assertMatch(true, levelzero_present(Man1)).

-endif.