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
        save_manifest/3,
        key_lookup/4,
        key_lookup/5,
        range_lookup/5,
        insert_manifest_entry/4,
        remove_manifest_entry/4,
        add_snapshot/4,
        release_snapshot/2,
        ready_to_delete/2
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(MANIFEST_FILEX, "man").
-define(MANIFEST_FP, "ledger_manifest").


%%%============================================================================
%%% API
%%%============================================================================

new_manifest() ->
    ets:new(manifest, [ordered_set]).

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

save_manifest(Manifest, RootPath, ManSQN) ->
    FP = filepath(RootPath, ManSQN, current_manifest),
    ets:tab2file(Manifest,
                    FP,
                    [{extended_info, [md5sum]}, {sync, true}]).

insert_manifest_entry(Manifest, ManSQN, Level, Entry) ->
    Key = {Level, Entry#manifest_entry.end_key, Entry#manifest_entry.filename},
    Value = {Entry#manifest_entry.start_key,
                {active, ManSQN},
                {tomb, infinity}},
    true = ets:insert_new(Manifest, {Key, Value}).

remove_manifest_entry(Manifest, ManSQN, Level, Entry) ->
    Key = {Level, Entry#manifest_entry.end_key, Entry#manifest_entry.filename},
    [{Key, Value0}] = ets:lookup(Manifest, Key),
    {StartKey, {active, ActiveSQN}, {tomb, infinity}} = Value0,
    Value1 = {StartKey, {active, ActiveSQN}, {tomb, ManSQN}},
    true = ets:insert(Manifest, {Key, Value1}).

key_lookup(Manifest, Level, Key, ManSQN) ->
    key_lookup(Manifest, Level, Key, ManSQN, false).

key_lookup(Manifest, Level, Key, ManSQN, GC) ->
    key_lookup(Manifest, Level, {Key, 0}, Key, ManSQN, GC).

range_lookup(Manifest, Level, StartKey, EndKey, ManSQN) ->
    range_lookup(Manifest, Level, {StartKey, 0}, StartKey, EndKey, [], ManSQN).

add_snapshot(SnapList0, Pid, ManifestSQN, Timeout) ->
    [{Pid, ManifestSQN, Timeout}|SnapList0].

release_snapshot(SnapList0, Pid) ->
    FilterFun =
        fun({P, SQN, TS}, Acc) ->
            case P of
                Pid ->
                    Acc;
                _ ->
                    [{P, SQN, TS}|Acc]
            end
        end,
    lists:foldl(FilterFun, [], SnapList0).

ready_to_delete(SnapList0, DeleteSQN) ->
    ready_to_delete(SnapList0, DeleteSQN, os:timestamp()).

%%%============================================================================
%%% Internal Functions
%%%============================================================================

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
    RootPath ++ "/" ++ ?MANIFEST_FP ++ "/".

filepath(RootPath, NewMSN, current_manifest) ->
    filepath(RootPath, manifest)  ++ "nonzero_"
                ++ integer_to_list(NewMSN) ++ "." ++ ?MANIFEST_FILEX.


open_manifestfile(_RootPath, []) ->
    leveled_log:log("P0013", []),
    new_manifest();
open_manifestfile(_RootPath, [0]) ->
    leveled_log:log("P0013", []),
    new_manifest();
open_manifestfile(RootPath, [TopManSQN|Rest]) ->
    CurrManFile = filepath(RootPath, TopManSQN, current_manifest),
    case ets:file2tab(CurrManFile, [{verify,true}]) of
        {error, Reason} ->
            leveled_log:log("P0033", [CurrManFile, Reason]),
            open_manifestfile(RootPath, Rest);
        {ok, Table} ->
            leveled_log:log("P0012", [TopManSQN]),
            Table
    end.

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
                    InRange = KeyToFind >= FirstKey,
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

range_lookup(Manifest, Level, {LastKey, LastFN}, SK, EK, Acc, ManSQN) ->
    case ets:next(Manifest, {Level, LastKey, LastFN}) of
        '$end_of_table' ->
            Acc;
        {Level, NextKey, NextFN} ->
            [{_K, V}] = ets:lookup(Manifest, {Level, NextKey, NextFN}),
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
                                            Acc ++ [NextFN],
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

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

rangequery_manifest_test() ->
    E1 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K93"},
                            filename="Z1"},
    E2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K97"},
                                end_key={o, "Bucket1", "K71", null},
                                filename="Z2"},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K75", null},
                            end_key={o, "Bucket1", "K993", null},
                            filename="Z3"},
    E4 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld1"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld7"}, "K93"},
                            filename="Z4"},
    E5 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld7"}, "K97"},
                            end_key={o, "Bucket1", "K78", null},
                            filename="Z5"},
    E6 = #manifest_entry{start_key={o, "Bucket1", "K81", null},
                            end_key={o, "Bucket1", "K996", null},
                            filename="Z6"},
    
    Manifest = open_manifestfile(dummy, []),
    insert_manifest_entry(Manifest, 1, 1, E1),
    insert_manifest_entry(Manifest, 1, 1, E2),
    insert_manifest_entry(Manifest, 1, 1, E3),
    insert_manifest_entry(Manifest, 1, 2, E4),
    insert_manifest_entry(Manifest, 1, 2, E5),
    insert_manifest_entry(Manifest, 1, 2, E6),
    
    SK1 = {o, "Bucket1", "K711", null},
    EK1 = {o, "Bucket1", "K999", null},
    RL1_1 = range_lookup(Manifest, 1, SK1, EK1, 1),
    ?assertMatch(["Z3"], RL1_1),
    RL1_2 = range_lookup(Manifest, 2, SK1, EK1, 1),
    ?assertMatch(["Z5", "Z6"], RL1_2),
    SK2 = {i, "Bucket1", {"Idx1", "Fld8"}, null},
    EK2 = {i, "Bucket1", {"Idx1", "Fld8"}, null},
    RL2_1 = range_lookup(Manifest, 1, SK2, EK2, 1),
    ?assertMatch(["Z1"], RL2_1),
    RL2_2 = range_lookup(Manifest, 2, SK2, EK2, 1),
    ?assertMatch(["Z5"], RL2_2),
    
    SK3 = {o, "Bucket1", "K994", null},
    EK3 = {o, "Bucket1", "K995", null},
    RL3_1 = range_lookup(Manifest, 1, SK3, EK3, 1),
    ?assertMatch([], RL3_1),
    RL3_2 = range_lookup(Manifest, 2, SK3, EK3, 1),
    ?assertMatch(["Z6"], RL3_2),
    
    E1_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld4"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K62"},
                            filename="Y1"},
    E2_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K67"},
                                end_key={o, "Bucket1", "K45", null},
                                filename="Y2"},
    E3_2 = #manifest_entry{start_key={o, "Bucket1", "K47", null},
                            end_key={o, "Bucket1", "K812", null},
                            filename="Y3"},
    E4_2 = #manifest_entry{start_key={o, "Bucket1", "K815", null},
                            end_key={o, "Bucket1", "K998", null},
                            filename="Y4"},
    
    insert_manifest_entry(Manifest, 2, 1, E1_2),
    insert_manifest_entry(Manifest, 2, 1, E2_2),
    insert_manifest_entry(Manifest, 2, 1, E3_2),
    insert_manifest_entry(Manifest, 2, 1, E4_2),
    remove_manifest_entry(Manifest, 2, 1, E1),
    remove_manifest_entry(Manifest, 2, 1, E2),
    remove_manifest_entry(Manifest, 2, 1, E3),
    
    RL1_1A = range_lookup(Manifest, 1, SK1, EK1, 1),
    ?assertMatch(["Z3"], RL1_1A),
    RL2_1A = range_lookup(Manifest, 1, SK2, EK2, 1),
    ?assertMatch(["Z1"], RL2_1A),
    RL3_1A = range_lookup(Manifest, 1, SK3, EK3, 1),
    ?assertMatch([], RL3_1A),
    
    RL1_1B = range_lookup(Manifest, 1, SK1, EK1, 2),
    ?assertMatch(["Y3", "Y4"], RL1_1B),
    RL2_1B = range_lookup(Manifest, 1, SK2, EK2, 2),
    ?assertMatch(["Y1"], RL2_1B),
    RL3_1B = range_lookup(Manifest, 1, SK3, EK3, 2),
    ?assertMatch(["Y4"], RL3_1B).



keyquery_manifest_test() ->
    E1 = #manifest_entry{start_key={o, "Bucket1", "K0001", null},
                            end_key={o, "Bucket1", "K0990", null},
                            filename="Z1"},
    E2 = #manifest_entry{start_key={o, "Bucket1", "K1003", null},
                                end_key={o, "Bucket1", "K3692", null},
                                filename="Z2"},
    E3 = #manifest_entry{start_key={o, "Bucket1", "K3750", null},
                            end_key={o, "Bucket1", "K9930", null},
                            filename="Z3"},
    
    
    Manifest0 = open_manifestfile(dummy, []),
    insert_manifest_entry(Manifest0, 1, 1, E1),
    insert_manifest_entry(Manifest0, 1, 1, E2),
    insert_manifest_entry(Manifest0, 1, 1, E3),
    
    RootPath = "../test",
    ok = filelib:ensure_dir(filepath(RootPath, manifest)),
    ok = save_manifest(Manifest0, RootPath, 1),
    true = ets:delete(Manifest0),
    ?assertMatch(true, filelib:is_file(filepath(RootPath,
                                                1,
                                                current_manifest))),
    
    BadFP = filepath(RootPath, 2, current_manifest),
    ok = file:write_file(BadFP, list_to_binary("nonsense")),
    ?assertMatch(true, filelib:is_file(BadFP)),
    
    Manifest = open_manifest(RootPath),
    
    K1 = {o, "Bucket1", "K0000", null},
    K2 = {o, "Bucket1", "K0001", null},
    K3 = {o, "Bucket1", "K0002", null},
    K4 = {o, "Bucket1", "K0990", null},
    K5 = {o, "Bucket1", "K0991", null},
    K6 = {o, "Bucket1", "K1003", null},
    K7 = {o, "Bucket1", "K1004", null},
    K8 = {o, "Bucket1", "K3692", null},
    K9 = {o, "Bucket1", "K3693", null},
    K10 = {o, "Bucket1", "K3750", null},
    K11 = {o, "Bucket1", "K3751", null},
    K12 = {o, "Bucket1", "K9930", null},
    K13 = {o, "Bucket1", "K9931", null},
    
    ?assertMatch(false, key_lookup(Manifest, 1, K1, 1)),
    ?assertMatch("Z1", key_lookup(Manifest, 1, K2, 1)),
    ?assertMatch("Z1", key_lookup(Manifest, 1, K3, 1)),
    ?assertMatch("Z1", key_lookup(Manifest, 1, K4, 1)),
    ?assertMatch(false, key_lookup(Manifest, 1, K5, 1)),
    ?assertMatch("Z2", key_lookup(Manifest, 1, K6, 1)),
    ?assertMatch("Z2", key_lookup(Manifest, 1, K7, 1)),
    ?assertMatch("Z2", key_lookup(Manifest, 1, K8, 1)),
    ?assertMatch(false, key_lookup(Manifest, 1, K9, 1)),
    ?assertMatch("Z3", key_lookup(Manifest, 1, K10, 1)),
    ?assertMatch("Z3", key_lookup(Manifest, 1, K11, 1)),
    ?assertMatch("Z3", key_lookup(Manifest, 1, K12, 1)),
    ?assertMatch(false, key_lookup(Manifest, 1, K13, 1)),
    
    E1_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld4"}, "K8"},
                            end_key={i, "Bucket1", {"Idx1", "Fld9"}, "K62"},
                            filename="Y1"},
    E2_2 = #manifest_entry{start_key={i, "Bucket1", {"Idx1", "Fld9"}, "K67"},
                                end_key={o, "Bucket1", "K45", null},
                                filename="Y2"},
    E3_2 = #manifest_entry{start_key={o, "Bucket1", "K47", null},
                            end_key={o, "Bucket1", "K812", null},
                            filename="Y3"},
    E4_2 = #manifest_entry{start_key={o, "Bucket1", "K815", null},
                            end_key={o, "Bucket1", "K998", null},
                            filename="Y4"},
    
    insert_manifest_entry(Manifest, 2, 1, E1_2),
    insert_manifest_entry(Manifest, 2, 1, E2_2),
    insert_manifest_entry(Manifest, 2, 1, E3_2),
    insert_manifest_entry(Manifest, 2, 1, E4_2),
    
    S1 = ets:info(Manifest, size),
    
    remove_manifest_entry(Manifest, 2, 1, E1),
    remove_manifest_entry(Manifest, 2, 1, E2),
    remove_manifest_entry(Manifest, 2, 1, E3),
    
    S2 = ets:info(Manifest, size),
    ?assertMatch(true, S2 == S1),
    
    ?assertMatch("Y2", key_lookup(Manifest, 1, K1, 2)),
    ?assertMatch("Y2", key_lookup(Manifest, 1, K10, 2)),
    ?assertMatch("Y4", key_lookup(Manifest, 1, K12, 2)),
    
    S3 = ets:info(Manifest, size),
    ?assertMatch(true, S3 == S1),
    
    ?assertMatch("Y2", key_lookup(Manifest, 1, K1, 2, {true, 2})),
    ?assertMatch("Y2", key_lookup(Manifest, 1, K10, 2, {true, 2})),
    ?assertMatch("Y4", key_lookup(Manifest, 1, K12, 2, {true, 2})),
    
    S4 = ets:info(Manifest, size),
    ?assertMatch(true, S4 == S1),
    
    ?assertMatch("Y2", key_lookup(Manifest, 1, K1, 3, {true, 3})),
    ?assertMatch("Y2", key_lookup(Manifest, 1, K10, 3, {true, 3})),
    ?assertMatch("Y4", key_lookup(Manifest, 1, K12, 3, {true, 3})),
    
    S5 = ets:info(Manifest, size),
    ?assertMatch(true, S5 < S1).

snapshot_test() ->
    Snap0 = [],
    
    ?assertMatch(true, ready_to_delete(Snap0, 1)),
    
    {MegaS0, S0, MicroS0} = os:timestamp(),

    Snap1 = add_snapshot(Snap0, pid_1, 3, {MegaS0, S0 + 100, MicroS0}),
    Snap2 = add_snapshot(Snap1, pid_2, 4, {MegaS0, S0 + 200, MicroS0}),
    Snap3 = add_snapshot(Snap2, pid_3, 4, {MegaS0, S0 + 150, MicroS0}),
    Snap4 = add_snapshot(Snap3, pid_4, 5, {MegaS0, S0 + 300, MicroS0}),
    
    ?assertMatch(true,
                    ready_to_delete(Snap4, 2, {MegaS0, S0, MicroS0})),
    ?assertMatch(false,
                    ready_to_delete(Snap4, 3, {MegaS0, S0, MicroS0})),
    ?assertMatch(true,
                    ready_to_delete(Snap4, 3, {MegaS0, S0 + 150, MicroS0})),
    ?assertMatch(false,
                    ready_to_delete(Snap4, 4, {MegaS0, S0 + 150, MicroS0})),
    ?assertMatch(true,
                    ready_to_delete(Snap4, 4, {MegaS0, S0 + 250, MicroS0})),
    
    Snap5 = release_snapshot(Snap4, pid_1),
    ?assertMatch(true,
                    ready_to_delete(Snap5, 3, {MegaS0, S0, MicroS0})).
    


-endif.