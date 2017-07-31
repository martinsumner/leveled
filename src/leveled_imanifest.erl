%% -------- Inker Manifest ---------
%% 


-module(leveled_imanifest).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
        generate_entry/1,
        add_entry/3,
        append_lastkey/3,
        remove_entry/2,
        find_entry/2,
        head_entry/1,
        to_list/1,
        from_list/1,
        reader/2,
        writer/3,
        printer/1,
        complete_filex/0
        
        ]).         

-define(MANIFEST_FILEX, "man").
-define(PENDING_FILEX, "pnd").
-define(SKIP_WIDTH, 16).

-type manifest() :: list({integer(), list()}).
%% The manifest is divided into blocks by sequence number, with each block
%% being a list of manifest entries for that SQN range.
-type manifest_entry() :: {integer(), string(), pid()|string(), any()}.
%% The Entry should have a pid() as the third element, but a string() may be
%% used in unit tests


%%%============================================================================
%%% API
%%%============================================================================

-spec generate_entry(pid()) -> list().
%% @doc
%% Generate a list with a single iManifest entry for a journal file.  Used
%% only by the clerk when creating new entries for compacted files.
generate_entry(Journal) ->
    {ok, NewFN} = leveled_cdb:cdb_complete(Journal),
    {ok, PidR} = leveled_cdb:cdb_open_reader(NewFN),
    case leveled_cdb:cdb_firstkey(PidR) of
        {StartSQN, _Type, _PK} ->
            LastKey = leveled_cdb:cdb_lastkey(PidR),
            [{StartSQN, NewFN, PidR, LastKey}];
        empty ->
            leveled_log:log("IC013", [NewFN]),
            []
    end.

-spec add_entry(manifest(), manifest_entry(), boolean()) -> manifest().
%% @doc
%% Add a new entry to the manifest, if this is the rolling of a new active
%% journal the boolean ToEnd can be used to indicate it should be simply
%% appended to the end of the manifest.
add_entry(Manifest, Entry, ToEnd) ->
    {SQN, FN, PidR, LastKey} = Entry,
    StrippedName = filename:rootname(FN),
    case ToEnd of
        true ->
            prepend_entry({SQN, StrippedName, PidR, LastKey}, Manifest);
        false ->
            Man0 = [{SQN, StrippedName, PidR, LastKey}|to_list(Manifest)],
            Man1 = lists:reverse(lists:sort(Man0)),
            from_list(Man1)
    end.

-spec append_lastkey(manifest(), pid(), any()) -> manifest().
%% @doc
%% On discovery of the last key in the last journal entry, the manifest can
%% be updated through this function to have the last key
append_lastkey(Manifest, Pid, LastKey) ->
    [{SQNMarker, SQNL}|ManifestTail] = Manifest,
    [{E_SQN, E_FN, E_P, E_LK}|SQNL_Tail] = SQNL,
    case {E_P, E_LK} of 
        {Pid, empty} ->
            UpdEntry = {E_SQN, E_FN, E_P, LastKey},
            [{SQNMarker, [UpdEntry|SQNL_Tail]}|ManifestTail];
        _ ->
            Manifest
    end.

-spec remove_entry(manifest(), manifest_entry()) -> manifest().
%% @doc
%% Remove an entry from a manifest (after compaction)
remove_entry(Manifest, Entry) ->
    {SQN, FN, _PidR, _LastKey} = Entry,
    leveled_log:log("I0013", [FN]),
    Man0 = lists:keydelete(SQN, 1, to_list(Manifest)),
    from_list(Man0).

-spec find_entry(integer(), manifest()) -> pid()|string().
%% @doc
%% Given a SQN find the relevant manifest_entry, returning just the pid() of
%% the journal file (which may be a string() in unit tests)
find_entry(SQN, [{SQNMarker, SubL}|_Tail]) when SQN >= SQNMarker ->
    find_subentry(SQN, SubL);
find_entry(SQN, [_TopEntry|Tail]) ->
    find_entry(SQN, Tail).

-spec head_entry(manifest()) -> manifest_entry().
%% @doc
%% Return the head manifets entry (the most recent journal)
head_entry(Manifest) ->
    [{_SQNMarker, SQNL}|_Tail] = Manifest,
    [HeadEntry|_SQNL_Tail] = SQNL,
    HeadEntry.
    
-spec to_list(manifest()) -> list().
%% @doc
%% Convert the manifest to a flat list
to_list(Manifest) ->
    FoldFun =
        fun({_SQNMarker, SubL}, Acc) ->
            Acc ++ SubL
        end,
    lists:foldl(FoldFun, [], Manifest).

-spec reader(integer(), string()) -> manifest().
%% @doc
%% Given a file path and a manifest SQN return the inker manifest
reader(SQN, RootPath) ->
    ManifestPath = leveled_inker:filepath(RootPath, manifest_dir),
    leveled_log:log("I0015", [ManifestPath, SQN]),
    {ok, MBin} = file:read_file(filename:join(ManifestPath,
                                                integer_to_list(SQN)
                                                ++ ".man")),
    from_list(lists:reverse(lists:sort(binary_to_term(MBin)))).
    
-spec writer(manifest(), integer(), string()) -> ok.
%% @doc
%% Given a manifest and a manifest SQN and a file path, save the manifest to
%% disk
writer(Manifest, ManSQN, RootPath) ->
    ManPath = leveled_inker:filepath(RootPath, manifest_dir),
    NewFN = filename:join(ManPath,
                            integer_to_list(ManSQN) ++ "." ++ ?MANIFEST_FILEX),
    TmpFN = filename:join(ManPath,
                            integer_to_list(ManSQN) ++ "." ++ ?PENDING_FILEX),
    MBin = term_to_binary(to_list(Manifest), [compressed]),
    case filelib:is_file(NewFN) of
        false ->
            leveled_log:log("I0016", [ManSQN]),
            ok = file:write_file(TmpFN, MBin),
            ok = file:rename(TmpFN, NewFN),
            ok
    end.

-spec printer(manifest()) -> ok.
%% @doc
%% Print the manifest to the log
printer(Manifest) ->
    lists:foreach(fun({SQN, FN, _PID, _LK}) ->
                         leveled_log:log("I0017", [SQN, FN]) end,
                    to_list(Manifest)).

-spec complete_filex() -> string().
%% @doc
%% Return the file extension to be used for a completed manifest file
complete_filex() ->
    ?MANIFEST_FILEX.


%%%============================================================================
%%% Internal Functions
%%%============================================================================

from_list(Manifest) ->
    % Manifest should already be sorted with the highest SQN at the head
    % This will be maintained so that we can fold from the left, and find
    % more recently added entries quicker - under the assumptions that fresh
    % reads are more common than stale reads
    lists:foldr(fun prepend_entry/2, [], Manifest).

prepend_entry(Entry, AccL) ->
    {SQN, _FN, _PidR, _LastKey} = Entry,
    case AccL of
        [] ->
            [{SQN, [Entry]}];
        [{SQNMarker, SubL}|Tail] ->
            case length(SubL) < ?SKIP_WIDTH of
                true ->
                    [{SQNMarker, [Entry|SubL]}|Tail];
                false ->
                    [{SQN, [Entry]}|AccL]
            end
    end.

find_subentry(SQN, [{ME_SQN, _FN, ME_P, _LK}|_Tail]) when SQN >= ME_SQN ->
    ME_P;
find_subentry(SQN, [_TopEntry|Tail]) ->
    find_subentry(SQN, Tail).
    
    
%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

build_testmanifest_aslist() ->
    ManifestMapFun =
        fun(N) ->
            NStr = integer_to_list(N),
            {max(1, N * 1000), "FN" ++ NStr, "pid" ++ NStr, "LK" ++ NStr}
        end,
    lists:map(ManifestMapFun, lists:reverse(lists:seq(0, 50))).

test_testmanifest(Man0) ->
    ?assertMatch("pid0", find_entry(1, Man0)),
    ?assertMatch("pid0", find_entry(2, Man0)),
    ?assertMatch("pid1", find_entry(1001, Man0)),
    ?assertMatch("pid20", find_entry(20000, Man0)),
    ?assertMatch("pid20", find_entry(20001, Man0)),
    ?assertMatch("pid20", find_entry(20999, Man0)),
    ?assertMatch("pid50", find_entry(99999, Man0)).

buildfromlist_test() ->
    ManL = build_testmanifest_aslist(),
    Man0 = from_list(ManL),
    test_testmanifest(Man0),
    ?assertMatch(ManL, to_list(Man0)).

buildfromend_test() ->
    ManL = build_testmanifest_aslist(),
    FoldFun =
        fun(E, Man) ->
            add_entry(Man, E, true)
        end,
    Man0 = lists:foldr(FoldFun, [], ManL),
    test_testmanifest(Man0),
    ?assertMatch(ManL, to_list(Man0)).

buildrandomfashion_test() ->
    ManL0 = build_testmanifest_aslist(),
    RandMapFun =
        fun(X) ->
            {leveled_rand:uniform(), X}
        end,    
    ManL1 = lists:map(RandMapFun, ManL0),
    ManL2 = lists:sort(ManL1),
    
    FoldFun =
        fun({_R, E}, Man) ->
            add_entry(Man, E, false)
        end,
    Man0 = lists:foldl(FoldFun, [], ManL2),
    
    test_testmanifest(Man0),
    ?assertMatch(ManL0, to_list(Man0)),
    
    RandomEntry = lists:nth(leveled_rand:uniform(50), ManL0),
    Man1 = remove_entry(Man0, RandomEntry),
    Man2 = add_entry(Man1, RandomEntry, false),
    
    test_testmanifest(Man2),
    ?assertMatch(ManL0, to_list(Man2)).

empty_active_journal_test() ->
    Path = "../test/journal/journal_files/",
    ok = filelib:ensure_dir(Path),
    {ok, ActJ} = leveled_cdb:cdb_open_writer(Path ++ "test_emptyactive_file.pnd"),
    ?assertMatch([], generate_entry(ActJ)),
    ?assertMatch(ok, file:delete(Path ++ "test_emptyactive_file.cdb")).

-endif.
