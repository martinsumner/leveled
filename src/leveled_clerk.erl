%% Controlling asynchronous work in leveleddb to manage compaction within a
%% level and cleaning out of old files across a level


-module(leveled_clerk).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        clerk_new/0,
        clerk_prompt/2,
        code_change/3,
        perform_merge/4]).      

-include_lib("eunit/include/eunit.hrl").

-record(state, {owner :: pid()}).

%%%============================================================================
%%% API
%%%============================================================================

clerk_new() ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    ok = gen_server:call(Pid, register, infinity),
    {ok, Pid}.
    

clerk_prompt(Pid, penciller) ->
    gen_server:cast(Pid, penciller_prompt, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call(register, From, State) ->
    {noreply, State#state{owner=From}}.

handle_cast({penciller_prompt, From}, State) ->
    case leveled_penciller:pcl_workforclerk(State#state.owner) of
        none ->
            io:format("Work prompted but none needed~n"),
            {noreply, State};
        WI ->
            {NewManifest, FilesToDelete} = merge(WI),
            UpdWI = WI#penciller_work{new_manifest=NewManifest,
                                        unreferenced_files=FilesToDelete},
            leveled_penciller:pcl_requestmanifestchange(From, UpdWI),
            {noreply, State}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

merge(WI) ->
    SrcLevel = WI#penciller_work.src_level,
    {Selection, UpdMFest1} = select_filetomerge(SrcLevel,
                                                WI#penciller_work.manifest),
    {{StartKey, EndKey}, SrcFile} = Selection,
    SrcFilename = leveled_sft:sft_getfilename(SrcFile),
    SinkFiles = get_item(SrcLevel + 1, UpdMFest1, []),
    SplitLists = lists:splitwith(fun(Ref) ->
                                        case {Ref#manifest_entry.start_key,
                                                Ref#manifest_entry.end_key} of
                                            {_, EK} when StartKey > EK ->
                                                false;
                                            {SK, _} when EndKey < SK ->
                                                false;
                                            _ ->
                                                true
                                        end end,
                                        SinkFiles),
    {Candidates, Others} = SplitLists,
    
    %% TODO:
    %% Need to work out if this is the top level
    %% And then tell merge process to create files at the top level
    %% Which will include the reaping of expired tombstones
                                        
    io:format("Merge from level ~w to merge into ~w files below",
                [SrcLevel, length(Candidates)]),
    
    MergedFiles = case length(Candidates) of
        0 ->
            %% If no overlapping candiates, manifest change only required
            %%
            %% TODO: need to think still about simply renaming when at 
            %% lower level
            [SrcFile];
        _ ->
            perform_merge({SrcFile, SrcFilename},
                            Candidates,
                            SrcLevel,
                            {WI#penciller_work.ledger_filepath,
                                WI#penciller_work.next_sqn})
    end,
    
    NewLevel = lists:sort(lists:append(MergedFiles, Others)),
    UpdMFest2 = lists:keyreplace(SrcLevel + 1,
                                    1,
                                    UpdMFest1,
                                    {SrcLevel, NewLevel}),
    
    {ok, Handle} = file:open(WI#penciller_work.manifest_file,
                                [binary, raw, write]),
    ok = file:write(Handle, term_to_binary(UpdMFest2)),
    ok = file:close(Handle),
    {UpdMFest2, Candidates}.
        
            
%% An algorithm for discovering which files to merge ....
%% We can find the most optimal file:
%% - The one with the most overlapping data below?
%% - The one that overlaps with the fewest files below?
%% - The smallest file?
%% We could try and be fair in some way (merge oldest first)
%% Ultimately, there is alack of certainty that being fair or optimal is
%% genuinely better - ultimately every file has to be compacted.
%%
%% Hence, the initial implementation is to select files to merge at random

select_filetomerge(SrcLevel, Manifest) ->
    {SrcLevel, LevelManifest} = lists:keyfind(SrcLevel, 1, Manifest),
    Selected = lists:nth(random:uniform(length(LevelManifest)),
                            LevelManifest),
    UpdManifest = lists:keyreplace(SrcLevel,
                                    1,
                                    Manifest,
                                    {SrcLevel,
                                        lists:delete(Selected,
                                                        LevelManifest)}),
    {Selected, UpdManifest}.
    
    

%% Assumption is that there is a single SFT from a higher level that needs
%% to be merged into multiple SFTs at a lower level.  This should create an
%% entirely new set of SFTs, and the calling process can then update the
%% manifest.
%%
%% Once the FileToMerge has been emptied, the remainder of the candidate list
%% needs to be placed in a remainder SFT that may be of a sub-optimal (small)
%% size.  This stops the need to perpetually roll over the whole level if the
%% level consists of already full files.  Some smartness may be required when
%% selecting the candidate list so that small files just outside the candidate
%% list be included to avoid a proliferation of small files.
%%
%% FileToMerge should be a tuple of {FileName, Pid} where the Pid is the Pid of
%% the gen_server leveled_sft process representing the file.
%%
%% CandidateList should be a list of {StartKey, EndKey, Pid} tuples
%% representing different gen_server leveled_sft processes, sorted by StartKey.
%%
%% The level is the level which the new files should be created at.

perform_merge(FileToMerge, CandidateList, Level, {Filepath, MSN}) ->
    {Filename, UpperSFTPid} = FileToMerge,
    io:format("Merge to be commenced for FileToMerge=~s with MSN=~w~n",
                    [Filename, MSN]),
    PointerList = lists:map(fun(P) -> {next, P, all} end, CandidateList),
    do_merge([{next, UpperSFTPid, all}],
                    PointerList, Level, {Filepath, MSN}, 0, []).

do_merge([], [], Level, {_Filepath, MSN}, FileCounter, OutList) ->
    io:format("Merge completed with MSN=~w Level=~w and FileCounter=~w~n",
                [MSN, Level, FileCounter]),
    OutList;
do_merge(KL1, KL2, Level, {Filepath, MSN}, FileCounter, OutList) ->
    FileName = lists:flatten(io_lib:format(Filepath ++ "_~w_~w.sft",
                                            [Level, FileCounter])),
    io:format("File to be created as part of MSN=~w Filename=~s~n",
                [MSN, FileName]),
    case leveled_sft:sft_new(FileName, KL1, KL2, Level) of
        {ok, _Pid, {error, Reason}} ->
            io:format("Exiting due to error~w~n", [Reason]);
        {ok, Pid, Reply} ->
            {{KL1Rem, KL2Rem}, SmallestKey, HighestKey} = Reply,
            ExtMan = lists:append(OutList,
                                    [#manifest_entry{start_key=SmallestKey,
                                                        end_key=HighestKey,
                                                        owner=Pid,
                                                        filename=FileName}]),
            do_merge(KL1Rem, KL2Rem, Level, {Filepath, MSN},
                            FileCounter + 1, ExtMan)
    end.


get_item(Index, List, Default) ->
    case lists:keysearch(Index, 1, List) of
        {value, {Index, Value}} ->
            Value;
        false ->
            Default
    end.


%%%============================================================================
%%% Test
%%%============================================================================


generate_randomkeys(Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Count, [], BucketRangeLow, BucketRangeHigh).

generate_randomkeys(0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Count, Acc, BucketLow, BRange) ->
    BNumber = string:right(integer_to_list(BucketLow + random:uniform(BRange)),
                                            4, $0),
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    RandKey = {{o,
                "Bucket" ++ BNumber,
                "Key" ++ KNumber},
                Count + 1,
                {active, infinity}, null},
    generate_randomkeys(Count - 1, [RandKey|Acc], BucketLow, BRange).

choose_pid_toquery([ManEntry|_T], Key) when
                        Key >= ManEntry#manifest_entry.start_key,
                        ManEntry#manifest_entry.end_key >= Key ->
    ManEntry#manifest_entry.owner;
choose_pid_toquery([_H|T], Key) ->
    choose_pid_toquery(T, Key).


find_randomkeys(_FList, 0, _Source) ->
    ok;
find_randomkeys(FList, Count, Source) ->
    K1 = leveled_sft:strip_to_keyonly(lists:nth(random:uniform(length(Source)),
                                                                Source)),
    P1 = choose_pid_toquery(FList, K1),
    FoundKV = leveled_sft:sft_get(P1, K1),
    case FoundKV of
        not_present ->
            io:format("Failed to find ~w in ~w~n", [K1, P1]),
            ?assertMatch(true, false);
        _ ->
            Found = leveled_sft:strip_to_keyonly(FoundKV),
            io:format("success finding ~w in ~w~n", [K1, P1]),
            ?assertMatch(K1, Found)
    end,
    find_randomkeys(FList, Count - 1, Source).


merge_file_test() ->
    KL1_L1 = lists:sort(generate_randomkeys(16000, 0, 1000)),
    {ok, PidL1_1, _} = leveled_sft:sft_new("../test/KL1_L1.sft",
                                            KL1_L1, [], 1),
    KL1_L2 = lists:sort(generate_randomkeys(16000, 0, 250)),
    {ok, PidL2_1, _} = leveled_sft:sft_new("../test/KL1_L2.sft",
                                            KL1_L2, [], 2),
    KL2_L2 = lists:sort(generate_randomkeys(16000, 250, 250)),
    {ok, PidL2_2, _} = leveled_sft:sft_new("../test/KL2_L2.sft",
                                            KL2_L2, [], 2),
    KL3_L2 = lists:sort(generate_randomkeys(16000, 500, 250)),
    {ok, PidL2_3, _} = leveled_sft:sft_new("../test/KL3_L2.sft",
                                            KL3_L2, [], 2),
    KL4_L2 = lists:sort(generate_randomkeys(16000, 750, 250)),
    {ok, PidL2_4, _} = leveled_sft:sft_new("../test/KL4_L2.sft",
                                            KL4_L2, [], 2),
    Result = perform_merge({"../test/KL1_L1.sft", PidL1_1},
                            [PidL2_1, PidL2_2, PidL2_3, PidL2_4],
                            2, {"../test/", 99}),
    lists:foreach(fun(ManEntry) ->
                        {o, B1, K1} = ManEntry#manifest_entry.start_key,
                        {o, B2, K2} = ManEntry#manifest_entry.end_key,
                        io:format("Result of ~s ~s and ~s ~s with Pid ~w~n",
                            [B1, K1, B2, K2, ManEntry#manifest_entry.owner]) end,
                        Result),
    io:format("Finding keys in KL1_L1~n"),
    ok = find_randomkeys(Result, 50, KL1_L1),
    io:format("Finding keys in KL1_L2~n"),
    ok = find_randomkeys(Result, 50, KL1_L2),
    io:format("Finding keys in KL2_L2~n"),
    ok = find_randomkeys(Result, 50, KL2_L2),
    io:format("Finding keys in KL3_L2~n"),
    ok = find_randomkeys(Result, 50, KL3_L2),
    io:format("Finding keys in KL4_L2~n"),
    ok = find_randomkeys(Result, 50, KL4_L2),
    leveled_sft:sft_clear(PidL1_1),
    leveled_sft:sft_clear(PidL2_1),
    leveled_sft:sft_clear(PidL2_2),
    leveled_sft:sft_clear(PidL2_3),
    leveled_sft:sft_clear(PidL2_4),
    lists:foreach(fun(ManEntry) ->
                    leveled_sft:sft_clear(ManEntry#manifest_entry.owner) end,
                    Result).


select_merge_file_test() ->
    L0 = [{{o, "B1", "K1"}, {o, "B3", "K3"}, dummy_pid}],
    L1 = [{{o, "B1", "K1"}, {o, "B2", "K2"}, dummy_pid},
            {{o, "B2", "K3"}, {o, "B4", "K4"}, dummy_pid}],
    Manifest = [{0, L0}, {1, L1}],
    {FileRef, NewManifest} = select_filetomerge(0, Manifest),
    ?assertMatch(FileRef, {{o, "B1", "K1"}, {o, "B3", "K3"}, dummy_pid}),
    ?assertMatch(NewManifest, [{0, []}, {1, L1}]).