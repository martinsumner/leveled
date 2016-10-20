%% -------- PENCILLER's CLERK ---------
%%
%% The Penciller's clerk is responsible for compaction work within the Ledger.
%%
%% The Clerk will periodically poll the Penciller to see if there is work for
%% it to complete, except if the Clerk has informed the Penciller that it has
%% readied a manifest change to be committed - in which case it will wait to
%% be called by the Penciller.
%%
%% -------- COMMITTING MANIFEST CHANGES ---------
%%
%% Once the Penciller has taken a manifest change, the SFT file owners which no
%% longer form part of the manifest will be marked for delete.  By marking for
%% deletion, the owners will poll to confirm when it is safe for them to be
%% deleted.
%%
%% It is imperative that the file is not marked for deletion until it is
%% certain that the manifest change has been committed.  Some uncollected
%% garbage is considered acceptable.
%%
%% The process of committing a manifest change is as follows:
%%
%% A - The Clerk completes a merge, and casts a prompt to the Penciller with
%% a work item describing the change
%%
%% B - The Penciller commits the change to disk, and then calls the Clerk to
%% confirm the manifest change
%%
%% C - The Clerk replies immediately to acknowledge this call, then marks the
%% removed files for deletion
%%
%% Shutdown < A/B - If the Penciller starts the shutdown process before the
%% merge is complete, in the shutdown the Penciller will call a request for the
%% manifest change which will pick up the pending change.  It will then confirm
%% the change, and now the Clerk will mark the files for delete before it
%% replies to the Penciller so it can complete the shutdown process (which will
%% prompt erasing of the removed files).
%%
%% The clerk will not request work on timeout if the committing of a manifest
%5 change is pending confirmation.
%%
%% -------- TIMEOUTS ---------
%%
%% The Penciller may prompt the Clerk to callback soon (i.e. reduce the
%% Timeout) if it has urgent work ready (i.e. it has written a L0 file).
%%
%% There will also be a natural quick timeout once the committing of a manifest
%% change has occurred.
%% 

-module(leveled_pclerk).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        clerk_new/1,
        clerk_prompt/1,
        clerk_manifestchange/3,
        code_change/3]).      

-include_lib("eunit/include/eunit.hrl").

-define(MAX_TIMEOUT, 2000).
-define(MIN_TIMEOUT, 200).

-record(state, {owner :: pid(),
                change_pending=false :: boolean(),
                work_item :: #penciller_work{}|null}).

%%%============================================================================
%%% API
%%%============================================================================

clerk_new(Owner) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    ok = gen_server:call(Pid, {register, Owner}, infinity),
    {ok, Pid}.

clerk_manifestchange(Pid, Action, Closing) ->
    gen_server:call(Pid, {manifest_change, Action, Closing}, infinity).

clerk_prompt(Pid) ->
    gen_server:cast(Pid, prompt).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register, Owner}, _From, State) ->
    {reply, ok, State#state{owner=Owner}, ?MIN_TIMEOUT};
handle_call({manifest_change, return, true}, _From, State) ->
    io:format("Request for manifest change from clerk on closing~n"),
    case State#state.change_pending of
        true ->
            WI = State#state.work_item,
            {reply, {ok, WI}, State};
        false ->
            {reply, no_change, State}
    end;
handle_call({manifest_change, confirm, Closing}, From, State) ->
    case Closing of
        true ->
            io:format("Confirmation of manifest change on closing~n"),
            WI = State#state.work_item,
            ok = mark_for_delete(WI#penciller_work.unreferenced_files,
                                           State#state.owner),
            {stop, normal, ok, State};
        false ->
            io:format("Prompted confirmation of manifest change~n"),
            gen_server:reply(From, ok),
            WI = State#state.work_item,
            mark_for_delete(WI#penciller_work.unreferenced_files,
                            State#state.owner),
            {noreply,
                State#state{work_item=null, change_pending=false},
                ?MIN_TIMEOUT}
    end.

handle_cast(prompt, State) ->
    {noreply, State, ?MIN_TIMEOUT}.

handle_info(timeout, State=#state{change_pending=Pnd}) when Pnd == false ->
    case requestandhandle_work(State) of
        {false, Timeout} ->
            {noreply, State, Timeout};
        {true, WI} ->
            % No timeout now as will wait for call to return manifest
            % change
            {noreply,
                State#state{change_pending=true, work_item=WI}}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

requestandhandle_work(State) ->
    case leveled_penciller:pcl_workforclerk(State#state.owner) of
        none ->
            io:format("Work prompted but none needed~n"),
            {false, ?MAX_TIMEOUT};
        WI ->
            {NewManifest, FilesToDelete} = merge(WI),
            UpdWI = WI#penciller_work{new_manifest=NewManifest,
                                        unreferenced_files=FilesToDelete},
            io:format("Clerk prompting Penciller regarding manifest change~n"),
            ok = leveled_penciller:pcl_promptmanifestchange(State#state.owner,
                                                            UpdWI),
            {true, UpdWI}
    end.    


merge(WI) ->
    SrcLevel = WI#penciller_work.src_level,
    {SrcF, UpdMFest1} = select_filetomerge(SrcLevel,
                                                WI#penciller_work.manifest),
    SinkFiles = get_item(SrcLevel + 1, UpdMFest1, []),
    {Candidates, Others} = check_for_merge_candidates(SrcF, SinkFiles),
    %% TODO:
    %% Need to work out if this is the top level
    %% And then tell merge process to create files at the top level
    %% Which will include the reaping of expired tombstones
    io:format("Merge from level ~w to merge into ~w files below~n",
                [SrcLevel, length(Candidates)]),
     
    MergedFiles = case length(Candidates) of
        0 ->
            %% If no overlapping candiates, manifest change only required
            %%
            %% TODO: need to think still about simply renaming when at 
            %% lower level
            io:format("File ~s to simply switch levels to level ~w~n",
                        [SrcF#manifest_entry.filename, SrcLevel + 1]),
            [SrcF];
        _ ->
            perform_merge({SrcF#manifest_entry.owner,
                            SrcF#manifest_entry.filename},
                            Candidates,
                            {SrcLevel, WI#penciller_work.target_is_basement},
                            {WI#penciller_work.ledger_filepath,
                                WI#penciller_work.next_sqn})
    end,  
    NewLevel = lists:sort(lists:append(MergedFiles, Others)),
    UpdMFest2 = lists:keystore(SrcLevel + 1,
                                1,
                                UpdMFest1,
                                {SrcLevel + 1, NewLevel}),
    
    ok = filelib:ensure_dir(WI#penciller_work.manifest_file),
    {ok, Handle} = file:open(WI#penciller_work.manifest_file,
                                [binary, raw, write]),
    ok = file:write(Handle, term_to_binary(UpdMFest2)),
    ok = file:close(Handle),
    case lists:member(SrcF, MergedFiles) of
        true ->
            {UpdMFest2, Candidates};
        false ->
            %% Can rub out src file as it is not part of output
            {UpdMFest2, Candidates ++ [SrcF]}
    end.
    

mark_for_delete([], _Penciller) ->
    ok;
mark_for_delete([Head|Tail], Penciller) ->
    leveled_sft:sft_setfordelete(Head#manifest_entry.owner, Penciller),
    mark_for_delete(Tail, Penciller).
    

check_for_merge_candidates(SrcF, SinkFiles) ->
    lists:partition(fun(Ref) ->
                        case {Ref#manifest_entry.start_key,
                            Ref#manifest_entry.end_key} of
                                {_, EK} when SrcF#manifest_entry.start_key > EK ->
                                    false;
                                {SK, _} when SrcF#manifest_entry.end_key < SK ->
                                    false;
                                _ ->
                                    true
                        end end,
                    SinkFiles).
    
            
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

perform_merge({SrcPid, SrcFN}, CandidateList, LevelInfo, {Filepath, MSN}) ->
    io:format("Merge to be commenced for FileToMerge=~s with MSN=~w~n",
                    [SrcFN, MSN]),
    PointerList = lists:map(fun(P) ->
                                {next, P#manifest_entry.owner, all} end,
                            CandidateList),
    do_merge([{next, SrcPid, all}],
                PointerList,
                LevelInfo,
                {Filepath, MSN},
                0,
                []).

do_merge([], [], {SrcLevel, _IsB}, {_Filepath, MSN}, FileCounter, OutList) ->
    io:format("Merge completed with MSN=~w Level=~w and FileCounter=~w~n",
                [MSN, SrcLevel, FileCounter]),
    OutList;
do_merge(KL1, KL2, {SrcLevel, IsB}, {Filepath, MSN}, FileCounter, OutList) ->
    FileName = lists:flatten(io_lib:format(Filepath ++ "_~w_~w.sft",
                                            [SrcLevel + 1, FileCounter])),
    io:format("File to be created as part of MSN=~w Filename=~s~n",
                [MSN, FileName]),
    % Attempt to trace intermittent eaccess failures
    false = filelib:is_file(FileName), 
    TS1 = os:timestamp(),
    {ok, Pid, Reply} = leveled_sft:sft_new(FileName, KL1, KL2, SrcLevel + 1),
    {{KL1Rem, KL2Rem}, SmallestKey, HighestKey} = Reply,
    ExtMan = lists:append(OutList,
                            [#manifest_entry{start_key=SmallestKey,
                                                end_key=HighestKey,
                                                owner=Pid,
                                                filename=FileName}]),
    MTime = timer:now_diff(os:timestamp(), TS1),
    io:format("File creation took ~w microseconds ~n", [MTime]),
    do_merge(KL1Rem, KL2Rem,
                {SrcLevel, IsB}, {Filepath, MSN},
                FileCounter + 1, ExtMan).


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

-ifdef(TEST).

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
                {Count + 1,
                {active, infinity}, null}},
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
    KV1 = lists:nth(random:uniform(length(Source)), Source),
    K1 = leveled_codec:strip_to_keyonly(KV1),
    P1 = choose_pid_toquery(FList, K1),
    FoundKV = leveled_sft:sft_get(P1, K1),
    Found = leveled_codec:strip_to_keyonly(FoundKV),
    io:format("success finding ~w in ~w~n", [K1, P1]),
    ?assertMatch(K1, Found),
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
    Result = perform_merge({PidL1_1, "../test/KL1_L1.sft"},
                            [#manifest_entry{owner=PidL2_1},
                                #manifest_entry{owner=PidL2_2},
                                #manifest_entry{owner=PidL2_3},
                                #manifest_entry{owner=PidL2_4}],
                            {2, false}, {"../test/", 99}),
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

select_merge_candidates_test() ->
    Sink1 = #manifest_entry{start_key = {o, "Bucket", "Key1"},
                                end_key = {o, "Bucket", "Key20000"}},
    Sink2 = #manifest_entry{start_key = {o, "Bucket", "Key20001"},
                                end_key = {o, "Bucket1", "Key1"}},
    Src1 = #manifest_entry{start_key = {o, "Bucket", "Key40001"},
                                end_key = {o, "Bucket", "Key60000"}},
    {Candidates, Others} = check_for_merge_candidates(Src1, [Sink1, Sink2]),
    ?assertMatch([Sink2], Candidates),
    ?assertMatch([Sink1], Others).


select_merge_file_test() ->
    L0 = [{{o, "B1", "K1"}, {o, "B3", "K3"}, dummy_pid}],
    L1 = [{{o, "B1", "K1"}, {o, "B2", "K2"}, dummy_pid},
            {{o, "B2", "K3"}, {o, "B4", "K4"}, dummy_pid}],
    Manifest = [{0, L0}, {1, L1}],
    {FileRef, NewManifest} = select_filetomerge(0, Manifest),
    ?assertMatch(FileRef, {{o, "B1", "K1"}, {o, "B3", "K3"}, dummy_pid}),
    ?assertMatch(NewManifest, [{0, []}, {1, L1}]).

-endif.