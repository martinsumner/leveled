%% -------- PENCILLER's CLERK ---------
%%
%% The Penciller's clerk is responsible for compaction work within the Ledger.
%%
%% The Clerk will periodically poll the Penciller to check there is no work
%% at level zero pending completion, and if not the Clerk will examine the
%% manifest to see if work is necessary.
%%
%% -------- COMMITTING MANIFEST CHANGES ---------
%%
%% Once the Penciller has taken a manifest change, the SST file owners which no
%% longer form part of the manifest will be marked for delete.  By marking for
%% deletion, the owners will poll to confirm when it is safe for them to be
%% deleted.
%%
%% It is imperative that the file is not marked for deletion until it is
%% certain that the manifest change has been committed.  Some uncollected
%% garbage is considered acceptable.
%%


-module(leveled_pclerk).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
        ]).

-export([
        clerk_new/2,
        clerk_prompt/1,
        clerk_push/2,
        clerk_close/1,
        clerk_promptdeletions/2
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(MAX_TIMEOUT, 2000).
-define(MIN_TIMEOUT, 200).

-record(state, {owner :: pid() | undefined,
                root_path :: string() | undefined,
                pending_deletions = dict:new() % OTP 16 does not like type
                }).

%%%============================================================================
%%% API
%%%============================================================================

clerk_new(Owner, Manifest) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    ok = gen_server:call(Pid, {load, Owner, Manifest}, infinity),
    leveled_log:log("PC001", [Pid, Owner]),
    {ok, Pid}.

clerk_prompt(Pid) ->
    gen_server:cast(Pid, prompt).

clerk_promptdeletions(Pid, ManifestSQN) ->
    gen_server:cast(Pid, {prompt_deletions, ManifestSQN}).

clerk_push(Pid, Work) ->
    gen_server:cast(Pid, {push_work, Work}).

clerk_close(Pid) ->
    gen_server:call(Pid, close, 20000).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({load, Owner, RootPath}, _From, State) ->
    {reply, ok, State#state{owner=Owner, root_path=RootPath}, ?MIN_TIMEOUT};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(prompt, State) ->
    handle_info(timeout, State);
handle_cast({push_work, Work}, State) ->
    {ManifestSQN, Deletions} = handle_work(Work, State),
    PDs = dict:store(ManifestSQN, Deletions, State#state.pending_deletions),
    leveled_log:log("PC022", [ManifestSQN]),
    {noreply, State#state{pending_deletions = PDs}, ?MAX_TIMEOUT};
handle_cast({prompt_deletions, ManifestSQN}, State) ->
    {Deletions, UpdD} = return_deletions(ManifestSQN,
                                            State#state.pending_deletions),
    ok = notify_deletions(Deletions, State#state.owner),
    {noreply, State#state{pending_deletions = UpdD}, ?MIN_TIMEOUT}.

handle_info(timeout, State) ->
    request_work(State),
    {noreply, State, ?MAX_TIMEOUT}.

terminate(Reason, _State) ->
    leveled_log:log("PC005", [self(), Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

request_work(State) ->
    ok = leveled_penciller:pcl_workforclerk(State#state.owner).

handle_work({SrcLevel, Manifest}, State) ->
    {UpdManifest, EntriesToDelete} = merge(SrcLevel,
                                            Manifest,
                                            State#state.root_path),
    leveled_log:log("PC007", []),
    SWMC = os:timestamp(),
    ok = leveled_penciller:pcl_manifestchange(State#state.owner,
                                                    UpdManifest),
    leveled_log:log_timer("PC017", [], SWMC),
    SWSM = os:timestamp(),
    ok = leveled_pmanifest:save_manifest(UpdManifest,
                                            State#state.root_path),
    leveled_log:log_timer("PC018", [], SWSM),
    {leveled_pmanifest:get_manifest_sqn(UpdManifest), EntriesToDelete}.

merge(SrcLevel, Manifest, RootPath) ->
    Src = leveled_pmanifest:mergefile_selector(Manifest, SrcLevel),
    NewSQN = leveled_pmanifest:get_manifest_sqn(Manifest) + 1,
    SinkList = leveled_pmanifest:merge_lookup(Manifest,
                                                SrcLevel + 1,
                                                Src#manifest_entry.start_key,
                                                Src#manifest_entry.end_key),
    Candidates = length(SinkList),
    leveled_log:log("PC008", [SrcLevel, Candidates]),
    case Candidates of
        0 ->
            leveled_log:log("PC009",
                                [Src#manifest_entry.filename, SrcLevel + 1]),
            Man0 = leveled_pmanifest:switch_manifest_entry(Manifest,
                                                            NewSQN,
                                                            SrcLevel,
                                                            Src),
            {Man0, []};
        _ ->
            SST_RP = leveled_penciller:sst_rootpath(RootPath),
            perform_merge(Manifest, Src, SinkList, SrcLevel, SST_RP, NewSQN)
    end.

notify_deletions([], _Penciller) ->
    ok;
notify_deletions([Head|Tail], Penciller) ->
    ok = leveled_sst:sst_setfordelete(Head#manifest_entry.owner, Penciller),
    notify_deletions(Tail, Penciller).
        

%% Assumption is that there is a single SST from a higher level that needs
%% to be merged into multiple SSTs at a lower level.  
%%
%% SrcLevel is the level of the src sst file, the sink should be srcLevel + 1

perform_merge(Manifest, Src, SinkList, SrcLevel, RootPath, NewSQN) ->
    leveled_log:log("PC010", [Src#manifest_entry.filename, NewSQN]),
    SrcList = [{next, Src, all}],
    MaxSQN = leveled_sst:sst_getmaxsequencenumber(Src#manifest_entry.owner),
    SinkLevel = SrcLevel + 1,
    SinkBasement = leveled_pmanifest:is_basement(Manifest, SinkLevel),
    Additions = do_merge(SrcList, SinkList,
                            SinkLevel, SinkBasement,
                            RootPath, NewSQN, MaxSQN,
                            []),
    RevertPointerFun =
        fun({next, ME, _SK}) ->
            ME
        end,
    SinkManifestList = lists:map(RevertPointerFun, SinkList),
    Man0 = leveled_pmanifest:replace_manifest_entry(Manifest,
                                                    NewSQN,
                                                    SinkLevel,
                                                    SinkManifestList,
                                                    Additions),
    Man2 = leveled_pmanifest:remove_manifest_entry(Man0,
                                                    NewSQN,
                                                    SrcLevel,
                                                    Src),
    {Man2, [Src|SinkManifestList]}.

do_merge([], [], SinkLevel, _SinkB, _RP, NewSQN, _MaxSQN, Additions) ->
    leveled_log:log("PC011", [NewSQN, SinkLevel, length(Additions)]),
    Additions;
do_merge(KL1, KL2, SinkLevel, SinkB, RP, NewSQN, MaxSQN, Additions) ->
    FileName = leveled_penciller:sst_filename(NewSQN,
                                                SinkLevel,
                                                length(Additions)),
    leveled_log:log("PC012", [NewSQN, FileName, SinkB]),
    TS1 = os:timestamp(),
    case leveled_sst:sst_new(RP, FileName,
                                KL1, KL2, SinkB, SinkLevel, MaxSQN) of
        empty ->
            leveled_log:log("PC013", [FileName]),
            do_merge([], [],
                        SinkLevel, SinkB,
                        RP, NewSQN, MaxSQN,
                        Additions);                        
        {ok, Pid, Reply} ->
            {{KL1Rem, KL2Rem}, SmallestKey, HighestKey} = Reply,
                Entry = #manifest_entry{start_key=SmallestKey,
                                            end_key=HighestKey,
                                            owner=Pid,
                                            filename=FileName},
                leveled_log:log_timer("PC015", [], TS1),
                do_merge(KL1Rem, KL2Rem,
                            SinkLevel, SinkB,
                            RP, NewSQN, MaxSQN,
                            Additions ++ [Entry])
    end.


return_deletions(ManifestSQN, PendingDeletionD) ->
    % The returning of deletions had been seperated out as a failure to fetch
    % here had caased crashes of the clerk.  The root cause of the failure to
    % fetch was the same clerk being asked to do the same work twice - and this
    % should be blocked now by the ongoing_work boolean in the Penciller
    % LoopData
    %
    % So this is now allowed to crash again
    PendingDeletions = dict:fetch(ManifestSQN, PendingDeletionD),
    leveled_log:log("PC021", [ManifestSQN]),
    {PendingDeletions, dict:erase(ManifestSQN, PendingDeletionD)}.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_randomkeys(Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Count, [], BucketRangeLow, BucketRangeHigh).

generate_randomkeys(0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Count, Acc, BucketLow, BRange) ->
    BNumber = string:right(integer_to_list(BucketLow + leveled_rand:uniform(BRange)),
                                            4, $0),
    KNumber = string:right(integer_to_list(leveled_rand:uniform(1000)), 4, $0),
    K = {o, "Bucket" ++ BNumber, "Key" ++ KNumber},
    RandKey = {K, {Count + 1,
                    {active, infinity},
                    leveled_codec:magic_hash(K),
                    null}},
    generate_randomkeys(Count - 1, [RandKey|Acc], BucketLow, BRange).


merge_file_test() ->
    KL1_L1 = lists:sort(generate_randomkeys(8000, 0, 1000)),
    {ok, PidL1_1, _} = leveled_sst:sst_new("../test/",
                                            "KL1_L1.sst",
                                            1,
                                            KL1_L1,
                                            999999),
    KL1_L2 = lists:sort(generate_randomkeys(8000, 0, 250)),
    {ok, PidL2_1, _} = leveled_sst:sst_new("../test/",
                                            "KL1_L2.sst",
                                            2,
                                            KL1_L2,
                                            999999),
    KL2_L2 = lists:sort(generate_randomkeys(8000, 250, 250)),
    {ok, PidL2_2, _} = leveled_sst:sst_new("../test/",
                                            "KL2_L2.sst",
                                            2,
                                            KL2_L2,
                                            999999),
    KL3_L2 = lists:sort(generate_randomkeys(8000, 500, 250)),
    {ok, PidL2_3, _} = leveled_sst:sst_new("../test/",
                                            "KL3_L2.sst",
                                            2,
                                            KL3_L2,
                                            999999),
    KL4_L2 = lists:sort(generate_randomkeys(8000, 750, 250)),
    {ok, PidL2_4, _} = leveled_sst:sst_new("../test/",
                                            "KL4_L2.sst",
                                            2,
                                            KL4_L2,
                                            999999),
    
    E1 = #manifest_entry{owner = PidL1_1,
                            filename = "./KL1_L1.sst",
                            end_key = lists:last(KL1_L1),
                            start_key = lists:nth(1, KL1_L1)},
    E2 = #manifest_entry{owner = PidL2_1,
                            filename = "./KL1_L2.sst",
                            end_key = lists:last(KL1_L2),
                            start_key = lists:nth(1, KL1_L2)},
    E3 = #manifest_entry{owner = PidL2_2,
                            filename = "./KL2_L2.sst",
                            end_key = lists:last(KL2_L2),
                            start_key = lists:nth(1, KL2_L2)},
    E4 = #manifest_entry{owner = PidL2_3,
                            filename = "./KL3_L2.sst",
                            end_key = lists:last(KL3_L2),
                            start_key = lists:nth(1, KL3_L2)},
    E5 = #manifest_entry{owner = PidL2_4,
                            filename = "./KL4_L2.sst",
                            end_key = lists:last(KL4_L2),
                            start_key = lists:nth(1, KL4_L2)},
    
    Man0 = leveled_pmanifest:new_manifest(),
    Man1 = leveled_pmanifest:insert_manifest_entry(Man0, 1, 2, E2),
    Man2 = leveled_pmanifest:insert_manifest_entry(Man1, 1, 2, E3),
    Man3 = leveled_pmanifest:insert_manifest_entry(Man2, 1, 2, E4),
    Man4 = leveled_pmanifest:insert_manifest_entry(Man3, 1, 2, E5),
    Man5 = leveled_pmanifest:insert_manifest_entry(Man4, 2, 1, E1),
    
    PointerList = lists:map(fun(ME) -> {next, ME, all} end,
                            [E2, E3, E4, E5]),
    {Man6, _Dels} = perform_merge(Man5, E1, PointerList, 1, "../test", 3),
    
    ?assertMatch(3, leveled_pmanifest:get_manifest_sqn(Man6)).

coverage_cheat_test() ->
    {ok, _State1} = code_change(null, #state{}, null).

-endif.
