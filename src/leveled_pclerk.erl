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
        clerk_close/1
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(MAX_TIMEOUT, 1000).
-define(MIN_TIMEOUT, 200).

-record(state, {owner :: pid(),
                root_path :: string()}).

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

clerk_close(Pid) ->
    gen_server:cast(Pid, close).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({load, Owner, RootPath}, _From, State) ->
    {reply, ok, State#state{owner=Owner, root_path=RootPath}, ?MIN_TIMEOUT}.

handle_cast(prompt, State) ->
    handle_info(timeout, State);
handle_cast(close, State) ->
    {stop, normal, State}.    

handle_info(timeout, State) ->
    case requestandhandle_work(State) of
        false ->
            {noreply, State, ?MAX_TIMEOUT};
        true ->
            % No timeout now as will wait for call to return manifest
            % change
            {noreply, State, ?MIN_TIMEOUT}
    end.


terminate(Reason, _State) ->
    leveled_log:log("PC005", [self(), Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

requestandhandle_work(State) ->
    case leveled_penciller:pcl_workforclerk(State#state.owner) of
        none ->
            leveled_log:log("PC006", []),
            false;
        {SrcLevel, Manifest} ->
            {UpdManifest, EntriesToDelete} = merge(SrcLevel,
                                                    Manifest,
                                                    State#state.root_path),
            leveled_log:log("PC007", []),
            ok = leveled_penciller:pcl_manifestchange(State#state.owner,
                                                            UpdManifest),
            ok = leveled_manifest:save_manifest(UpdManifest,
                                                    State#state.root_path),
            ok = notify_deletions(EntriesToDelete, State#state.owner),
            true
    end.    


merge(SrcLevel, Manifest, RootPath) ->
    Src = leveled_manifest:mergefile_selector(Manifest, SrcLevel),
    NewSQN = leveled_manifest:get_manifest_sqn(Manifest) + 1,
    SinkList = leveled_manifest:merge_lookup(Manifest,
                                                SrcLevel + 1,
                                                Src#manifest_entry.start_key,
                                                Src#manifest_entry.end_key),
    Candidates = length(SinkList),
    leveled_log:log("PC008", [SrcLevel, Candidates]),
    case Candidates of
        0 ->
            %% If no overlapping candiates, manifest change only required
            %%
            %% TODO: need to think still about simply renaming when at 
            %% lower level
            leveled_log:log("PC009",
                                [Src#manifest_entry.filename, SrcLevel + 1]),
            Man0 = leveled_manifest:remove_manifest_entry(Manifest,
                                                            NewSQN,
                                                            SrcLevel,
                                                            Src),
            Man1 = leveled_manifest:insert_manifest_entry(Man0,
                                                            NewSQN,
                                                            SrcLevel + 1,
                                                            Src),
            {Man1, []};
        _ ->
            FilePath = leveled_penciller:filepath(RootPath,
                                                    NewSQN,
                                                    new_merge_files),
            perform_merge(Manifest, Src, SinkList, SrcLevel, FilePath, NewSQN)
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
    SrcList = [{next, Src#manifest_entry.owner, all}],
    SinkPointerList = leveled_manifest:pointer_convert(Manifest, SinkList),
    MaxSQN = leveled_sst:sst_getmaxsequencenumber(Src#manifest_entry.owner),
    SinkLevel = SrcLevel + 1,
    SinkBasement = leveled_manifest:is_basement(Manifest, SinkLevel),
    Man0 = do_merge(SrcList, SinkPointerList,
                        SinkLevel, SinkBasement,
                        RootPath, NewSQN, MaxSQN,
                        0, Manifest),
    RemoveFun =
        fun(Entry, AccMan) ->
            leveled_manifest:remove_manifest_entry(AccMan,
                                                    NewSQN,
                                                    SinkLevel,
                                                    Entry)
        end,
    Man1 = lists:foldl(RemoveFun, Man0, SinkList),
    leveled_manifest:remove_manifest_entry(Man1, NewSQN, SrcLevel, Src).

do_merge([], [], SinkLevel, _SinkB, _RP, NewSQN, _MaxSQN, Counter, Man0) ->
    leveled_log:log("PC011", [NewSQN, SinkLevel, Counter]),
    Man0;
do_merge(KL1, KL2, SinkLevel, SinkB, RP, NewSQN, MaxSQN, Counter, Man0) ->
    FileName = lists:flatten(io_lib:format(RP ++ "_~w_~w.sst",
                                            [SinkLevel, Counter])),
    leveled_log:log("PC012", [NewSQN, FileName]),
    TS1 = os:timestamp(),
    case leveled_sst:sst_new(FileName, KL1, KL2, SinkB, SinkLevel, MaxSQN) of
        empty ->
            leveled_log:log("PC013", [FileName]),
            Man0;                        
        {ok, Pid, Reply} ->
            {{KL1Rem, KL2Rem}, SmallestKey, HighestKey} = Reply,
                Entry = #manifest_entry{start_key=SmallestKey,
                                            end_key=HighestKey,
                                            owner=Pid,
                                            filename=FileName},
                Man1 = leveled_manifest:insert_manifest_entry(Man0,
                                                                NewSQN,
                                                                SinkLevel,
                                                                Entry),
                leveled_log:log_timer("PC015", [], TS1),
                do_merge(KL1Rem, KL2Rem,
                            SinkLevel, SinkB,
                            RP, NewSQN, MaxSQN,
                            Counter + 1, Man1)
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
    K = {o, "Bucket" ++ BNumber, "Key" ++ KNumber},
    RandKey = {K, {Count + 1,
                    {active, infinity},
                    leveled_codec:magic_hash(K),
                    null}},
    generate_randomkeys(Count - 1, [RandKey|Acc], BucketLow, BRange).


merge_file_test() ->
    KL1_L1 = lists:sort(generate_randomkeys(8000, 0, 1000)),
    {ok, PidL1_1, _} = leveled_sst:sst_new("../test/KL1_L1.sst",
                                            1,
                                            KL1_L1,
                                            undefined),
    KL1_L2 = lists:sort(generate_randomkeys(8000, 0, 250)),
    {ok, PidL2_1, _} = leveled_sst:sst_new("../test/KL1_L2.sst",
                                            2,
                                            KL1_L2,
                                            undefined),
    KL2_L2 = lists:sort(generate_randomkeys(8000, 250, 250)),
    {ok, PidL2_2, _} = leveled_sst:sst_new("../test/KL2_L2.sst",
                                            2,
                                            KL2_L2,
                                            undefined),
    KL3_L2 = lists:sort(generate_randomkeys(8000, 500, 250)),
    {ok, PidL2_3, _} = leveled_sst:sst_new("../test/KL3_L2.sst",
                                            2,
                                            KL3_L2,
                                            undefined),
    KL4_L2 = lists:sort(generate_randomkeys(8000, 750, 250)),
    {ok, PidL2_4, _} = leveled_sst:sst_new("../test/KL4_L2.sst",
                                            2,
                                            KL4_L2,
                                            undefined),
    E1 = #manifest_entry{owner = PidL1_1, filename = "../test/KL1_L1.sst"},
    E2 = #manifest_entry{owner = PidL2_1, filename = "../test/KL1_L2.sst"},
    E3 = #manifest_entry{owner = PidL2_2, filename = "../test/KL2_L2.sst"},
    E4 = #manifest_entry{owner = PidL2_3, filename = "../test/KL3_L2.sst"},
    E5 = #manifest_entry{owner = PidL2_4, filename = "../test/KL4_L2.sst"},
    
    Man0 = leveled_manifest:new_manifest(),
    Man1 = leveled_manifest:insert_manifest_entry(Man0, 1, 2, E1),
    Man2 = leveled_manifest:insert_manifest_entry(Man1, 1, 2, E1),
    Man3 = leveled_manifest:insert_manifest_entry(Man2, 1, 2, E1),
    Man4 = leveled_manifest:insert_manifest_entry(Man3, 1, 2, E1),
    Man5 = leveled_manifest:insert_manifest_entry(Man4, 2, 1, E1),
    
    Man6 = perform_merge(Man5, E1, [E2, E3, E4, E5], 1, "../test", 3),
    
    ?assertMatch(3, leveled_manifest:get_manifest_sqn(Man6)).

coverage_cheat_test() ->
    {ok, _State1} = code_change(null, #state{}, null).

-endif.
