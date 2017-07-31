%% -------- Inker's Clerk ---------
%%
%% The Inker's clerk runs compaction jobs on behalf of the Inker, informing the
%% Inker of any manifest changes when complete.
%%
%% -------- Value Compaction ---------
%%
%% Compaction requires the Inker to have four different types of keys
%% * stnd - A standard key of the form {SQN, stnd, LedgerKey} which maps to a
%% value of {Object, KeyDeltas}
%% * tomb - A tombstone for a LedgerKey {SQN, tomb, LedgerKey}
%% * keyd - An object containing key deltas only of the form
%% {SQN, keyd, LedgerKey} which maps to a value of {KeyDeltas}
%%
%% Each LedgerKey has a Tag, and for each Tag there should be a compaction
%% strategy, which will be set to one of the following:
%% * retain - KeyDeltas must be retained permanently, only values can be
%% compacted (if replaced or not_present in the ledger)
%% * recalc - The full object can be removed through comapction (if replaced or
%% not_present in the ledger), as each object with that tag can have the Key
%% Deltas recreated by passing into an assigned recalc function {LedgerKey,
%% SQN, Object, KeyDeltas, PencillerSnapshot}
%% * recovr - At compaction time this is equivalent to recalc, only KeyDeltas
%% are lost when reloading the Ledger from the Journal, and it is assumed that
%% those deltas will be resolved through external anti-entropy (e.g. read
%% repair or AAE) - or alternatively the risk of loss of persisted data from
%% the ledger is accepted for this data type
%% 
%% During the compaction process for the Journal, the file chosen for
%% compaction is scanned in SQN order, and a FilterFun is passed (which will
%% normally perform a check against a snapshot of the persisted part of the
%% Ledger).  If the given key is of type stnd, and this object is no longer the
%% active object under the LedgerKey, then the object can be compacted out of
%% the journal.  This will lead to either its removal (if the strategy for the
%% Tag is recovr or recalc), or its replacement with a KeyDelta object.
%%
%% Tombstones cannot be reaped through this compaction process.
%%
%% Currently, KeyDeltas are also reaped if the LedgerKey has been updated and
%% the Tag has a recovr strategy.  This may be the case when KeyDeltas are used
%% as a way of directly representing a change, and where anti-entropy can
%% recover from a loss.
%%
%% -------- Removing Compacted Files ---------
%%
%% Once a compaction job is complete, and the manifest change has been
%% committed, the individual journal files will get a deletion prompt.  The
%% Journal processes should copy the file to the waste folder, before erasing
%% themselves.
%%
%% The Inker will have a waste duration setting, and before running compaction
%% should delete all over-age items (using the file modified date) from the
%% waste.
%%
%% -------- Tombstone Reaping ---------
%%
%% Value compaction does not remove tombstones from the database, and so a
%% separate compaction job is required for this.
%%
%% Tombstones can only be reaped for Tags set to recovr or recalc.
%%
%% The tombstone reaping process should select a file to compact, and then
%% take that file and discover the LedgerKeys of all reapable tombstones.
%% The lesger should then be scanned from SQN 0 looking for unreaped objects
%% before the tombstone.  If no ushc objects exist for that tombstone, it can
%% now be reaped as part of the compaction job.
%%
%% Other tombstones cannot be reaped, as otherwise on laoding a ledger an old
%% version of the object may re-emerge.

-module(leveled_iclerk).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        clerk_new/1,
        clerk_compact/7,
        clerk_hashtablecalc/3,
        clerk_stop/1,
        code_change/3]).      

-export([schedule_compaction/3]).

-include_lib("eunit/include/eunit.hrl").

-define(JOURNAL_FILEX, "cdb").
-define(PENDING_FILEX, "pnd").
-define(SAMPLE_SIZE, 200).
-define(BATCH_SIZE, 32).
-define(BATCHES_TO_CHECK, 8).
%% How many consecutive files to compact in one run
-define(MAX_COMPACTION_RUN, 8).
%% Sliding scale to allow preference of longer runs up to maximum
-define(SINGLEFILE_COMPACTION_TARGET, 40.0).
-define(MAXRUN_COMPACTION_TARGET, 70.0).
-define(CRC_SIZE, 4).
-define(DEFAULT_RELOAD_STRATEGY, leveled_codec:inker_reload_strategy([])).
-define(DEFAULT_WASTE_RETENTION_PERIOD, 86400).
-define(INTERVALS_PER_HOUR, 4).

-record(state, {inker :: pid() | undefined,
                max_run_length :: integer() | undefined,
                cdb_options,
                waste_retention_period :: integer() | undefined,
                waste_path :: string() | undefined,
                reload_strategy = ?DEFAULT_RELOAD_STRATEGY :: list()}).

-record(candidate, {low_sqn :: integer() | undefined,
                    filename :: string() | undefined,
                    journal :: pid() | undefined,
                    compaction_perc :: float() | undefined}).


%%%============================================================================
%%% API
%%%============================================================================

clerk_new(InkerClerkOpts) ->
    gen_server:start(?MODULE, [InkerClerkOpts], []).
    
clerk_compact(Pid, Checker, InitiateFun, CloseFun, FilterFun, Inker, TimeO) ->
    gen_server:cast(Pid,
                    {compact,
                    Checker,
                    InitiateFun,
                    CloseFun,
                    FilterFun,
                    Inker,
                    TimeO}).

clerk_hashtablecalc(HashTree, StartPos, CDBpid) ->
    {ok, Clerk} = gen_server:start(?MODULE, [#iclerk_options{}], []),
    gen_server:cast(Clerk, {hashtable_calc, HashTree, StartPos, CDBpid}).

clerk_stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([IClerkOpts]) ->
    ReloadStrategy = IClerkOpts#iclerk_options.reload_strategy,
    CDBopts = IClerkOpts#iclerk_options.cdb_options,
    WP = CDBopts#cdb_options.waste_path,
    WRP = case IClerkOpts#iclerk_options.waste_retention_period of
                undefined ->
                    ?DEFAULT_WASTE_RETENTION_PERIOD;
                WRP0 ->
                    WRP0
            end,
    MRL = case IClerkOpts#iclerk_options.max_run_length of
                undefined ->
                    ?MAX_COMPACTION_RUN;
                MRL0 ->
                    MRL0
            end,
            
    {ok, #state{max_run_length = MRL,
                        inker = IClerkOpts#iclerk_options.inker,
                        cdb_options = CDBopts,
                        reload_strategy = ReloadStrategy,
                        waste_path = WP,
                        waste_retention_period = WRP}}.

handle_call(_Msg, _From, State) ->
    {reply, not_supported, State}.

handle_cast({compact, Checker, InitiateFun, CloseFun, FilterFun, Inker, _TO},
                State) ->
    % Empty the waste folder
    clear_waste(State),
    % Need to fetch manifest at start rather than have it be passed in
    % Don't want to process a queued call waiting on an old manifest
    [_Active|Manifest] = leveled_inker:ink_getmanifest(Inker),
    MaxRunLength = State#state.max_run_length,
    {FilterServer, MaxSQN} = InitiateFun(Checker),
    CDBopts = State#state.cdb_options,
    
    Candidates = scan_all_files(Manifest, FilterFun, FilterServer, MaxSQN),
    BestRun0 = assess_candidates(Candidates, MaxRunLength),
    case score_run(BestRun0, MaxRunLength) of
        Score when Score > 0.0 ->
            BestRun1 = sort_run(BestRun0),
            print_compaction_run(BestRun1, MaxRunLength),
            ManifestSlice = compact_files(BestRun1,
                                            CDBopts,
                                            FilterFun,
                                            FilterServer,
                                            MaxSQN,
                                            State#state.reload_strategy),
            FilesToDelete = lists:map(fun(C) ->
                                            {C#candidate.low_sqn,
                                                C#candidate.filename,
                                                C#candidate.journal,
                                                undefined}
                                            end,
                                        BestRun1),
            leveled_log:log("IC002", [length(FilesToDelete)]),
            case is_process_alive(Inker) of
                true ->
                    update_inker(Inker,
                                    ManifestSlice,
                                    FilesToDelete),
                    ok = CloseFun(FilterServer),
                    {noreply, State}
            end;
        Score ->
            leveled_log:log("IC003", [Score]),
            ok = leveled_inker:ink_compactioncomplete(Inker),
            ok = CloseFun(FilterServer),
            {noreply, State}
    end;
handle_cast({hashtable_calc, HashTree, StartPos, CDBpid}, State) ->
    {IndexList, HashTreeBin} = leveled_cdb:hashtable_calc(HashTree, StartPos),
    ok = leveled_cdb:cdb_returnhashtable(CDBpid, IndexList, HashTreeBin),
    {stop, normal, State};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal, _State) ->
    ok;
terminate(Reason, _State) ->
    leveled_log:log("IC001", [Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% External functions
%%%============================================================================

%% Schedule the next compaction event for this store.  Chooses a random
%% interval, and then a random start time within the first third
%% of the interval.
%%
%% The number of Compaction runs per day can be set.  This doesn't guaranteee
%% those runs, but uses the assumption there will be n runs when scheduling
%% the next one
%%
%% Compaction Hours should be the list of hours during the day (based on local
%% time when compcation can be scheduled to run)
%% e.g. [0, 1, 2, 3, 4, 21, 22, 23]
%% Runs per day is the number of compaction runs per day that should be
%% scheduled - expected to be a small integer, probably 1
%%
%% Current TS should be the outcome of os:timestamp()
%%

schedule_compaction(CompactionHours, RunsPerDay, CurrentTS) ->
    % We chedule the next interval by acting as if we were scheduing all
    % n intervals at random, but then only chose the next one.  After each
    % event is occurred the random process is repeated to determine the next
    % event to schedule i.e. the unused schedule is discarded.
    
    IntervalLength = 60 div ?INTERVALS_PER_HOUR,
    TotalHours = length(CompactionHours),
    
    LocalTime = calendar:now_to_local_time(CurrentTS),
    {{NowY, NowMon, NowD},
        {NowH, NowMin, _NowS}} = LocalTime,
    CurrentInterval = {NowH, NowMin div IntervalLength + 1},
    
    % Randomly select an hour and an interval for each of the runs expected
    % today.
    RandSelect =
        fun(_X) ->
            {lists:nth(leveled_rand:uniform(TotalHours), CompactionHours),
                leveled_rand:uniform(?INTERVALS_PER_HOUR)}
        end,
    RandIntervals = lists:sort(lists:map(RandSelect,
                                            lists:seq(1, RunsPerDay))),
    
    % Pick the next interval from the list.  The intervals before current time
    % are considered as intervals tomorrow, so will only be next if there are
    % no other today
    CheckNotBefore = fun(A) -> A =< CurrentInterval end,
    {TooEarly, MaybeOK} = lists:splitwith(CheckNotBefore, RandIntervals),
    {NextDate, {NextH, NextI}} = 
        case MaybeOK of
            [] ->
                % Use first interval picked tomorrow if none of selected run times
                % are today
                Tmrw = calendar:date_to_gregorian_days(NowY, NowMon, NowD) + 1,
                {calendar:gregorian_days_to_date(Tmrw),
                    lists:nth(1, TooEarly)};
            _ ->
                {{NowY, NowMon, NowD}, lists:nth(1, MaybeOK)}
        end,
    
    % Calculate the offset in seconds to this next interval
    NextS0 = NextI * (IntervalLength * 60)
                - leveled_rand:uniform(IntervalLength * 60),
    NextM = NextS0 div 60,
    NextS = NextS0 rem 60,
    TimeDiff = calendar:time_difference(LocalTime,
                                        {NextDate, {NextH, NextM, NextS}}),
    {Days, {Hours, Mins, Secs}} = TimeDiff,
    Days * 86400 + Hours * 3600 + Mins * 60 + Secs.
    

%%%============================================================================
%%% Internal functions
%%%============================================================================


check_single_file(CDB, FilterFun, FilterServer, MaxSQN, SampleSize, BatchSize) ->
    FN = leveled_cdb:cdb_filename(CDB),
    PositionList = leveled_cdb:cdb_getpositions(CDB, SampleSize),
    KeySizeList = fetch_inbatches(PositionList, BatchSize, CDB, []),
    
    FoldFunForSizeCompare =
        fun(KS, {ActSize, RplSize}) ->
            case KS of
                {{SQN, _Type, PK}, Size} ->
                    Check = FilterFun(FilterServer, PK, SQN),
                    case {Check, SQN > MaxSQN} of
                        {true, _} ->
                            {ActSize + Size - ?CRC_SIZE, RplSize};
                        {false, true} ->
                            {ActSize + Size - ?CRC_SIZE, RplSize};
                        _ ->
                            {ActSize, RplSize + Size - ?CRC_SIZE}
                    end;
                _ ->
                    {ActSize, RplSize}
            end
            end,
            
    R0 = lists:foldl(FoldFunForSizeCompare, {0, 0}, KeySizeList),
    {ActiveSize, ReplacedSize} = R0,
    Score = case ActiveSize + ReplacedSize of
                0 ->
                    100.0;
                _ ->
                    100 * ActiveSize / (ActiveSize + ReplacedSize)
            end,
    leveled_log:log("IC004", [FN, Score]),
    Score.

scan_all_files(Manifest, FilterFun, FilterServer, MaxSQN) ->
    scan_all_files(Manifest, FilterFun, FilterServer, MaxSQN, []).

scan_all_files([], _FilterFun, _FilterServer, _MaxSQN, CandidateList) ->
    CandidateList;
scan_all_files([Entry|Tail], FilterFun, FilterServer, MaxSQN, CandidateList) ->
    {LowSQN, FN, JournalP, _LK} = Entry,
    CpctPerc = check_single_file(JournalP,
                                    FilterFun,
                                    FilterServer,
                                    MaxSQN,
                                    ?SAMPLE_SIZE,
                                    ?BATCH_SIZE),
    scan_all_files(Tail,
                    FilterFun,
                    FilterServer,
                    MaxSQN,
                    CandidateList ++
                        [#candidate{low_sqn = LowSQN,
                                    filename = FN,
                                    journal = JournalP,
                                    compaction_perc = CpctPerc}]).

fetch_inbatches([], _BatchSize, _CDB, CheckedList) ->
    CheckedList;
fetch_inbatches(PositionList, BatchSize, CDB, CheckedList) ->
    {Batch, Tail} = if
                        length(PositionList) >= BatchSize ->
                            lists:split(BatchSize, PositionList);
                        true ->
                            {PositionList, []}
                    end,
    KL_List = leveled_cdb:cdb_directfetch(CDB, Batch, key_size),
    fetch_inbatches(Tail, BatchSize, CDB, CheckedList ++ KL_List).


assess_candidates(AllCandidates, MaxRunLength) when is_integer(MaxRunLength) ->
    % This will take the defaults for other params.
    % Unit tests should pass tuple as params including tested defaults
    assess_candidates(AllCandidates, 
                        {MaxRunLength,
                            ?MAXRUN_COMPACTION_TARGET,
                            ?SINGLEFILE_COMPACTION_TARGET});
assess_candidates(AllCandidates, Params) ->
    NaiveBestRun = assess_candidates(AllCandidates, Params, [], []),
    MaxRunLength = element(1, Params),
    case length(AllCandidates) of
        L when L > MaxRunLength, MaxRunLength > 1 ->
            %% Assess with different offsets from the start
            AssessFold = 
                fun(Counter, BestRun) ->
                    SubList = lists:nthtail(Counter, AllCandidates),
                    assess_candidates(SubList, Params, [], BestRun)
                end,

            lists:foldl(AssessFold, 
                            NaiveBestRun, 
                            lists:seq(1, MaxRunLength - 1));
        _ ->
            NaiveBestRun
    end.

assess_candidates([], _Params, _CurrentRun0, BestAssessment) ->
    BestAssessment;
assess_candidates([HeadC|Tail], Params, CurrentRun0, BestAssessment) ->
    CurrentRun1 = choose_best_assessment(CurrentRun0 ++ [HeadC],
                                            [HeadC],
                                            Params),
    assess_candidates(Tail,
                        Params,
                        CurrentRun1,
                        choose_best_assessment(CurrentRun1,
                                                BestAssessment,
                                                Params)).


choose_best_assessment(RunToAssess, BestRun, Params) ->
    {MaxRunLength, _MR_CT, _SF_CT} = Params,
    case length(RunToAssess) of
        LR1 when LR1 > MaxRunLength ->
            BestRun;
        _ ->
            AssessScore = score_run(RunToAssess, Params),
            BestScore = score_run(BestRun, Params),
            if
                AssessScore > BestScore ->
                    RunToAssess;
                true ->
                    BestRun
            end
    end.    

score_run(Run, MaxRunLength) when is_integer(MaxRunLength) ->
    Params = {MaxRunLength, 
                    ?MAXRUN_COMPACTION_TARGET,
                    ?SINGLEFILE_COMPACTION_TARGET},
    score_run(Run, Params);
score_run([], _Params) ->
    0.0;
score_run(Run, {MaxRunLength, MR_CT, SF_CT}) ->
    TargetIncr = 
        case MaxRunLength of
            1 ->
                0.0;
            MaxRunSize ->
                (MR_CT - SF_CT) / (MaxRunSize - 1)
        end,
    Target = SF_CT +  TargetIncr * (length(Run) - 1),
    RunTotal = lists:foldl(fun(Cand, Acc) ->
                                Acc + Cand#candidate.compaction_perc end,
                            0.0,
                            Run),
    Target - RunTotal / length(Run).


print_compaction_run(BestRun, MaxRunLength) ->
    leveled_log:log("IC005", [length(BestRun),
                                score_run(BestRun, MaxRunLength)]),
    lists:foreach(fun(File) ->
                        leveled_log:log("IC006", [File#candidate.filename])
                        end,
                    BestRun).

sort_run(RunOfFiles) ->
    CompareFun = fun(Cand1, Cand2) ->
                    Cand1#candidate.low_sqn =< Cand2#candidate.low_sqn end,
    lists:sort(CompareFun, RunOfFiles).

update_inker(Inker, ManifestSlice, FilesToDelete) ->
    {ok, ManSQN} = leveled_inker:ink_updatemanifest(Inker,
                                                    ManifestSlice,
                                                    FilesToDelete),
    ok = leveled_inker:ink_compactioncomplete(Inker),
    leveled_log:log("IC007", []),
    lists:foreach(fun({_SQN, _FN, J2D, _LK}) ->
                        leveled_cdb:cdb_deletepending(J2D,
                                                        ManSQN,
                                                        Inker)
                        end,
                    FilesToDelete),
                    ok.

compact_files(BestRun, CDBopts, FilterFun, FilterServer, MaxSQN, RStrategy) ->
    BatchesOfPositions = get_all_positions(BestRun, []),
    compact_files(BatchesOfPositions,
                                CDBopts,
                                null,
                                FilterFun,
                                FilterServer,
                                MaxSQN,
                                RStrategy,
                                []).


compact_files([], _CDBopts, null, _FilterFun, _FilterServer, _MaxSQN,
                            _RStrategy, ManSlice0) ->
    ManSlice0;
compact_files([], _CDBopts, ActiveJournal0, _FilterFun, _FilterServer, _MaxSQN,
                            _RStrategy, ManSlice0) ->
    ManSlice1 = ManSlice0 ++ leveled_imanifest:generate_entry(ActiveJournal0),
    ManSlice1;
compact_files([Batch|T], CDBopts, ActiveJournal0,
                            FilterFun, FilterServer, MaxSQN, 
                            RStrategy, ManSlice0) ->
    {SrcJournal, PositionList} = Batch,
    KVCs0 = leveled_cdb:cdb_directfetch(SrcJournal,
                                        PositionList,
                                        key_value_check),
    KVCs1 = filter_output(KVCs0,
                            FilterFun,
                            FilterServer,
                            MaxSQN,
                            RStrategy),
    {ActiveJournal1, ManSlice1} = write_values(KVCs1,
                                                CDBopts, 
                                                ActiveJournal0,
                                                ManSlice0),
    compact_files(T, CDBopts, ActiveJournal1, FilterFun, FilterServer, MaxSQN,
                                RStrategy, ManSlice1).

get_all_positions([], PositionBatches) ->
    PositionBatches;
get_all_positions([HeadRef|RestOfBest], PositionBatches) ->
    SrcJournal = HeadRef#candidate.journal,
    Positions = leveled_cdb:cdb_getpositions(SrcJournal, all),
    leveled_log:log("IC008", [HeadRef#candidate.filename, length(Positions)]),
    Batches = split_positions_into_batches(lists:sort(Positions),
                                            SrcJournal,
                                            []),
    get_all_positions(RestOfBest, PositionBatches ++ Batches).

split_positions_into_batches([], _Journal, Batches) ->
    Batches;
split_positions_into_batches(Positions, Journal, Batches) ->
    {ThisBatch, Tail} = if
                            length(Positions) > ?BATCH_SIZE ->
                                lists:split(?BATCH_SIZE, Positions);
                            true ->
                                {Positions, []}
                        end,
    split_positions_into_batches(Tail,
                                    Journal,
                                    Batches ++ [{Journal, ThisBatch}]).


filter_output(KVCs, FilterFun, FilterServer, MaxSQN, ReloadStrategy) ->
    lists:foldl(fun(KVC0, Acc) ->
                    R = leveled_codec:compact_inkerkvc(KVC0, ReloadStrategy),
                    case R of
                        skip ->
                            Acc;
                        {TStrat, KVC1} ->
                            {K, _V, CrcCheck} = KVC0,
                            {SQN, LedgerKey} = leveled_codec:from_journalkey(K),
                            KeyValid = FilterFun(FilterServer, LedgerKey, SQN),
                            case {KeyValid, CrcCheck, SQN > MaxSQN, TStrat} of
                                {false, true, false, retain} ->
                                    Acc ++ [KVC1];
                                {false, true, false, _} ->
                                    Acc;
                                _ ->
                                    Acc ++ [KVC0]
                            end
                    end
                    end,
                [],
                KVCs).
    

write_values([], _CDBopts, Journal0, ManSlice0) ->
    {Journal0, ManSlice0};
write_values(KVCList, CDBopts, Journal0, ManSlice0) ->
    KVList = lists:map(fun({K, V, _C}) ->
                            % Compress the value as part of compaction
                            {K, leveled_codec:maybe_compress(V)}
                            end,
                        KVCList),
    {ok, Journal1} = case Journal0 of
                            null ->
                                {TK, _TV} = lists:nth(1, KVList),
                                {SQN, _LK} = leveled_codec:from_journalkey(TK),
                                FP = CDBopts#cdb_options.file_path,
                                FN = leveled_inker:filepath(FP,
                                                            SQN,
                                                            compact_journal),
                                leveled_log:log("IC009", [FN]),
                                leveled_cdb:cdb_open_writer(FN,
                                                            CDBopts);
                            _ ->
                                {ok, Journal0}
                        end,
    R = leveled_cdb:cdb_mput(Journal1, KVList),
    case R of
        ok ->
            {Journal1, ManSlice0};
        roll ->
            ManSlice1 = ManSlice0 ++ leveled_imanifest:generate_entry(Journal1),
            write_values(KVCList, CDBopts, null, ManSlice1)
    end.
                        
clear_waste(State) ->
    WP = State#state.waste_path,
    WRP = State#state.waste_retention_period,
    {ok, ClearedJournals} = file:list_dir(WP),
    N = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
    lists:foreach(fun(DelJ) ->
                        LMD = filelib:last_modified(WP ++ DelJ),
                        case N - calendar:datetime_to_gregorian_seconds(LMD) of
                            LMD_Delta when LMD_Delta >= WRP ->
                                ok = file:delete(WP ++ DelJ),
                                leveled_log:log("IC010", [WP ++ DelJ]);
                            LMD_Delta ->
                                leveled_log:log("IC011", [WP ++ DelJ,
                                                            LMD_Delta]),
                                ok
                        end
                        end,
                    ClearedJournals).


%%%============================================================================
%%% Test
%%%============================================================================


-ifdef(TEST).

schedule_test() ->
    schedule_test_bycount(1),
    schedule_test_bycount(2),
    schedule_test_bycount(4).

schedule_test_bycount(N) ->
    CurrentTS = {1490,883918,94000}, % Actually 30th March 2017 15:27
    SecondsToCompaction0 = schedule_compaction([16], N, CurrentTS),
    io:format("Seconds to compaction ~w~n", [SecondsToCompaction0]),
    ?assertMatch(true, SecondsToCompaction0 > 1800),
    ?assertMatch(true, SecondsToCompaction0 < 5700),
    SecondsToCompaction1 = schedule_compaction([14], N, CurrentTS), % tomorrow!
    io:format("Seconds to compaction ~w~n", [SecondsToCompaction1]),
    ?assertMatch(true, SecondsToCompaction1 > 81000),
    ?assertMatch(true, SecondsToCompaction1 < 84300).


simple_score_test() ->
    Run1 = [#candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 76.0},
            #candidate{compaction_perc = 70.0}],
    ?assertMatch(-4.0, score_run(Run1, 4)),
    Run2 = [#candidate{compaction_perc = 75.0}],
    ?assertMatch(-35.0, score_run(Run2, 4)),
    ?assertMatch(0.0, score_run([], 4)),
    Run3 = [#candidate{compaction_perc = 100.0}],
    ?assertMatch(-60.0, score_run(Run3, 4)).

score_compare_test() ->
    Run1 = [#candidate{compaction_perc = 55.0},
            #candidate{compaction_perc = 55.0},
            #candidate{compaction_perc = 56.0},
            #candidate{compaction_perc = 50.0}],
    ?assertMatch(16.0, score_run(Run1, 4)),
    Run2 = [#candidate{compaction_perc = 55.0}],
    ?assertMatch(Run1, 
                    choose_best_assessment(Run1, 
                                            Run2, 
                                            {4, 60.0, 40.0})),
    ?assertMatch(Run2, 
                    choose_best_assessment(Run1 ++ Run2, 
                                            Run2, 
                                            {4, 60.0, 40.0})).

file_gc_test() ->
    State = #state{waste_path="test/waste/",
                    waste_retention_period=1},
    ok = filelib:ensure_dir(State#state.waste_path),
    file:write_file(State#state.waste_path ++ "1.cdb", term_to_binary("Hello")),
    timer:sleep(1100),
    file:write_file(State#state.waste_path ++ "2.cdb", term_to_binary("Hello")),
    clear_waste(State),
    {ok, ClearedJournals} = file:list_dir(State#state.waste_path),
    ?assertMatch(["2.cdb"], ClearedJournals),
    timer:sleep(1100),
    clear_waste(State),
    {ok, ClearedJournals2} = file:list_dir(State#state.waste_path),
    ?assertMatch([], ClearedJournals2).

find_bestrun_test() ->
%% Tests dependent on these defaults
%% -define(MAX_COMPACTION_RUN, 4).
%% -define(SINGLEFILE_COMPACTION_TARGET, 40.0).
%% -define(MAXRUN_COMPACTION_TARGET, 60.0).
%% Tested first with blocks significant as no back-tracking
    Params = {4, 60.0, 40.0},
    Block1 = [#candidate{compaction_perc = 55.0},
                #candidate{compaction_perc = 65.0},
                #candidate{compaction_perc = 42.0},
                #candidate{compaction_perc = 50.0}],
    Block2 = [#candidate{compaction_perc = 38.0},
                #candidate{compaction_perc = 75.0},
                #candidate{compaction_perc = 75.0},
                #candidate{compaction_perc = 45.0}],
    Block3 = [#candidate{compaction_perc = 70.0},
                #candidate{compaction_perc = 100.0},
                #candidate{compaction_perc = 100.0},
                #candidate{compaction_perc = 100.0}],
    Block4 = [#candidate{compaction_perc = 55.0},
                #candidate{compaction_perc = 56.0},
                #candidate{compaction_perc = 57.0},
                #candidate{compaction_perc = 40.0}],
    Block5 = [#candidate{compaction_perc = 60.0},
                #candidate{compaction_perc = 60.0}],
    CList0 = Block1 ++ Block2 ++ Block3 ++ Block4 ++ Block5,
    ?assertMatch(Block4, assess_candidates(CList0, Params, [], [])),
    CList1 = CList0 ++ [#candidate{compaction_perc = 20.0}],
    ?assertMatch([#candidate{compaction_perc = 20.0}],
                    assess_candidates(CList1, Params, [], [])),
    CList2 = Block4 ++ Block3 ++ Block2 ++ Block1 ++ Block5,
    ?assertMatch(Block4, assess_candidates(CList2, Params, [], [])),
    CList3 = Block5 ++ Block1 ++ Block2 ++ Block3 ++ Block4,
    ?assertMatch([#candidate{compaction_perc = 42.0},
                        #candidate{compaction_perc = 50.0},
                        #candidate{compaction_perc = 38.0}],
                    assess_candidates(CList3, Params)),
    %% Now do some back-tracking to get a genuinely optimal solution without
    %% needing to re-order
    ?assertMatch([#candidate{compaction_perc = 42.0},
                        #candidate{compaction_perc = 50.0},
                        #candidate{compaction_perc = 38.0}],
                    assess_candidates(CList0, Params)),
    ?assertMatch([#candidate{compaction_perc = 42.0},
                        #candidate{compaction_perc = 50.0},
                        #candidate{compaction_perc = 38.0}],
                    assess_candidates(CList0, setelement(1, Params, 5))),
    ?assertMatch([#candidate{compaction_perc = 42.0},
                        #candidate{compaction_perc = 50.0},
                        #candidate{compaction_perc = 38.0},
                        #candidate{compaction_perc = 75.0},
                        #candidate{compaction_perc = 75.0},
                        #candidate{compaction_perc = 45.0}], 
                    assess_candidates(CList0, setelement(1, Params, 6))).

test_ledgerkey(Key) ->
    {o, "Bucket", Key, null}.

test_inkerkv(SQN, Key, V, IdxSpecs) ->
    leveled_codec:to_inkerkv(test_ledgerkey(Key), SQN, V, IdxSpecs).

fetch_testcdb(RP) ->
    FN1 = leveled_inker:filepath(RP, 1, new_journal),
    {ok,
        CDB1} = leveled_cdb:cdb_open_writer(FN1,
                                            #cdb_options{binary_mode=true}),
    {K1, V1} = test_inkerkv(1, "Key1", "Value1", []),
    {K2, V2} = test_inkerkv(2, "Key2", "Value2", []),
    {K3, V3} = test_inkerkv(3, "Key3", "Value3", []),
    {K4, V4} = test_inkerkv(4, "Key1", "Value4", []),
    {K5, V5} = test_inkerkv(5, "Key1", "Value5", []),
    {K6, V6} = test_inkerkv(6, "Key1", "Value6", []),
    {K7, V7} = test_inkerkv(7, "Key1", "Value7", []),
    {K8, V8} = test_inkerkv(8, "Key1", "Value8", []),
    ok = leveled_cdb:cdb_put(CDB1, K1, V1),
    ok = leveled_cdb:cdb_put(CDB1, K2, V2),
    ok = leveled_cdb:cdb_put(CDB1, K3, V3),
    ok = leveled_cdb:cdb_put(CDB1, K4, V4),
    ok = leveled_cdb:cdb_put(CDB1, K5, V5),
    ok = leveled_cdb:cdb_put(CDB1, K6, V6),
    ok = leveled_cdb:cdb_put(CDB1, K7, V7),
    ok = leveled_cdb:cdb_put(CDB1, K8, V8),
    {ok, FN2} = leveled_cdb:cdb_complete(CDB1),
    leveled_cdb:cdb_open_reader(FN2, #cdb_options{binary_mode=true}).

check_single_file_test() ->
    RP = "../test/journal",
    {ok, CDB} = fetch_testcdb(RP),
    LedgerSrv1 = [{8, {o, "Bucket", "Key1", null}},
                    {2, {o, "Bucket", "Key2", null}},
                    {3, {o, "Bucket", "Key3", null}}],
    LedgerFun1 = fun(Srv, Key, ObjSQN) ->
                    case lists:keyfind(ObjSQN, 1, Srv) of
                        {ObjSQN, Key} ->
                            true;
                        _ ->
                            false
                    end end,
    Score1 = check_single_file(CDB, LedgerFun1, LedgerSrv1, 9, 8, 4),
    ?assertMatch(37.5, Score1),
    LedgerFun2 = fun(_Srv, _Key, _ObjSQN) -> true end,
    Score2 = check_single_file(CDB, LedgerFun2, LedgerSrv1, 9, 8, 4),
    ?assertMatch(100.0, Score2),
    Score3 = check_single_file(CDB, LedgerFun1, LedgerSrv1, 9, 8, 3),
    ?assertMatch(37.5, Score3),
    Score4 = check_single_file(CDB, LedgerFun1, LedgerSrv1, 4, 8, 4),
    ?assertMatch(75.0, Score4),
    ok = leveled_cdb:cdb_deletepending(CDB),
    ok = leveled_cdb:cdb_destroy(CDB).


compact_single_file_setup() ->
    RP = "../test/journal",
    {ok, CDB} = fetch_testcdb(RP),
    Candidate = #candidate{journal = CDB,
                            low_sqn = 1,
                            filename = "test",
                            compaction_perc = 37.5},
    LedgerSrv1 = [{8, {o, "Bucket", "Key1", null}},
                    {2, {o, "Bucket", "Key2", null}},
                    {3, {o, "Bucket", "Key3", null}}],
    LedgerFun1 = fun(Srv, Key, ObjSQN) ->
                    case lists:keyfind(ObjSQN, 1, Srv) of
                        {ObjSQN, Key} ->
                            true;
                        _ ->
                            false
                    end end,
    CompactFP = leveled_inker:filepath(RP, journal_compact_dir),
    ok = filelib:ensure_dir(CompactFP),
    {Candidate, LedgerSrv1, LedgerFun1, CompactFP, CDB}.

compact_single_file_recovr_test() ->
    {Candidate,
        LedgerSrv1,
        LedgerFun1,
        CompactFP,
        CDB} = compact_single_file_setup(),
    [{LowSQN, FN, PidR, _LastKey}] =
        compact_files([Candidate],
                        #cdb_options{file_path=CompactFP, binary_mode=true},
                        LedgerFun1,
                        LedgerSrv1,
                        9,
                        [{?STD_TAG, recovr}]),
    io:format("FN of ~s~n", [FN]),
    ?assertMatch(2, LowSQN),
    ?assertMatch(probably,
                    leveled_cdb:cdb_keycheck(PidR,
                                                {8,
                                                    stnd,
                                                    test_ledgerkey("Key1")})),
    ?assertMatch(missing, leveled_cdb:cdb_get(PidR,
                                                {7,
                                                    stnd,
                                                    test_ledgerkey("Key1")})),
    ?assertMatch(missing, leveled_cdb:cdb_get(PidR,
                                                {1,
                                                    stnd,
                                                    test_ledgerkey("Key1")})),
    RKV1 = leveled_cdb:cdb_get(PidR,
                                {2,
                                    stnd,
                                    test_ledgerkey("Key2")}),
    ?assertMatch({{_, _}, {"Value2", []}}, leveled_codec:from_inkerkv(RKV1)),
    ok = leveled_cdb:cdb_deletepending(CDB),
    ok = leveled_cdb:cdb_destroy(CDB).


compact_single_file_retain_test() ->
    {Candidate,
        LedgerSrv1,
        LedgerFun1,
        CompactFP,
        CDB} = compact_single_file_setup(),
    [{LowSQN, FN, PidR, _LK}] =
        compact_files([Candidate],
                        #cdb_options{file_path=CompactFP, binary_mode=true},
                        LedgerFun1,
                        LedgerSrv1,
                        9,
                        [{?STD_TAG, retain}]),
    io:format("FN of ~s~n", [FN]),
    ?assertMatch(1, LowSQN),
    ?assertMatch(probably,
                    leveled_cdb:cdb_keycheck(PidR,
                                                {8,
                                                    stnd,
                                                    test_ledgerkey("Key1")})),
    ?assertMatch(missing, leveled_cdb:cdb_get(PidR,
                                                {7,
                                                    stnd,
                                                    test_ledgerkey("Key1")})),
    ?assertMatch(missing, leveled_cdb:cdb_get(PidR,
                                                {1,
                                                    stnd,
                                                    test_ledgerkey("Key1")})),
    RKV1 = leveled_cdb:cdb_get(PidR,
                                        {2,
                                            stnd,
                                            test_ledgerkey("Key2")}),
    ?assertMatch({{_, _}, {"Value2", []}}, leveled_codec:from_inkerkv(RKV1)),
    ok = leveled_cdb:cdb_deletepending(CDB),
    ok = leveled_cdb:cdb_destroy(CDB).

compact_empty_file_test() ->
    RP = "../test/journal",
    FN1 = leveled_inker:filepath(RP, 1, new_journal),
    CDBopts = #cdb_options{binary_mode=true},
    {ok, CDB1} = leveled_cdb:cdb_open_writer(FN1, CDBopts),
    ok = leveled_cdb:cdb_put(CDB1, {1, stnd, test_ledgerkey("Key1")}, <<>>),
    {ok, FN2} = leveled_cdb:cdb_complete(CDB1),
    {ok, CDB2} = leveled_cdb:cdb_open_reader(FN2),
    LedgerSrv1 = [{8, {o, "Bucket", "Key1", null}},
                    {2, {o, "Bucket", "Key2", null}},
                    {3, {o, "Bucket", "Key3", null}}],
    LedgerFun1 = fun(_Srv, _Key, _ObjSQN) -> false end,
    Score1 = check_single_file(CDB2, LedgerFun1, LedgerSrv1, 9, 8, 4),
    ?assertMatch(100.0, Score1).

compare_candidate_test() ->
    Candidate1 = #candidate{low_sqn=1},
    Candidate2 = #candidate{low_sqn=2},
    Candidate3 = #candidate{low_sqn=3},
    Candidate4 = #candidate{low_sqn=4},
    ?assertMatch([Candidate1, Candidate2, Candidate3, Candidate4],
                sort_run([Candidate3, Candidate2, Candidate4, Candidate1])).       

compact_singlefile_totwosmallfiles_test() ->
    RP = "../test/journal",
    CP = "../test/journal/journal_file/post_compact/",
    ok = filelib:ensure_dir(CP),
    FN1 = leveled_inker:filepath(RP, 1, new_journal),
    CDBoptsLarge = #cdb_options{binary_mode=true, max_size=30000000},
    {ok, CDB1} = leveled_cdb:cdb_open_writer(FN1, CDBoptsLarge),
    lists:foreach(fun(X) ->
                        LK = test_ledgerkey("Key" ++ integer_to_list(X)),
                        Value = leveled_rand:rand_bytes(1024),
                        {IK, IV} = leveled_codec:to_inkerkv(LK, X, Value, []),
                        ok = leveled_cdb:cdb_put(CDB1, IK, IV)
                        end,
                    lists:seq(1, 1000)),
    {ok, NewName} = leveled_cdb:cdb_complete(CDB1),
    {ok, CDBr} = leveled_cdb:cdb_open_reader(NewName),
    CDBoptsSmall = #cdb_options{binary_mode=true, max_size=400000, file_path=CP},
    BestRun1 = [#candidate{low_sqn=1,
                            filename=leveled_cdb:cdb_filename(CDBr),
                            journal=CDBr,
                            compaction_perc=50.0}],
    FakeFilterFun = fun(_FS, _LK, SQN) -> SQN rem 2 == 0 end,
    
    ManifestSlice = compact_files(BestRun1,
                                    CDBoptsSmall,
                                    FakeFilterFun,
                                    null,
                                    900,
                                    [{?STD_TAG, recovr}]),
    ?assertMatch(2, length(ManifestSlice)),
    lists:foreach(fun({_SQN, _FN, CDB, _LK}) ->
                        ok = leveled_cdb:cdb_deletepending(CDB),
                        ok = leveled_cdb:cdb_destroy(CDB)
                        end,
                    ManifestSlice),
    ok = leveled_cdb:cdb_deletepending(CDBr),
    ok = leveled_cdb:cdb_destroy(CDBr).

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null),
    {reply, not_supported, _State2} = handle_call(null, null, #state{}),
    terminate(error, #state{}).    

-endif.
