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
        code_change/3]).

-export([clerk_new/1,
        clerk_compact/6,
        clerk_hashtablecalc/3,
        clerk_trim/3,
        clerk_promptdeletions/3,
        clerk_stop/1,
        clerk_loglevel/2,
        clerk_addlogs/2,
        clerk_removelogs/2]).   

-export([schedule_compaction/3]).

-include_lib("eunit/include/eunit.hrl").

-define(JOURNAL_FILEX, "cdb").
-define(PENDING_FILEX, "pnd").
-define(SAMPLE_SIZE, 100).
-define(BATCH_SIZE, 32).
-define(BATCHES_TO_CHECK, 8).
-define(CRC_SIZE, 4).
-define(DEFAULT_RELOAD_STRATEGY, leveled_codec:inker_reload_strategy([])).
-define(INTERVALS_PER_HOUR, 4).
-define(MAX_COMPACTION_RUN, 8).
-define(SINGLEFILE_COMPACTION_TARGET, 50.0).
-define(MAXRUNLENGTH_COMPACTION_TARGET, 75.0).

-record(state, {inker :: pid() | undefined,
                max_run_length :: integer() | undefined,
                cdb_options = #cdb_options{} :: #cdb_options{},
                waste_retention_period :: integer() | undefined,
                waste_path :: string() | undefined,
                reload_strategy = ?DEFAULT_RELOAD_STRATEGY :: list(),
                singlefile_compactionperc  = ?SINGLEFILE_COMPACTION_TARGET :: float(),
                maxrunlength_compactionperc = ?MAXRUNLENGTH_COMPACTION_TARGET ::float(),
                compression_method = native :: lz4|native,
                scored_files = [] :: list(candidate()),
                scoring_state :: scoring_state()|undefined}).

-record(candidate, {low_sqn :: integer() | undefined,
                    filename :: string() | undefined,
                    journal :: pid() | undefined,
                    compaction_perc :: float() | undefined}).

-record(scoring_state, {filter_fun :: fun(),
                        filter_server :: pid(),
                        max_sqn :: non_neg_integer(),
                        close_fun :: fun(),
                        start_time :: erlang:timestamp()}).

-type iclerk_options() :: #iclerk_options{}.
-type candidate() :: #candidate{}.
-type scoring_state() :: #scoring_state{}.
-type score_parameters() :: {integer(), float(), float()}.
    % Score parameters are a tuple 
    % - of maximum run length; how long a run of consecutive files can be for 
    % one compaction run
    % - maximum run compaction target; percentage space which should be 
    % released from a compaction run of the maximum length to make it a run
    % worthwhile of compaction (released space is 100.0 - target e.g. 70.0 
    % means that 30.0% should be released) 
    % - single_file compaction target; percentage space which should be 
    % released from a compaction run of a single file to make it a run
    % worthwhile of compaction (released space is 100.0 - target e.g. 70.0 
    % means that 30.0% should be released)

%%%============================================================================
%%% API
%%%============================================================================

-spec clerk_new(iclerk_options()) -> {ok, pid()}.
%% @doc
%% Generate a new clerk
clerk_new(InkerClerkOpts) ->
    gen_server:start_link(?MODULE, [leveled_log:get_opts(), InkerClerkOpts], []).

-spec clerk_compact(pid(), pid(), 
                    fun(), fun(), fun(),  
                    list()) -> ok.
%% @doc
%% Trigger a compaction for this clerk if the threshold of data recovery has 
%% been met
clerk_compact(Pid, Checker, InitiateFun, CloseFun, FilterFun, Manifest) ->
    gen_server:cast(Pid,
                    {compact,
                    Checker,
                    InitiateFun,
                    CloseFun,
                    FilterFun,
                    Manifest}).

-spec clerk_trim(pid(), integer(), list()) -> ok.
%% @doc
%% Trim the Inker back to the persisted SQN
clerk_trim(Pid, PersistedSQN, ManifestAsList) ->
    gen_server:cast(Pid, {trim, PersistedSQN, ManifestAsList}).

-spec clerk_promptdeletions(pid(), pos_integer(), list()) -> ok. 
%% @doc
%%
clerk_promptdeletions(Pid, ManifestSQN, DeletedFiles) ->
    gen_server:cast(Pid, {prompt_deletions, ManifestSQN, DeletedFiles}).

-spec clerk_hashtablecalc(ets:tid(), integer(), pid()) -> ok.
%% @doc
%% Spawn a dedicated clerk for the process of calculating the binary view
%% of the hastable in the CDB file - so that the file is not blocked during
%% this calculation
clerk_hashtablecalc(HashTree, StartPos, CDBpid) ->
    {ok, Clerk} = gen_server:start_link(?MODULE, [leveled_log:get_opts(),
                                                  #iclerk_options{}], []),
    gen_server:cast(Clerk, {hashtable_calc, HashTree, StartPos, CDBpid}).

-spec clerk_stop(pid()) -> ok.
%% @doc
%% Stop the clerk
clerk_stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

-spec clerk_loglevel(pid(), leveled_log:log_level()) -> ok.
%% @doc
%% Change the log level of the Journal
clerk_loglevel(Pid, LogLevel) ->
    gen_server:cast(Pid, {log_level, LogLevel}).

-spec clerk_addlogs(pid(), list(string())) -> ok.
%% @doc
%% Add to the list of forced logs, a list of more forced logs
clerk_addlogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {add_logs, ForcedLogs}).

-spec clerk_removelogs(pid(), list(string())) -> ok.
%% @doc
%% Remove from the list of forced logs, a list of forced logs
clerk_removelogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {remove_logs, ForcedLogs}).


-spec clerk_scorefilelist(pid(), list(candidate())) -> ok.
%% @doc
%% Score the file at the head of the list and then send the tail of the list to
%% be scored
clerk_scorefilelist(Pid, []) ->
    gen_server:cast(Pid, scoring_complete);
clerk_scorefilelist(Pid, CandidateList) ->
    gen_server:cast(Pid, {score_filelist, CandidateList}).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([LogOpts, IClerkOpts]) ->
    leveled_log:save(LogOpts),
    ReloadStrategy = IClerkOpts#iclerk_options.reload_strategy,
    CDBopts = IClerkOpts#iclerk_options.cdb_options,
    WP = CDBopts#cdb_options.waste_path,
    WRP = IClerkOpts#iclerk_options.waste_retention_period,
    
    MRL = 
        case IClerkOpts#iclerk_options.max_run_length of
            undefined ->
                ?MAX_COMPACTION_RUN;
            MRL0 ->
                MRL0
        end,
    
    SFL_CompPerc =
        case IClerkOpts#iclerk_options.singlefile_compactionperc of
            undefined ->
                ?SINGLEFILE_COMPACTION_TARGET;
            SFLCP when is_float(SFLCP) ->
                SFLCP
        end,
    MRL_CompPerc =
        case IClerkOpts#iclerk_options.maxrunlength_compactionperc of
            undefined ->
                ?MAXRUNLENGTH_COMPACTION_TARGET;
            MRLCP when is_float(MRLCP) ->
                MRLCP
        end,

    {ok, #state{max_run_length = MRL,
                        inker = IClerkOpts#iclerk_options.inker,
                        cdb_options = CDBopts,
                        reload_strategy = ReloadStrategy,
                        waste_path = WP,
                        waste_retention_period = WRP,
                        singlefile_compactionperc = SFL_CompPerc,
                        maxrunlength_compactionperc = MRL_CompPerc,
                        compression_method = 
                            IClerkOpts#iclerk_options.compression_method}}.

handle_call(stop, _From, State) ->
    case State#state.scoring_state of
        undefined ->
            ok;
        ScoringState ->
            % Closed when scoring files, and so need to shutdown FilterServer
            % to close down neatly
        CloseFun = ScoringState#scoring_state.close_fun,
        FilterServer = ScoringState#scoring_state.filter_server,
        CloseFun(FilterServer)
    end,
    {stop, normal, ok, State}.

handle_cast({compact, Checker, InitiateFun, CloseFun, FilterFun, Manifest0},
                State) ->
    % Empty the waste folder
    clear_waste(State),
    SW = os:timestamp(), 
        % Clock to record the time it takes to calculate the potential for
        % compaction
    
    % Need to fetch manifest at start rather than have it be passed in
    % Don't want to process a queued call waiting on an old manifest
    [_Active|Manifest] = Manifest0,
    {FilterServer, MaxSQN} = InitiateFun(Checker),
    ok = clerk_scorefilelist(self(), Manifest),
    ScoringState =
        #scoring_state{filter_fun = FilterFun,
                        filter_server = FilterServer,
                        max_sqn = MaxSQN,
                        close_fun = CloseFun,
                        start_time = SW},
    {noreply, State#state{scored_files = [], scoring_state = ScoringState}};
handle_cast({score_filelist, [Entry|Tail]}, State) ->
    Candidates = State#state.scored_files,
    {LowSQN, FN, JournalP, _LK} = Entry,
    ScoringState = State#state.scoring_state,
    CpctPerc = check_single_file(JournalP,
                                    ScoringState#scoring_state.filter_fun,
                                    ScoringState#scoring_state.filter_server,
                                    ScoringState#scoring_state.max_sqn,
                                    ?SAMPLE_SIZE,
                                    ?BATCH_SIZE),
    Candidate =
        #candidate{low_sqn = LowSQN,
                    filename = FN,
                    journal = JournalP,
                    compaction_perc = CpctPerc},
    ok = clerk_scorefilelist(self(), Tail),
    {noreply, State#state{scored_files = [Candidate|Candidates]}};
handle_cast(scoring_complete, State) ->
    MaxRunLength = State#state.max_run_length,
    CDBopts = State#state.cdb_options,
    Candidates = lists:reverse(State#state.scored_files),
    ScoringState = State#state.scoring_state,
    FilterFun = ScoringState#scoring_state.filter_fun,
    FilterServer = ScoringState#scoring_state.filter_server,
    MaxSQN = ScoringState#scoring_state.max_sqn,
    CloseFun = ScoringState#scoring_state.close_fun,
    SW = ScoringState#scoring_state.start_time,
    ScoreParams =
        {MaxRunLength, 
            State#state.maxrunlength_compactionperc, 
            State#state.singlefile_compactionperc},
    {BestRun0, Score} = assess_candidates(Candidates, ScoreParams),
    leveled_log:log_timer("IC003", [Score, length(BestRun0)], SW),
    case Score > 0.0 of
        true ->
            BestRun1 = sort_run(BestRun0),
            print_compaction_run(BestRun1, ScoreParams),
            ManifestSlice = compact_files(BestRun1,
                                            CDBopts,
                                            FilterFun,
                                            FilterServer,
                                            MaxSQN,
                                            State#state.reload_strategy,
                                            State#state.compression_method),
            FilesToDelete = lists:map(fun(C) ->
                                            {C#candidate.low_sqn,
                                                C#candidate.filename,
                                                C#candidate.journal,
                                                undefined}
                                            end,
                                        BestRun1),
            leveled_log:log("IC002", [length(FilesToDelete)]),
            ok = CloseFun(FilterServer),
            ok = leveled_inker:ink_clerkcomplete(State#state.inker,
                                                    ManifestSlice,
                                                    FilesToDelete);
        false ->
            ok = CloseFun(FilterServer),
            ok = leveled_inker:ink_clerkcomplete(State#state.inker, [], [])
    end,
    {noreply, State#state{scoring_state = undefined}, hibernate};
handle_cast({trim, PersistedSQN, ManifestAsList}, State) ->
    FilesToDelete = 
        leveled_imanifest:find_persistedentries(PersistedSQN, ManifestAsList),
    leveled_log:log("IC007", []),
    ok = leveled_inker:ink_clerkcomplete(State#state.inker, [], FilesToDelete),
    {noreply, State};
handle_cast({prompt_deletions, ManifestSQN, FilesToDelete}, State) ->
    lists:foreach(fun({_SQN, _FN, J2D, _LK}) ->
                        leveled_cdb:cdb_deletepending(J2D,
                                                        ManifestSQN,
                                                        State#state.inker)
                        end,
                    FilesToDelete),
    {noreply, State};
handle_cast({hashtable_calc, HashTree, StartPos, CDBpid}, State) ->
    {IndexList, HashTreeBin} = leveled_cdb:hashtable_calc(HashTree, StartPos),
    ok = leveled_cdb:cdb_returnhashtable(CDBpid, IndexList, HashTreeBin),
    {stop, normal, State};
handle_cast({log_level, LogLevel}, State) ->
    ok = leveled_log:set_loglevel(LogLevel),
    CDBopts = State#state.cdb_options,
    CDBopts0 = CDBopts#cdb_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{cdb_options = CDBopts0}};
handle_cast({add_logs, ForcedLogs}, State) ->
    ok = leveled_log:add_forcedlogs(ForcedLogs),
    CDBopts = State#state.cdb_options,
    CDBopts0 = CDBopts#cdb_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{cdb_options = CDBopts0}};
handle_cast({remove_logs, ForcedLogs}, State) ->
    ok = leveled_log:remove_forcedlogs(ForcedLogs),
    CDBopts = State#state.cdb_options,
    CDBopts0 = CDBopts#cdb_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{cdb_options = CDBopts0}}.

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

-spec schedule_compaction(list(integer()), 
                            integer(), 
                            {integer(), integer(), integer()}) -> integer().
%% @doc
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


%% @doc
%% Get a score for a single CDB file in the journal.  This will pull out a bunch 
%% of keys and sizes at random in an efficient way (by scanning the hashtable
%% then just picking the key and size information of disk).
%% 
%% The score should represent a percentage which is the size of the file by 
%% comparison to the original file if compaction was to be run.  So if a file 
%% can be reduced in size by 30% the score will be 70%.
%% 
%% The score is based on a random sample - so will not be consistent between 
%% calls.
check_single_file(CDB, FilterFun, FilterServer, MaxSQN, SampleSize, BatchSize) ->
    FN = leveled_cdb:cdb_filename(CDB),
    PositionList = leveled_cdb:cdb_getpositions(CDB, SampleSize),
    KeySizeList = fetch_inbatches(PositionList, BatchSize, CDB, []),
    Score = 
        size_comparison_score(KeySizeList, FilterFun, FilterServer, MaxSQN),
    leveled_log:log("IC004", [FN, Score]),
    Score.

size_comparison_score(KeySizeList, FilterFun, FilterServer, MaxSQN) ->
    FoldFunForSizeCompare =
        fun(KS, {ActSize, RplSize}) ->
            case KS of
                {{SQN, Type, PK}, Size} ->
                    MayScore =
                        leveled_codec:is_compaction_candidate({SQN, Type, PK}),
                    case MayScore of
                        false ->
                            {ActSize + Size - ?CRC_SIZE, RplSize};
                        true ->
                            Check = FilterFun(FilterServer, PK, SQN),
                            case {Check, SQN > MaxSQN} of
                                {true, _} ->
                                    {ActSize + Size - ?CRC_SIZE, RplSize};
                                {false, true} ->
                                    {ActSize + Size - ?CRC_SIZE, RplSize};
                                _ ->
                                    {ActSize, RplSize + Size - ?CRC_SIZE}
                            end
                    end;
                _ ->
                    % There is a key which is not in expected format
                    % Not that the key-size list has been filtered for
                    % errors by leveled_cdb - but this doesn't know the 
                    % expected format of the key
                    {ActSize, RplSize}
            end
            end,
            
    R0 = lists:foldl(FoldFunForSizeCompare, {0, 0}, KeySizeList),
    {ActiveSize, ReplacedSize} = R0,
    case ActiveSize + ReplacedSize of
        0 ->
            100.0;
        _ ->
            100 * ActiveSize / (ActiveSize + ReplacedSize)
    end.


fetch_inbatches([], _BatchSize, CDB, CheckedList) ->
    ok = leveled_cdb:cdb_clerkcomplete(CDB),
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


-spec assess_candidates(list(candidate()), score_parameters()) 
                                            -> {list(candidate()), float()}.
%% @doc
%% For each run length we need to assess all the possible runs of candidates,
%% to determine which is the best score - to be put forward as the best
%% candidate run for compaction.
%% 
%% Although this requires many loops over the list of the candidate, as the
%% file scores have already been calculated the cost per loop should not be
%% a high burden.  Reducing the maximum run length, will reduce the cost of
%% this exercise should be a problem.
%%
%% The score parameters are used to produce the score of the compaction run,
%% with a higher score being better.  The parameters are the maximum run 
%% length and the compaction targets (for max run length and single file).
%% The score of an individual file is the approximate percentage of the space
%% that would be retained after compaction (e.g. 100 less the percentage of 
%% space wasted by historic objects). 
%%
%% So a file score of 60% indicates that 40% of the space would be 
%% reclaimed following compaction.  A single file target of 50% would not be
%% met for this file.  However, if there are 4 consecutive files scoring 60%,
%% and the maximum run length is 4, and the maximum run length compaction
%% target is 70% - then this run of four files would be a viable candidate
%% for compaction.
assess_candidates(AllCandidates, Params) ->
    MaxRunLength = min(element(1, Params), length(AllCandidates)),
    NaiveBestRun = lists:sublist(AllCandidates, MaxRunLength),
        % This will end up being scored twice, but lets take a guess at
        % the best scoring run to take into the loop
    FoldFun =
        fun(RunLength, Best) ->
            assess_for_runlength(RunLength, AllCandidates, Params, Best)
        end,
    % Check all run lengths to find the best candidate.  Reverse the list of
    % run lengths, so that longer runs win on equality of score
    lists:foldl(FoldFun, 
                {NaiveBestRun, score_run(NaiveBestRun, Params)}, 
                lists:reverse(lists:seq(1, MaxRunLength))).


-spec assess_for_runlength(integer(), list(candidate()), score_parameters(), 
                            {list(candidate()), float()}) 
                                -> {list(candidate()), float()}.
%% @doc
%% For a given run length, calculate the scores for all consecutive runs of
%% files, comparing the score with the best run which has beens een so far.
%% The best is a tuple of the actual run of candidates, along with the score
%% achieved for that run
assess_for_runlength(RunLength, AllCandidates, Params, Best) ->
    NumberOfRuns = 1 + length(AllCandidates) - RunLength,
    FoldFun =
        fun(Offset, {BestRun, BestScore}) ->
            Run = lists:sublist(AllCandidates, Offset, RunLength),
            Score = score_run(Run, Params),
            case Score > BestScore of
                true -> {Run, Score};
                false -> {BestRun, BestScore}
            end
        end,
    lists:foldl(FoldFun, Best, lists:seq(1, NumberOfRuns)).


-spec score_run(list(candidate()), score_parameters()) -> float().
%% @doc
%% Score a run.  Caluclate the avergae score across all the files in the run, 
%% and deduct that from a target score.  Good candidate runs for comapction
%% have larger (positive) scores.  Bad candidate runs for compaction have 
%% negative scores.
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


print_compaction_run(BestRun, ScoreParams) ->
    leveled_log:log("IC005", [length(BestRun),
                                score_run(BestRun, ScoreParams)]),
    lists:foreach(fun(File) ->
                        leveled_log:log("IC006", [File#candidate.filename])
                        end,
                    BestRun).

sort_run(RunOfFiles) ->
    CompareFun = fun(Cand1, Cand2) ->
                    Cand1#candidate.low_sqn =< Cand2#candidate.low_sqn end,
    lists:sort(CompareFun, RunOfFiles).

compact_files(BestRun, CDBopts, FilterFun, FilterServer, 
                                            MaxSQN, RStrategy, PressMethod) ->
    BatchesOfPositions = get_all_positions(BestRun, []),
    compact_files(BatchesOfPositions,
                                CDBopts,
                                null,
                                FilterFun,
                                FilterServer,
                                MaxSQN,
                                RStrategy,
                                PressMethod,
                                []).


compact_files([], _CDBopts, null, _FilterFun, _FilterServer, _MaxSQN,
                            _RStrategy, _PressMethod, ManSlice0) ->
    ManSlice0;
compact_files([], _CDBopts, ActiveJournal0, _FilterFun, _FilterServer, _MaxSQN,
                            _RStrategy, _PressMethod, ManSlice0) ->
    ManSlice1 = ManSlice0 ++ leveled_imanifest:generate_entry(ActiveJournal0),
    ManSlice1;
compact_files([Batch|T], CDBopts, ActiveJournal0,
                            FilterFun, FilterServer, MaxSQN, 
                            RStrategy, PressMethod, ManSlice0) ->
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
                                                ManSlice0,
                                                PressMethod),
    % The inker's clerk will no longer need these (potentially large) binaries,
    % so force garbage collection at this point.  This will mean when we roll
    % each CDB file there will be no remaining references to the binaries that
    % have been transferred and the memory can immediately be cleared
    garbage_collect(),
    compact_files(T, CDBopts, ActiveJournal1, FilterFun, FilterServer, MaxSQN,
                                RStrategy, PressMethod, ManSlice1).

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


%% @doc
%% For the Keys and values taken from the Journal file, which are required
%% in the compacted journal file.  To be required, they must still be active
%% (i.e. be the current SQN for that LedgerKey in the Ledger).  However, if
%% it is not active, we still need to retain some information if for this
%% object tag we want to be able to rebuild the KeyStore by relaoding the 
%% KeyDeltas (the retain reload strategy)
%%
%% If the reload strategy is recalc, we assume that we can reload by
%% recalculating the KeyChanges by looking at the object when we reload.  So
%% old objects can be discarded.
%%
%% If the strategy is skip, we don't care about KeyDeltas.  Note though, that
%% if the ledger is deleted it may not be possible to safely rebuild a KeyStore
%% if it contains index entries.  The hot_backup approach is also not safe with
%% a `skip` strategy.
filter_output(KVCs, FilterFun, FilterServer, MaxSQN, ReloadStrategy) ->
    FoldFun =
        fun(KVC0, Acc) ->
            case KVC0 of
                {_InkKey, crc_wonky, false} ->
                    % Bad entry, disregard, don't check
                    Acc;
                {JK, JV, _Check} ->
                    {SQN, LK} =
                        leveled_codec:from_journalkey(JK),
                    CompactStrategy =
                        leveled_codec:get_tagstrategy(LK, ReloadStrategy),
                    KeyValid = FilterFun(FilterServer, LK, SQN),
                    IsInMemory = SQN > MaxSQN,
                    case {KeyValid or IsInMemory, CompactStrategy} of
                        {true, _} ->
                            % This entry may still be required regardless of
                            % strategy
                            [KVC0|Acc];
                        {false, retain} ->
                            % If we have a retain strategy, it can't be
                            % discarded - but the value part is no longer
                            % required as this version has been replaced
                            {JK0, JV0} =
                                leveled_codec:revert_to_keydeltas(JK, JV),
                            [{JK0, JV0, null}|Acc];
                        {false, _} ->
                            % This is out of date and not retained - discard
                            Acc
                    end
            end
        end,
    lists:reverse(lists:foldl(FoldFun, [], KVCs)).


write_values([], _CDBopts, Journal0, ManSlice0, _PressMethod) ->
    {Journal0, ManSlice0};
write_values(KVCList, CDBopts, Journal0, ManSlice0, PressMethod) ->
    KVList = 
        lists:map(fun({K, V, _C}) ->
                            % Compress the value as part of compaction
                        {K, leveled_codec:maybe_compress(V, PressMethod)}
                    end,
                    KVCList),
    {ok, Journal1} = 
        case Journal0 of
            null ->
                {TK, _TV} = lists:nth(1, KVList),
                {SQN, _LK} = leveled_codec:from_journalkey(TK),
                FP = CDBopts#cdb_options.file_path,
                FN = leveled_inker:filepath(FP, SQN, compact_journal),
                leveled_log:log("IC009", [FN]),
                leveled_cdb:cdb_open_writer(FN, CDBopts);
            _ ->
                {ok, Journal0}
        end,
    R = leveled_cdb:cdb_mput(Journal1, KVList),
    case R of
        ok ->
            {Journal1, ManSlice0};
        roll ->
            ManSlice1 = ManSlice0 ++ leveled_imanifest:generate_entry(Journal1),
            write_values(KVCList, CDBopts, null, ManSlice1, PressMethod)
    end.
                        
clear_waste(State) ->
    case State#state.waste_path of
        undefined ->
            ok;
        WP ->
            WRP = State#state.waste_retention_period,
            {ok, ClearedJournals} = file:list_dir(WP),
            N = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            DeleteJournalFun =
                fun(DelJ) ->
                    LMD = filelib:last_modified(WP ++ DelJ),
                    case N - calendar:datetime_to_gregorian_seconds(LMD) of
                        LMD_Delta when LMD_Delta >= WRP ->
                            ok = file:delete(WP ++ DelJ),
                            leveled_log:log("IC010", [WP ++ DelJ]);
                        LMD_Delta ->
                            leveled_log:log("IC011", [WP ++ DelJ, LMD_Delta]),
                            ok
                    end
                end,
            lists:foreach(DeleteJournalFun, ClearedJournals)
    end.


%%%============================================================================
%%% Test
%%%============================================================================


-ifdef(TEST).

schedule_test() ->
    schedule_test_bycount(1),
    schedule_test_bycount(2),
    schedule_test_bycount(4).

schedule_test_bycount(N) ->
    LocalTimeAsDateTime = {{2017,3,30},{15,27,0}},
    CurrentTS= local_time_to_now(LocalTimeAsDateTime),
    SecondsToCompaction0 = schedule_compaction([16], N, CurrentTS),
    io:format("Seconds to compaction ~w~n", [SecondsToCompaction0]),
    ?assertMatch(true, SecondsToCompaction0 > 1800),
    ?assertMatch(true, SecondsToCompaction0 < 5700),
    SecondsToCompaction1 = schedule_compaction([14], N, CurrentTS), % tomorrow!
    io:format("Seconds to compaction ~w for count ~w~n", 
                [SecondsToCompaction1, N]),
    ?assertMatch(true, SecondsToCompaction1 >= 81180),
    ?assertMatch(true, SecondsToCompaction1 =< 84780).

local_time_to_now(DateTime) ->
    [UTC] = calendar:local_time_to_universal_time_dst(DateTime),
    Epoch = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    Seconds = calendar:datetime_to_gregorian_seconds(UTC) - Epoch,
    {Seconds div 1000000, Seconds rem 1000000, 0}.

simple_score_test() ->
    Run1 = [#candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 76.0},
            #candidate{compaction_perc = 70.0}],
    ?assertMatch(-4.0, score_run(Run1, {4, 70.0, 40.0})),
    Run2 = [#candidate{compaction_perc = 75.0}],
    ?assertMatch(-35.0, score_run(Run2, {4, 70.0, 40.0})),
    ?assertMatch(0.0, score_run([], {4, 40.0, 70.0})),
    Run3 = [#candidate{compaction_perc = 100.0}],
    ?assertMatch(-60.0, score_run(Run3, {4, 70.0, 40.0})).

file_gc_test() ->
    State = #state{waste_path="test/test_area/waste/",
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


check_bestrun(CandidateList, Params) ->
    {BestRun, _Score} = assess_candidates(CandidateList, Params),
    lists:map(fun(C) -> C#candidate.filename end, BestRun).

find_bestrun_test() ->
%% Tests dependent on these defaults
%% -define(MAX_COMPACTION_RUN, 4).
%% -define(SINGLEFILE_COMPACTION_TARGET, 40.0).
%% -define(MAXRUNLENGTH_COMPACTION_TARGET, 60.0).
%% Tested first with blocks significant as no back-tracking
    Params = {4, 60.0, 40.0},
    Block1 = [#candidate{compaction_perc = 55.0, filename = "a"},
                #candidate{compaction_perc = 65.0, filename = "b"},
                #candidate{compaction_perc = 42.0, filename = "c"},
                #candidate{compaction_perc = 50.0, filename = "d"}],
    Block2 = [#candidate{compaction_perc = 38.0, filename = "e"},
                #candidate{compaction_perc = 75.0, filename = "f"},
                #candidate{compaction_perc = 75.0, filename = "g"},
                #candidate{compaction_perc = 45.0, filename = "h"}],
    Block3 = [#candidate{compaction_perc = 70.0, filename = "i"},
                #candidate{compaction_perc = 100.0, filename = "j"},
                #candidate{compaction_perc = 100.0, filename = "k"},
                #candidate{compaction_perc = 100.0, filename = "l"}],
    Block4 = [#candidate{compaction_perc = 55.0, filename = "m"},
                #candidate{compaction_perc = 56.0, filename = "n"},
                #candidate{compaction_perc = 57.0, filename = "o"},
                #candidate{compaction_perc = 40.0, filename = "p"}],
    Block5 = [#candidate{compaction_perc = 60.0, filename = "q"},
                #candidate{compaction_perc = 60.0, filename = "r"}],
    CList0 = Block1 ++ Block2 ++ Block3 ++ Block4 ++ Block5,
    ?assertMatch(["b", "c", "d", "e"], check_bestrun(CList0, Params)),
    CList1 = CList0 ++ [#candidate{compaction_perc = 20.0, filename="s"}],
    ?assertMatch(["s"], check_bestrun(CList1, Params)),
    CList2 = Block4 ++ Block3 ++ Block2 ++ Block1 ++ Block5,
    ?assertMatch(["h", "a", "b", "c"], check_bestrun(CList2, Params)),
    CList3 = Block5 ++ Block1 ++ Block2 ++ Block3 ++ Block4,
    ?assertMatch(["b", "c", "d", "e"],check_bestrun(CList3, Params)).

handle_emptycandidatelist_test() ->
    ?assertMatch({[], 0.0}, assess_candidates([], {4, 60.0, 40.0})).

test_ledgerkey(Key) ->
    {o, "Bucket", Key, null}.

test_inkerkv(SQN, Key, V, IdxSpecs) ->
    leveled_codec:to_inkerkv(test_ledgerkey(Key), SQN, V, IdxSpecs, 
                                native, false).

fetch_testcdb(RP) ->
    FN1 = leveled_inker:filepath(RP, 1, new_journal),
    {ok,
        CDB1} = leveled_cdb:cdb_open_writer(FN1,
                                            #cdb_options{binary_mode=true}),
    {K1, V1} = test_inkerkv(1, "Key1", "Value1", {[], infinity}),
    {K2, V2} = test_inkerkv(2, "Key2", "Value2", {[], infinity}),
    {K3, V3} = test_inkerkv(3, "Key3", "Value3", {[], infinity}),
    {K4, V4} = test_inkerkv(4, "Key1", "Value4", {[], infinity}),
    {K5, V5} = test_inkerkv(5, "Key1", "Value5", {[], infinity}),
    {K6, V6} = test_inkerkv(6, "Key1", "Value6", {[], infinity}),
    {K7, V7} = test_inkerkv(7, "Key1", "Value7", {[], infinity}),
    {K8, V8} = test_inkerkv(8, "Key1", "Value8", {[], infinity}),
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
    RP = "test/test_area/",
    ok = filelib:ensure_dir(leveled_inker:filepath(RP, journal_dir)),
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
    RP = "test/test_area/",
    ok = filelib:ensure_dir(leveled_inker:filepath(RP, journal_dir)),
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
    CDBOpts = #cdb_options{binary_mode=true},
    [{LowSQN, FN, _PidOldR, LastKey}] =
        compact_files([Candidate],
                        CDBOpts#cdb_options{file_path=CompactFP},
                        LedgerFun1,
                        LedgerSrv1,
                        9,
                        [{?STD_TAG, recovr}],
                        native),
    io:format("FN of ~s~n", [FN]),
    ?assertMatch(2, LowSQN),
    {ok, PidR} = leveled_cdb:cdb_reopen_reader(FN, LastKey, CDBOpts),
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
    ?assertMatch({{_, _}, {"Value2", {[], infinity}}}, 
                    leveled_codec:from_inkerkv(RKV1)),
    ok = leveled_cdb:cdb_close(PidR),
    ok = leveled_cdb:cdb_deletepending(CDB),
    ok = leveled_cdb:cdb_destroy(CDB).


compact_single_file_retain_test() ->
    {Candidate,
        LedgerSrv1,
        LedgerFun1,
        CompactFP,
        CDB} = compact_single_file_setup(),
    CDBOpts = #cdb_options{binary_mode=true},
    [{LowSQN, FN, _PidOldR, LastKey}] =
        compact_files([Candidate],
                        CDBOpts#cdb_options{file_path=CompactFP},
                        LedgerFun1,
                        LedgerSrv1,
                        9,
                        [{?STD_TAG, retain}],
                        native),
    io:format("FN of ~s~n", [FN]),
    ?assertMatch(1, LowSQN),
    {ok, PidR} = leveled_cdb:cdb_reopen_reader(FN, LastKey, CDBOpts),
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
    ?assertMatch({{_, _}, {"Value2", {[], infinity}}}, 
                    leveled_codec:from_inkerkv(RKV1)),
    ok = leveled_cdb:cdb_close(PidR),
    ok = leveled_cdb:cdb_deletepending(CDB),
    ok = leveled_cdb:cdb_destroy(CDB).

compact_empty_file_test() ->
    RP = "test/test_area/",
    ok = filelib:ensure_dir(leveled_inker:filepath(RP, journal_dir)),
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
    ?assertMatch(100.0, Score1),
    ok = leveled_cdb:cdb_deletepending(CDB2),
    ok = leveled_cdb:cdb_destroy(CDB2).

compare_candidate_test() ->
    Candidate1 = #candidate{low_sqn=1},
    Candidate2 = #candidate{low_sqn=2},
    Candidate3 = #candidate{low_sqn=3},
    Candidate4 = #candidate{low_sqn=4},
    ?assertMatch([Candidate1, Candidate2, Candidate3, Candidate4],
                sort_run([Candidate3, Candidate2, Candidate4, Candidate1])).       

compact_singlefile_totwosmallfiles_test_() ->
    {timeout, 60, fun compact_singlefile_totwosmallfiles_testto/0}.

compact_singlefile_totwosmallfiles_testto() ->
    RP = "test/test_area/",
    CP = "test/test_area/journal/journal_file/post_compact/",
    ok = filelib:ensure_dir(CP),
    FN1 = leveled_inker:filepath(RP, 1, new_journal),
    CDBoptsLarge = #cdb_options{binary_mode=true, max_size=30000000},
    {ok, CDB1} = leveled_cdb:cdb_open_writer(FN1, CDBoptsLarge),
    lists:foreach(fun(X) ->
                        LK = test_ledgerkey("Key" ++ integer_to_list(X)),
                        Value = leveled_rand:rand_bytes(1024),
                        {IK, IV} = 
                            leveled_codec:to_inkerkv(LK, X, Value, 
                                                        {[], infinity},
                                                        native, true),
                        ok = leveled_cdb:cdb_put(CDB1, IK, IV)
                        end,
                    lists:seq(1, 1000)),
    {ok, NewName} = leveled_cdb:cdb_complete(CDB1),
    {ok, CDBr} = leveled_cdb:cdb_open_reader(NewName),
    CDBoptsSmall = 
        #cdb_options{binary_mode=true, max_size=400000, file_path=CP},
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
                                    [{?STD_TAG, recovr}],
                                    native),
    ?assertMatch(2, length(ManifestSlice)),
    lists:foreach(fun({_SQN, _FN, CDB, _LK}) ->
                        ok = leveled_cdb:cdb_deletepending(CDB),
                        ok = leveled_cdb:cdb_destroy(CDB)
                        end,
                    ManifestSlice),
    ok = leveled_cdb:cdb_deletepending(CDBr),
    ok = leveled_cdb:cdb_destroy(CDBr).

size_score_test() ->
    KeySizeList = 
        [{{1, ?INKT_STND, "Key1"}, 104},
            {{2, ?INKT_STND, "Key2"}, 124},
            {{3, ?INKT_STND, "Key3"}, 144},
            {{4, ?INKT_STND, "Key4"}, 154},
            {{5, ?INKT_STND, "Key5", "Subk1"}, 164},
            {{6, ?INKT_STND, "Key6"}, 174},
            {{7, ?INKT_STND, "Key7"}, 184}],
    MaxSQN = 6,
    CurrentList = ["Key1", "Key4", "Key5", "Key6"],
    FilterFun = fun(L, K, _SQN) -> lists:member(K, L) end,
    Score = size_comparison_score(KeySizeList, FilterFun, CurrentList, MaxSQN),
    ?assertMatch(true, Score > 69.0),
    ?assertMatch(true, Score < 70.0).

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null),
    terminate(error, #state{}).    

-endif.
