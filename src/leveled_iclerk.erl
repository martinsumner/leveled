

-module(leveled_iclerk).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        clerk_new/1,
        clerk_compact/4,
        clerk_remove/2,
        clerk_stop/1,
        code_change/3]).      

-include_lib("eunit/include/eunit.hrl").

-define(JOURNAL_FILEX, "cdb").
-define(PENDING_FILEX, "pnd").
-define(SAMPLE_SIZE, 200).
-define(BATCH_SIZE, 16).
-define(BATCHES_TO_CHECK, 8).
%% How many consecutive files to compact in one run
-define(MAX_COMPACTION_RUN, 4).
%% Sliding scale to allow preference of longer runs up to maximum
-define(SINGLEFILE_COMPACTION_TARGET, 60.0).
-define(MAXRUN_COMPACTION_TARGET, 80.0).

-record(state, {inker :: pid(),
                    max_run_length :: integer(),
                    cdb_options}).

-record(candidate, {low_sqn :: integer(),
                    filename :: string(),
                    journal :: pid(),
                    compaction_perc :: float()}).


%%%============================================================================
%%% API
%%%============================================================================

clerk_new(InkerClerkOpts) ->
    gen_server:start(?MODULE, [InkerClerkOpts], []).
    
clerk_remove(Pid, Removals) ->
    gen_server:cast(Pid, {remove, Removals}),
    ok.

clerk_compact(Pid, Penciller, Inker, Timeout) ->
    clerk_compact(Pid, Penciller, Inker, Timeout).

clerk_stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([IClerkOpts]) ->
    case IClerkOpts#iclerk_options.max_run_length of
        undefined ->
            {ok, #state{max_run_length = ?MAX_COMPACTION_RUN,
                        inker = IClerkOpts#iclerk_options.inker,
                        cdb_options = IClerkOpts#iclerk_options.cdb_options}};
        MRL ->
            {ok, #state{max_run_length = MRL,
                        inker = IClerkOpts#iclerk_options.inker,
                        cdb_options = IClerkOpts#iclerk_options.cdb_options}}
    end.

handle_call(_Msg, _From, State) ->
    {reply, not_supprted, State}.

handle_cast({compact, Penciller, Inker, _Timeout}, State) ->
    % Need to fetch manifest at start rather than have it be passed in
    % Don't want to process a queued call waiting on an old manifest
    Manifest = leveled_inker:ink_getmanifest(Inker),
    MaxRunLength = State#state.max_run_length,
    PclOpts = #penciller_options{start_snapshot = true,
                                    source_penciller = Penciller,
                                    requestor = self()},
    FilterFun = fun leveled_penciller:pcl_checksequencenumber/3,
    FilterServer = leveled_penciller:pcl_start(PclOpts),
    ok = leveled_penciller:pcl_loadsnapshot(FilterServer, []),
    Candidates = scan_all_files(Manifest,
                                FilterFun,
                                FilterServer),
    BestRun = assess_candidates(Candidates, MaxRunLength),
    case score_run(BestRun, MaxRunLength) of
        Score when Score > 0 ->
            print_compaction_run(BestRun, MaxRunLength),
            CDBopts = State#state.cdb_options,
            {ManifestSlice,
                PromptDelete} = compact_files(BestRun,
                                                CDBopts,
                                                FilterFun,
                                                FilterServer),
            FilesToDelete = lists:map(fun(C) ->
                                            {C#candidate.low_sqn,
                                                C#candidate.filename,
                                                C#candidate.journal}
                                            end,
                                        BestRun),
            ok = leveled_inker:ink_updatemanifest(Inker,
                                                    ManifestSlice,
                                                    PromptDelete,
                                                    FilesToDelete),
            {noreply, State};
        Score ->
            io:format("No compaction run as highest score=~w~n", [Score]),
            {noreply, State}
    end;
handle_cast({remove, _Removals}, State) ->
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================


check_single_file(CDB, FilterFun, FilterServer, SampleSize, BatchSize) ->
    PositionList = leveled_cdb:cdb_getpositions(CDB, SampleSize),
    KeySizeList = fetch_inbatches(PositionList, BatchSize, CDB, []),
    R0 = lists:foldl(fun(KS, {ActSize, RplSize}) ->
                            {{SQN, PK}, Size} = KS,
                            Check = FilterFun(FilterServer, PK, SQN),
                            case Check of
                                true ->
                                    {ActSize + Size, RplSize};
                                false ->
                                    {ActSize, RplSize + Size}
                            end end,
                        {0, 0},
                        KeySizeList),
    {ActiveSize, ReplacedSize} = R0,
    100 * ActiveSize / (ActiveSize + ReplacedSize).

scan_all_files(Manifest, FilterFun, FilterServer) ->
    scan_all_files(Manifest, FilterFun, FilterServer, []).

scan_all_files([], _FilterFun, _FilterServer, CandidateList) ->
    CandidateList;
scan_all_files([Entry|Tail], FilterFun, FilterServer, CandidateList) ->
    {LowSQN, FN, JournalP} = Entry,
    CpctPerc = check_single_file(JournalP,
                                    FilterFun,
                                    FilterServer,
                                    ?SAMPLE_SIZE,
                                    ?BATCH_SIZE),
    scan_all_files(Tail,
                    FilterFun,
                    FilterServer,
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

assess_candidates(AllCandidates, MaxRunLength) ->
    NaiveBestRun = assess_candidates(AllCandidates, MaxRunLength, [], []),
    case length(AllCandidates) of
        L when L > MaxRunLength, MaxRunLength > 1 ->
            %% Assess with different offsets from the start
            SqL = lists:seq(1, MaxRunLength - 1),
            lists:foldl(fun(Counter, BestRun) ->
                                SubList = lists:nthtail(Counter,
                                                            AllCandidates),
                                assess_candidates(SubList,
                                                    MaxRunLength,
                                                    [],
                                                    BestRun)
                                end,
                            NaiveBestRun,
                            SqL);
        _ ->
            NaiveBestRun
    end.

assess_candidates([], _MaxRunLength, _CurrentRun0, BestAssessment) ->
    BestAssessment;
assess_candidates([HeadC|Tail], MaxRunLength, CurrentRun0, BestAssessment) ->
    CurrentRun1 = choose_best_assessment(CurrentRun0 ++ [HeadC],
                                            [HeadC],
                                            MaxRunLength),
    assess_candidates(Tail,
                        MaxRunLength,
                        CurrentRun1,
                        choose_best_assessment(CurrentRun1,
                                                BestAssessment,
                                                MaxRunLength)).


choose_best_assessment(RunToAssess, BestRun, MaxRunLength) ->
    case length(RunToAssess) of
        LR1 when LR1 > MaxRunLength ->
            BestRun;
        _ ->
            AssessScore = score_run(RunToAssess, MaxRunLength),
            BestScore = score_run(BestRun, MaxRunLength),
            if
                AssessScore > BestScore ->
                    RunToAssess;
                true ->
                    BestRun
            end
    end.    
        
score_run([], _MaxRunLength) ->
    0.0;
score_run(Run, MaxRunLength) ->
    TargetIncr = case MaxRunLength of
                        1 ->
                            0.0;
                        MaxRunSize ->
                            (?MAXRUN_COMPACTION_TARGET
                                - ?SINGLEFILE_COMPACTION_TARGET)
                                    / (MaxRunSize - 1)
                    end,
    Target = ?SINGLEFILE_COMPACTION_TARGET +  TargetIncr * (length(Run) - 1),
    RunTotal = lists:foldl(fun(Cand, Acc) ->
                                Acc + Cand#candidate.compaction_perc end,
                            0.0,
                            Run),
    Target - RunTotal / length(Run).


print_compaction_run(BestRun, MaxRunLength) ->
    io:format("Compaction to be performed on ~w files with score of ~w~n",
                    [length(BestRun), score_run(BestRun, MaxRunLength)]),
    lists:foreach(fun(File) ->
                        io:format("Filename ~s is part of compaction run~n",
                                        [File#candidate.filename])
                        end,
                    BestRun).

compact_files([], _CDBopts, _FilterFun, _FilterServer) ->
    {[], 0};
compact_files(BestRun, CDBopts, FilterFun, FilterServer) ->
    BatchesOfPositions = get_all_positions(BestRun, []),
    compact_files(BatchesOfPositions,
                                CDBopts,
                                null,
                                FilterFun,
                                FilterServer,
                                [],
                                true).

compact_files([], _CDBopts, _ActiveJournal0, _FilterFun, _FilterServer,
                            ManSlice0, PromptDelete0) ->
    %% Need to close the active file
    {ManSlice0, PromptDelete0};
compact_files([Batch|T], CDBopts, ActiveJournal0, FilterFun, FilterServer,
                            ManSlice0, PromptDelete0) ->
    {SrcJournal, PositionList} = Batch,
    KVCs0 = leveled_cdb:cdb_directfetch(SrcJournal,
                                        PositionList,
                                        key_value_check),
    R0 = filter_output(KVCs0,
                        FilterFun,
                        FilterServer),
    {KVCs1, PromptDelete1} = R0,
    PromptDelete2 = case {PromptDelete0, PromptDelete1} of
                        {true, true} ->
                            true;
                        _ ->
                            false
                    end,
    {ActiveJournal1, ManSlice1} = write_values(KVCs1,
                                                CDBopts, 
                                                ActiveJournal0,
                                                ManSlice0),
    compact_files(T, CDBopts, ActiveJournal1, FilterFun, FilterServer,
                                ManSlice1, PromptDelete2).

get_all_positions([], PositionBatches) ->
    PositionBatches;
get_all_positions([HeadRef|RestOfBest], PositionBatches) ->
    SrcJournal = HeadRef#candidate.journal,
    Positions = leveled_cdb:cdb_getpositions(SrcJournal, all),
    Batches = split_positions_into_batches(Positions, SrcJournal, []),
    get_all_positions(RestOfBest, PositionBatches ++ Batches).

split_positions_into_batches([], _Journal, Batches) ->
    Batches;
split_positions_into_batches(Positions, Journal, Batches) ->
    {ThisBatch, Tail} = lists:split(?BATCH_SIZE, Positions),
    split_positions_into_batches(Tail,
                                    Journal,
                                    Batches ++ [{Journal, ThisBatch}]).


filter_output(KVCs, FilterFun, FilterServer) ->
    lists:foldl(fun(KVC, {Acc, PromptDelete}) ->
                        {{SQN, PK}, _V, CrcCheck} = KVC,
                        KeyValid = FilterFun(FilterServer, PK, SQN),
                        case {KeyValid, CrcCheck} of
                            {true, true} ->
                                {Acc ++ [KVC], PromptDelete};
                            {false, _} ->
                                {Acc, PromptDelete};
                            {_, false} ->
                                io:format("Corrupted value found for " ++ "
                                            Key ~w at SQN ~w~n", [PK, SQN]),
                                {Acc, false}
                        end
                        end,
                    {[], true},
                    KVCs).
    

write_values([KVC|Rest], CDBopts, ActiveJournal0, ManSlice0) ->
    {{SQN, PK}, V, _CrcCheck} = KVC,
    {ok, ActiveJournal1} = case ActiveJournal0 of
                                null ->
                                    FP = CDBopts#cdb_options.file_path,
                                    FN = leveled_inker:filepath(FP,
                                                                SQN,
                                                                new_journal),
                                    leveled_cdb:cdb_open_writer(FN,
                                                                CDBopts);
                                _ ->
                                    {ok, ActiveJournal0}
                            end,
    R = leveled_cdb:cdb_put(ActiveJournal1, {SQN, PK}, V),
    case R of
        ok ->
            write_values(Rest, CDBopts, ActiveJournal1, ManSlice0);
        roll ->
            {ok, NewFN} = leveled_cdb:cdb_complete(ActiveJournal1),
            {ok, PidR} = leveled_cdb:cdb_open_reader(NewFN),
            {StartSQN, _PK} = leveled_cdb:cdb_firstkey(PidR),
            ManSlice1 = ManSlice0 ++ [{StartSQN, NewFN, PidR}],
            write_values(Rest, CDBopts, null, ManSlice1)
    end.
    

                        
                    
    

    
    

%%%============================================================================
%%% Test
%%%============================================================================


-ifdef(TEST).

simple_score_test() ->
    Run1 = [#candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 76.0},
            #candidate{compaction_perc = 70.0}],
    ?assertMatch(6.0, score_run(Run1, 4)),
    Run2 = [#candidate{compaction_perc = 75.0}],
    ?assertMatch(-15.0, score_run(Run2, 4)),
    ?assertMatch(0.0, score_run([], 4)).

score_compare_test() ->
    Run1 = [#candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 75.0},
            #candidate{compaction_perc = 76.0},
            #candidate{compaction_perc = 70.0}],
    ?assertMatch(6.0, score_run(Run1, 4)),
    Run2 = [#candidate{compaction_perc = 75.0}],
    ?assertMatch(Run1, choose_best_assessment(Run1, Run2, 4)),
    ?assertMatch(Run2, choose_best_assessment(Run1 ++ Run2, Run2, 4)).

find_bestrun_test() ->
%% Tests dependent on these defaults
%% -define(MAX_COMPACTION_RUN, 4).
%% -define(SINGLEFILE_COMPACTION_TARGET, 60.0).
%% -define(MAXRUN_COMPACTION_TARGET, 80.0).
%% Tested first with blocks significant as no back-tracking
    Block1 = [#candidate{compaction_perc = 75.0},
                #candidate{compaction_perc = 85.0},
                #candidate{compaction_perc = 62.0},
                #candidate{compaction_perc = 70.0}],
    Block2 = [#candidate{compaction_perc = 58.0},
                #candidate{compaction_perc = 95.0},
                #candidate{compaction_perc = 95.0},
                #candidate{compaction_perc = 65.0}],
    Block3 = [#candidate{compaction_perc = 90.0},
                #candidate{compaction_perc = 100.0},
                #candidate{compaction_perc = 100.0},
                #candidate{compaction_perc = 100.0}],
    Block4 = [#candidate{compaction_perc = 75.0},
                #candidate{compaction_perc = 76.0},
                #candidate{compaction_perc = 76.0},
                #candidate{compaction_perc = 60.0}],
    Block5 = [#candidate{compaction_perc = 80.0},
                #candidate{compaction_perc = 80.0}],
    CList0 = Block1 ++ Block2 ++ Block3 ++ Block4 ++ Block5,
    ?assertMatch(Block4, assess_candidates(CList0, 4, [], [])),
    CList1 = CList0 ++ [#candidate{compaction_perc = 20.0}],
    ?assertMatch([#candidate{compaction_perc = 20.0}],
                    assess_candidates(CList1, 4, [], [])),
    CList2 = Block4 ++ Block3 ++ Block2 ++ Block1 ++ Block5,
    ?assertMatch(Block4, assess_candidates(CList2, 4, [], [])),
    CList3 = Block5 ++ Block1 ++ Block2 ++ Block3 ++ Block4,
    ?assertMatch([#candidate{compaction_perc = 62.0},
                        #candidate{compaction_perc = 70.0},
                        #candidate{compaction_perc = 58.0}],
                    assess_candidates(CList3, 4, [], [])),
    %% Now do some back-tracking to get a genuinely optimal solution without
    %% needing to re-order
    ?assertMatch([#candidate{compaction_perc = 62.0},
                        #candidate{compaction_perc = 70.0},
                        #candidate{compaction_perc = 58.0}],
                    assess_candidates(CList0, 4)),
    ?assertMatch([#candidate{compaction_perc = 62.0},
                        #candidate{compaction_perc = 70.0},
                        #candidate{compaction_perc = 58.0}],
                    assess_candidates(CList0, 5)),
    ?assertMatch([#candidate{compaction_perc = 62.0},
                        #candidate{compaction_perc = 70.0},
                        #candidate{compaction_perc = 58.0},
                        #candidate{compaction_perc = 95.0},
                        #candidate{compaction_perc = 95.0},
                        #candidate{compaction_perc = 65.0}], 
                    assess_candidates(CList0, 6)).

check_single_file_test() ->
    RP = "../test/journal",
    FN1 = leveled_inker:filepath(RP, 1, new_journal),
    {ok, CDB1} = leveled_cdb:cdb_open_writer(FN1, #cdb_options{}),
    {K1, V1} = {{1, "Key1"}, term_to_binary("Value1")},
    {K2, V2} = {{2, "Key2"}, term_to_binary("Value2")},
    {K3, V3} = {{3, "Key3"}, term_to_binary("Value3")},
    {K4, V4} = {{4, "Key1"}, term_to_binary("Value4")},
    {K5, V5} = {{5, "Key1"}, term_to_binary("Value5")},
    {K6, V6} = {{6, "Key1"}, term_to_binary("Value6")},
    {K7, V7} = {{7, "Key1"}, term_to_binary("Value7")},
    {K8, V8} = {{8, "Key1"}, term_to_binary("Value8")},
    ok = leveled_cdb:cdb_put(CDB1, K1, V1),
    ok = leveled_cdb:cdb_put(CDB1, K2, V2),
    ok = leveled_cdb:cdb_put(CDB1, K3, V3),
    ok = leveled_cdb:cdb_put(CDB1, K4, V4),
    ok = leveled_cdb:cdb_put(CDB1, K5, V5),
    ok = leveled_cdb:cdb_put(CDB1, K6, V6),
    ok = leveled_cdb:cdb_put(CDB1, K7, V7),
    ok = leveled_cdb:cdb_put(CDB1, K8, V8),
    {ok, FN2} = leveled_cdb:cdb_complete(CDB1),
    {ok, CDB2} = leveled_cdb:cdb_open_reader(FN2),
    LedgerSrv1 = [{8, "Key1"}, {2, "Key2"}, {3, "Key3"}],
    LedgerFun1 = fun(Srv, Key, ObjSQN) ->
                    case lists:keyfind(ObjSQN, 1, Srv) of
                        {ObjSQN, Key} ->
                            true;
                        _ ->
                            false
                    end end,
    Score1 = check_single_file(CDB2, LedgerFun1, LedgerSrv1, 8, 4),
    ?assertMatch(37.5, Score1),
    LedgerFun2 = fun(_Srv, _Key, _ObjSQN) -> true end,
    Score2 = check_single_file(CDB2, LedgerFun2, LedgerSrv1, 8, 4),
    ?assertMatch(100.0, Score2),
    Score3 = check_single_file(CDB2, LedgerFun1, LedgerSrv1, 8, 3),
    ?assertMatch(37.5, Score3),
    ok = leveled_cdb:cdb_destroy(CDB2).
    

-endif.