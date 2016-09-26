

-module(leveled_iclerk).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        clerk_new/1,
        clerk_compact/5,
        clerk_remove/2,
        clerk_stop/1,
        code_change/3]).      

-include_lib("eunit/include/eunit.hrl").

-define(SAMPLE_SIZE, 200).
-define(BATCH_SIZE, 16).
-define(BATCHES_TO_CHECK, 8).

-record(state, {owner :: pid(),
                penciller_snapshot :: pid()}).

%%%============================================================================
%%% API
%%%============================================================================

clerk_new(Owner) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    ok = gen_server:call(Pid, {register, Owner}, infinity),
    {ok, Pid}.
    
clerk_remove(Pid, Removals) ->
    gen_server:cast(Pid, {remove, Removals}),
    ok.

clerk_compact(Pid, Manifest, ManifestSQN, Penciller, Timeout) ->
    gen_server:cast(Pid, {compact, Manifest, ManifestSQN, Penciller, Timeout}).

clerk_stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register, Owner}, _From, State) ->
    {reply, ok, State#state{owner=Owner}}.

handle_cast({compact, Manifest, _ManifestSQN, Penciller, _Timeout}, State) ->
    PclOpts = #penciller_options{start_snapshot = true,
                                    source_penciller = Penciller,
                                    requestor = self()},
    PclSnap = leveled_penciller:pcl_start(PclOpts),
    ok = leveled_penciller:pcl_loadsnapshot(PclSnap, []),
    _CandidateList = scan_all_files(Manifest, PclSnap),
    %% TODO - Lots
    {noreply, State};
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


check_single_file(CDB, PclSnap, SampleSize, BatchSize) ->
    PositionList = leveled_cdb:cdb_getpositions(CDB, SampleSize),
    KeySizeList = fetch_inbatches(PositionList, BatchSize, CDB, []),
    R0 = lists:foldl(fun(KS, {ActSize, RplSize}) ->
                            {{PK, SQN}, Size} = KS,
                            Chk = leveled_pcl:pcl_checksequencenumber(PclSnap,
                                                                        PK,
                                                                        SQN),
                            case Chk of
                                true ->
                                    {ActSize + Size, RplSize};
                                false ->
                                    {ActSize, RplSize + Size}
                            end end,
                        {0, 0},
                        KeySizeList),
    {ActiveSize, ReplacedSize} = R0,
    100 * (ActiveSize / (ActiveSize + ReplacedSize)).

scan_all_files(Manifest, Penciller) ->
    scan_all_files(Manifest, Penciller, []).

scan_all_files([], _Penciller, CandidateList) ->
    CandidateList;
scan_all_files([{LowSQN, FN, JournalP}|Tail], Penciller, CandidateList) ->
    CompactPerc = check_single_file(JournalP,
                                        Penciller,
                                        ?SAMPLE_SIZE,
                                        ?BATCH_SIZE),
    scan_all_files(Tail, Penciller, CandidateList ++
                                        [{LowSQN, FN, JournalP, CompactPerc}]).

fetch_inbatches([], _BatchSize, _CDB, CheckedList) ->
    CheckedList;
fetch_inbatches(PositionList, BatchSize, CDB, CheckedList) ->
    {Batch, Tail} = lists:split(BatchSize, PositionList),
    KL_List = leveled_cdb:direct_fetch(CDB, Batch, key_size),
    fetch_inbatches(Tail, BatchSize, CDB, CheckedList ++ KL_List).





%%%============================================================================
%%% Test
%%%============================================================================
