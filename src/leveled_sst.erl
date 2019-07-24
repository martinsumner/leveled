%% -------- SST (Variant) ---------
%%
%% A FSM module intended to wrap a persisted, ordered view of Keys and Values
%%
%% The persisted view is built from a list (which may be created by merging
%% multiple lists).  The list is built first, then the view is created in bulk.
%%
%% -------- Slots ---------
%%
%% The view is built from sublists referred to as slot.  Each slot is up to 128
%% keys and values in size.  Three strategies have been benchmarked for the 
%% slot: a skiplist, a gb-tree, four blocks of flat lists with an index. 
%%
%% Skiplist:
%% build and serialise slot - 3233 microseconds
%% de-serialise and check * 128 - 14669 microseconds
%% flatten back to list - 164 microseconds
%%
%% GBTree:
%% build and serialise tree - 1433 microseconds
%% de-serialise and check * 128 - 15263 microseconds
%% flatten back to list - 175 microseconds
%%
%% Indexed Blocks:
%% build and serialise slot 342 microseconds
%% de-deserialise and check * 128 - 6746 microseconds
%% flatten back to list - 187 microseconds
%%
%% The negative side of using Indexed Blocks is the storage of the index.  In 
%% the original implementation this was stored on fadvised disk (the index in 
%% this case was a rice-encoded view of which block the object is in).  In this
%% implementation it is cached in memory -requiring 2-bytes per key to be kept
%% in memory.
%%
%% -------- Blooms ---------
%%
%% There is a bloom for each slot - based on two hashes and 8 bits per key.
%%
%% Hashing for blooms is a challenge, as the slot is a slice of an ordered
%% list of keys with a fixed format.  It is likely that the keys may vary by
%% only one or two ascii characters, and there is a desire to avoid the
%% overhead of cryptographic hash functions that may be able to handle this.
%%
%% -------- Summary ---------
%%
%% Each file has a summary - which is the 128 keys at the top of each slot in 
%% a skiplist, with some basic metadata about the slot stored as the value.
%%
%% The summary is stored seperately to the slots (within the same file).
%%
%% -------- CRC Checks ---------
%%
%% Every attempt to either read a summary or a slot off disk will also include 
%% a CRC check.  If the CRC check fails non-presence is assumed (the data 
%% within is assumed to be entirely lost).  The data can be recovered by either
%% using a recoverable strategy in transaction log compaction, and triggering
%% the transaction log replay; or by using a higher level for of anti-entropy
%% (i.e. make Riak responsible).


-module(leveled_sst).

-behaviour(gen_fsm).

-ifdef(fsm_deprecated).
-compile({nowarn_deprecated_function, 
            [{gen_fsm, start_link, 3},
                {gen_fsm, sync_send_event, 2},
                {gen_fsm, sync_send_event, 3},
                {gen_fsm, send_event, 2},
                {gen_fsm, send_all_state_event, 2}]}).
-endif.

-include("include/leveled.hrl").

-define(LOOK_SLOTSIZE, 128). % Maximum of 128
-define(LOOK_BLOCKSIZE, {24, 32}). % 4x + y = ?LOOK_SLOTSIZE
-define(NOLOOK_SLOTSIZE, 256).
-define(NOLOOK_BLOCKSIZE, {56, 32}). % 4x + y = ?NOLOOK_SLOTSIZE
-define(COMPRESSION_LEVEL, 1).
-define(BINARY_SETTINGS, [{compressed, ?COMPRESSION_LEVEL}]).
-define(MERGE_SCANWIDTH, 16).
-define(DISCARD_EXT, ".discarded").
-define(DELETE_TIMEOUT, 10000).
-define(TREE_TYPE, idxt).
-define(TREE_SIZE, 4).
-define(TIMING_SAMPLECOUNTDOWN, 20000).
-define(TIMING_SAMPLESIZE, 100).
-define(CACHE_SIZE, 32).
-define(BLOCK_LENGTHS_LENGTH, 20).
-define(LMD_LENGTH, 4).
-define(FLIPPER32, 4294967295).
-define(COMPRESS_AT_LEVEL, 1).
-define(INDEX_MODDATE, true).
-define(USE_SET_FOR_SPEED, 64).
-define(STARTUP_TIMEOUT, 10000).

-include_lib("eunit/include/eunit.hrl").

-export([init/1,
        handle_sync_event/4,
        handle_event/3,
        handle_info/3,
        terminate/3,
        code_change/4,
        starting/2,
        starting/3,
        reader/2,
        reader/3,
        delete_pending/2,
        delete_pending/3]).

-export([sst_new/6,
            sst_new/8,
            sst_newlevelzero/7,
            sst_open/4,
            sst_get/2,
            sst_get/3,
            sst_expandpointer/5,
            sst_getmaxsequencenumber/1,
            sst_setfordelete/2,
            sst_clear/1,
            sst_checkready/1,
            sst_switchlevels/2,
            sst_deleteconfirmed/1,
            sst_close/1]).

-export([tune_seglist/1, extract_hash/1, member_check/2]).

-export([in_range/3]).

-record(slot_index_value, {slot_id :: integer(),
                            start_position :: integer(),
                            length :: integer()}).

-record(summary,    {first_key :: tuple(),
                        last_key :: tuple(),
                        index :: tuple() | undefined,
                        size :: integer(),
                        max_sqn :: integer()}).

-type press_method() 
        :: lz4|native|none.
-type range_endpoint() 
        :: all|leveled_codec:ledger_key().
-type slot_pointer() 
        :: {pointer, pid(), integer(), range_endpoint(), range_endpoint()}.
-type sst_pointer() 
            % Used in sst_new
        :: {next, 
            leveled_pmanifest:manifest_entry(), 
            range_endpoint()}.
-type sst_closed_pointer() 
            % used in expand_list_by_pointer 
            % (close point is added by maybe_expand_pointer
        :: {next, 
            leveled_pmanifest:manifest_entry(), 
            range_endpoint(),
            range_endpoint()}.
-type expandable_pointer() 
        :: slot_pointer()|sst_closed_pointer().
-type expanded_pointer() 
        :: leveled_codec:ledger_kv()|expandable_pointer().
-type binaryslot_element()
        :: {tuple(), tuple()}|{binary(), integer(), tuple(), tuple()}.
-type tuned_seglist()
        :: false|
            {sets, sets:set(non_neg_integer())}|
            {list, list(non_neg_integer())}.
-type sst_options() 
        :: #sst_options{}. 

%% yield_blockquery is used to detemrine if the work necessary to process a
%% range query beyond the fetching the slot should be managed from within
%% this process, or should be handled by the calling process.
%% Handling within the calling process may lead to extra binary heap garbage
%% see Issue 52.  Handling within the SST process may lead to contention and
%% extra copying.  Files at the top of the tree yield, those lower down don't.

-record(state,      
                {summary,
                    handle :: file:fd() | undefined,
                    penciller :: pid() | undefined | false,
                    root_path,
                    filename,
                    yield_blockquery = false :: boolean(),
                    blockindex_cache,
                    compression_method = native :: press_method(),
                    index_moddate = ?INDEX_MODDATE :: boolean(),
                    timings = no_timing :: sst_timings(),
                    timings_countdown = 0 :: integer(),
                    starting_pid :: pid()|undefined,
                    fetch_cache = array:new([{size, ?CACHE_SIZE}]),
                    new_slots :: list()|undefined,
                    deferred_startup_tuple :: tuple()|undefined,
                    level :: non_neg_integer()|undefined}).

-record(sst_timings, 
                {sample_count = 0 :: integer(),
                    index_query_time = 0 :: integer(),
                    lookup_cache_time = 0 :: integer(),
                    slot_index_time = 0 :: integer(),
                    fetch_cache_time = 0 :: integer(),
                    slot_fetch_time = 0 :: integer(),
                    noncached_block_time = 0 :: integer(),
                    lookup_cache_count = 0 :: integer(),
                    slot_index_count = 0 :: integer(),
                    fetch_cache_count = 0 :: integer(),
                    slot_fetch_count = 0 :: integer(),
                    noncached_block_count = 0 :: integer()}).

-record(build_timings,
                {slot_hashlist = 0 :: integer(),
                    slot_serialise = 0 :: integer(),
                    slot_finish = 0 :: integer(),
                    fold_toslot = 0 :: integer()}).

-type sst_state() :: #state{}.
-type sst_timings() :: no_timing|#sst_timings{}.
-type build_timings() :: no_timing|#build_timings{}.

-export_type([expandable_pointer/0, press_method/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec sst_open(string(), string(), sst_options(), non_neg_integer())
            -> {ok, pid(), 
                    {leveled_codec:ledger_key(), leveled_codec:ledger_key()}, 
                    binary()}.
%% @doc
%% Open an SST file at a given path and filename.  The first and last keys
%% are returned in response to the request - so that those keys can be used
%% in manifests to understand what range of keys are covered by the SST file.
%% All keys in the file should be between the first and last key in erlang
%% term order.
%%
%% The filename should include the file extension.
sst_open(RootPath, Filename, OptsSST, Level) ->
    {ok, Pid} = gen_fsm:start_link(?MODULE, [], []),
    case gen_fsm:sync_send_event(Pid,
                                    {sst_open,
                                        RootPath, Filename, OptsSST, Level},
                                    infinity) of
        {ok, {SK, EK}, Bloom} ->
            {ok, Pid, {SK, EK}, Bloom}
    end.

-spec sst_new(string(), string(), integer(), 
                    list(leveled_codec:ledger_kv()), 
                    integer(), sst_options()) 
            -> {ok, pid(), 
                    {leveled_codec:ledger_key(), leveled_codec:ledger_key()}, 
                    binary()}.
%% @doc
%% Start a new SST file at the assigned level passing in a list of Key, Value
%% pairs.  This should not be used for basement levels or unexpanded Key/Value
%% lists as merge_lists will not be called.
sst_new(RootPath, Filename, Level, KVList, MaxSQN, OptsSST) ->
    sst_new(RootPath, Filename, Level,
            KVList, MaxSQN, OptsSST, ?INDEX_MODDATE).

sst_new(RootPath, Filename, Level, KVList, MaxSQN, OptsSST, IndexModDate) ->
    {ok, Pid} = gen_fsm:start_link(?MODULE, [], []),
    PressMethod0 = compress_level(Level, OptsSST#sst_options.press_method),
    OptsSST0 = OptsSST#sst_options{press_method = PressMethod0},
    {[], [], SlotList, FK}  =
        merge_lists(KVList, OptsSST0, IndexModDate),
    case gen_fsm:sync_send_event(Pid,
                                    {sst_new,
                                        RootPath,
                                        Filename,
                                        Level,
                                        {SlotList, FK},
                                        MaxSQN,
                                        OptsSST0,
                                        IndexModDate,
                                        self()},
                                    infinity) of
        {ok, {SK, EK}, Bloom} ->
            {ok, Pid, {SK, EK}, Bloom}
    end.

-spec sst_new(string(), string(), 
                list(leveled_codec:ledger_kv()|sst_pointer()), 
                list(leveled_codec:ledger_kv()|sst_pointer()),
                boolean(), integer(), 
                integer(), sst_options())
            -> empty|{ok, pid(), 
                {{list(leveled_codec:ledger_kv()), 
                        list(leveled_codec:ledger_kv())}, 
                    leveled_codec:ledger_key(), 
                    leveled_codec:ledger_key()}, 
                    binary()}.
%% @doc
%% Start a new SST file at the assigned level passing in a two lists of
%% {Key, Value} pairs to be merged.  The merge_lists function will use the
%% IsBasement boolean to determine if expired keys or tombstones can be
%% deleted.
%%
%% The remainder of the lists is returned along with the StartKey and EndKey
%% so that the remainder cna be  used in the next file in the merge.  It might
%% be that the merge_lists returns nothing (for example when a basement file is
%% all tombstones) - and the atome empty is returned in this case so that the
%% file is not added to the manifest.
sst_new(RootPath, Filename, 
        KVL1, KVL2, IsBasement, Level, 
        MaxSQN, OptsSST) ->
    sst_new(RootPath, Filename, 
        KVL1, KVL2, IsBasement, Level, 
        MaxSQN, OptsSST, ?INDEX_MODDATE).

sst_new(RootPath, Filename, 
        KVL1, KVL2, IsBasement, Level, 
        MaxSQN, OptsSST, IndexModDate) ->
    PressMethod0 = compress_level(Level, OptsSST#sst_options.press_method),
    OptsSST0 = OptsSST#sst_options{press_method = PressMethod0},
    {Rem1, Rem2, SlotList, FK} = 
        merge_lists(KVL1, KVL2, {IsBasement, Level},
                    OptsSST0, IndexModDate),
    case SlotList of
        [] ->
            empty;
        _ ->
            {ok, Pid} = gen_fsm:start_link(?MODULE, [], []),
            case gen_fsm:sync_send_event(Pid,
                                            {sst_new,
                                                RootPath,
                                                Filename,
                                                Level,
                                                {SlotList, FK},
                                                MaxSQN,
                                                OptsSST0,
                                                IndexModDate,
                                                self()},
                                            infinity) of
                {ok, {SK, EK}, Bloom} ->
                    {ok, Pid, {{Rem1, Rem2}, SK, EK}, Bloom}
            end
    end.

-spec sst_newlevelzero(string(), string(),
                            integer(), fun()|list(), pid()|undefined, integer(), 
                            sst_options()) ->
                                        {ok, pid(), noreply}.
%% @doc
%% Start a new file at level zero.  At this level the file size is not fixed -
%% it will be as big as the input.  Also the KVList is not passed in, it is 
%% fetched slot by slot using the FetchFun
sst_newlevelzero(RootPath, Filename, 
                    Slots, Fetcher, Penciller,
                    MaxSQN, OptsSST) ->
    PressMethod0 = compress_level(0, OptsSST#sst_options.press_method),
    OptsSST0 = OptsSST#sst_options{press_method = PressMethod0},
    {ok, Pid} = gen_fsm:start_link(?MODULE, [], []),
    % Initiate the file into the "starting" state
    ok = gen_fsm:sync_send_event(Pid,
                                {sst_newlevelzero,
                                    RootPath,
                                    Filename,
                                    Penciller,
                                    MaxSQN,
                                    OptsSST0,
                                    ?INDEX_MODDATE}, 
                                infinity),
    ok = 
        case is_list(Fetcher) of
            true ->
                gen_fsm:send_event(Pid, {complete_l0startup, Fetcher});
            false ->
                % Fetcher is a function
                gen_fsm:send_event(Pid, {sst_returnslot, none, Fetcher, Slots})
                % Start the fetch loop (async).  Having the fetch loop running
                % on async message passing means that the SST file can now be
                % closed while the fetch loop is still completing
        end,
    {ok, Pid, noreply}.


-spec sst_get(pid(), leveled_codec:ledger_key())
                                    -> leveled_codec:ledger_kv()|not_present.
%% @doc
%% Return a Key, Value pair matching a Key or not_present if the Key is not in
%% the store.  The segment_hash function is used to accelerate the seeking of
%% keys, sst_get/3 should be used directly if this has already been calculated
sst_get(Pid, LedgerKey) ->
    sst_get(Pid, LedgerKey, leveled_codec:segment_hash(LedgerKey)).

-spec sst_get(pid(), leveled_codec:ledger_key(), leveled_codec:segment_hash())
                                    -> leveled_codec:ledger_kv()|not_present.
%% @doc
%% Return a Key, Value pair matching a Key or not_present if the Key is not in
%% the store (with the magic hash precalculated).
sst_get(Pid, LedgerKey, Hash) ->
    gen_fsm:sync_send_event(Pid, {get_kv, LedgerKey, Hash}, infinity).

-spec sst_getmaxsequencenumber(pid()) -> integer().
%% @doc
%% Get the maximume sequence number for this SST file
sst_getmaxsequencenumber(Pid) ->
    gen_fsm:sync_send_event(Pid, get_maxsequencenumber, infinity).

-spec sst_expandpointer(expandable_pointer(), 
                            list(expandable_pointer()), 
                            pos_integer(),
                            leveled_codec:segment_list(),
                            non_neg_integer())
                                            -> list(expanded_pointer()).
%% @doc
%% Expand out a list of pointer to return a list of Keys and Values with a
%% tail of pointers (once the ScanWidth has been satisfied).
%% Folding over keys in a store uses this function, although this function
%% does not directly call the gen_server - it does so by sst_getfilteredslots
%% or sst_getfilteredrange depending on the nature of the pointer.
sst_expandpointer(Pointer, MorePointers, ScanWidth, SegmentList, LowLastMod) ->
    expand_list_by_pointer(Pointer, MorePointers, ScanWidth,
                            SegmentList, LowLastMod).


-spec sst_setfordelete(pid(), pid()|false) -> ok.
%% @doc
%% If the SST is no longer in use in the active ledger it can be set for
%% delete.  Once set for delete it will poll the Penciller pid to see if
%% it is yet safe to be deleted (i.e. because all snapshots which depend
%% on it have finished).  No polling will be done if the Penciller pid
%% is 'false'
sst_setfordelete(Pid, Penciller) ->
    gen_fsm:sync_send_event(Pid, {set_for_delete, Penciller}, infinity).

-spec sst_clear(pid()) -> ok.
%% @doc
%% For this file to be closed and deleted
sst_clear(Pid) ->
    gen_fsm:sync_send_event(Pid, {set_for_delete, false}, infinity),
    gen_fsm:sync_send_event(Pid, close).

-spec sst_deleteconfirmed(pid()) -> ok.
%% @doc
%% Allows a penciller to confirm to a SST file that it can be cleared, as it
%% is no longer in use
sst_deleteconfirmed(Pid) ->
    gen_fsm:send_event(Pid, close).

-spec sst_checkready(pid()) -> {ok, string(), 
                                leveled_codec:ledger_key(), 
                                leveled_codec:ledger_key()}.
%% @doc
%% If a file has been set to be built, check that it has been built.  Returns
%% the filename and the {startKey, EndKey} for the manifest.
sst_checkready(Pid) ->
    %% Only used in test
    gen_fsm:sync_send_event(Pid, background_complete).

-spec sst_switchlevels(pid(), pos_integer()) -> ok.
%% @doc
%% Notify the SST file that it is now working at a new level
%% This simply prompts a GC on the PID now (as this may now be a long-lived
%% file, so don't want all the startup state to be held on memory - want to
%% proactively drop it
sst_switchlevels(Pid, NewLevel) ->
    gen_fsm:send_event(Pid, {switch_levels, NewLevel}).

-spec sst_close(pid()) -> ok.
%% @doc
%% Close the file
sst_close(Pid) ->
    gen_fsm:sync_send_event(Pid, close).

-spec sst_printtimings(pid()) -> ok.
%% @doc
%% The state of the FSM keeps track of timings of operations, and this can
%% forced to be printed.
%% Used in unit tests to force the printing of timings
sst_printtimings(Pid) ->
    gen_fsm:sync_send_event(Pid, print_timings).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, starting, #state{}}.

starting({sst_open, RootPath, Filename, OptsSST, Level}, _From, State) ->
    leveled_log:save(OptsSST#sst_options.log_options),
    {UpdState, Bloom} = 
        read_file(Filename,
                    State#state{root_path=RootPath},
                    OptsSST#sst_options.pagecache_level >= Level),
    Summary = UpdState#state.summary,
    {reply,
        {ok, {Summary#summary.first_key, Summary#summary.last_key}, Bloom},
        reader,
        UpdState#state{level = Level}};
starting({sst_new, 
            RootPath, Filename, Level, 
            {SlotList, FirstKey}, MaxSQN,
            OptsSST, IdxModDate, StartingPID}, _From, State) ->
    SW = os:timestamp(),
    leveled_log:save(OptsSST#sst_options.log_options),
    PressMethod = OptsSST#sst_options.press_method,
    {Length, SlotIndex, BlockIndex, SlotsBin, Bloom} = 
        build_all_slots(SlotList),
    SummaryBin = 
        build_table_summary(SlotIndex, Level, FirstKey, Length, MaxSQN, Bloom),
    ActualFilename = 
        write_file(RootPath, Filename, SummaryBin, SlotsBin,
                    PressMethod, IdxModDate),
    YBQ = Level =< 2,
    {UpdState, Bloom} = 
        read_file(ActualFilename,
                    State#state{root_path=RootPath, yield_blockquery=YBQ},
                    OptsSST#sst_options.pagecache_level >= Level),
    Summary = UpdState#state.summary,
    leveled_log:log_timer("SST08",
                            [ActualFilename, Level, Summary#summary.max_sqn],
                            SW),
    erlang:send_after(?STARTUP_TIMEOUT, self(), tidyup_after_startup),
        % always want to have an opportunity to GC - so force the timeout to
        % occur whether or not there is an intervening message
    {reply,
        {ok, {Summary#summary.first_key, Summary#summary.last_key}, Bloom},
        reader,
        UpdState#state{blockindex_cache = BlockIndex,
                        starting_pid = StartingPID,
                        level = Level}};
starting({sst_newlevelzero, RootPath, Filename,
                    Penciller, MaxSQN,
                    OptsSST, IdxModDate}, _From, State) -> 
    DeferredStartupTuple = 
        {RootPath, Filename, Penciller, MaxSQN, OptsSST, IdxModDate},
    {reply, ok, starting,
        State#state{deferred_startup_tuple = DeferredStartupTuple, level = 0}};
starting(close, _From, State) ->
    % No file should have been created, so nothing to close.
    {stop, normal, ok, State}.
    
starting({complete_l0startup, Slots}, State) ->
    starting(complete_l0startup, State#state{new_slots = Slots});
starting(complete_l0startup, State) ->
    {RootPath, Filename, Penciller, MaxSQN, OptsSST, IdxModDate} =
        State#state.deferred_startup_tuple,
    SW0 = os:timestamp(),
    FetchedSlots = State#state.new_slots,
    leveled_log:save(OptsSST#sst_options.log_options),
    PressMethod = OptsSST#sst_options.press_method,
    FetchFun = fun(Slot) -> lists:nth(Slot, FetchedSlots) end,
    KVList = leveled_pmem:to_list(length(FetchedSlots), FetchFun),
    Time0 = timer:now_diff(os:timestamp(), SW0),
    
    SW1 = os:timestamp(),
    {[], [], SlotList, FirstKey} =
        merge_lists(KVList, OptsSST, IdxModDate),
    Time1 = timer:now_diff(os:timestamp(), SW1),

    SW2 = os:timestamp(),
    {SlotCount, SlotIndex, BlockIndex, SlotsBin,Bloom} =
        build_all_slots(SlotList),
    Time2 = timer:now_diff(os:timestamp(), SW2),
    
    SW3 = os:timestamp(),
    SummaryBin = 
        build_table_summary(SlotIndex, 0, FirstKey, SlotCount, MaxSQN, Bloom),
    Time3 = timer:now_diff(os:timestamp(), SW3),
    
    SW4 = os:timestamp(),
    ActualFilename = 
        write_file(RootPath, Filename, SummaryBin, SlotsBin,
                    PressMethod, IdxModDate),
    {UpdState, Bloom} = 
        read_file(ActualFilename,
                    State#state{root_path=RootPath,
                                yield_blockquery=true,
                                % Important to empty this from state rather
                                % than carry it through to the next stage
                                new_slots=undefined,
                                deferred_startup_tuple=undefined},
                    true),
    Summary = UpdState#state.summary,
    Time4 = timer:now_diff(os:timestamp(), SW4),
    
    leveled_log:log_timer("SST08",
                            [ActualFilename, 0, Summary#summary.max_sqn],
                            SW0),
    leveled_log:log("SST11", [Time0, Time1, Time2, Time3, Time4]),

    case Penciller of
        undefined ->
            {next_state, 
                reader, 
                UpdState#state{blockindex_cache = BlockIndex}};
        _ ->
            leveled_penciller:pcl_confirml0complete(Penciller,
                                                    UpdState#state.filename,
                                                    Summary#summary.first_key,
                                                    Summary#summary.last_key,
                                                    Bloom),
            {next_state, 
                reader, 
                UpdState#state{blockindex_cache = BlockIndex}}
    end;
starting({sst_returnslot, FetchedSlot, FetchFun, SlotCount}, State) ->
    Self = self(),
    FetchedSlots = 
        case FetchedSlot of
            none ->
                [];
            _ ->
                [FetchedSlot|State#state.new_slots]
        end, 
    case length(FetchedSlots) == SlotCount of
        true ->
            gen_fsm:send_event(Self, complete_l0startup),
            {next_state,
                starting,
                % Reverse the slots so that they are back in the expected
                % order
                State#state{new_slots = lists:reverse(FetchedSlots)}};
        false ->
            ReturnFun =
                fun(NextSlot) ->
                    gen_fsm:send_event(Self, 
                                        {sst_returnslot, NextSlot,
                                            FetchFun, SlotCount})
                end,
            FetchFun(length(FetchedSlots) + 1, ReturnFun),
            {next_state,
                starting,
                State#state{new_slots = FetchedSlots}}
    end.

reader({get_kv, LedgerKey, Hash}, _From, State) ->
    % Get a KV value and potentially take sample timings
    {Result, UpdState, UpdTimings} = 
        fetch(LedgerKey, Hash, State, State#state.timings),

    {UpdTimings0, CountDown} = 
        update_statetimings(UpdTimings,
                            State#state.timings_countdown,
                            State#state.level),
    
    {reply, Result, reader, UpdState#state{timings = UpdTimings0,
                                            timings_countdown = CountDown}};
reader({get_kvrange, StartKey, EndKey, ScanWidth, SegList, LowLastMod},
                                                            _From, State) ->
    {SlotsToFetchBinList, SlotsToPoint} = fetch_range(StartKey,
                                                        EndKey,
                                                        ScanWidth,
                                                        SegList,
                                                        LowLastMod,
                                                        State),
    
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    
    case State#state.yield_blockquery of
        true ->
            {reply,
                {yield, 
                    SlotsToFetchBinList, 
                    SlotsToPoint, 
                    PressMethod,
                    IdxModDate},
                reader,
                State};
        false ->
            {L, BIC} = 
                binaryslot_reader(SlotsToFetchBinList, 
                                    PressMethod, IdxModDate, SegList),
            FoldFun = 
                fun(CacheEntry, Cache) ->
                    case CacheEntry of
                        {_ID, none} ->
                            Cache;
                        {ID, Header} ->
                            array:set(ID - 1, binary:copy(Header), Cache)
                    end
                end,
            BlockIdxC0 = lists:foldl(FoldFun, State#state.blockindex_cache, BIC),
            {reply, 
                L ++ SlotsToPoint, 
                reader, 
                State#state{blockindex_cache = BlockIdxC0}}
    end;
reader({get_slots, SlotList, SegList, LowLastMod}, _From, State) ->
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    SlotBins = 
        read_slots(State#state.handle, 
                    SlotList, 
                    {SegList, LowLastMod, State#state.blockindex_cache},
                    State#state.compression_method,
                    State#state.index_moddate),
    {reply, {SlotBins, PressMethod, IdxModDate}, reader, State};
reader(get_maxsequencenumber, _From, State) ->
    Summary = State#state.summary,
    {reply, Summary#summary.max_sqn, reader, State};
reader(print_timings, _From, State) ->
    log_timings(State#state.timings, State#state.level),
    {reply, ok, reader, State};
reader({set_for_delete, Penciller}, _From, State) ->
    leveled_log:log("SST06", [State#state.filename]),
    {reply,
        ok,
        delete_pending,
        State#state{penciller=Penciller},
        ?DELETE_TIMEOUT};
reader(background_complete, _From, State) ->
    Summary = State#state.summary,
    {reply,
        {ok,
            State#state.filename,
            Summary#summary.first_key,
            Summary#summary.last_key},
        reader,
        State};
reader(close, _From, State) ->
    ok = file:close(State#state.handle),
    {stop, normal, ok, State}.

reader({switch_levels, NewLevel}, State) ->
    {next_state, reader, State#state{level = NewLevel}, hibernate}.


delete_pending({get_kv, LedgerKey, Hash}, _From, State) ->
    {Result, UpdState, _Ts} = fetch(LedgerKey, Hash, State, no_timing),
    {reply, Result, delete_pending, UpdState, ?DELETE_TIMEOUT};
delete_pending({get_kvrange, StartKey, EndKey, ScanWidth, SegList, LowLastMod},
                                                            _From, State) ->
    {SlotsToFetchBinList, SlotsToPoint} = fetch_range(StartKey,
                                                        EndKey,
                                                        ScanWidth,
                                                        SegList,
                                                        LowLastMod,
                                                        State),
    % Always yield as about to clear and de-reference
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    {reply,
        {yield, SlotsToFetchBinList, SlotsToPoint, PressMethod, IdxModDate},
        delete_pending,
        State,
        ?DELETE_TIMEOUT};
delete_pending({get_slots, SlotList, SegList, LowLastMod}, _From, State) ->
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    SlotBins = 
        read_slots(State#state.handle, 
                    SlotList, 
                    {SegList, LowLastMod, State#state.blockindex_cache},
                    PressMethod,
                    IdxModDate),
    {reply, 
        {SlotBins, PressMethod, IdxModDate}, 
        delete_pending, 
        State, 
        ?DELETE_TIMEOUT};
delete_pending(close, _From, State) ->
    leveled_log:log("SST07", [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = file:delete(filename:join(State#state.root_path,
                                    State#state.filename)),
    {stop, normal, ok, State}.

delete_pending(timeout, State) ->
    case State#state.penciller of
        false ->
            ok = leveled_sst:sst_deleteconfirmed(self());
        PCL ->
            FN = State#state.filename,
            ok = leveled_penciller:pcl_confirmdelete(PCL, FN, self())
    end,
    % If the next thing is another timeout - may be long-running snapshot, so
    % back-off
    {next_state, delete_pending, State, leveled_rand:uniform(10) * ?DELETE_TIMEOUT};
delete_pending(close, State) ->
    leveled_log:log("SST07", [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = file:delete(filename:join(State#state.root_path,
                                    State#state.filename)),
    {stop, normal, State}.

handle_sync_event(_Msg, _From, StateName, State) ->
    {reply, undefined, StateName, State}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_info(tidyup_after_startup, delete_pending, State) ->
    % No need to GC, this file is to be shutdown.  This message may have
    % interrupted the delete timeout, so timeout straight away
    {next_state, delete_pending, State, 0};
handle_info(tidyup_after_startup, StateName, State) ->
    case is_process_alive(State#state.starting_pid) of
        true ->
            {next_state, StateName, State, hibernate};
        false ->
            {stop, normal, State}
    end.

terminate(normal, delete_pending, _State) ->
    ok;
terminate(Reason, _StateName, State) ->
    leveled_log:log("SST04", [Reason, State#state.filename]).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


%%%============================================================================
%%% External Functions
%%%============================================================================

-spec expand_list_by_pointer(expandable_pointer(), 
                                list(expandable_pointer()), 
                                pos_integer())
                                                -> list(expanded_pointer()).
%% @doc
%% Expand a list of pointers, maybe ending up with a list of keys and values
%% with a tail of pointers
%% By defauls will not have a segment filter, or a low last_modified_date, but
%% they can be used. Range checking a last modified date must still be made on
%% the output - at this stage the low last_modified_date has been used to bulk
%% skip those slots not containing any information over the low last modified
%% date
expand_list_by_pointer(Pointer, Tail, Width) ->
    expand_list_by_pointer(Pointer, Tail, Width, false).

%% TODO until leveled_penciller updated
expand_list_by_pointer(Pointer, Tail, Width, SegList) ->
    expand_list_by_pointer(Pointer, Tail, Width, SegList, 0).

-spec expand_list_by_pointer(expandable_pointer(), 
                                list(expandable_pointer()), 
                                pos_integer(),
                                leveled_codec:segment_list(),
                                non_neg_integer())
                                                -> list(expanded_pointer()).
%% @doc
%% With filters (as described in expand_list_by_pointer/3
expand_list_by_pointer({pointer, SSTPid, Slot, StartKey, EndKey}, 
                                        Tail, Width, SegList, LowLastMod) ->
    FoldFun =
        fun(X, {Pointers, Remainder}) ->
            case length(Pointers) of
                L when L < Width ->    
                    case X of
                        {pointer, SSTPid, S, SK, EK} ->
                            {Pointers ++ [{pointer, S, SK, EK}], Remainder};
                        _ ->
                            {Pointers, Remainder ++ [X]}
                    end;
                _ ->
                    {Pointers, Remainder ++ [X]}
            end
            end,
    InitAcc = {[{pointer, Slot, StartKey, EndKey}], []},
    {AccPointers, AccTail} = lists:foldl(FoldFun, InitAcc, Tail),
    ExpPointers = sst_getfilteredslots(SSTPid,
                                        AccPointers,
                                        SegList,
                                        LowLastMod),
    lists:append(ExpPointers, AccTail);
expand_list_by_pointer({next, ManEntry, StartKey, EndKey}, 
                                        Tail, Width, SegList, LowLastMod) ->
    SSTPid = ManEntry#manifest_entry.owner,
    leveled_log:log("SST10", [SSTPid, is_process_alive(SSTPid)]),
    ExpPointer = sst_getfilteredrange(SSTPid, 
                                        StartKey,
                                        EndKey, 
                                        Width,
                                        SegList,
                                        LowLastMod),
    ExpPointer ++ Tail.


-spec sst_getkvrange(pid(), 
                        range_endpoint(), 
                        range_endpoint(),  
                        integer()) 
                            -> list(leveled_codec:ledger_kv()|slot_pointer()).
%% @doc
%% Get a range of {Key, Value} pairs as a list between StartKey and EndKey
%% (inclusive).  The ScanWidth is the maximum size of the range, a pointer
%% will be placed on the tail of the resulting list if results expand beyond
%% the Scan Width
sst_getkvrange(Pid, StartKey, EndKey, ScanWidth) ->
    sst_getfilteredrange(Pid, StartKey, EndKey, ScanWidth, false, 0). 


-spec sst_getfilteredrange(pid(), 
                            range_endpoint(), 
                            range_endpoint(),  
                            integer(),
                            leveled_codec:segment_list(),
                            non_neg_integer()) 
                            -> list(leveled_codec:ledger_kv()|slot_pointer()).
%% @doc
%% Get a range of {Key, Value} pairs as a list between StartKey and EndKey
%% (inclusive).  The ScanWidth is the maximum size of the range, a pointer
%% will be placed on the tail of the resulting list if results expand beyond
%% the Scan Width
%%
%% To make the range open-ended (either to start, end or both) the all atom
%% can be used in place of the Key tuple.
%%
%% A segment list can also be passed, which inidcates a subset of segment 
%% hashes of interest in the query.  
%%
%% TODO: Optimise this so that passing a list of segments that tune to the
%% same hash is faster - perhaps provide an exportable function in 
%% leveled_tictac
sst_getfilteredrange(Pid, StartKey, EndKey, ScanWidth, SegList, LowLastMod) ->
    SegList0 = tune_seglist(SegList),
    case gen_fsm:sync_send_event(Pid,
                                    {get_kvrange, 
                                        StartKey, EndKey, 
                                        ScanWidth, SegList0, LowLastMod},
                                    infinity) of
        {yield, SlotsToFetchBinList, SlotsToPoint, PressMethod, IdxModDate} ->
            {L, _BIC} = 
                binaryslot_reader(SlotsToFetchBinList,
                                    PressMethod, IdxModDate, SegList0),
            L ++ SlotsToPoint;
        Reply ->
            Reply
    end.

-spec sst_getslots(pid(), list(slot_pointer()))
                                        -> list(leveled_codec:ledger_kv()).
%% @doc
%% Get a list of slots by their ID. The slot will be converted from the binary
%% to term form outside of the FSM loop, this is to stop the copying of the 
%% converted term to the calling process.
sst_getslots(Pid, SlotList) ->
    sst_getfilteredslots(Pid, SlotList, false, 0).

-spec sst_getfilteredslots(pid(), 
                            list(slot_pointer()), 
                            leveled_codec:segment_list(),
                            non_neg_integer())
                                        -> list(leveled_codec:ledger_kv()).
%% @doc
%% Get a list of slots by their ID. The slot will be converted from the binary
%% to term form outside of the FSM loop
%%
%% A list of 16-bit integer Segment IDs can be passed to filter the keys 
%% returned (not precisely - with false results returned in addition).  Use 
%% false as a SegList to not filter.
%% An integer can be provided which gives a floor for the LastModified Date
%% of the object, if the object is to be covered by the query
sst_getfilteredslots(Pid, SlotList, SegList, LowLastMod) ->
    SegL0 = tune_seglist(SegList),
    {SlotBins, PressMethod, IdxModDate} = 
        gen_fsm:sync_send_event(Pid, 
                                {get_slots, SlotList, SegL0, LowLastMod},
                                infinity),
    {L, _BIC} = binaryslot_reader(SlotBins, PressMethod, IdxModDate, SegL0),
    L.


-spec find_pos(binary(),
                non_neg_integer()|
                    {list, list(non_neg_integer())}|
                    {sets, sets:set(non_neg_integer())},
                list(non_neg_integer()),
                non_neg_integer()) -> list(non_neg_integer()).
%% @doc
%% Find a list of positions where there is an element with a matching segment
%% ID to the expected segments (which cna either be a single segment, a list of
%% segments or a set of segments depending on size.
find_pos(<<>>, _Hash, PosList, _Count) ->
    PosList;
find_pos(<<1:1/integer, PotentialHit:15/integer, T/binary>>,
                                        Checker, PosList, Count) ->
    case member_check(PotentialHit, Checker) of
        true ->
            find_pos(T, Checker, PosList ++ [Count], Count + 1);
        false ->
            find_pos(T, Checker, PosList, Count + 1)
    end;
find_pos(<<0:1/integer, NHC:7/integer, T/binary>>, Checker, PosList, Count) ->
    find_pos(T, Checker, PosList, Count + NHC + 1).


-spec member_check(non_neg_integer(), 
                    non_neg_integer()|
                        {list, list(non_neg_integer())}|
                        {sets, sets:set(non_neg_integer())}) -> boolean().
member_check(Hash, Hash) ->
    true;
member_check(Hash, {list, HashList}) ->
    lists:member(Hash, HashList);
member_check(Hash, {sets, HashSet}) ->
    sets:is_element(Hash, HashSet);
member_check(_Miss, _Checker) ->
    false.


extract_hash({SegHash, _ExtraHash}) when is_integer(SegHash) ->
    tune_hash(SegHash);
extract_hash(NotHash) ->
    NotHash.

cache_hash({_SegHash, ExtraHash}) when is_integer(ExtraHash) ->
    ExtraHash band (?CACHE_SIZE - 1).

-spec tune_hash(non_neg_integer()) -> non_neg_integer().
%% @doc
%% Only 15 bits of the hash is ever interesting
tune_hash(SegHash) ->
    SegHash band 32767.

-spec tune_seglist(leveled_codec:segment_list()) -> tuned_seglist().
%% @doc
%% Only 15 bits of the hash is ever interesting
tune_seglist(SegList) ->
    case is_list(SegList) of 
        true ->
            SL0 = lists:usort(lists:map(fun tune_hash/1, SegList)),
            case length(SL0) > ?USE_SET_FOR_SPEED of
                true ->
                    {sets, sets:from_list(SL0)};
                false ->
                    {list, SL0}
            end;
        false ->
            false
    end.


%%%============================================================================
%%% Internal Functions
%%%============================================================================

-spec fetch(tuple(), 
            {integer(), integer()}|integer(), 
            sst_state(), sst_timings()) 
                        -> {not_present|tuple(), sst_state(), sst_timings()}.
%% @doc
%%
%% Fetch a key from the store, potentially taking timings.  Result should be
%% not_present if the key is not in the store.
fetch(LedgerKey, Hash, State, Timings0) ->
    SW0 = os:timestamp(),

    Summary = State#state.summary,
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    Slot = lookup_slot(LedgerKey, Summary#summary.index),
    
    {SW1, Timings1} = update_timings(SW0, Timings0, index_query, true),
    
    SlotID = Slot#slot_index_value.slot_id,
    CachedBlockIdx = 
        array:get(SlotID - 1, State#state.blockindex_cache),
    {SW2, Timings2} = update_timings(SW1, Timings1, lookup_cache, true),

    case extract_header(CachedBlockIdx, IdxModDate) of 
        none ->
            SlotBin = read_slot(State#state.handle, Slot),
            {Result, Header} = 
                binaryslot_get(SlotBin, LedgerKey, Hash, PressMethod, IdxModDate),
            BlockIndexCache = 
                array:set(SlotID - 1,
                            binary:copy(Header),
                            State#state.blockindex_cache),
            {_SW3, Timings3} = 
                update_timings(SW2, Timings2, noncached_block, false),
            {Result, 
                State#state{blockindex_cache = BlockIndexCache}, 
                Timings3};
        {BlockLengths, _LMD, PosBin} ->
            PosList = find_pos(PosBin, extract_hash(Hash), [], 0),
            case PosList of 
                [] ->
                    {_SW3, Timings3} =
                        update_timings(SW2, Timings2, slot_index, false),
                    {not_present, State, Timings3};
                _ ->
                    {SW3, Timings3} =
                        update_timings(SW2, Timings2, slot_index, true),
                    FetchCache = State#state.fetch_cache,
                    CacheHash = cache_hash(Hash),
                    case array:get(CacheHash, FetchCache) of 
                        {LedgerKey, V} ->
                            {_SW4, Timings4} = 
                                update_timings(SW3, 
                                                Timings3, 
                                                fetch_cache, 
                                                false),
                            {{LedgerKey, V}, State, Timings4};
                        _ ->
                            StartPos = Slot#slot_index_value.start_position,
                            Result = 
                                check_blocks(PosList,
                                                {State#state.handle, StartPos},
                                                BlockLengths,
                                                byte_size(PosBin),
                                                LedgerKey, 
                                                PressMethod,
                                                IdxModDate,
                                                not_present),
                            FetchCache0 = 
                                array:set(CacheHash, Result, FetchCache),
                            {_SW4, Timings4} = 
                                update_timings(SW3, 
                                                Timings3, 
                                                slot_fetch, 
                                                false),
                            {Result, 
                                State#state{fetch_cache = FetchCache0}, 
                                Timings4}
                    end
            end 
    end.


-spec fetch_range(tuple(), tuple(), integer(),
                    leveled_codec:segment_list(), non_neg_integer(), 
                    sst_state()) -> {list(), list()}.
%% @doc
%% Fetch the contents of the SST file for a given key range.  This will 
%% pre-fetch some results, and append pointers for additional results.
%%
%% A filter can be provided based on the Segment ID (usable for hashable 
%% objects not no_lookup entries) to accelerate the query if the 5-arity
%% version is used 
fetch_range(StartKey, EndKey, ScanWidth, SegList, LowLastMod, State) ->
    Summary = State#state.summary,
    Handle = State#state.handle,
    {Slots, RTrim} = lookup_slots(StartKey, EndKey, Summary#summary.index),
    Self = self(),
    SL = length(Slots),
    
    ExpandedSlots = 
        case SL of
            1 ->
                [Slot] = Slots,
                case RTrim of
                    true ->
                        [{pointer, Self, Slot, StartKey, EndKey}];
                    false ->
                        [{pointer, Self, Slot, StartKey, all}]
                end;
            N ->
                {LSlot, MidSlots, RSlot} = 
                    case N of
                        2 ->
                            [Slot1, Slot2] = Slots,
                            {Slot1, [], Slot2};
                        N ->
                            [Slot1|_Rest] = Slots,
                            SlotN = lists:last(Slots),
                            {Slot1, lists:sublist(Slots, 2, N - 2), SlotN}
                    end,
                MidSlotPointers = lists:map(fun(S) ->
                                                {pointer, Self, S, all, all}
                                                end,
                                            MidSlots),
                case RTrim of
                    true ->
                        [{pointer, Self, LSlot, StartKey, all}] ++
                            MidSlotPointers ++
                            [{pointer, Self, RSlot, all, EndKey}];
                    false ->
                        [{pointer, Self, LSlot, StartKey, all}] ++
                            MidSlotPointers ++
                            [{pointer, Self, RSlot, all, all}]
                end
        end,
    {SlotsToFetch, SlotsToPoint} = 
        case ScanWidth of
            SW when SW >= SL ->
                {ExpandedSlots, []};
            _ ->
                lists:split(ScanWidth, ExpandedSlots)
        end,

    SlotsToFetchBinList = 
        read_slots(Handle, 
                    SlotsToFetch, 
                    {SegList, LowLastMod, State#state.blockindex_cache},
                    State#state.compression_method,
                    State#state.index_moddate),
    {SlotsToFetchBinList, SlotsToPoint}.

-spec compress_level(integer(), press_method()) -> press_method().
%% @doc
%% disable compression at higher levels for improved performance
compress_level(Level, _PressMethod) when Level < ?COMPRESS_AT_LEVEL ->
    none;
compress_level(_Level, PressMethod) ->
    PressMethod.

write_file(RootPath, Filename, SummaryBin, SlotsBin,
            PressMethod, IdxModDate) ->
    SummaryLength = byte_size(SummaryBin),
    SlotsLength = byte_size(SlotsBin),
    {PendingName, FinalName} = generate_filenames(Filename),
    FileVersion = gen_fileversion(PressMethod, IdxModDate),
    ok = file:write_file(filename:join(RootPath, PendingName),
                            <<FileVersion:8/integer,
                                SlotsLength:32/integer,
                                SummaryLength:32/integer,    
                                SlotsBin/binary,
                                SummaryBin/binary>>,
                            [raw]),
    case filelib:is_file(filename:join(RootPath, FinalName)) of
        true ->
            AltName = filename:join(RootPath, filename:basename(FinalName))
                        ++ ?DISCARD_EXT,
            leveled_log:log("SST05", [FinalName, AltName]),
            ok = file:rename(filename:join(RootPath, FinalName), AltName);
        false ->
            ok
    end,
    file:rename(filename:join(RootPath, PendingName),
                filename:join(RootPath, FinalName)),
    FinalName.

read_file(Filename, State, LoadPageCache) ->
    {Handle, FileVersion, SummaryBin} = 
        open_reader(filename:join(State#state.root_path, Filename),
                    LoadPageCache),
    UpdState0 = imp_fileversion(FileVersion, State),
    {Summary, Bloom, SlotList} = read_table_summary(SummaryBin),
    BlockIndexCache = array:new([{size, Summary#summary.size},
                                    {default, none}]),
    UpdState1 = UpdState0#state{blockindex_cache = BlockIndexCache},
    SlotIndex = from_list(SlotList),
    UpdSummary = Summary#summary{index = SlotIndex},
    leveled_log:log("SST03", [Filename,
                                Summary#summary.size,
                                Summary#summary.max_sqn]),
    {UpdState1#state{summary = UpdSummary,
                    handle = Handle,
                    filename = Filename},
        Bloom}.

gen_fileversion(PressMethod, IdxModDate) ->
    % Native or none can be treated the same once written, as reader 
    % does not need to know as compression info will be in header of the 
    % block
    Bit1 = 
        case PressMethod of 
            lz4 -> 1;
            native -> 0;
            none -> 0
        end,
    Bit2 =
        case IdxModDate of
            true ->
                2;
            false ->
                0
        end,
    Bit1+ Bit2.

imp_fileversion(VersionInt, State) ->
    UpdState0 = 
        case VersionInt band 1 of 
            0 ->
                State#state{compression_method = native};
            1 ->
                State#state{compression_method = lz4}
        end,
    UpdState1 =
        case VersionInt band 2 of
            0 ->
                UpdState0#state{index_moddate = false};
            2 ->
                UpdState0#state{index_moddate = true}
        end,
    UpdState1.

open_reader(Filename, LoadPageCache) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    {ok, Lengths} = file:pread(Handle, 0, 9),
    <<FileVersion:8/integer, 
        SlotsLength:32/integer, 
        SummaryLength:32/integer>> = Lengths,
    case LoadPageCache of
        true ->
            file:advise(Handle, 9, SlotsLength, will_need);
        false ->
            ok
    end,
    {ok, SummaryBin} = file:pread(Handle, SlotsLength + 9, SummaryLength),
    {Handle, FileVersion, SummaryBin}.

build_table_summary(SlotIndex, _Level, FirstKey, SlotCount, MaxSQN, Bloom) ->
    [{LastKey, _LastV}|_Rest] = SlotIndex,
    Summary = #summary{first_key = FirstKey,
                        last_key = LastKey,
                        size = SlotCount,
                        max_sqn = MaxSQN},
    SummBin = 
        term_to_binary({Summary, Bloom, lists:reverse(SlotIndex)}, 
                        ?BINARY_SETTINGS),
    SummCRC = hmac(SummBin),
    <<SummCRC:32/integer, SummBin/binary>>.

read_table_summary(BinWithCheck) ->
    <<SummCRC:32/integer, SummBin/binary>> = BinWithCheck,
    CRCCheck = hmac(SummBin),
    if
        CRCCheck == SummCRC ->
            % If not might it might be possible to rebuild from all the slots
            binary_to_term(SummBin)
    end.


build_all_slots(SlotList) ->
    SlotCount = length(SlotList),
    {SlotIndex, BlockIndex, SlotsBin, HashLists} = 
        build_all_slots(SlotList,
                            9,
                            1,
                            [],
                            array:new([{size, SlotCount}, 
                                        {default, none}]),
                            <<>>,
                            []),
    Bloom = leveled_ebloom:create_bloom(HashLists),
    {SlotCount, SlotIndex, BlockIndex, SlotsBin, Bloom}.

build_all_slots([], _Pos, _SlotID,
                    SlotIdxAcc, BlockIdxAcc, SlotBinAcc, HashLists) ->
    {SlotIdxAcc, BlockIdxAcc, SlotBinAcc, HashLists};
build_all_slots([SlotD|Rest], Pos, SlotID,
                    SlotIdxAcc, BlockIdxAcc, SlotBinAcc, HashLists) ->
    {BlockIdx, SlotBin, HashList, LastKey} = SlotD,
    Length = byte_size(SlotBin),
    SlotIndexV = #slot_index_value{slot_id = SlotID,
                                    start_position = Pos,
                                    length = Length},
    build_all_slots(Rest,
                    Pos + Length,
                    SlotID + 1,
                    [{LastKey, SlotIndexV}|SlotIdxAcc],
                    array:set(SlotID - 1, BlockIdx, BlockIdxAcc),
                    <<SlotBinAcc/binary, SlotBin/binary>>,
                    lists:append(HashLists, HashList)).


generate_filenames(RootFilename) ->
    Ext = filename:extension(RootFilename),
    Components = filename:split(RootFilename),
    case Ext of
        [] ->
            {filename:join(Components) ++ ".pnd",
                filename:join(Components) ++ ".sst"};
        Ext ->
            DN = filename:dirname(RootFilename),
            FP_NOEXT = filename:basename(RootFilename, Ext),
            {filename:join(DN, FP_NOEXT) ++ ".pnd",
                filename:join(DN, FP_NOEXT) ++ ".sst"}
    end.    


-spec serialise_block(any(), press_method()) -> binary().
%% @doc
%% Convert term to binary
%% Function split out to make it easier to experiment with different 
%% compression methods.  Also, perhaps standardise applictaion of CRC
%% checks
serialise_block(Term, lz4) ->
    {ok, Bin} = lz4:pack(term_to_binary(Term)),
    CRC32 = hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>;
serialise_block(Term, native) ->
    Bin = term_to_binary(Term, ?BINARY_SETTINGS),
    CRC32 = hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>;
serialise_block(Term, none) ->
    Bin = term_to_binary(Term),
    CRC32 = hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>.


-spec deserialise_block(binary(), press_method()) -> any().
%% @doc
%% Convert binary to term
%% Function split out to make it easier to experiment with different 
%% compression methods.  
%%
%% If CRC check fails we treat all the data as missing 
deserialise_block(Bin, PressMethod) ->
    BinS = byte_size(Bin) - 4,
    <<TermBin:BinS/binary, CRC32:32/integer>> = Bin,
    case hmac(TermBin) of 
        CRC32 ->
            deserialise_checkedblock(TermBin, PressMethod);
        _ ->
            []
    end.

deserialise_checkedblock(Bin, lz4) ->
    {ok, Bin0} = lz4:unpack(Bin),
    binary_to_term(Bin0);
deserialise_checkedblock(Bin, _Other) ->
    % native or none can be treated the same
    binary_to_term(Bin).



-spec hmac(binary()|integer()) -> integer().
%% @doc
%% Perform a CRC check on an input
hmac(Bin) when is_binary(Bin) ->
    erlang:crc32(Bin);
hmac(Int) when is_integer(Int) ->
    Int bxor ?FLIPPER32.

%%%============================================================================
%%% SlotIndex Implementation
%%%============================================================================

%% The Slot Index is stored as a flat (sorted) list of {Key, Slot} where Key
%% is the last key within the slot.
%%
%% This implementation of the SlotIndex uses leveled_tree

from_list(SlotList) ->
    leveled_tree:from_orderedlist(SlotList, ?TREE_TYPE, ?TREE_SIZE).

lookup_slot(Key, Tree) ->
    StartKeyFun =
        fun(_V) ->
            all
        end,
    % The penciller should never ask for presence out of range - so will
    % always return a slot (As we don't compare to StartKey)
    {_LK, Slot} = leveled_tree:search(Key, Tree, StartKeyFun),
    Slot.

lookup_slots(StartKey, EndKey, Tree) ->
    StartKeyFun =
        fun(_V) ->
            all
        end,
    MapFun =
        fun({_LK, Slot}) ->
            Slot
        end,
    SlotList = leveled_tree:search_range(StartKey, EndKey, Tree, StartKeyFun),
    {EK, _EndSlot} = lists:last(SlotList),
    {lists:map(MapFun, SlotList), not leveled_codec:endkey_passed(EK, EndKey)}.


%%%============================================================================
%%% Slot Implementation
%%%============================================================================

%% Implementing a slot has gone through numerous iterations.  One of the most
%% critical considerations has been the cost of the binary_to_term and 
%% term_to_binary calls for different sizes of slots and different data types.
%%
%% Microbenchmarking indicated that flat lists were the fastest at sst build
%% time.  However, the lists need scanning at query time - and so give longer
%% lookups.  Bigger slots did better at term_to_binary time.  However
%% binary_to_term is an often repeated task, and this is better with smaller
%% slots.  
%%
%% The outcome has been to divide the slot into four small blocks to minimise 
%% the binary_to_term time.  A binary index is provided for the slot for all 
%% Keys that are directly fetchable (i.e. standard keys not index keys).
%%
%% The division and use of a list saves about 100 microseconds per fetch when 
%% compared to using a 128-member gb:tree.
%%
%% The binary index is cacheable and doubles as a not_present filter, as it is
%% based on a 17-bit hash (so 0.0039 fpr).


-spec accumulate_positions(leveled_codec:ledger_kv(),
                            {binary(),
                                non_neg_integer(),
                                list(non_neg_integer()),
                                leveled_codec:last_moddate()}) ->
                                    {binary(),
                                        non_neg_integer(),
                                        list(non_neg_integer()),
                                        leveled_codec:last_moddate()}.
%% @doc
%% Fold function use to accumulate the position information needed to
%% populate the summary of the slot
accumulate_positions({K, V}, {PosBinAcc, NoHashCount, HashAcc, LMDAcc}) ->
    {_SQN, H1, LMD} = leveled_codec:strip_to_indexdetails({K, V}),
    LMDAcc0 = take_max_lastmoddate(LMD, LMDAcc),
    PosH1 = extract_hash(H1),
    case is_integer(PosH1) of 
        true ->
            case NoHashCount of 
                0 ->
                    {<<1:1/integer, PosH1:15/integer,PosBinAcc/binary>>,
                        0,
                        [H1|HashAcc],
                        LMDAcc0};
                N ->
                    % The No Hash Count is an integer between 0 and 127
                    % and so at read time should count NHC + 1
                    NHC = N - 1,
                    {<<1:1/integer,
                            PosH1:15/integer, 
                            0:1/integer,
                            NHC:7/integer, 
                            PosBinAcc/binary>>,
                        0,
                        HashAcc,
                        LMDAcc0}
            end;
        false ->
            {PosBinAcc, NoHashCount + 1, HashAcc, LMDAcc0}
    end.


-spec take_max_lastmoddate(leveled_codec:last_moddate(),
                            leveled_codec:last_moddate()) -> 
                                leveled_codec:last_moddate().
%% @doc
%% Get the last modified date.  If no Last Modified Date on any object, can't
%% add the accelerator and should check each object in turn
take_max_lastmoddate(undefined, _LMDAcc) ->
    ?FLIPPER32;
take_max_lastmoddate(LMD, LMDAcc) ->
    max(LMD, LMDAcc).

-spec generate_binary_slot(leveled_codec:maybe_lookup(),
                            list(leveled_codec:ledger_kv()),
                            press_method(),
                            boolean(),
                            build_timings()) ->
                                {{binary(),
                                    binary(),
                                    list(integer()),
                                    leveled_codec:ledger_key()},
                                    build_timings()}.
%% @doc
%% Generate the serialised slot to be used when storing this sublist of keys
%% and values
generate_binary_slot(Lookup, KVL, PressMethod, IndexModDate, BuildTimings0) ->
    
    SW0 = os:timestamp(),
    
    {HashL, PosBinIndex, LMD} = 
        case Lookup of
            lookup ->
                InitAcc = {<<>>, 0, [], 0},
                {PosBinIndex0, NHC, HashL0, LMD0} = 
                    lists:foldr(fun accumulate_positions/2, InitAcc, KVL),
                PosBinIndex1 = 
                    case NHC of
                        0 ->
                            PosBinIndex0;
                        _ ->
                            N = NHC - 1,
                            <<0:1/integer, N:7/integer, PosBinIndex0/binary>>
                    end,
                {HashL0, PosBinIndex1, LMD0};
            no_lookup ->
                {[], <<0:1/integer, 127:7/integer>>, 0}
        end,
    
    BuildTimings1 = update_buildtimings(SW0, BuildTimings0, slot_hashlist),
    SW1 = os:timestamp(), 

    {SideBlockSize, MidBlockSize} =
        case Lookup of
            lookup ->
                ?LOOK_BLOCKSIZE;
            no_lookup ->
                ?NOLOOK_BLOCKSIZE
        end,

    {B1, B2, B3, B4, B5} = 
        case length(KVL) of 
            L when L =< SideBlockSize ->
                {serialise_block(KVL, PressMethod),
                    <<0:0>>, 
                    <<0:0>>, 
                    <<0:0>>,
                    <<0:0>>};
            L when L =< 2 * SideBlockSize ->
                {KVLA, KVLB} = lists:split(SideBlockSize, KVL),
                {serialise_block(KVLA, PressMethod),
                    serialise_block(KVLB, PressMethod),
                    <<0:0>>, 
                    <<0:0>>,
                    <<0:0>>};
            L when L =< (2 * SideBlockSize + MidBlockSize) ->
                {KVLA, KVLB_Rest} = lists:split(SideBlockSize, KVL),
                {KVLB, KVLC} = lists:split(SideBlockSize, KVLB_Rest),
                {serialise_block(KVLA, PressMethod),
                    serialise_block(KVLB, PressMethod),
                    serialise_block(KVLC, PressMethod),
                    <<0:0>>,
                    <<0:0>>};
            L when L =< (3 * SideBlockSize + MidBlockSize) ->
                {KVLA, KVLB_Rest} = lists:split(SideBlockSize, KVL),
                {KVLB, KVLC_Rest} = lists:split(SideBlockSize, KVLB_Rest),
                {KVLC, KVLD} = lists:split(MidBlockSize, KVLC_Rest),
                {serialise_block(KVLA, PressMethod),
                    serialise_block(KVLB, PressMethod),
                    serialise_block(KVLC, PressMethod),
                    serialise_block(KVLD, PressMethod),
                    <<0:0>>};
            L when L =< (4 * SideBlockSize + MidBlockSize) ->
                {KVLA, KVLB_Rest} = lists:split(SideBlockSize, KVL),
                {KVLB, KVLC_Rest} = lists:split(SideBlockSize, KVLB_Rest),
                {KVLC, KVLD_Rest} = lists:split(MidBlockSize, KVLC_Rest),
                {KVLD, KVLE} = lists:split(SideBlockSize, KVLD_Rest),
                {serialise_block(KVLA, PressMethod),
                    serialise_block(KVLB, PressMethod),
                    serialise_block(KVLC, PressMethod),
                    serialise_block(KVLD, PressMethod),
                    serialise_block(KVLE, PressMethod)}
        end,

    BuildTimings2 = update_buildtimings(SW1, BuildTimings1, slot_serialise),
    SW2 = os:timestamp(), 

    B1P = 
        case IndexModDate of 
            true ->
                byte_size(PosBinIndex) + ?BLOCK_LENGTHS_LENGTH + ?LMD_LENGTH;
            false ->
                byte_size(PosBinIndex) + ?BLOCK_LENGTHS_LENGTH
        end,
    CheckB1P = hmac(B1P),
    B1L = byte_size(B1),
    B2L = byte_size(B2),
    B3L = byte_size(B3),
    B4L = byte_size(B4),
    B5L = byte_size(B5),
    Header = 
        case IndexModDate of 
            true ->
                <<B1L:32/integer, 
                    B2L:32/integer, 
                    B3L:32/integer, 
                    B4L:32/integer,
                    B5L:32/integer,
                    LMD:32/integer,
                    PosBinIndex/binary>>;
            false ->
                <<B1L:32/integer, 
                    B2L:32/integer, 
                    B3L:32/integer, 
                    B4L:32/integer,
                    B5L:32/integer,
                    PosBinIndex/binary>>
        end,
    CheckH = hmac(Header),
    SlotBin = <<CheckB1P:32/integer, B1P:32/integer,
                    CheckH:32/integer, Header/binary, 
                    B1/binary, B2/binary, B3/binary, B4/binary, B5/binary>>,
    
    {LastKey, _LV} = lists:last(KVL),

    BuildTimings3 = update_buildtimings(SW2, BuildTimings2, slot_finish),

    {{Header, SlotBin, HashL, LastKey}, BuildTimings3}.


-spec check_blocks(list(integer()), 
                    binary()|{file:io_device(), integer()},
                    binary(),
                    integer(),
                    leveled_codec:ledger_key()|false,
                    press_method(),
                    boolean(),
                    list()|not_present) -> list()|not_present.
%% @doc
%% Acc should start as not_present if LedgerKey is a key, and a list if
%% LedgerKey is false

check_blocks([], _BlockPointer, _BlockLengths, _PosBinLength,
                _LedgerKeyToCheck, _PressMethod, _IdxModDate, not_present) ->
    not_present;
check_blocks([], _BlockPointer, _BlockLengths, _PosBinLength,
                _LedgerKeyToCheck, _PressMethod, _IdxModDate, Acc) ->
    lists:reverse(Acc);
check_blocks([Pos|Rest], BlockPointer, BlockLengths, PosBinLength,
                LedgerKeyToCheck, PressMethod, IdxModDate, Acc) ->
    {BlockNumber, BlockPos} = revert_position(Pos),
    BlockBin = 
        read_block(BlockPointer, 
                    BlockLengths, 
                    PosBinLength,
                    BlockNumber,
                    additional_offset(IdxModDate)),
    BlockL = deserialise_block(BlockBin, PressMethod),
    {K, V} = lists:nth(BlockPos, BlockL),
    case K of 
        LedgerKeyToCheck ->
            {K, V};
        _ ->
            case LedgerKeyToCheck of 
                false ->
                    check_blocks(Rest, BlockPointer, 
                                    BlockLengths, PosBinLength, 
                                    LedgerKeyToCheck, PressMethod, IdxModDate,
                                    [{K, V}|Acc]);
                _ ->
                    check_blocks(Rest, BlockPointer, 
                                    BlockLengths, PosBinLength, 
                                    LedgerKeyToCheck, PressMethod, IdxModDate,
                                    Acc)
            end
    end.

-spec additional_offset(boolean()) -> pos_integer().
%% @doc
%% 4-byte CRC, 4-byte pos, 4-byte CRC, 5x4 byte lengths, 4 byte LMD
%% LMD may not be present
additional_offset(true) ->
    ?BLOCK_LENGTHS_LENGTH + 4 + 4 + 4 + ?LMD_LENGTH;
additional_offset(false) ->
    ?BLOCK_LENGTHS_LENGTH + 4 + 4 + 4.


read_block({Handle, StartPos}, BlockLengths, PosBinLength, BlockID, AO) ->
    {Offset, Length} = block_offsetandlength(BlockLengths, BlockID),
    {ok, BlockBin} = file:pread(Handle,
                                StartPos
                                    + Offset
                                    + PosBinLength
                                    + AO,
                                Length),
    BlockBin;
read_block(SlotBin, BlockLengths, PosBinLength, BlockID, AO) ->
    {Offset, Length} = block_offsetandlength(BlockLengths, BlockID),
    StartPos = Offset + PosBinLength + AO,
    <<_Pre:StartPos/binary, BlockBin:Length/binary, _Rest/binary>> = SlotBin,
    BlockBin.

read_slot(Handle, Slot) ->
    {ok, SlotBin} = file:pread(Handle,
                                Slot#slot_index_value.start_position,
                                Slot#slot_index_value.length),
    SlotBin.


pointer_mapfun(Pointer) ->
    {Slot, SK, EK} = 
        case Pointer of 
            {pointer, _Pid, Slot0, SK0, EK0} ->
                {Slot0, SK0, EK0};
            {pointer, Slot0, SK0, EK0} ->
                {Slot0, SK0, EK0}
        end,

    {Slot#slot_index_value.start_position, 
        Slot#slot_index_value.length,
        Slot#slot_index_value.slot_id,
        SK, 
        EK}.

-spec binarysplit_mapfun(binary(), integer()) -> fun().
%% @doc
%% Return a function that can pull individual slot binaries from a binary
%% covering multiple slots
binarysplit_mapfun(MultiSlotBin, StartPos) -> 
    fun({SP, L, ID, SK, EK}) -> 
        Start = SP - StartPos,
        <<_Pre:Start/binary, SlotBin:L/binary, _Post/binary>> = MultiSlotBin,
        {SlotBin, ID, SK, EK}
    end.


-spec read_slots(file:io_device(), list(), 
                    {false|list(), non_neg_integer(), binary()},
                    press_method(), boolean()) -> list(binaryslot_element()).
%% @doc
%% The reading of sots will return a list of either 2-tuples containing 
%% {K, V} pairs - or 3-tuples containing {Binary, SK, EK}.  The 3 tuples 
%% can be exploded into lists of {K, V} pairs using the binaryslot_reader/4
%% function
%%
%% Reading slots is generally unfiltered, but in the sepcial case when 
%% querting across slots when only matching segment IDs are required the 
%% BlockIndexCache can be used
%%
%% Note that false positives will be passed through.  It is important that
%% any key comparison between levels should allow for a non-matching key to
%% be considered as superior to a matching key - as otherwise a matching key
%% may be intermittently removed from the result set
read_slots(Handle, SlotList, {false, 0, _BlockIndexCache},
                _PressMethod, _IdxModDate) ->
    % No list of segments passed or useful Low LastModified Date
    % Just read slots in SlotList
    read_slotlist(SlotList, Handle);
read_slots(Handle, SlotList, {SegList, LowLastMod, BlockIndexCache}, 
                PressMethod, IdxModDate) ->
    % List of segments passed so only {K, V} pairs matching those segments
    % should be returned.  This required the {K, V} pair to have been added 
    % with the appropriate hash - if the pair were added with no_lookup as 
    % the hash value this will fial unexpectedly.
    BinMapFun = 
        fun(Pointer, Acc) ->
            {SP, _L, ID, SK, EK} = pointer_mapfun(Pointer),
            CachedHeader = array:get(ID - 1, BlockIndexCache),
            case extract_header(CachedHeader, IdxModDate) of
                none ->
                    % If there is an attempt to use the seg list query and the
                    % index block cache isn't cached for any part this may be 
                    % slower as each slot will be read in turn
                    Acc ++ read_slotlist([Pointer], Handle);
                {BlockLengths, LMD, BlockIdx} ->
                    % If there is a BlockIndex cached then we can use it to 
                    % check to see if any of the expected segments are 
                    % present without lifting the slot off disk. Also the 
                    % fact that we know position can be used to filter out 
                    % other keys
                    %
                    % Note that LMD will be 0 if the indexing of last mod
                    % date was not enable at creation time.  So in this
                    % case the filter should always map
                    case LowLastMod > LMD of
                        true ->
                            % The highest LMD on the slot was before the
                            % LowLastMod date passed in the query - therefore
                            % there are no interesting modifications in this
                            % slot - it is all too old
                            Acc;
                        false ->
                            case SegList of
                                false ->
                                    % Need all the slot now
                                    Acc ++ read_slotlist([Pointer], Handle);
                                _SL ->
                                    % Need to find just the right keys
                                    PositionList = 
                                        find_pos(BlockIdx, SegList, [], 0),
                                    % Note check_blocks should return [] if
                                    % PositionList is empty (which it may be)
                                    KVL =
                                        check_blocks(PositionList,
                                                        {Handle, SP}, 
                                                        BlockLengths, 
                                                        byte_size(BlockIdx),
                                                        false,
                                                        PressMethod,
                                                        IdxModDate,
                                                        []),
                                    % There is no range passed through to the
                                    % binaryslot_reader, so these results need
                                    % to be filtered
                                    FilterFun =
                                        fun(KV) -> in_range(KV, SK, EK) end,
                                    Acc ++ lists:filter(FilterFun, KVL)
                            end
                    end
            end 
        end,
    lists:foldl(BinMapFun, [], SlotList).


-spec in_range(leveled_codec:ledger_kv(),
                range_endpoint(), range_endpoint()) -> boolean().
%% @doc
%% Is the ledger key in the range.
in_range({_LK, _LV}, all, all) ->
    true;
in_range({LK, _LV}, all, EK) ->
    not leveled_codec:endkey_passed(EK, LK);
in_range({LK, LV}, SK, EK) ->
    (LK >= SK) and in_range({LK, LV}, all, EK).


read_slotlist(SlotList, Handle) ->
    LengthList = lists:map(fun pointer_mapfun/1, SlotList),
    {MultiSlotBin, StartPos} = read_length_list(Handle, LengthList),
    lists:map(binarysplit_mapfun(MultiSlotBin, StartPos), LengthList).


-spec binaryslot_reader(list(binaryslot_element()), 
                            press_method(),
                            boolean(),
                            leveled_codec:segment_list()) 
                                -> {list({tuple(), tuple()}), 
                                    list({integer(), binary()})}.
%% @doc
%% Read the binary slots converting them to {K, V} pairs if they were not 
%% already {K, V} pairs.  If they are already {K, V} pairs it is assumed
%% that they have already been range checked before extraction.
%%
%% Keys which are still to be extracted from the slot, are accompanied at
%% this function by the range against which the keys need to be checked. 
%% This range is passed with the slot to binaryslot_trimmedlist which should
%% open the slot block by block, filtering individual keys where the endpoints
%% of the block are outside of the range, and leaving blocks already proven to
%% be outside of the range unopened.
binaryslot_reader(SlotBinsToFetch, PressMethod, IdxModDate, SegList) ->
    % Two accumulators are added.
    % One to collect the list of keys and values found in the binary slots
    % (subject to range filtering if the slot is still deserialised at this
    % stage.  
    % The second accumulator extracts the header information from the slot, so
    % that the cache can be built for that slot.  This is used by the handling
    % of get_kvreader calls.  This means that slots which are only used in
    % range queries can still populate their block_index caches (on the FSM
    % loop state), and those caches can be used for future queries.
    binaryslot_reader(SlotBinsToFetch, 
                        PressMethod, IdxModDate, SegList, [], []).

binaryslot_reader([], _PressMethod, _IdxModDate, _SegList, Acc, BIAcc) ->
    {Acc, BIAcc};
binaryslot_reader([{SlotBin, ID, SK, EK}|Tail], 
                    PressMethod, IdxModDate, SegList, Acc, BIAcc) ->
    % The start key and end key here, may not the start key and end key the
    % application passed into the query.  If the slot is known to lie entirely
    % inside the range, on either of both sides, the SK and EK may be
    % substituted for the 'all' key work to indicate there is no need for
    % entries in this slot to be trimmed from either or both sides.
    {TrimmedL, BICache} = 
        binaryslot_trimmedlist(SlotBin, 
                                SK, EK, 
                                PressMethod,
                                IdxModDate,
                                SegList),
    binaryslot_reader(Tail, 
                        PressMethod,
                        IdxModDate,
                        SegList,
                        Acc ++ TrimmedL,
                        [{ID, BICache}|BIAcc]);
binaryslot_reader([{K, V}|Tail], 
                    PressMethod, IdxModDate, SegList, Acc, BIAcc) ->
    % These entries must already have been filtered for membership inside any
    % range used in the query.
    binaryslot_reader(Tail, 
                        PressMethod, IdxModDate, SegList,
                        Acc ++ [{K, V}], BIAcc).
                    

read_length_list(Handle, LengthList) ->
    StartPos = element(1, lists:nth(1, LengthList)),
    EndPos = element(1, lists:last(LengthList)) 
                + element(2, lists:last(LengthList)),
    {ok, MultiSlotBin} = file:pread(Handle, StartPos, EndPos - StartPos),
    {MultiSlotBin, StartPos}.


-spec extract_header(binary()|none, boolean()) ->
                                        {binary(), integer(), binary()}|none.
%% @doc
%% Helper for extracting the binaries from the header ignoring the missing LMD
%% if LMD is not indexed
extract_header(none, _IdxModDate) ->
    none; % used when the block cache has returned none 
extract_header(Header, true) ->
    BL = ?BLOCK_LENGTHS_LENGTH,
    <<BlockLengths:BL/binary, LMD:32/integer, PosBinIndex/binary>> = Header,
    {BlockLengths, LMD, PosBinIndex};
extract_header(Header, false) ->
    BL = ?BLOCK_LENGTHS_LENGTH,
    <<BlockLengths:BL/binary, PosBinIndex/binary>> = Header,
    {BlockLengths, 0, PosBinIndex}.

binaryslot_get(FullBin, Key, Hash, PressMethod, IdxModDate) ->
    case crc_check_slot(FullBin) of 
        {Header, Blocks} ->
            {BlockLengths, _LMD, PosBinIndex} =
                extract_header(Header, IdxModDate),
            PosList = find_pos(PosBinIndex,
                                extract_hash(Hash), 
                                [], 
                                0),
            {fetch_value(PosList, BlockLengths, Blocks, Key, PressMethod),
                Header};
        crc_wonky ->
            {not_present,
                none}
    end.

binaryslot_tolist(FullBin, PressMethod, IdxModDate) ->
    BlockFetchFun = 
        fun(Length, {Acc, Bin}) ->
            case Length of 
                0 ->
                    {Acc, Bin};
                _ ->
                    <<Block:Length/binary, Rest/binary>> = Bin,
                    {Acc ++ deserialise_block(Block, PressMethod), Rest}
            end
        end,

    {Out, _Rem} = 
        case crc_check_slot(FullBin) of 
            {Header, Blocks} ->
                {BlockLengths, _LMD, _PosBinIndex} =
                    extract_header(Header, IdxModDate),
                <<B1L:32/integer,
                    B2L:32/integer,
                    B3L:32/integer,
                    B4L:32/integer,
                    B5L:32/integer>> = BlockLengths,
                lists:foldl(BlockFetchFun,
                                {[], Blocks},
                                [B1L, B2L, B3L, B4L, B5L]);
            crc_wonky ->
                {[], <<>>}
        end,
    Out.


binaryslot_trimmedlist(FullBin, all, all,
                            PressMethod, IdxModDate, false) ->
    {binaryslot_tolist(FullBin, PressMethod, IdxModDate), none};
binaryslot_trimmedlist(FullBin, StartKey, EndKey,
                            PressMethod, IdxModDate, SegList) ->
    LTrimFun = fun({K, _V}) -> K < StartKey end,
    RTrimFun = fun({K, _V}) -> not leveled_codec:endkey_passed(EndKey, K) end,
    BlockCheckFun = 
        fun(Block, {Acc, Continue}) ->
            case {Block, Continue} of 
                {<<>>, _} ->
                    {Acc, false};
                {_, true} ->
                    BlockList =
                        case is_binary(Block) of
                            true ->
                                deserialise_block(Block, PressMethod);
                            false ->
                                Block
                        end,
                    case fetchend_rawblock(BlockList) of 
                        {LastKey, _LV} when StartKey > LastKey ->
                            {Acc, true};
                        {LastKey, _LV} ->
                            {_LDrop, RKeep} = lists:splitwith(LTrimFun, 
                                                                BlockList),
                            case leveled_codec:endkey_passed(EndKey, 
                                                                LastKey) of
                                true ->
                                    {LKeep, _RDrop} 
                                        = lists:splitwith(RTrimFun, RKeep),
                                    {Acc ++ LKeep, false};
                                false ->
                                    {Acc ++ RKeep, true}
                            end;
                        _ ->
                            {Acc, true}
                    end;
                {_ , false} ->
                    {Acc, false}
            end
        end,
    
    case {crc_check_slot(FullBin), SegList} of
        % It will be more effecient to check a subset of blocks.  To work out
        % the best subset we always look in the middle block of 5, and based on
        % the first and last keys of that middle block when compared to the Start
        % and EndKey of the query determines a subset of blocks
        %
        % This isn't perfectly efficient, esepcially if the query overlaps Block2
        % and Block3 (as Block 1 will also be checked), but finessing this last
        % scenario is hard to do in concise code
        {{Header, Blocks}, false} ->
            {BlockLengths, _LMD, _PosBinIndex} =
                extract_header(Header, IdxModDate),
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                B4L:32/integer,
                B5L:32/integer>> = BlockLengths,
            <<Block1:B1L/binary, Block2:B2L/binary,
                MidBlock:B3L/binary,
                Block4:B4L/binary, Block5:B5L/binary>> = Blocks,
            BlocksToCheck = 
                case B3L of
                    0 ->
                        [Block1, Block2];
                    _ ->    
                        MidBlockList = 
                            deserialise_block(MidBlock, PressMethod),
                        {MidFirst, _} = lists:nth(1, MidBlockList),
                        {MidLast, _} = lists:last(MidBlockList),
                        Split = {StartKey > MidLast,
                                    StartKey >= MidFirst,
                                    leveled_codec:endkey_passed(EndKey,
                                                                    MidFirst),
                                    leveled_codec:endkey_passed(EndKey,
                                                                    MidLast)},
                        case Split of
                            {true, _, _, _} ->
                                [Block4, Block5];
                            {false, true, false, true} ->
                                [MidBlockList];
                            {false, true, false, false} ->
                                [MidBlockList, Block4, Block5];
                            {false, false, true, true} ->
                                [Block1, Block2];
                            {false, false, false, true} ->
                                [Block1, Block2, MidBlockList];
                            _ ->
                                [Block1, Block2, MidBlockList, Block4, Block5]
                        end
                end,
            {Acc, _Continue} = lists:foldl(BlockCheckFun, {[], true}, BlocksToCheck),
            {Acc, none};
        {{Header, _Blocks}, SegList} ->
            {BlockLengths, _LMD, BlockIdx} = extract_header(Header, IdxModDate),
            PosList = find_pos(BlockIdx, SegList, [], 0),
            KVL = check_blocks(PosList,
                                FullBin,
                                BlockLengths,
                                byte_size(BlockIdx), 
                                false, 
                                PressMethod,
                                IdxModDate,
                                []),
            {KVL, Header};  
        {crc_wonky, _} ->
            {[], none}
    end.



crc_check_slot(FullBin) ->
    <<CRC32PBL:32/integer, 
        PosBL:32/integer, 
        CRC32H:32/integer,
        Rest/binary>> = FullBin,
    PosBL0 = min(PosBL, byte_size(FullBin) - 12), 
        % If the position has been bit-flipped to beyond the maximum possible
        % length, use the maximum possible length
    <<Header:PosBL0/binary, Blocks/binary>> = Rest,
    case {hmac(Header), hmac(PosBL0)} of 
        {CRC32H, CRC32PBL} ->
            {Header, Blocks};
        _ ->
            leveled_log:log("SST09", []),
            crc_wonky
    end.

block_offsetandlength(BlockLengths, BlockID) ->
    case BlockID of
        1 ->
            <<B1L:32/integer, _BR/binary>> = BlockLengths,
            {0, B1L};
        2 ->
            <<B1L:32/integer, B2L:32/integer, _BR/binary>> = BlockLengths,
            {B1L, B2L};
        3 ->
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                _BR/binary>> = BlockLengths,
            {B1L + B2L, B3L};
        4 ->
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                B4L:32/integer,
                _BR/binary>> = BlockLengths,
            {B1L + B2L + B3L, B4L};
        5 ->
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                B4L:32/integer,
                B5L:32/integer>> = BlockLengths,
            {B1L + B2L + B3L + B4L, B5L}
    end.

fetch_value([], _BlockLengths, _Blocks, _Key, _PressMethod) ->
    not_present;
fetch_value([Pos|Rest], BlockLengths, Blocks, Key, PressMethod) ->
    {BlockNumber, BlockPos} = revert_position(Pos),
    {Offset, Length} = block_offsetandlength(BlockLengths, BlockNumber),
    <<_Pre:Offset/binary, Block:Length/binary, _Rest/binary>> = Blocks,
    RawBlock = deserialise_block(Block, PressMethod),
    case fetchfrom_rawblock(BlockPos, RawBlock) of 
        {K, V} when K == Key ->
            {K, V};
        _ -> 
            fetch_value(Rest, BlockLengths, Blocks, Key, PressMethod)
    end.

fetchfrom_rawblock(_BlockPos, []) ->
    not_present;
fetchfrom_rawblock(BlockPos, RawBlock) ->
    lists:nth(BlockPos, RawBlock).

fetchend_rawblock([]) ->
    not_present;
fetchend_rawblock(RawBlock) ->
    lists:last(RawBlock).


revert_position(Pos) ->
    {SideBlockSize, MidBlockSize} = ?LOOK_BLOCKSIZE,
    case Pos < 2 * SideBlockSize of
        true ->
            {(Pos div SideBlockSize) + 1, (Pos rem SideBlockSize) + 1};
        false ->
            case Pos < (2 * SideBlockSize + MidBlockSize) of
                true ->
                    {3, ((Pos - 2 * SideBlockSize) rem MidBlockSize) + 1};
                false ->
                    TailPos = Pos - 2 * SideBlockSize - MidBlockSize,
                    {(TailPos div SideBlockSize) + 4,
                        (TailPos rem SideBlockSize) + 1}
            end
    end.



%%%============================================================================
%%% Merge Functions
%%%============================================================================

%% The source lists are merged into lists of slots before the file is created
%% At Level zero, there will be a single source list - and this will always be
%% split into standard size slots
%%
%% At lower levels there will be two source lists and they will need to be
%% merged to ensure that the best conflicting answer survives and compactable
%% KV pairs are discarded.
%%
%% At lower levels slots can be larger if there are no lookup keys present in
%% the slot.  This is to slow the growth of the manifest/number-of-files when
%% large numbers of index keys are present - as well as improving compression
%% ratios in the Ledger.
%%
%% The outcome of merge_lists/3 and merge_lists/5 should be an list of slots.
%% Each slot should be ordered by Key and be of the form {Flag, KVList}, where
%% Flag can either be lookup or no-lookup.  The list of slots should also be
%% ordered by Key (i.e. the first key in the slot)
%%
%% For merging  ...
%% Compare the keys at the head of the list, and either skip that "best" key or
%% identify as the next key.
%%
%% The logic needs to change if the file is in the basement level, as keys with
%% expired timestamps need not be written at this level
%%
%% The best key is considered to be the lowest key in erlang term order.  If
%% there are matching keys then the highest sequence number must be chosen and
%% any lower sequence numbers should be compacted out of existence

-spec merge_lists(list(), sst_options(), boolean()) 
                            -> {list(), list(), list(tuple()), tuple()|null}.
%% @doc
%%
%% Merge from asingle list (i.e. at Level 0)
merge_lists(KVList1, SSTOpts, IdxModDate) ->
    SlotCount = length(KVList1) div ?LOOK_SLOTSIZE, 
    {[],
        [],
        split_lists(KVList1, [],
                    SlotCount, SSTOpts#sst_options.press_method, IdxModDate),
        element(1, lists:nth(1, KVList1))}.


split_lists([], SlotLists, 0, _PressMethod, _IdxModDate) ->
    lists:reverse(SlotLists);
split_lists(LastPuff, SlotLists, 0, PressMethod, IdxModDate) ->
    {SlotD, _} = 
        generate_binary_slot(lookup, LastPuff, PressMethod, IdxModDate, no_timing),
    lists:reverse([SlotD|SlotLists]);
split_lists(KVList1, SlotLists, N, PressMethod, IdxModDate) ->
    {Slot, KVListRem} = lists:split(?LOOK_SLOTSIZE, KVList1),
    {SlotD, _} = 
        generate_binary_slot(lookup, Slot, PressMethod, IdxModDate, no_timing),
    split_lists(KVListRem, [SlotD|SlotLists], N - 1, PressMethod, IdxModDate).


-spec merge_lists(list(), list(), tuple(), sst_options(), boolean()) ->
                                {list(), list(), list(tuple()), tuple()|null}.
%% @doc
%% Merge lists when merging across more thna one file.  KVLists that are 
%% provided may include pointers to fetch more Keys/Values from the source
%% file
merge_lists(KVList1, KVList2, LevelInfo, SSTOpts, IndexModDate) ->
    merge_lists(KVList1, KVList2, 
                LevelInfo, 
                [], null, 0, 
                SSTOpts#sst_options.max_sstslots,
                SSTOpts#sst_options.press_method,
                IndexModDate,
                #build_timings{}).


merge_lists(KVL1, KVL2, LI, SlotList, FirstKey, MaxSlots, MaxSlots, 
                                            _PressMethod, _IdxModDate, T0) ->
    % This SST file is full, move to complete file, and return the 
    % remainder
    log_buildtimings(T0, LI),
    {KVL1, KVL2, lists:reverse(SlotList), FirstKey};
merge_lists([], [], LI, SlotList, FirstKey, _SlotCount, _MaxSlots,
                                            _PressMethod, _IdxModDate, T0) ->
    % the source files are empty, complete the file 
    log_buildtimings(T0, LI),
    {[], [], lists:reverse(SlotList), FirstKey};
merge_lists(KVL1, KVL2, LI, SlotList, FirstKey, SlotCount, MaxSlots,
                                            PressMethod, IdxModDate, T0) ->
    % Form a slot by merging the two lists until the next 128 K/V pairs have 
    % been determined
    SW = os:timestamp(),
    {KVRem1, KVRem2, Slot, FK0} =
        form_slot(KVL1, KVL2, LI, no_lookup, 0, [], FirstKey),
    T1 = update_buildtimings(SW, T0, fold_toslot),
    case Slot of
        {_, []} ->
            % There were no actual keys in the slot (maybe some expired)
            merge_lists(KVRem1,
                        KVRem2,
                        LI,
                        SlotList,
                        FK0,
                        SlotCount,
                        MaxSlots,
                        PressMethod,
                        IdxModDate,
                        T1);
        {Lookup, KVL} ->
            % Convert the list of KVs for the slot into a binary, and related
            % metadata
            {SlotD, T2} = 
                generate_binary_slot(Lookup, KVL, PressMethod, IdxModDate, T1),
            merge_lists(KVRem1,
                        KVRem2,
                        LI,
                        [SlotD|SlotList],
                        FK0,
                        SlotCount + 1,
                        MaxSlots,
                        PressMethod,
                        IdxModDate,
                        T2)
    end.

form_slot([], [], _LI, Type, _Size, Slot, FK) ->
    {[], [], {Type, lists:reverse(Slot)}, FK};
form_slot(KVList1, KVList2, _LI, lookup, ?LOOK_SLOTSIZE, Slot, FK) ->
    {KVList1, KVList2, {lookup, lists:reverse(Slot)}, FK};
form_slot(KVList1, KVList2, _LI, no_lookup, ?NOLOOK_SLOTSIZE, Slot, FK) ->
    {KVList1, KVList2, {no_lookup, lists:reverse(Slot)}, FK};
form_slot(KVList1, KVList2, {IsBasement, TS}, lookup, Size, Slot, FK) ->
    case {key_dominates(KVList1, KVList2, {IsBasement, TS}), FK} of
        {{{next_key, TopKV}, Rem1, Rem2}, _} ->
            form_slot(Rem1,
                        Rem2,
                        {IsBasement, TS},
                        lookup,
                        Size + 1,
                        [TopKV|Slot],
                        FK);
        {{skipped_key, Rem1, Rem2}, _} ->
            form_slot(Rem1, Rem2, {IsBasement, TS}, lookup, Size, Slot, FK)
    end;
form_slot(KVList1, KVList2, {IsBasement, TS}, no_lookup, Size, Slot, FK) ->
    case key_dominates(KVList1, KVList2, {IsBasement, TS}) of
        {{next_key, {TopK, TopV}}, Rem1, Rem2} ->
            FK0 =
                case FK of
                    null ->
                        TopK;
                    _ ->
                        FK
                end,
            case leveled_codec:to_lookup(TopK) of
                no_lookup ->
                    form_slot(Rem1,
                                Rem2,
                                {IsBasement, TS},
                                no_lookup,
                                Size + 1,
                                [{TopK, TopV}|Slot],
                                FK0);
                lookup ->
                    case Size >= ?LOOK_SLOTSIZE of
                        true ->
                            {KVList1,
                                KVList2,
                                {no_lookup, lists:reverse(Slot)},
                                FK};
                        false ->
                            form_slot(Rem1,
                                        Rem2,
                                        {IsBasement, TS},
                                        lookup,
                                        Size + 1,
                                        [{TopK, TopV}|Slot],
                                        FK0)
                    end
            end;        
        {skipped_key, Rem1, Rem2} ->
            form_slot(Rem1, Rem2, {IsBasement, TS}, no_lookup, Size, Slot, FK)
    end.

key_dominates(KL1, KL2, Level) ->
    key_dominates_expanded(maybe_expand_pointer(KL1),
                            maybe_expand_pointer(KL2),
                            Level).

key_dominates_expanded([H1|T1], [], Level) ->
    case leveled_codec:maybe_reap_expiredkey(H1, Level) of
        true ->
            {skipped_key, T1, []};
        false ->
            {{next_key, H1}, T1, []}
    end;
key_dominates_expanded([], [H2|T2], Level) ->
    case leveled_codec:maybe_reap_expiredkey(H2, Level) of
        true ->
            {skipped_key, [], T2};
        false ->
            {{next_key, H2}, [], T2}
    end;
key_dominates_expanded([H1|T1], [H2|T2], Level) ->
    case leveled_codec:key_dominates(H1, H2) of
        left_hand_first ->
            case leveled_codec:maybe_reap_expiredkey(H1, Level) of
                true ->
                    {skipped_key, T1, [H2|T2]};
                false ->
                    {{next_key, H1}, T1, [H2|T2]}
            end;
        right_hand_first ->
            case leveled_codec:maybe_reap_expiredkey(H2, Level) of
                true ->
                    {skipped_key, [H1|T1], T2};
                false ->
                    {{next_key, H2}, [H1|T1], T2}
            end;
        left_hand_dominant ->
            {skipped_key, [H1|T1], T2};
        right_hand_dominant ->
            {skipped_key, T1, [H2|T2]}
    end.


%% When a list is provided it may include a pointer to gain another batch of
%% entries from the same file, or a new batch of entries from another file
%%
%% This resultant list should include the Tail of any pointers added at the
%% end of the list

maybe_expand_pointer([]) ->
    [];
maybe_expand_pointer([{pointer, SSTPid, Slot, StartKey, all}|Tail]) ->
    expand_list_by_pointer({pointer, SSTPid, Slot, StartKey, all},
                            Tail,
                            ?MERGE_SCANWIDTH);
maybe_expand_pointer([{next, ManEntry, StartKey}|Tail]) ->
    expand_list_by_pointer({next, ManEntry, StartKey, all},
                            Tail,
                            ?MERGE_SCANWIDTH);
maybe_expand_pointer(List) ->
    List.
    


%%%============================================================================
%%% Timing Functions
%%%============================================================================

-spec update_buildtimings(erlang:timestamp(), build_timings(), atom()) 
                                                            -> build_timings().
%% @doc
%%
%% Timings taken from the build of a SST file.  
%%
%% There is no sample window, but the no_timing status is still used for 
%% level zero files where we're not breaking down the build time in this way.
update_buildtimings(_SW, no_timing, _Stage) ->
    no_timing;
update_buildtimings(SW, Timings, Stage) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    case Stage of 
        slot_hashlist ->
            HLT = Timings#build_timings.slot_hashlist + Timer,
            Timings#build_timings{slot_hashlist = HLT};
        slot_serialise ->
            SST = Timings#build_timings.slot_serialise + Timer,
            Timings#build_timings{slot_serialise = SST};
        slot_finish ->
            SFT = Timings#build_timings.slot_finish + Timer,
            Timings#build_timings{slot_finish = SFT};
        fold_toslot ->
            FST = Timings#build_timings.fold_toslot + Timer,
            Timings#build_timings{fold_toslot = FST}
    end.

-spec log_buildtimings(build_timings(), tuple()) -> ok.
%% @doc
%%
%% Log out the time spent during the merge lists part of the SST build
log_buildtimings(Timings, LI) ->
    leveled_log:log("SST13", [Timings#build_timings.fold_toslot,
                                Timings#build_timings.slot_hashlist,
                                Timings#build_timings.slot_serialise,
                                Timings#build_timings.slot_finish,
                                element(1, LI),
                                element(2, LI)]).


-spec update_statetimings(sst_timings(), integer(), non_neg_integer()) 
                                            -> {sst_timings(), integer()}.
%% @doc
%%
%% The timings state is either in countdown to the next set of samples of
%% we are actively collecting a sample.  Active collection take place 
%% when the countdown is 0.  Once the sample has reached the expected count
%% then there is a log of that sample, and the countdown is restarted.
%%
%% Outside of sample windows the timings object should be set to the atom
%% no_timing.  no_timing is a valid state for the cdb_timings type.
update_statetimings(no_timing, 0, _Level) ->
    {#sst_timings{}, 0};
update_statetimings(Timings, 0, Level) ->
    case Timings#sst_timings.sample_count of 
        SC when SC >= ?TIMING_SAMPLESIZE ->
            log_timings(Timings, Level),
                % If file at lower level wait longer before tsking another
                % sample
            {no_timing,
                leveled_rand:uniform(2 * ?TIMING_SAMPLECOUNTDOWN)};
        _SC ->
            {Timings, 0}
    end;
update_statetimings(no_timing, N, _Level) ->
    {no_timing, N - 1}.

log_timings(no_timing, _Level) ->
    ok;
log_timings(Timings, Level) ->
    leveled_log:log("SST12", [Level,
                                Timings#sst_timings.sample_count, 
                                Timings#sst_timings.index_query_time,
                                Timings#sst_timings.lookup_cache_time,
                                Timings#sst_timings.slot_index_time,
                                Timings#sst_timings.fetch_cache_time,
                                Timings#sst_timings.slot_fetch_time,
                                Timings#sst_timings.noncached_block_time,
                                Timings#sst_timings.slot_index_count,
                                Timings#sst_timings.fetch_cache_count,
                                Timings#sst_timings.slot_fetch_count,
                                Timings#sst_timings.noncached_block_count]).


update_timings(_SW, no_timing, _Stage, _Continue) ->
    {no_timing, no_timing};
update_timings(SW, Timings, Stage, Continue) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    Timings0 = 
        case Stage of 
            index_query ->
                IQT = Timings#sst_timings.index_query_time,
                Timings#sst_timings{index_query_time = IQT + Timer};
            lookup_cache ->
                TBT = Timings#sst_timings.lookup_cache_time,
                Timings#sst_timings{lookup_cache_time = TBT + Timer};
            slot_index ->
                SIT = Timings#sst_timings.slot_index_time,
                Timings#sst_timings{slot_index_time = SIT + Timer};
            fetch_cache ->
                FCT = Timings#sst_timings.fetch_cache_time,
                Timings#sst_timings{fetch_cache_time = FCT + Timer};
            slot_fetch ->
                SFT = Timings#sst_timings.slot_fetch_time,
                Timings#sst_timings{slot_fetch_time = SFT + Timer};
            noncached_block ->
                NCT = Timings#sst_timings.noncached_block_time,
                Timings#sst_timings{noncached_block_time = NCT + Timer}
        end,
    case Continue of 
        true ->
            {os:timestamp(), Timings0};
        false ->
            Timings1 = 
                case Stage of 
                    slot_index ->
                        SIC = Timings#sst_timings.slot_index_count,
                        Timings0#sst_timings{slot_index_count = SIC + 1};
                    fetch_cache ->
                        FCC = Timings#sst_timings.fetch_cache_count,
                        Timings0#sst_timings{fetch_cache_count = FCC + 1};
                    slot_fetch ->
                        SFC = Timings#sst_timings.slot_fetch_count,
                        Timings0#sst_timings{slot_fetch_count = SFC + 1};
                    noncached_block ->
                        NCC = Timings#sst_timings.noncached_block_count,
                        Timings0#sst_timings{noncached_block_count = NCC + 1}
                end,
            SC = Timings1#sst_timings.sample_count,
            {no_timing, Timings1#sst_timings{sample_count = SC + 1}}
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-define(TEST_AREA, "test/test_area/").

testsst_new(RootPath, Filename, Level, KVList, MaxSQN, PressMethod) ->
    OptsSST = 
        #sst_options{press_method=PressMethod,
                        log_options=leveled_log:get_opts()},
    sst_new(RootPath, Filename, Level, KVList, MaxSQN, OptsSST, false).

testsst_new(RootPath, Filename, 
            KVL1, KVL2, IsBasement, Level, MaxSQN, PressMethod) ->
    OptsSST = 
        #sst_options{press_method=PressMethod,
                        log_options=leveled_log:get_opts()},
    sst_new(RootPath, Filename, KVL1, KVL2, IsBasement, Level, MaxSQN,
            OptsSST, false).

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Seqn,
                        Count,
                        [],
                        BucketRangeLow,
                        BucketRangeHigh).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BRand = leveled_rand:uniform(BRange),
    BNumber =
        lists:flatten(io_lib:format("B~6..0B", [BucketLow + BRand])),
    KNumber =
        lists:flatten(io_lib:format("K~8..0B", [leveled_rand:uniform(1000000)])),
    LK = leveled_codec:to_ledgerkey("Bucket" ++ BNumber, "Key" ++ KNumber, o),
    Chunk = leveled_rand:rand_bytes(64),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, Seqn, Chunk, 64, infinity),
    MD = element(4, MV),
    ?assertMatch(undefined, element(3, MD)),
    MD0 = [{magic_md, [<<0:32/integer>>, base64:encode(Chunk)]}],
    MV0 = setelement(4, MV, setelement(3, MD, MD0)),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{LK, MV0}|Acc],
                        BucketLow,
                        BRange).


generate_indexkeys(Count) ->
    generate_indexkeys(Count, []).

generate_indexkeys(0, IndexList) ->
    IndexList;
generate_indexkeys(Count, IndexList) ->
    Changes = generate_indexkey(leveled_rand:uniform(8000), Count),
    generate_indexkeys(Count - 1, IndexList ++ Changes).

generate_indexkey(Term, Count) ->
    IndexSpecs = [{add, "t1_int", Term}],
    leveled_codec:idx_indexspecs(IndexSpecs, 
                                    "Bucket", 
                                    "Key" ++ integer_to_list(Count), 
                                    Count, 
                                    infinity).


form_slot_test() ->
    % If a skip key happens, mustn't switch to loookup by accident as could be
    % over the expected size
    SkippingKV = {{o, "B1", "K9999", null}, {9999, tomb, 1234567, {}}},
    Slot = [{{o, "B1", "K5", null}, {5, active, 99234567, {}}}],
    R1 = form_slot([SkippingKV], [],
                    {true, 99999999},
                    no_lookup,
                    ?LOOK_SLOTSIZE + 1,
                    Slot,
                    {o, "B1", "K5", null}),
    ?assertMatch({[], [], {no_lookup, Slot}, {o, "B1", "K5", null}}, R1).

merge_tombstonelist_test() ->
    % Merge lists with nothing but tombstones
    SkippingKV1 = {{o, "B1", "K9995", null}, {9995, tomb, 1234567, {}}},
    SkippingKV2 = {{o, "B1", "K9996", null}, {9996, tomb, 1234567, {}}},
    SkippingKV3 = {{o, "B1", "K9997", null}, {9997, tomb, 1234567, {}}},
    SkippingKV4 = {{o, "B1", "K9998", null}, {9998, tomb, 1234567, {}}},
    SkippingKV5 = {{o, "B1", "K9999", null}, {9999, tomb, 1234567, {}}},
    R = merge_lists([SkippingKV1, SkippingKV3, SkippingKV5],
                        [SkippingKV2, SkippingKV4],
                        {true, 9999999},
                        #sst_options{press_method = native,
                                        max_sstslots = 256},
                        ?INDEX_MODDATE),
    ?assertMatch({[], [], [], null}, R).

indexed_list_test() ->
    io:format(user, "~nIndexed list timing test:~n", []),
    N = 150,
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 4)),
    KVL1 = lists:sublist(KVL0, ?LOOK_SLOTSIZE),

    SW0 = os:timestamp(),

    {{_PosBinIndex1, FullBin, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, KVL1, native, ?INDEX_MODDATE, no_timing),
    io:format(user,
                "Indexed list created slot in ~w microseconds of size ~w~n",
                [timer:now_diff(os:timestamp(), SW0), byte_size(FullBin)]),

    {TestK1, TestV1} = lists:nth(20, KVL1),
    MH1 = leveled_codec:segment_hash(TestK1),
    {TestK2, TestV2} = lists:nth(40, KVL1),
    MH2 = leveled_codec:segment_hash(TestK2),
    {TestK3, TestV3} = lists:nth(60, KVL1),
    MH3 = leveled_codec:segment_hash(TestK3),
    {TestK4, TestV4} = lists:nth(80, KVL1),
    MH4 = leveled_codec:segment_hash(TestK4),
    {TestK5, TestV5} = lists:nth(100, KVL1),
    MH5 = leveled_codec:segment_hash(TestK5),

    test_binary_slot(FullBin, TestK1, MH1, {TestK1, TestV1}),
    test_binary_slot(FullBin, TestK2, MH2, {TestK2, TestV2}),
    test_binary_slot(FullBin, TestK3, MH3, {TestK3, TestV3}),
    test_binary_slot(FullBin, TestK4, MH4, {TestK4, TestV4}),
    test_binary_slot(FullBin, TestK5, MH5, {TestK5, TestV5}).


indexed_list_mixedkeys_test() ->
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, 50, 1, 4)),
    KVL1 = lists:sublist(KVL0, 33),
    Keys = lists:ukeysort(1, generate_indexkeys(60) ++ KVL1),

    {{_PosBinIndex1, FullBin, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, Keys, native, ?INDEX_MODDATE, no_timing),

    {TestK1, TestV1} = lists:nth(4, KVL1),
    MH1 = leveled_codec:segment_hash(TestK1),
    {TestK2, TestV2} = lists:nth(8, KVL1),
    MH2 = leveled_codec:segment_hash(TestK2),
    {TestK3, TestV3} = lists:nth(12, KVL1),
    MH3 = leveled_codec:segment_hash(TestK3),
    {TestK4, TestV4} = lists:nth(16, KVL1),
    MH4 = leveled_codec:segment_hash(TestK4),
    {TestK5, TestV5} = lists:nth(20, KVL1),
    MH5 = leveled_codec:segment_hash(TestK5),

    test_binary_slot(FullBin, TestK1, MH1, {TestK1, TestV1}),
    test_binary_slot(FullBin, TestK2, MH2, {TestK2, TestV2}),
    test_binary_slot(FullBin, TestK3, MH3, {TestK3, TestV3}),
    test_binary_slot(FullBin, TestK4, MH4, {TestK4, TestV4}),
    test_binary_slot(FullBin, TestK5, MH5, {TestK5, TestV5}).

indexed_list_mixedkeys2_test() ->
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, 50, 1, 4)),
    KVL1 = lists:sublist(KVL0, 33),
    IdxKeys1 = lists:ukeysort(1, generate_indexkeys(30)),
    IdxKeys2 = lists:ukeysort(1, generate_indexkeys(30)),
    % this isn't actually ordered correctly
    Keys = IdxKeys1 ++ KVL1 ++ IdxKeys2,
    {{_Header, FullBin, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, Keys, native, ?INDEX_MODDATE, no_timing),
    lists:foreach(fun({K, V}) ->
                        MH = leveled_codec:segment_hash(K),
                        test_binary_slot(FullBin, K, MH, {K, V})
                        end,
                    KVL1).

indexed_list_allindexkeys_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(150)), 
                            ?LOOK_SLOTSIZE),
    {{HeaderT, FullBinT, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, Keys, native, true, no_timing),
    {{HeaderF, FullBinF, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, Keys, native, false, no_timing),
    EmptySlotSize = ?LOOK_SLOTSIZE - 1,
    LMD = ?FLIPPER32,
    ?assertMatch(<<_BL:20/binary, LMD:32/integer, EmptySlotSize:8/integer>>, 
                    HeaderT),
    ?assertMatch(<<_BL:20/binary, EmptySlotSize:8/integer>>, 
                    HeaderF),
    % SW = os:timestamp(),
    BinToListT = binaryslot_tolist(FullBinT, native, true),
    BinToListF = binaryslot_tolist(FullBinF, native, false),
    % io:format(user,
    %             "Indexed list flattened in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]),
    ?assertMatch(Keys, BinToListT),
    ?assertMatch({Keys, none}, binaryslot_trimmedlist(FullBinT,
                                                        all, all, 
                                                        native, 
                                                        true,
                                                        false)),
    ?assertMatch(Keys, BinToListF),
    ?assertMatch({Keys, none}, binaryslot_trimmedlist(FullBinF,
                                                        all, all, 
                                                        native, 
                                                        false,
                                                        false)).

indexed_list_allindexkeys_nolookup_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(1000)),
                            ?NOLOOK_SLOTSIZE),
    {{Header, FullBin, _HL, _LK}, no_timing} = 
        generate_binary_slot(no_lookup, Keys, native, ?INDEX_MODDATE,no_timing),
    ?assertMatch(<<_BL:20/binary, _LMD:32/integer, 127:8/integer>>, Header),
    % SW = os:timestamp(),
    BinToList = binaryslot_tolist(FullBin, native, ?INDEX_MODDATE),
    % io:format(user,
    %             "Indexed list flattened in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]),
    ?assertMatch(Keys, BinToList),
    ?assertMatch({Keys, none}, binaryslot_trimmedlist(FullBin, 
                                                        all, all, 
                                                        native,
                                                        ?INDEX_MODDATE,
                                                        false)).

indexed_list_allindexkeys_trimmed_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(150)), 
                            ?LOOK_SLOTSIZE),
    {{Header, FullBin, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, Keys, native, ?INDEX_MODDATE,no_timing),
    EmptySlotSize = ?LOOK_SLOTSIZE - 1,
    ?assertMatch(<<_BL:20/binary, _LMD:32/integer, EmptySlotSize:8/integer>>,
                    Header),
    ?assertMatch({Keys, none}, binaryslot_trimmedlist(FullBin,
                                                        {i, 
                                                            "Bucket", 
                                                            {"t1_int", 0},
                                                            null}, 
                                                        {i, 
                                                            "Bucket", 
                                                            {"t1_int", 99999},
                                                            null},
                                                        native,
                                                        ?INDEX_MODDATE,
                                                        false)),
    
    {SK1, _} = lists:nth(10, Keys),
    {EK1, _} = lists:nth(100, Keys),
    R1 = lists:sublist(Keys, 10, 91),
    {O1, none} = binaryslot_trimmedlist(FullBin, SK1, EK1, 
                                        native, ?INDEX_MODDATE, false),
    ?assertMatch(91, length(O1)),
    ?assertMatch(R1, O1),

    {SK2, _} = lists:nth(10, Keys),
    {EK2, _} = lists:nth(20, Keys),
    R2 = lists:sublist(Keys, 10, 11),
    {O2, none} = binaryslot_trimmedlist(FullBin, SK2, EK2,
                                        native, ?INDEX_MODDATE, false),
    ?assertMatch(11, length(O2)),
    ?assertMatch(R2, O2),

    {SK3, _} = lists:nth(?LOOK_SLOTSIZE - 1, Keys),
    {EK3, _} = lists:nth(?LOOK_SLOTSIZE, Keys),
    R3 = lists:sublist(Keys, ?LOOK_SLOTSIZE - 1, 2),
    {O3, none} = binaryslot_trimmedlist(FullBin, SK3, EK3,
                                        native, ?INDEX_MODDATE, false),
    ?assertMatch(2, length(O3)),
    ?assertMatch(R3, O3).


indexed_list_mixedkeys_bitflip_test() ->
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, 50, 1, 4)),
    KVL1 = lists:sublist(KVL0, 33),
    Keys = lists:ukeysort(1, generate_indexkeys(60) ++ KVL1),
    {{Header, SlotBin, _HL, LK}, no_timing} = 
        generate_binary_slot(lookup, Keys, native, ?INDEX_MODDATE, no_timing),
    
    ?assertMatch(LK, element(1, lists:last(Keys))),

    <<B1L:32/integer, 
        _B2L:32/integer, 
        _B3L:32/integer, 
        _B4L:32/integer, 
        _B5L:32/integer,
        _LMD:32/integer,
        PosBin/binary>> = Header,
    
    TestKey1 = element(1, lists:nth(1, KVL1)), 
    TestKey2 = element(1, lists:nth(33, KVL1)), 
    MH1 = leveled_codec:segment_hash(TestKey1),
    MH2 = leveled_codec:segment_hash(TestKey2),

    test_binary_slot(SlotBin, TestKey1, MH1, lists:nth(1, KVL1)),
    test_binary_slot(SlotBin, TestKey2, MH2, lists:nth(33, KVL1)),
    ToList = binaryslot_tolist(SlotBin, native, ?INDEX_MODDATE),
    ?assertMatch(Keys, ToList),

    [Pos1] = find_pos(PosBin, extract_hash(MH1), [], 0),
    [Pos2] = find_pos(PosBin, extract_hash(MH2), [], 0),
    {BN1, _BP1} = revert_position(Pos1),
    {BN2, _BP2} = revert_position(Pos2),
    {Offset1, Length1} = block_offsetandlength(Header, BN1),
    {Offset2, Length2} = block_offsetandlength(Header, BN2),

    SlotBin1 = flip_byte(SlotBin, byte_size(Header) + 12 + Offset1, Length1),
    SlotBin2 = flip_byte(SlotBin, byte_size(Header) + 12 + Offset2, Length2),

    test_binary_slot(SlotBin2, TestKey1, MH1, lists:nth(1, KVL1)),
    test_binary_slot(SlotBin1, TestKey2, MH2, lists:nth(33, KVL1)),

    test_binary_slot(SlotBin1, TestKey1, MH1, not_present),
    test_binary_slot(SlotBin2, TestKey2, MH2, not_present),

    ToList1 = binaryslot_tolist(SlotBin1, native, ?INDEX_MODDATE),
    ToList2 = binaryslot_tolist(SlotBin2, native, ?INDEX_MODDATE),

    ?assertMatch(true, is_list(ToList1)),
    ?assertMatch(true, is_list(ToList2)),
    ?assertMatch(true, length(ToList1) > 0),
    ?assertMatch(true, length(ToList2) > 0),
    ?assertMatch(true, length(ToList1) < length(Keys)),
    ?assertMatch(true, length(ToList2) < length(Keys)),

    SlotBin3 = flip_byte(SlotBin, byte_size(Header) + 12, B1L),

    {SK1, _} = lists:nth(10, Keys),
    {EK1, _} = lists:nth(20, Keys),
    {O1, none} = binaryslot_trimmedlist(SlotBin3, SK1, EK1,
                                        native, ?INDEX_MODDATE, false),
    ?assertMatch([], O1),
    
    SlotBin4 = flip_byte(SlotBin, 0, 20),
    SlotBin5 = flip_byte(SlotBin, 20, byte_size(Header) - 20 - 12),

    test_binary_slot(SlotBin4, TestKey1, MH1, not_present),
    test_binary_slot(SlotBin5, TestKey1, MH1, not_present),
    ToList4 = binaryslot_tolist(SlotBin4, native, ?INDEX_MODDATE),
    ToList5 = binaryslot_tolist(SlotBin5, native, ?INDEX_MODDATE),
    ?assertMatch([], ToList4),
    ?assertMatch([], ToList5),
    {O4, none} = binaryslot_trimmedlist(SlotBin4, SK1, EK1,
                                        native, ?INDEX_MODDATE, false),
    {O5, none} = binaryslot_trimmedlist(SlotBin4, SK1, EK1,
                                        native, ?INDEX_MODDATE, false),
    ?assertMatch([], O4),
    ?assertMatch([], O5).


flip_byte(Binary, Offset, Length) ->
    Byte1 = leveled_rand:uniform(Length) + Offset - 1,
    <<PreB1:Byte1/binary, A:8/integer, PostByte1/binary>> = Binary,
    case A of 
        0 ->
            <<PreB1:Byte1/binary, 255:8/integer, PostByte1/binary>>;
        _ ->
            <<PreB1:Byte1/binary, 0:8/integer, PostByte1/binary>>
    end.


test_binary_slot(FullBin, Key, Hash, ExpectedValue) ->
    % SW = os:timestamp(),
    {ReturnedValue, _Header} =
        binaryslot_get(FullBin, Key, Hash, native, ?INDEX_MODDATE),
    ?assertMatch(ExpectedValue, ReturnedValue).
    % io:format(user, "Fetch success in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]).

    
merge_test() ->
    filelib:ensure_dir(?TEST_AREA),
    merge_tester(fun testsst_new/6, fun testsst_new/8).


merge_tester(NewFunS, NewFunM) ->
    N = 3000,
    KVL1 = lists:ukeysort(1, generate_randomkeys(N + 1, N, 1, 20)),
    KVL2 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 20)),
    KVL3 = lists:ukeymerge(1, KVL1, KVL2),
    SW0 = os:timestamp(),
    {ok, P1, {FK1, LK1}, _Bloom1} = 
        NewFunS(?TEST_AREA, "level1_src", 1, KVL1, 6000, native),
    {ok, P2, {FK2, LK2}, _Bloom2} = 
        NewFunS(?TEST_AREA, "level2_src", 2, KVL2, 3000, native),
    ExpFK1 = element(1, lists:nth(1, KVL1)),
    ExpLK1 = element(1, lists:last(KVL1)),
    ExpFK2 = element(1, lists:nth(1, KVL2)),
    ExpLK2 = element(1, lists:last(KVL2)),
    ?assertMatch(ExpFK1, FK1),
    ?assertMatch(ExpFK2, FK2),
    ?assertMatch(ExpLK1, LK1),
    ?assertMatch(ExpLK2, LK2),
    ML1 = [{next, #manifest_entry{owner = P1}, FK1}],
    ML2 = [{next, #manifest_entry{owner = P2}, FK2}],
    NewR = 
        NewFunM(?TEST_AREA, "level2_merge", ML1, ML2, false, 2, N * 2, native),
    {ok, P3, {{Rem1, Rem2}, FK3, LK3}, _Bloom3} = NewR,
    ?assertMatch([], Rem1),
    ?assertMatch([], Rem2),
    ?assertMatch(true, FK3 == min(FK1, FK2)),
    io:format("LK1 ~w LK2 ~w LK3 ~w~n", [LK1, LK2, LK3]),
    ?assertMatch(true, LK3 == max(LK1, LK2)),
    io:format(user,
                "Created and merged two files of size ~w in ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW0)]),
    
    SW1 = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(P3, K))
                        end,
                    KVL3),
    io:format(user,
                "Checked presence of all ~w objects in ~w microseconds~n",
                [length(KVL3), timer:now_diff(os:timestamp(), SW1)]),
    
    ok = sst_close(P1),
    ok = sst_close(P2),
    ok = sst_close(P3),
    ok = file:delete(?TEST_AREA ++ "/level1_src.sst"),
    ok = file:delete(?TEST_AREA ++ "/level2_src.sst"),
    ok = file:delete(?TEST_AREA ++ "/level2_merge.sst").
    

simple_persisted_range_test() ->
    simple_persisted_range_tester(fun testsst_new/6).

simple_persisted_range_tester(SSTNewFun) ->
    {RP, Filename} = {?TEST_AREA, "simple_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 16, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} = 
        SSTNewFun(RP, Filename, 1, KVList1, length(KVList1), native),
    
    {o, B, K, null} = LastKey,
    SK1 = {o, B, K, 0},
    EK1 = {o, B, K, 1},
    FetchListA1 = sst_getkvrange(Pid, SK1, EK1, 1),
    ?assertMatch([], FetchListA1),
    
    SK2 = element(1, lists:nth(127, KVList1)),
    SK3 = element(1, lists:nth(128, KVList1)),
    SK4 = element(1, lists:nth(129, KVList1)),
    SK5 = element(1, lists:nth(130, KVList1)),
    
    EK2 = element(1, lists:nth(255, KVList1)),
    EK3 = element(1, lists:nth(256, KVList1)),
    EK4 = element(1, lists:nth(257, KVList1)),
    EK5 = element(1, lists:nth(258, KVList1)),
    
    TestFun =
        fun({SK, EK}) ->
            FetchList = sst_getkvrange(Pid, SK, EK, 4),
            ?assertMatch(SK, element(1, lists:nth(1, FetchList))),
            ?assertMatch(EK, element(1, lists:last(FetchList)))
        end,
    
    TL2 = lists:map(fun(EK) -> {SK2, EK} end, [EK2, EK3, EK4, EK5]),
    TL3 = lists:map(fun(EK) -> {SK3, EK} end, [EK2, EK3, EK4, EK5]),
    TL4 = lists:map(fun(EK) -> {SK4, EK} end, [EK2, EK3, EK4, EK5]),
    TL5 = lists:map(fun(EK) -> {SK5, EK} end, [EK2, EK3, EK4, EK5]),
    lists:foreach(TestFun, TL2 ++ TL3 ++ TL4 ++ TL5).                


simple_persisted_rangesegfilter_test() ->
    simple_persisted_rangesegfilter_tester(fun testsst_new/6).

simple_persisted_rangesegfilter_tester(SSTNewFun) ->
    {RP, Filename} = {?TEST_AREA, "range_segfilter_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 16, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} = 
        SSTNewFun(RP, Filename, 1, KVList1, length(KVList1), native),
    
    SK1 = element(1, lists:nth(124, KVList1)),
    SK2 = element(1, lists:nth(126, KVList1)),
    SK3 = element(1, lists:nth(128, KVList1)),
    SK4 = element(1, lists:nth(130, KVList1)),
    SK5 = element(1, lists:nth(132, KVList1)),
    
    EK1 = element(1, lists:nth(252, KVList1)),
    EK2 = element(1, lists:nth(254, KVList1)),
    EK3 = element(1, lists:nth(256, KVList1)),
    EK4 = element(1, lists:nth(258, KVList1)),
    EK5 = element(1, lists:nth(260, KVList1)),
    
    GetSegFun = 
        fun(LK) ->
            extract_hash(
                leveled_codec:strip_to_segmentonly(
                    lists:keyfind(LK, 1, KVList1)))
        end,
    SegList = 
        lists:map(GetSegFun,
                    [SK1, SK2, SK3, SK4, SK5, EK1, EK2, EK3, EK4, EK5]),
    
    TestFun =
        fun(StartKey, EndKey, OutList) ->
            RangeKVL =
                sst_getfilteredrange(Pid, StartKey, EndKey, 4, SegList, 0),
            RangeKL = lists:map(fun({LK0, _LV0}) -> LK0 end, RangeKVL),
            ?assertMatch(true, lists:member(StartKey, RangeKL)),
            ?assertMatch(true, lists:member(EndKey, RangeKL)),
            CheckOutFun =
                fun(OutKey) ->
                    ?assertMatch(false, lists:member(OutKey, RangeKL))
                end,
            lists:foreach(CheckOutFun, OutList)
        end,
    
    lists:foldl(fun(SK0, Acc) -> 
                    TestFun(SK0, EK1, [EK2, EK3, EK4, EK5] ++ Acc),
                    [SK0|Acc]
                end,
                [],
                [SK1, SK2, SK3, SK4, SK5]),
    lists:foldl(fun(SK0, Acc) -> 
                    TestFun(SK0, EK2, [EK3, EK4, EK5] ++ Acc),
                    [SK0|Acc]
                end,
                [],
                [SK1, SK2, SK3, SK4, SK5]),
    lists:foldl(fun(SK0, Acc) -> 
                    TestFun(SK0, EK3, [EK4, EK5] ++ Acc),
                    [SK0|Acc]
                end,
                [],
                [SK1, SK2, SK3, SK4, SK5]),
    lists:foldl(fun(SK0, Acc) -> 
                    TestFun(SK0, EK4, [EK5] ++ Acc),
                    [SK0|Acc]
                end,
                [],
                [SK1, SK2, SK3, SK4, SK5]),
    
    ok = sst_clear(Pid).
    


additional_range_test() ->
    % Test fetching ranges that fall into odd situations with regards to the
    % summayr index
    % - ranges which fall between entries in summary
    % - ranges which go beyond the end of the range of the sst
    % - ranges which match to an end key in the summary index
    IK1 = lists:foldl(fun(X, Acc) ->
                            Acc ++ generate_indexkey(X, X)
                        end,
                        [],
                        lists:seq(1, ?NOLOOK_SLOTSIZE)),
    Gap = 2,
    IK2 = lists:foldl(fun(X, Acc) ->
                            Acc ++ generate_indexkey(X, X)
                        end,
                        [],
                        lists:seq(?NOLOOK_SLOTSIZE + Gap + 1,
                                    2 * ?NOLOOK_SLOTSIZE + Gap)),
    {ok, P1, {{Rem1, Rem2}, SK, EK}, _Bloom1} = 
        testsst_new(?TEST_AREA, "range1_src", IK1, IK2, false, 1, 9999, native),
    ?assertMatch([], Rem1),
    ?assertMatch([], Rem2),
    ?assertMatch(SK, element(1, lists:nth(1, IK1))),
    ?assertMatch(EK, element(1, lists:last(IK2))),
    
    % Basic test - checking scanwidth
    R1 = sst_getkvrange(P1, SK, EK, 1),
    ?assertMatch(?NOLOOK_SLOTSIZE + 1, length(R1)),
    QR1 = lists:sublist(R1, ?NOLOOK_SLOTSIZE), 
    ?assertMatch(IK1, QR1),
    R2 = sst_getkvrange(P1, SK, EK, 2),
    ?assertMatch(?NOLOOK_SLOTSIZE * 2, length(R2)),
    QR2 = lists:sublist(R2, ?NOLOOK_SLOTSIZE),
    QR3 = lists:sublist(R2, ?NOLOOK_SLOTSIZE + 1, 2 * ?NOLOOK_SLOTSIZE),
    ?assertMatch(IK1, QR2),
    ?assertMatch(IK2, QR3),
    
    % Testing the gap
    [GapSKV] = generate_indexkey(?NOLOOK_SLOTSIZE + 1, ?NOLOOK_SLOTSIZE + 1),
    [GapEKV] = generate_indexkey(?NOLOOK_SLOTSIZE + 2, ?NOLOOK_SLOTSIZE + 2),
    R3 = sst_getkvrange(P1, element(1, GapSKV), element(1, GapEKV), 1),
    ?assertMatch([], R3),
    
    % Testing beyond the range
    [PastEKV] = generate_indexkey(2 * ?NOLOOK_SLOTSIZE + Gap + 1,
                                    2 * ?NOLOOK_SLOTSIZE + Gap + 1),
    R4 = sst_getkvrange(P1, element(1, GapSKV), element(1, PastEKV), 2),
    ?assertMatch(IK2, R4),
    R5 = sst_getkvrange(P1, SK, element(1, PastEKV), 2),
    IKAll = IK1 ++ IK2,
    ?assertMatch(IKAll, R5),
    [MidREKV] = generate_indexkey(?NOLOOK_SLOTSIZE + Gap + 2,
                                    ?NOLOOK_SLOTSIZE + Gap + 2),
    io:format(user, "Mid second range to past range test~n", []),
    R6 = sst_getkvrange(P1, element(1, MidREKV), element(1, PastEKV), 2),
    Exp6 = lists:sublist(IK2, 2, length(IK2)),
    ?assertMatch(Exp6, R6),
    
    % Testing at a slot end
    Slot1EK = element(1, lists:last(IK1)),
    R7 = sst_getkvrange(P1, SK, Slot1EK, 2),
    ?assertMatch(IK1, R7).
    
    % Testing beyond end (should never happen if manifest behaves)
    % Test blows up anyway
    % R8 = sst_getkvrange(P1, element(1, PastEKV), element(1, PastEKV), 2),
    % ?assertMatch([], R8).
    

simple_persisted_slotsize_test() ->
    simple_persisted_slotsize_tester(fun testsst_new/6).


simple_persisted_slotsize_tester(SSTNewFun) ->
    {RP, Filename} = {?TEST_AREA, "simple_slotsize_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 2, 1, 20),
    KVList1 = lists:sublist(lists:ukeysort(1, KVList0),
                            ?LOOK_SLOTSIZE),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} = 
        SSTNewFun(RP, Filename, 1, KVList1, length(KVList1), native),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(Pid, K))
                        end,
                    KVList1),
    ok = sst_close(Pid),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).


delete_pending_test_() ->
    {timeout, 30, fun delete_pending_tester/0}.

delete_pending_tester() ->
    % Confirm no race condition between the GC call and the delete timeout
    {RP, Filename} = {?TEST_AREA, "deletepending_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 32, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} = 
        testsst_new(RP, Filename, 1, KVList1, length(KVList1), native),
    timer:sleep(2000),
    leveled_sst:sst_setfordelete(Pid, false),
    timer:sleep(?DELETE_TIMEOUT + 1000),
    ?assertMatch(false, is_process_alive(Pid)).


simple_persisted_test_() ->
    {timeout, 60, fun simple_persisted_test_bothformats/0}.

simple_persisted_test_bothformats() ->
    simple_persisted_tester(fun testsst_new/6).

simple_persisted_tester(SSTNewFun) ->
    Level = 3,
    {RP, Filename} = {?TEST_AREA, "simple_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 32, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} = 
        SSTNewFun(RP, Filename, Level, KVList1, length(KVList1), native),
    
    B0 = check_binary_references(Pid),

    SW0 = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(Pid, K))
                        end,
                    KVList1),
    io:format(user,
                "Checking for ~w keys (once) in file with cache hit took ~w "
                    ++ "microseconds~n",
                [length(KVList1), timer:now_diff(os:timestamp(), SW0)]),
    SW1 = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(Pid, K)),
                        ?assertMatch({K, V}, sst_get(Pid, K))
                        end,
                    KVList1),
    io:format(user,
                "Checking for ~w keys (twice) in file with cache hit took ~w "
                    ++ "microseconds~n",
                [length(KVList1), timer:now_diff(os:timestamp(), SW1)]),
    ok = sst_printtimings(Pid),
    KVList2 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 32, 1, 20),
    MapFun =
        fun({K, V}, Acc) ->
            In = lists:keymember(K, 1, KVList1),
            case {K > FirstKey, LastKey > K, In} of
                {true, true, false} ->
                    [{K, leveled_codec:segment_hash(K), V}|Acc];
                _ ->
                    Acc
            end
        end,
    KVList3 = lists:foldl(MapFun, [], KVList2),
    SW2 = os:timestamp(),
    lists:foreach(fun({K, H, _V}) ->
                        ?assertMatch(not_present, sst_get(Pid, K, H))
                        end,
                    KVList3),
    io:format(user,
                "Checking for ~w missing keys took ~w microseconds~n",
                [length(KVList3), timer:now_diff(os:timestamp(), SW2)]),
    ok = sst_printtimings(Pid),
    FetchList1 = sst_getkvrange(Pid, all, all, 2),
    FoldFun = fun(X, Acc) ->
                    case X of
                        {pointer, P, S, SK, EK} ->
                            Acc ++ sst_getslots(P, [{pointer, P, S, SK, EK}]);
                        _ ->
                            Acc ++ [X]
                    end end,
    FetchedList1 = lists:foldl(FoldFun, [], FetchList1),
    ?assertMatch(KVList1, FetchedList1),
    
    {TenthKey, _v10} = lists:nth(10, KVList1),
    {Three000Key, _v300} = lists:nth(300, KVList1),
    SubKVList1 = lists:sublist(KVList1, 10, 291),
    SubKVList1L = length(SubKVList1),
    FetchList2 = sst_getkvrange(Pid, TenthKey, Three000Key, 2),
    ?assertMatch(pointer, element(1, lists:last(FetchList2))),
    FetchedList2 = lists:foldl(FoldFun, [], FetchList2),
    ?assertMatch(SubKVList1L, length(FetchedList2)),
    ?assertMatch(SubKVList1, FetchedList2),
    
    {Eight000Key, _v800} = lists:nth(800, KVList1),
    SubKVListA1 = lists:sublist(KVList1, 10, 791),
    SubKVListA1L = length(SubKVListA1),
    FetchListA2 = sst_getkvrange(Pid, TenthKey, Eight000Key, 2),
    ?assertMatch(pointer, element(1, lists:last(FetchListA2))),
    FetchedListA2 = lists:foldl(FoldFun, [], FetchListA2),
    ?assertMatch(SubKVListA1L, length(FetchedListA2)),
    ?assertMatch(SubKVListA1, FetchedListA2),
    
    FetchListB2 = sst_getkvrange(Pid, TenthKey, Eight000Key, 4),
    ?assertMatch(pointer, element(1, lists:last(FetchListB2))),
    FetchedListB2 = lists:foldl(FoldFun, [], FetchListB2),
    ?assertMatch(SubKVListA1L, length(FetchedListB2)),
    ?assertMatch(SubKVListA1, FetchedListB2),
    
    FetchListB3 = sst_getkvrange(Pid,
                                    Eight000Key,
                                    {o, null, null, null},
                                    4),
    FetchedListB3 = lists:foldl(FoldFun, [], FetchListB3),
    SubKVListA3 = lists:nthtail(800 - 1, KVList1),
    SubKVListA3L = length(SubKVListA3),
    io:format("Length expected ~w~n", [SubKVListA3L]),
    ?assertMatch(SubKVListA3L, length(FetchedListB3)),
    ?assertMatch(SubKVListA3, FetchedListB3),
    
    io:format("Eight hundredth key ~w~n", [Eight000Key]),
    FetchListB4 = sst_getkvrange(Pid,
                                    Eight000Key,
                                    Eight000Key,
                                    4),
    FetchedListB4 = lists:foldl(FoldFun, [], FetchListB4),
    ?assertMatch([{Eight000Key, _v800}], FetchedListB4),
    
    B1 = check_binary_references(Pid),

    ok = sst_close(Pid),

    io:format(user, "Reopen SST file~n", []),
    OptsSST = #sst_options{press_method=native,
                            log_options=leveled_log:get_opts()},
    {ok, OpenP, {FirstKey, LastKey}, _Bloom} =
        sst_open(RP, Filename ++ ".sst", OptsSST, Level),

    B2 = check_binary_references(OpenP),

    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP, K)),
                        ?assertMatch({K, V}, sst_get(OpenP, K))
                        end,
                    KVList1),

    garbage_collect(OpenP),
    B3 = check_binary_references(OpenP),
    ?assertMatch(0, B2), % Opens with an empty cache
    ?assertMatch(true, B3 > B2), % Now has headers in cache
    ?assertMatch(false, B3 > B0 * 2), 
        % Not significantly bigger than when created new
    ?assertMatch(false, B3 > B1 * 2), 
        % Not significantly bigger than when created new

    ok = sst_close(OpenP),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).

check_binary_references(Pid) ->
    garbage_collect(Pid),
    {binary, BinList} = process_info(Pid, binary),
    TotalBinMem = 
        lists:foldl(fun({_R, BM, _RC}, Acc) -> Acc + BM end, 0, BinList),
    io:format(user, "Total binary memory ~w~n", [TotalBinMem]),
    TotalBinMem.

key_dominates_test() ->
    KV1 = {{o, "Bucket", "Key1", null}, {5, {active, infinity}, 0, []}},
    KV2 = {{o, "Bucket", "Key3", null}, {6, {active, infinity}, 0, []}},
    KV3 = {{o, "Bucket", "Key2", null}, {3, {active, infinity}, 0, []}},
    KV4 = {{o, "Bucket", "Key4", null}, {7, {active, infinity}, 0, []}},
    KV5 = {{o, "Bucket", "Key1", null}, {4, {active, infinity}, 0, []}},
    KV6 = {{o, "Bucket", "Key1", null}, {99, {tomb, 999}, 0, []}},
    KV7 = {{o, "Bucket", "Key1", null}, {99, tomb, 0, []}},
    KL1 = [KV1, KV2],
    KL2 = [KV3, KV4],
    ?assertMatch({{next_key, KV1}, [KV2], KL2},
                    key_dominates(KL1, KL2, {undefined, 1})),
    ?assertMatch({{next_key, KV1}, KL2, [KV2]},
                    key_dominates(KL2, KL1, {undefined, 1})),
    ?assertMatch({skipped_key, KL2, KL1},
                    key_dominates([KV5|KL2], KL1, {undefined, 1})),
    ?assertMatch({{next_key, KV1}, [KV2], []},
                    key_dominates(KL1, [], {undefined, 1})),
    ?assertMatch({skipped_key, [KV6|KL2], [KV2]},
                    key_dominates([KV6|KL2], KL1, {undefined, 1})),
    ?assertMatch({{next_key, KV6}, KL2, [KV2]},
                    key_dominates([KV6|KL2], [KV2], {undefined, 1})),
    ?assertMatch({skipped_key, [KV6|KL2], [KV2]},
                    key_dominates([KV6|KL2], KL1, {true, 1})),
    ?assertMatch({skipped_key, [KV6|KL2], [KV2]},
                    key_dominates([KV6|KL2], KL1, {true, 1000})),
    ?assertMatch({{next_key, KV6}, KL2, [KV2]},
                    key_dominates([KV6|KL2], [KV2], {true, 1})),
    ?assertMatch({skipped_key, KL2, [KV2]},
                    key_dominates([KV6|KL2], [KV2], {true, 1000})),
    ?assertMatch({skipped_key, [], []},
                    key_dominates([KV6], [], {true, 1000})),
    ?assertMatch({skipped_key, [], []},
                    key_dominates([], [KV6], {true, 1000})),
    ?assertMatch({{next_key, KV6}, [], []},
                    key_dominates([KV6], [], {true, 1})),
    ?assertMatch({{next_key, KV6}, [], []},
                    key_dominates([], [KV6], {true, 1})),
    ?assertMatch({skipped_key, [], []},
                    key_dominates([KV7], [], {true, 1})),
    ?assertMatch({skipped_key, [], []},
                    key_dominates([], [KV7], {true, 1})),
    ?assertMatch({skipped_key, [KV7|KL2], [KV2]},
                    key_dominates([KV7|KL2], KL1, {undefined, 1})),
    ?assertMatch({{next_key, KV7}, KL2, [KV2]},
                    key_dominates([KV7|KL2], [KV2], {undefined, 1})),
    ?assertMatch({skipped_key, [KV7|KL2], [KV2]},
                    key_dominates([KV7|KL2], KL1, {true, 1})),
    ?assertMatch({skipped_key, KL2, [KV2]},
                    key_dominates([KV7|KL2], [KV2], {true, 1})).

nonsense_coverage_test() ->
    {ok, Pid} = gen_fsm:start_link(?MODULE, [], []),
    ok = gen_fsm:send_all_state_event(Pid, nonsense),
    ?assertMatch({ok, reader, #state{}}, code_change(nonsense,
                                                        reader,
                                                        #state{},
                                                        nonsense)),
    ?assertMatch({reply, undefined, reader, #state{}},
                    handle_sync_event("hello", self(), reader, #state{})),
                    
    SampleBin = <<0:128/integer>>,
    FlippedBin = flip_byte(SampleBin, 0, 16),
    ?assertMatch(false, FlippedBin == SampleBin).

hashmatching_bytreesize_test() ->
    B = <<"Bucket">>,
    V = leveled_head:riak_metadata_to_binary(term_to_binary([{"actor1", 1}]), 
                                                <<1:32/integer, 
                                                    0:32/integer,
                                                    0:32/integer>>),
    GenKeyFun =
        fun(X) ->
            LK = 
                {?RIAK_TAG, 
                    B, 
                    list_to_binary("Key" ++ integer_to_list(X)), 
                    null},
            LKV = leveled_codec:generate_ledgerkv(LK, 
                                                    X, 
                                                    V, 
                                                    byte_size(V), 
                                                    {active, infinity}),
            {_Bucket, _Key, MetaValue, _Hashes, _LastMods} = LKV,
            {LK, MetaValue}
        end,
    KVL = lists:map(GenKeyFun, lists:seq(1, 128)),
    {{PosBinIndex1, _FullBin, _HL, _LK}, no_timing} = 
        generate_binary_slot(lookup, KVL, native, ?INDEX_MODDATE, no_timing),
    check_segment_match(PosBinIndex1, KVL, small),
    check_segment_match(PosBinIndex1, KVL, medium).


check_segment_match(PosBinIndex1, KVL, TreeSize) ->
    CheckFun =
        fun({{_T, B, K, null}, _V}) ->
            Seg = 
                leveled_tictac:get_segment(
                        leveled_tictac:keyto_segment32(<<B/binary, K/binary>>),
                        TreeSize),
            SegList0 = tune_seglist([Seg]),
            PosList = find_pos(PosBinIndex1, SegList0, [], 0),
            ?assertMatch(true, length(PosList) >= 1)
        end,
    lists:foreach(CheckFun, KVL).

timings_test() ->
    SW = os:timestamp(),
    timer:sleep(1),
    {no_timing, T1} = update_timings(SW, #sst_timings{}, slot_index, false),
    {no_timing, T2} = update_timings(SW, T1, slot_fetch, false),
    {no_timing, T3} = update_timings(SW, T2, noncached_block, false),
    timer:sleep(1),
    {_, T4} = update_timings(SW, T3, slot_fetch, true),
    ?assertMatch(3, T4#sst_timings.sample_count),
    ?assertMatch(1, T4#sst_timings.slot_fetch_count),
    ?assertMatch(true, T4#sst_timings.slot_fetch_time > 
                            T3#sst_timings.slot_fetch_time).

take_max_lastmoddate_test() ->
    % TODO: Remove this test
    % Temporarily added to make dialyzer happy (until we've made use of last
    % modified dates
    ?assertMatch(1, take_max_lastmoddate(0, 1)).

stopstart_test() ->
    {ok, Pid} = gen_fsm:start_link(?MODULE, [], []),
    % check we can close in the starting state.  This may happen due to the 
    % fetcher on new level zero files working in a loop
    ok = sst_close(Pid).

stop_whenstarter_stopped_test_() ->
    {timeout, 60, fun() -> stop_whenstarter_stopped_testto() end}.

stop_whenstarter_stopped_testto() ->
    RP = spawn(fun receive_fun/0),
    spawn(fun() -> start_sst_fun(RP) end),
    TestFun =
        fun(X, Acc) ->
            case Acc of
                false -> false;
                true -> 
                    timer:sleep(X),
                    is_process_alive(RP)
            end
        end,
    ?assertMatch(false, lists:foldl(TestFun, true, [10000, 2000, 2000, 2000])).
    

receive_fun() ->
    receive
        {sst_pid, SST_P} ->
            timer:sleep(?STARTUP_TIMEOUT + 1000),
            ?assertMatch(false, is_process_alive(SST_P))
    end.

start_sst_fun(ProcessToInform) ->
    N = 3000,
    KVL1 = lists:ukeysort(1, generate_randomkeys(N + 1, N, 1, 20)),
    OptsSST = 
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, P1, {_FK1, _LK1}, _Bloom1} = 
        sst_new(?TEST_AREA, "level1_src", 1, KVL1, 6000, OptsSST),
    ProcessToInform ! {sst_pid, P1}.


-endif.
