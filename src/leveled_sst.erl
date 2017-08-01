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

-include("include/leveled.hrl").

-define(MAX_SLOTS, 256).
-define(LOOK_SLOTSIZE, 128). % This is not configurable
-define(LOOK_BLOCKSIZE, {24, 32}). 
-define(NOLOOK_SLOTSIZE, 256).
-define(NOLOOK_BLOCKSIZE, {56, 32}). 
-define(COMPRESSION_LEVEL, 1).
-define(BINARY_SETTINGS, [{compressed, ?COMPRESSION_LEVEL}]).
% -define(LEVEL_BLOOM_BITS, [{0, 8}, {1, 10}, {2, 8}, {default, 6}]).
-define(MERGE_SCANWIDTH, 16).
-define(DISCARD_EXT, ".discarded").
-define(DELETE_TIMEOUT, 10000).
-define(TREE_TYPE, idxt).
-define(TREE_SIZE, 4).

-include_lib("eunit/include/eunit.hrl").

-export([init/1,
        handle_sync_event/4,
        handle_event/3,
        handle_info/3,
        terminate/3,
        code_change/4,
        starting/2,
        starting/3,
        reader/3,
        delete_pending/2,
        delete_pending/3]).

-export([sst_new/5,
            sst_new/7,
            sst_newlevelzero/6,
            sst_open/2,
            sst_get/2,
            sst_get/3,
            sst_getkvrange/4,
            sst_getslots/2,
            sst_getmaxsequencenumber/1,
            sst_setfordelete/2,
            sst_clear/1,
            sst_checkready/1,
            sst_deleteconfirmed/1,
            sst_close/1]).

-export([expand_list_by_pointer/3]).


-record(slot_index_value, {slot_id :: integer(),
                            start_position :: integer(),
                            length :: integer(),
                            bloom :: binary()}).

-record(summary,    {first_key :: tuple(),
                        last_key :: tuple(),
                     index :: tuple() | undefined,
                        size :: integer(),
                        max_sqn :: integer()}).

%% yield_blockquery is used to detemrine if the work necessary to process a
%% range query beyond the fetching the slot should be managed from within
%% this process, or should be handled by the calling process.
%% Handling within the calling process may lead to extra binary heap garbage
%% see Issue 52.  Handling within the SST process may lead to contention and
%% extra copying.  Files at the top of the tree yield, those lower down don't.

-record(state,      {summary,
                     handle :: file:fd() | undefined,
                     sst_timings :: tuple() | undefined,
                     penciller :: pid() | undefined,
                        root_path,
                        filename,
                        yield_blockquery = false :: boolean(),
                        blockindex_cache}).


%%%============================================================================
%%% API
%%%============================================================================

-spec sst_open(string(), string()) -> {ok, pid(), {tuple(), tuple()}}.
%% @doc
%% Open an SST file at a given path and filename.  The first and last keys
%% are returned in response to the request - so that those keys can be used
%% in manifests to understand what range of keys are covered by the SST file.
%% All keys in the file should be between the first and last key in erlang
%% term order.
%%
%% The filename should include the file extension.
sst_open(RootPath, Filename) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    case gen_fsm:sync_send_event(Pid,
                                    {sst_open, RootPath, Filename},
                                    infinity) of
        {ok, {SK, EK}} ->
            {ok, Pid, {SK, EK}}
    end.

-spec sst_new(string(), string(), integer(), list(), integer()) ->
                                            {ok, pid(), {tuple(), tuple()}}.
%% @doc
%% Start a new SST file at the assigned level passing in a list of Key, Value
%% pairs.  This should not be used for basement levels or unexpanded Key/Value
%% lists as merge_lists will not be called.
sst_new(RootPath, Filename, Level, KVList, MaxSQN) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    {[], [], SlotList, FK}  = merge_lists(KVList),
    case gen_fsm:sync_send_event(Pid,
                                    {sst_new,
                                        RootPath,
                                        Filename,
                                        Level,
                                        {SlotList, FK},
                                        MaxSQN},
                                    infinity) of
        {ok, {SK, EK}} ->
            {ok, Pid, {SK, EK}}
    end.

-spec sst_new(string(), string(), list(), list(),
                boolean(), integer(), integer()) ->
                    empty|{ok, pid(), {{list(), list()}, tuple(), tuple()}}.
%% @doc
%% Start a new SST file at the assigned level passing in a two lists of
%% {Key, Value} pairs to be merged.  The merge_lists function will use the
%% IsBasement boolean to determine if expired keys or tombstones can be
%% deleted.
%%
%% The remainder of the lists is returned along with the StartKey and EndKey
%% so that the remainder cna be  used in the next file in the merge.  It might
%% be that the merge_lists returns nothin (for example when a basement file is
%% all tombstones) - and the atome empty is returned in this case so that the
%% file is not added to the manifest.
sst_new(RootPath, Filename, KVL1, KVL2, IsBasement, Level, MaxSQN) ->
    {Rem1, Rem2, SlotList, FK} = merge_lists(KVL1, KVL2, {IsBasement, Level}),
    case SlotList of
        [] ->
            empty;
        _ ->
            {ok, Pid} = gen_fsm:start(?MODULE, [], []),
            case gen_fsm:sync_send_event(Pid,
                                            {sst_new,
                                                RootPath,
                                                Filename,
                                                Level,
                                                {SlotList, FK},
                                                MaxSQN},
                                            infinity) of
                {ok, {SK, EK}} ->
                    {ok, Pid, {{Rem1, Rem2}, SK, EK}}
            end
    end.

-spec sst_newlevelzero(string(), string(),
                            integer(), fun(), pid()|undefined, integer()) ->
                                                        {ok, pid(), noreply}.
%% @doc
%% Start a new file at level zero.  At this level the file size is not fixed -
%% it will be as big as the input.  Also the KVList is not passed in, it is 
%% fetched slot by slot using the FetchFun
sst_newlevelzero(RootPath, Filename, Slots, FetchFun, Penciller, MaxSQN) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    gen_fsm:send_event(Pid,
                        {sst_newlevelzero,
                            RootPath,
                            Filename,
                            Slots,
                            FetchFun,
                            Penciller,
                            MaxSQN}),
    {ok, Pid, noreply}.

-spec sst_get(pid(), tuple()) -> tuple()|not_present.
%% @doc
%% Return a Key, Value pair matching a Key or not_present if the Key is not in
%% the store.  The magic_hash function is used to accelerate the seeking of
%% keys, sst_get/3 should be used directly if this has already been calculated
sst_get(Pid, LedgerKey) ->
    sst_get(Pid, LedgerKey, leveled_codec:magic_hash(LedgerKey)).

-spec sst_get(pid(), tuple(), integer()) -> tuple()|not_present.
%% @doc
%% Return a Key, Value pair matching a Key or not_present if the Key is not in
%% the store (with the magic hash precalculated).
sst_get(Pid, LedgerKey, Hash) ->
    gen_fsm:sync_send_event(Pid, {get_kv, LedgerKey, Hash}, infinity).

-spec sst_getkvrange(pid(), tuple()|all, tuple()|all, integer()) -> list().
%% @doc
%% Get a range of {Key, Value} pairs as a list between StartKey and EndKey
%% (inclusive).  The ScanWidth is the maximum size of the range, a pointer
%% will be placed on the tail of the resulting list if results expand beyond
%% the Scan Width
%%
%% To make the range open-ended (either ta start, end or both) the all atom
%% can be use din place of the Key tuple.
sst_getkvrange(Pid, StartKey, EndKey, ScanWidth) ->
    case gen_fsm:sync_send_event(Pid,
                                    {get_kvrange, StartKey, EndKey, ScanWidth},
                                    infinity) of
        {yield, SlotsToFetchBinList, SlotsToPoint} ->
            FetchFun =
                fun({SlotBin, SK, EK}, Acc) ->
                    Acc ++ binaryslot_trimmedlist(SlotBin, SK, EK)
                end,
            lists:foldl(FetchFun, [], SlotsToFetchBinList) ++ SlotsToPoint;
        Reply ->
            Reply
    end.

-spec sst_getslots(pid(), list()) -> list().
%% @doc
%% Get a list of slots by their ID. The slot will be converted from the binary
%% to term form outside of the FSM loop
sst_getslots(Pid, SlotList) ->
    SlotBins = gen_fsm:sync_send_event(Pid, {get_slots, SlotList}, infinity),
    FetchFun =
        fun({SlotBin, SK, EK}, Acc) ->
            Acc ++ binaryslot_trimmedlist(SlotBin, SK, EK)
        end,
    lists:foldl(FetchFun, [], SlotBins).

-spec sst_getmaxsequencenumber(pid()) -> integer().
%% @doc
%% Get the maximume sequence number for this SST file
sst_getmaxsequencenumber(Pid) ->
    gen_fsm:sync_send_event(Pid, get_maxsequencenumber, infinity).

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
    gen_fsm:sync_send_event(Pid, close, 1000).

-spec sst_deleteconfirmed(pid()) -> ok.
%% @doc
%% Allows a penciller to confirm to a SST file that it can be cleared, as it
%% is no longer in use
sst_deleteconfirmed(Pid) ->
    gen_fsm:send_event(Pid, close).

-spec sst_checkready(pid()) -> {ok, string(), tuple(), tuple()}.
%% @doc
%% If a file has been set to be built, check that it has been built.  Returns
%% the filename and the {startKey, EndKey} for the manifest.
sst_checkready(Pid) ->
    %% Only used in test
    gen_fsm:sync_send_event(Pid, background_complete, 100).

-spec sst_close(pid()) -> ok.
%% @doc
%% Close the file
sst_close(Pid) ->
    gen_fsm:sync_send_event(Pid, close, 2000).

-spec sst_printtimings(pid()) -> ok.
%% @doc
%% The state of the FSM keeps track of timings of operations, and this can
%% forced to be printed.
%% Used in unit tests to force the printing of timings
sst_printtimings(Pid) ->
    gen_fsm:sync_send_event(Pid, print_timings, 1000).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, starting, #state{}}.

starting({sst_open, RootPath, Filename}, _From, State) ->
    UpdState = read_file(Filename, State#state{root_path=RootPath}),
    Summary = UpdState#state.summary,
    {reply,
        {ok, {Summary#summary.first_key, Summary#summary.last_key}},
        reader,
        UpdState};
starting({sst_new, RootPath, Filename, Level, {SlotList, FirstKey}, MaxSQN},
                                                            _From, State) ->
    SW = os:timestamp(),
    {Length, 
        SlotIndex, 
        BlockIndex, 
        SlotsBin} = build_all_slots(SlotList),
    SummaryBin = build_table_summary(SlotIndex,
                                        Level,
                                        FirstKey,
                                        Length,
                                        MaxSQN),
    ActualFilename = write_file(RootPath, Filename, SummaryBin, SlotsBin),
    YBQ = Level =< 2,
    UpdState = read_file(ActualFilename,
                            State#state{root_path=RootPath,
                                        yield_blockquery=YBQ}),
    Summary = UpdState#state.summary,
    leveled_log:log_timer("SST08",
                            [ActualFilename, Level, Summary#summary.max_sqn],
                            SW),
    {reply,
        {ok, {Summary#summary.first_key, Summary#summary.last_key}},
        reader,
        UpdState#state{blockindex_cache = BlockIndex}}.

starting({sst_newlevelzero, RootPath, Filename,
                    Slots, FetchFun, Penciller, MaxSQN}, State) ->
    SW = os:timestamp(),
    KVList = leveled_pmem:to_list(Slots, FetchFun),
    {[], [], SlotList, FirstKey} = merge_lists(KVList),
    {SlotCount, 
        SlotIndex, 
        BlockIndex, 
        SlotsBin} = build_all_slots(SlotList),
    SummaryBin = build_table_summary(SlotIndex,
                                        0,
                                        FirstKey,
                                        SlotCount,
                                        MaxSQN),
    ActualFilename = write_file(RootPath, Filename, SummaryBin, SlotsBin),
    UpdState = read_file(ActualFilename,
                            State#state{root_path = RootPath,
                                        yield_blockquery = true}),
    Summary = UpdState#state.summary,
    leveled_log:log_timer("SST08",
                            [ActualFilename, 0, Summary#summary.max_sqn],
                            SW),
    case Penciller of
        undefined ->
            {next_state, reader, UpdState#state{blockindex_cache = BlockIndex}};
        _ ->
            leveled_penciller:pcl_confirml0complete(Penciller,
                                                    UpdState#state.filename,
                                                    Summary#summary.first_key,
                                                    Summary#summary.last_key),
            {next_state, reader, UpdState#state{blockindex_cache = BlockIndex}}
    end.


reader({get_kv, LedgerKey, Hash}, _From, State) ->
    SW = os:timestamp(),
    {Result, Stage, _SlotID, UpdState} = fetch(LedgerKey, Hash, State),
    UpdTimings = leveled_log:sst_timing(State#state.sst_timings, SW, Stage),
    {reply, Result, reader, UpdState#state{sst_timings = UpdTimings}};
reader({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    {SlotsToFetchBinList, SlotsToPoint} = fetch_range(StartKey,
                                                        EndKey,
                                                        ScanWidth,
                                                        State),
    case State#state.yield_blockquery of
        true ->
            {reply,
                {yield, SlotsToFetchBinList, SlotsToPoint},
                reader,
                State};
        false ->
            FetchFun =
                fun({SlotBin, SK, EK}, Acc) ->
                    Acc ++ binaryslot_trimmedlist(SlotBin, SK, EK)
                end,
            {reply,
                lists:foldl(FetchFun, [], SlotsToFetchBinList) ++ SlotsToPoint,
                reader,
                State}
    end;
reader({get_slots, SlotList}, _From, State) ->
    SlotBins = read_slots(State#state.handle, SlotList),
    {reply, SlotBins, reader, State};
reader(get_maxsequencenumber, _From, State) ->
    Summary = State#state.summary,
    {reply, Summary#summary.max_sqn, reader, State};
reader(print_timings, _From, State) ->
    io:format(user, "~nTimings of ~w~n", [State#state.sst_timings]),
    {reply, ok, reader, State#state{sst_timings = undefined}};
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


delete_pending({get_kv, LedgerKey, Hash}, _From, State) ->
    {Result, _Stage, _SlotID, UpdState} = fetch(LedgerKey, Hash, State),
    {reply, Result, delete_pending, UpdState, ?DELETE_TIMEOUT};
delete_pending({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    {SlotsToFetchBinList, SlotsToPoint} = fetch_range(StartKey,
                                                        EndKey,
                                                        ScanWidth,
                                                        State),
    % Always yield as about to clear and de-reference
    {reply,
        {yield, SlotsToFetchBinList, SlotsToPoint}, 
        delete_pending,
        State,
        ?DELETE_TIMEOUT};
delete_pending({get_slots, SlotList}, _From, State) ->
    SlotBins = read_slots(State#state.handle, SlotList),
    {reply, SlotBins, delete_pending, State, ?DELETE_TIMEOUT};
delete_pending(close, _From, State) ->
    leveled_log:log("SST07", [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = file:delete(filename:join(State#state.root_path,
                                    State#state.filename)),
    {stop, normal, ok, State}.

delete_pending(timeout, State) ->
    ok = leveled_penciller:pcl_confirmdelete(State#state.penciller,
                                               State#state.filename,
                                               self()),
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

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

terminate(normal, delete_pending, _State) ->
    ok;
terminate(Reason, _StateName, State) ->
    leveled_log:log("SST04", [Reason, State#state.filename]).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


%%%============================================================================
%%% Internal Functions
%%%============================================================================

fetch(LedgerKey, Hash, State) ->
    Summary = State#state.summary,
    Slot = lookup_slot(LedgerKey, Summary#summary.index),
    SlotID = Slot#slot_index_value.slot_id,
    Bloom = Slot#slot_index_value.bloom,
    case leveled_tinybloom:check_hash(Hash, Bloom) of
        false ->
            {not_present, tiny_bloom, SlotID, State};
        true ->
            CachedBlockIdx = array:get(SlotID - 1, 
                                        State#state.blockindex_cache),
            case CachedBlockIdx of 
                none ->
                    SlotBin = read_slot(State#state.handle, Slot),
                    {Result,
                        BlockLengths,
                        BlockIdx} = binaryslot_get(SlotBin, LedgerKey, Hash),
                    BlockIndexCache = array:set(SlotID - 1, 
                                                <<BlockLengths/binary,
                                                    BlockIdx/binary>>,
                                                State#state.blockindex_cache),
                    {Result, 
                        slot_fetch, 
                        Slot#slot_index_value.slot_id,
                        State#state{blockindex_cache = BlockIndexCache}};
                <<BlockLengths:24/binary, BlockIdx/binary>> ->
                    PosList = find_pos(BlockIdx, 
                                        double_hash(Hash, LedgerKey), 
                                        [], 
                                        0),
                    case PosList of 
                        [] ->
                            {not_present, slot_bloom,  SlotID, State};
                        _ ->
                            Result = check_blocks(PosList,
                                                    State#state.handle,
                                                    Slot,
                                                    BlockLengths,
                                                    LedgerKey),
                            {Result, slot_fetch, SlotID, State}
                    end 
            end
    end.


fetch_range(StartKey, EndKey, ScanWidth, State) ->
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

    SlotsToFetchBinList = read_slots(Handle, SlotsToFetch),
    {SlotsToFetchBinList, SlotsToPoint}.


write_file(RootPath, Filename, SummaryBin, SlotsBin) ->
    SummaryLength = byte_size(SummaryBin),
    SlotsLength = byte_size(SlotsBin),
    {PendingName, FinalName} = generate_filenames(Filename),
    ok = file:write_file(filename:join(RootPath, PendingName),
                            <<SlotsLength:32/integer,
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

read_file(Filename, State) ->
    {Handle, SummaryBin} = open_reader(filename:join(State#state.root_path,
                                                        Filename)),
    {Summary, SlotList} = read_table_summary(SummaryBin),
    BlockIndexCache = array:new([{size, Summary#summary.size},
                                    {default, none}]),
    UpdState = State#state{blockindex_cache = BlockIndexCache},
    SlotIndex = from_list(SlotList),
    UpdSummary = Summary#summary{index = SlotIndex},
    leveled_log:log("SST03", [Filename,
                                Summary#summary.size,
                                Summary#summary.max_sqn]),
    UpdState#state{summary = UpdSummary,
                    handle = Handle,
                    filename = Filename}.

open_reader(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    {ok, Lengths} = file:pread(Handle, 0, 8),
    <<SlotsLength:32/integer, SummaryLength:32/integer>> = Lengths,
    {ok, SummaryBin} = file:pread(Handle, SlotsLength + 8, SummaryLength),
    {Handle, SummaryBin}.

build_table_summary(SlotIndex, _Level, FirstKey, SlotCount, MaxSQN) ->
    [{LastKey, _LastV}|_Rest] = SlotIndex,
    Summary = #summary{first_key = FirstKey,
                        last_key = LastKey,
                        size = SlotCount,
                        max_sqn = MaxSQN},
    SummBin = term_to_binary({Summary, lists:reverse(SlotIndex)},
                                ?BINARY_SETTINGS),
    SummCRC = erlang:crc32(SummBin),
    <<SummCRC:32/integer, SummBin/binary>>.

read_table_summary(BinWithCheck) ->
    <<SummCRC:32/integer, SummBin/binary>> = BinWithCheck,
    CRCCheck = erlang:crc32(SummBin),
    if
        CRCCheck == SummCRC ->
            % If not might it might be possible to rebuild from all the slots
            binary_to_term(SummBin)
    end.


build_all_slots(SlotList) ->
    SlotCount = length(SlotList),
    BuildResponse = build_all_slots(SlotList,
                                    8,
                                    1,
                                    [],
                                    array:new([{size, SlotCount}, 
                                                {default, none}]),
                                    <<>>),
    {SlotIndex, BlockIndex, SlotsBin} = BuildResponse,
    {SlotCount, SlotIndex, BlockIndex, SlotsBin}.

build_all_slots([], _Pos, _SlotID,
                                    SlotIdxAcc, BlockIdxAcc, SlotBinAcc) ->
    {SlotIdxAcc, BlockIdxAcc, SlotBinAcc};
build_all_slots([SlotD|Rest], Pos, SlotID,
                                    SlotIdxAcc, BlockIdxAcc, SlotBinAcc) ->
    {BlockIdx, SlotBin, HashList, LastKey} = SlotD,
    Length = byte_size(SlotBin),
    Bloom = leveled_tinybloom:create_bloom(HashList),
    SlotIndexV = #slot_index_value{slot_id = SlotID,
                                    start_position = Pos,
                                    length = Length,
                                    bloom = Bloom},
    build_all_slots(Rest,
                    Pos + Length,
                    SlotID + 1,
                    [{LastKey, SlotIndexV}|SlotIdxAcc],
                    array:set(SlotID - 1, BlockIdx, BlockIdxAcc),
                    <<SlotBinAcc/binary, SlotBin/binary>>).


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


generate_binary_slot(Lookup, KVL) ->
    
    HashFoldFun =
        fun({K, V}, {PosBinAcc, NoHashCount, HashAcc}) ->
            
            {_SQN, H1} = leveled_codec:strip_to_seqnhashonly({K, V}),
            case is_integer(H1) of 
                true ->
                    PosH1 = double_hash(H1, K),
                    case NoHashCount of 
                        0 ->
                            {<<1:1/integer, 
                                    PosH1:15/integer, 
                                    PosBinAcc/binary>>,
                                0,
                                [H1|HashAcc]};
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
                                HashAcc}
                    end;
                false ->
                    {PosBinAcc, NoHashCount + 1, HashAcc}
            end
         
         end,
    
    {HashL, PosBinIndex} = 
        case Lookup of
            lookup ->
                {PosBinIndex0,
                    NHC,
                    HashL0} = lists:foldr(HashFoldFun, {<<>>, 0, []}, KVL),
                PosBinIndex1 = 
                    case NHC of
                        0 ->
                            PosBinIndex0;
                        _ ->
                            N = NHC - 1,
                            <<0:1/integer, N:7/integer, PosBinIndex0/binary>>
                    end,
                {HashL0, PosBinIndex1};
            no_lookup ->
                {[], <<0:1/integer, 127:7/integer>>}
        end,

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
                {term_to_binary(KVL, ?BINARY_SETTINGS),
                    <<0:0>>, 
                    <<0:0>>, 
                    <<0:0>>,
                    <<0:0>>};
            L when L =< 2 * SideBlockSize ->
                {KVLA, KVLB} = lists:split(SideBlockSize, KVL),
                {term_to_binary(KVLA, ?BINARY_SETTINGS),
                    term_to_binary(KVLB, ?BINARY_SETTINGS),
                    <<0:0>>, 
                    <<0:0>>,
                    <<0:0>>};
            L when L =< (2 * SideBlockSize + MidBlockSize) ->
                {KVLA, KVLB_Rest} = lists:split(SideBlockSize, KVL),
                {KVLB, KVLC} = lists:split(SideBlockSize, KVLB_Rest),
                {term_to_binary(KVLA, ?BINARY_SETTINGS),
                    term_to_binary(KVLB, ?BINARY_SETTINGS),
                    term_to_binary(KVLC, ?BINARY_SETTINGS),
                    <<0:0>>,
                    <<0:0>>};
            L when L =< (3 * SideBlockSize + MidBlockSize) ->
                {KVLA, KVLB_Rest} = lists:split(SideBlockSize, KVL),
                {KVLB, KVLC_Rest} = lists:split(SideBlockSize, KVLB_Rest),
                {KVLC, KVLD} = lists:split(MidBlockSize, KVLC_Rest),
                {term_to_binary(KVLA, ?BINARY_SETTINGS),
                    term_to_binary(KVLB, ?BINARY_SETTINGS),
                    term_to_binary(KVLC, ?BINARY_SETTINGS),
                    term_to_binary(KVLD, ?BINARY_SETTINGS),
                    <<0:0>>};
            L when L =< (4 * SideBlockSize + MidBlockSize) ->
                {KVLA, KVLB_Rest} = lists:split(SideBlockSize, KVL),
                {KVLB, KVLC_Rest} = lists:split(SideBlockSize, KVLB_Rest),
                {KVLC, KVLD_Rest} = lists:split(MidBlockSize, KVLC_Rest),
                {KVLD, KVLE} = lists:split(SideBlockSize, KVLD_Rest),
                {term_to_binary(KVLA, ?BINARY_SETTINGS),
                    term_to_binary(KVLB, ?BINARY_SETTINGS),
                    term_to_binary(KVLC, ?BINARY_SETTINGS),
                    term_to_binary(KVLD, ?BINARY_SETTINGS),
                    term_to_binary(KVLE, ?BINARY_SETTINGS)}
        end,

    B1P = byte_size(PosBinIndex),
    B1L = byte_size(B1),
    B2L = byte_size(B2),
    B3L = byte_size(B3),
    B4L = byte_size(B4),
    B5L = byte_size(B5),
    Lengths = <<B1P:32/integer,
                B1L:32/integer, 
                B2L:32/integer, 
                B3L:32/integer, 
                B4L:32/integer,
                B5L:32/integer>>,
    SlotBin = <<Lengths/binary, 
                PosBinIndex/binary, 
                B1/binary, B2/binary, B3/binary, B4/binary, B5/binary>>,
    CRC32 = erlang:crc32(SlotBin),
    FullBin = <<CRC32:32/integer, SlotBin/binary>>,
    
    {LastKey, _LV} = lists:last(KVL),

    {<<Lengths/binary, PosBinIndex/binary>>, FullBin, HashL, LastKey}.


check_blocks([], _Handle, _Slot, _BlockLengths, _LedgerKey) ->
    not_present;
check_blocks([Pos|Rest], Handle, Slot, BlockLengths, LedgerKey) ->
    {BlockNumber, BlockPos} = revert_position(Pos),
    BlockBin = read_block(Handle, Slot, BlockLengths, BlockNumber),
    BlockL = binary_to_term(BlockBin),
    {K, V} = lists:nth(BlockPos, BlockL),
    case K of 
        LedgerKey ->
            {K, V};
        _ ->
            check_blocks(Rest, Handle, Slot, BlockLengths, LedgerKey)
    end.


read_block(Handle, Slot, BlockLengths, BlockID) ->
    {BlockPos, Offset, Length} = block_offsetandlength(BlockLengths, BlockID),
    {ok, BlockBin} = file:pread(Handle,
                                Slot#slot_index_value.start_position
                                    + BlockPos
                                    + Offset
                                    + 28,
                                    % 4-byte CRC, 4 byte pos, 5x4 byte lengths
                                Length),
    BlockBin.

read_slot(Handle, Slot) ->
    {ok, SlotBin} = file:pread(Handle,
                                Slot#slot_index_value.start_position,
                                Slot#slot_index_value.length),
    SlotBin.

read_slots(Handle, SlotList) ->
    PointerMapFun =
        fun(Pointer) ->
            {Slot, SK, EK} = 
                case Pointer of 
                    {pointer, _Pid, Slot0, SK0, EK0} ->
                        {Slot0, SK0, EK0};
                    {pointer, Slot0, SK0, EK0} ->
                        {Slot0, SK0, EK0}
                end,

            {Slot#slot_index_value.start_position, 
                Slot#slot_index_value.length,
                SK, 
                EK}
        end,
    
    LengthList = lists:map(PointerMapFun, SlotList),
    StartPos = element(1, lists:nth(1, LengthList)),
    EndPos = element(1, lists:last(LengthList)) 
                + element(2, lists:last(LengthList)),
    {ok, MultiSlotBin} = file:pread(Handle, StartPos, EndPos - StartPos),

    BinSplitMapFun = 
        fun({SP, L, SK, EK}) -> 
                    Start = SP - StartPos,
                    <<_Pre:Start/binary, 
                        SlotBin:L/binary, 
                        _Post/binary>> = MultiSlotBin,
                    {SlotBin, SK, EK}
        end,
    
    lists:map(BinSplitMapFun, LengthList).


binaryslot_get(FullBin, Key, Hash) ->
    case crc_check_slot(FullBin) of 
        {BlockLengths, Rest} ->
            <<B1P:32/integer, _R/binary>> = BlockLengths, 
            <<PosBinIndex:B1P/binary, Blocks/binary>> = Rest,
            PosList = find_pos(PosBinIndex,
                                double_hash(Hash, Key), 
                                [], 
                                0),
            {fetch_value(PosList, BlockLengths, Blocks, Key),
                BlockLengths,
                PosBinIndex};
        crc_wonky ->
            {not_present,
                none,
                none}
    end.

binaryslot_tolist(FullBin) ->
    BlockFetchFun = 
        fun(Length, {Acc, Bin}) ->
            case Length of 
                0 ->
                    {Acc, Bin};
                _ ->
                    <<Block:Length/binary, Rest/binary>> = Bin,
                    {Acc ++ binary_to_term(Block), Rest}
            end
        end,

    {Out, _Rem} = 
        case crc_check_slot(FullBin) of 
            {BlockLengths, RestBin} ->
                <<B1P:32/integer,
                    B1L:32/integer,
                    B2L:32/integer,
                    B3L:32/integer,
                    B4L:32/integer,
                    B5L:32/integer>> = BlockLengths,
                <<_PosBinIndex:B1P/binary, Blocks/binary>> = RestBin,
                lists:foldl(BlockFetchFun,
                                {[], Blocks},
                                [B1L, B2L, B3L, B4L, B5L]);
            crc_wonky ->
                {[], <<>>}
        end,
    Out.


binaryslot_trimmedlist(FullBin, all, all) ->
    binaryslot_tolist(FullBin);
binaryslot_trimmedlist(FullBin, StartKey, EndKey) ->
    LTrimFun = fun({K, _V}) -> K < StartKey end,
    RTrimFun = fun({K, _V}) -> not leveled_codec:endkey_passed(EndKey, K) end,
    
    % It will be more effecient to check a subset of blocks.  To work out
    % the best subset we always look in the middle block of 5, and based on
    % the first and last keys of that middle block when compared to the Start
    % and EndKey of the query determines a subset of blocks
    %
    % This isn't perfectly efficient, esepcially if the query overlaps Block2
    % and Block3 (as Block 1 will also be checked), but finessing this last
    % scenario is hard to do in concise code
    BlocksToCheck = 
        case crc_check_slot(FullBin) of 
            {BlockLengths, RestBin} ->
                <<B1P:32/integer,
                    B1L:32/integer,
                    B2L:32/integer,
                    B3L:32/integer,
                    B4L:32/integer,
                    B5L:32/integer>> = BlockLengths,
                <<_PosBinIndex:B1P/binary,
                    Block1:B1L/binary, Block2:B2L/binary,
                    MidBlock:B3L/binary,
                    Block4:B4L/binary, Block5:B5L/binary>> = RestBin,
                case B3L of
                    0 ->
                        [Block1, Block2];
                    _ ->    
                        MidBlockList = binary_to_term(MidBlock),
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
                end;                 
            crc_wonky ->
                []
        end,

    
    BlockCheckFun = 
        fun(Block, {Acc, Continue}) ->
            case {Block, Continue} of 
                {<<>>, _} ->
                    {Acc, false};
                {_, true} ->
                    BlockList =
                        case is_binary(Block) of
                            true ->
                                binary_to_term(Block);
                            false ->
                                Block
                        end,
                    {LastKey, _LV} = lists:last(BlockList),
                    case StartKey > LastKey of
                        true ->
                            {Acc, true};
                        false ->
                            {_LDrop, RKeep} = lists:splitwith(LTrimFun, 
                                                                BlockList),
                            case leveled_codec:endkey_passed(EndKey, LastKey) of
                                true ->
                                    {LKeep, _RDrop} = lists:splitwith(RTrimFun, RKeep),
                                    {Acc ++ LKeep, false};
                                false ->
                                    {Acc ++ RKeep, true}
                            end
                    end;
                {_ , false} ->
                    {Acc, false}
            end
        end,

    {Acc, _Continue} = lists:foldl(BlockCheckFun, {[], true}, BlocksToCheck),
    Acc.


crc_check_slot(FullBin) ->
    <<CRC32:32/integer, SlotBin/binary>> = FullBin,
    case erlang:crc32(SlotBin) of 
        CRC32 ->
            <<BlockLengths:24/binary, Rest/binary>> = SlotBin,
            {BlockLengths, Rest};
        _ ->
            leveled_log:log("SST09", []),
            crc_wonky
    end.

block_offsetandlength(BlockLengths, BlockID) ->
    <<BlocksPos:32/integer, BlockLengths0:20/binary>> = BlockLengths,
    case BlockID of
        1 ->
            <<B1L:32/integer, _BR/binary>> = BlockLengths0,
            {BlocksPos, 0, B1L};
        2 ->
            <<B1L:32/integer, B2L:32/integer, _BR/binary>> = BlockLengths0,
            {BlocksPos, B1L, B2L};
        3 ->
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                _BR/binary>> = BlockLengths0,
            {BlocksPos, B1L + B2L, B3L};
        4 ->
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                B4L:32/integer,
                _BR/binary>> = BlockLengths0,
            {BlocksPos, B1L + B2L + B3L, B4L};
        5 ->
            <<B1L:32/integer,
                B2L:32/integer,
                B3L:32/integer,
                B4L:32/integer,
                B5L:32/integer,
                _BR/binary>> = BlockLengths0,
            {BlocksPos, B1L + B2L + B3L + B4L, B5L}
    end.

double_hash(Hash, Key) ->
    H2 = erlang:phash2(Key),
    (Hash bxor H2) band 32767.

fetch_value([], _BlockLengths, _Blocks, _Key) ->
    not_present;
fetch_value([Pos|Rest], BlockLengths, Blocks, Key) ->
    {BlockNumber, BlockPos} = revert_position(Pos),
    {_BlockPos,
        Offset,
        Length} = block_offsetandlength(BlockLengths, BlockNumber),
    <<_Pre:Offset/binary, Block:Length/binary, _Rest/binary>> = Blocks,
    BlockL = binary_to_term(Block),
    {K, V} = lists:nth(BlockPos, BlockL),
    case K of 
        Key ->
            {K, V};
        _ -> 
            fetch_value(Rest, BlockLengths, Blocks, Key)
    end.


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

find_pos(<<>>, _Hash, PosList, _Count) ->
    PosList;
find_pos(<<1:1/integer, Hash:15/integer, T/binary>>, Hash, PosList, Count) ->
    find_pos(T, Hash, PosList ++ [Count], Count + 1);
find_pos(<<1:1/integer, _Miss:15/integer, T/binary>>, Hash, PosList, Count) ->
    find_pos(T, Hash, PosList, Count + 1);
find_pos(<<0:1/integer, NHC:7/integer, T/binary>>, Hash, PosList, Count) ->
    find_pos(T, Hash, PosList, Count + NHC + 1).



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
%% The outcome of merge_lists/1 and merge_lists/3 should be an list of slots.
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

merge_lists(KVList1) ->
    SlotCount = length(KVList1) div ?LOOK_SLOTSIZE, 
    {[],
        [],
        split_lists(KVList1, [], SlotCount),
        element(1, lists:nth(1, KVList1))}.

split_lists([], SlotLists, 0) ->
    lists:reverse(SlotLists);
split_lists(LastPuff, SlotLists, 0) ->
    SlotD = generate_binary_slot(lookup, LastPuff),
    lists:reverse([SlotD|SlotLists]);
split_lists(KVList1, SlotLists, N) ->
    {Slot, KVListRem} = lists:split(?LOOK_SLOTSIZE, KVList1),
    SlotD = generate_binary_slot(lookup, Slot),
    split_lists(KVListRem, [SlotD|SlotLists], N - 1).

merge_lists(KVList1, KVList2, LevelInfo) ->
    merge_lists(KVList1, KVList2, LevelInfo, [], null, 0).

merge_lists(KVList1, KVList2, _LI, SlotList, FirstKey, ?MAX_SLOTS) ->
    {KVList1, KVList2, lists:reverse(SlotList), FirstKey};
merge_lists([], [], _LI, SlotList, FirstKey, _SlotCount) ->
    {[], [], lists:reverse(SlotList), FirstKey};
merge_lists(KVList1, KVList2, LI, SlotList, FirstKey, SlotCount) ->
    {KVRem1, KVRem2, Slot, FK0} =
        form_slot(KVList1, KVList2, LI, no_lookup, 0, [], FirstKey),
    case Slot of
        {_, []} ->
            merge_lists(KVRem1,
                        KVRem2,
                        LI,
                        SlotList,
                        FK0,
                        SlotCount);
        {Lookup, KVL} ->
            SlotD = generate_binary_slot(Lookup, KVL),
            merge_lists(KVRem1,
                        KVRem2,
                        LI,
                        [SlotD|SlotList],
                        FK0,
                        SlotCount + 1)
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
    

expand_list_by_pointer({pointer, SSTPid, Slot, StartKey, EndKey}, Tail, Width) ->
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
    ExpPointers = leveled_sst:sst_getslots(SSTPid, AccPointers),
    lists:append(ExpPointers, AccTail);
expand_list_by_pointer({next, ManEntry, StartKey, EndKey}, Tail, Width) ->
    SSTPid = ManEntry#manifest_entry.owner,
    leveled_log:log("SST10", [SSTPid, is_process_alive(SSTPid)]),
    ExpPointer = leveled_sst:sst_getkvrange(SSTPid, StartKey, EndKey, Width),
    ExpPointer ++ Tail.



%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

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
    BNumber = string:right(integer_to_list(BucketLow + BRand), 4, $0),
    KNumber = string:right(integer_to_list(leveled_rand:uniform(1000)), 6, $0),
    LK = leveled_codec:to_ledgerkey("Bucket" ++ BNumber, "Key" ++ KNumber, o),
    Chunk = leveled_rand:rand_bytes(64),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, Seqn, Chunk, 64, infinity),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{LK, MV}|Acc],
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
    % Merge lists wiht nothing but tombstones
    SkippingKV1 = {{o, "B1", "K9995", null}, {9995, tomb, 1234567, {}}},
    SkippingKV2 = {{o, "B1", "K9996", null}, {9996, tomb, 1234567, {}}},
    SkippingKV3 = {{o, "B1", "K9997", null}, {9997, tomb, 1234567, {}}},
    SkippingKV4 = {{o, "B1", "K9998", null}, {9998, tomb, 1234567, {}}},
    SkippingKV5 = {{o, "B1", "K9999", null}, {9999, tomb, 1234567, {}}},
    R = merge_lists([SkippingKV1, SkippingKV3, SkippingKV5],
                        [SkippingKV2, SkippingKV4],
                        {true, 9999999}),
    ?assertMatch({[], [], [], null}, R).

indexed_list_test() ->
    io:format(user, "~nIndexed list timing test:~n", []),
    N = 150,
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 4)),
    KVL1 = lists:sublist(KVL0, 128),

    SW0 = os:timestamp(),

    {_PosBinIndex1, FullBin, _HL, _LK} = generate_binary_slot(lookup, KVL1),
    io:format(user,
                "Indexed list created slot in ~w microseconds of size ~w~n",
                [timer:now_diff(os:timestamp(), SW0), byte_size(FullBin)]),

    {TestK1, TestV1} = lists:nth(20, KVL1),
    MH1 = leveled_codec:magic_hash(TestK1),
    {TestK2, TestV2} = lists:nth(40, KVL1),
    MH2 = leveled_codec:magic_hash(TestK2),
    {TestK3, TestV3} = lists:nth(60, KVL1),
    MH3 = leveled_codec:magic_hash(TestK3),
    {TestK4, TestV4} = lists:nth(80, KVL1),
    MH4 = leveled_codec:magic_hash(TestK4),
    {TestK5, TestV5} = lists:nth(100, KVL1),
    MH5 = leveled_codec:magic_hash(TestK5),

    test_binary_slot(FullBin, TestK1, MH1, {TestK1, TestV1}),
    test_binary_slot(FullBin, TestK2, MH2, {TestK2, TestV2}),
    test_binary_slot(FullBin, TestK3, MH3, {TestK3, TestV3}),
    test_binary_slot(FullBin, TestK4, MH4, {TestK4, TestV4}),
    test_binary_slot(FullBin, TestK5, MH5, {TestK5, TestV5}).


indexed_list_mixedkeys_test() ->
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, 50, 1, 4)),
    KVL1 = lists:sublist(KVL0, 33),
    Keys = lists:ukeysort(1, generate_indexkeys(60) ++ KVL1),

    {_PosBinIndex1, FullBin, _HL, _LK} = generate_binary_slot(lookup, Keys),

    {TestK1, TestV1} = lists:nth(4, KVL1),
    MH1 = leveled_codec:magic_hash(TestK1),
    {TestK2, TestV2} = lists:nth(8, KVL1),
    MH2 = leveled_codec:magic_hash(TestK2),
    {TestK3, TestV3} = lists:nth(12, KVL1),
    MH3 = leveled_codec:magic_hash(TestK3),
    {TestK4, TestV4} = lists:nth(16, KVL1),
    MH4 = leveled_codec:magic_hash(TestK4),
    {TestK5, TestV5} = lists:nth(20, KVL1),
    MH5 = leveled_codec:magic_hash(TestK5),

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
    {_PosBinIndex1, FullBin, _HL, _LK} = generate_binary_slot(lookup, Keys),
    lists:foreach(fun({K, V}) ->
                        MH = leveled_codec:magic_hash(K),
                        test_binary_slot(FullBin, K, MH, {K, V})
                        end,
                    KVL1).

indexed_list_allindexkeys_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(150)), 128),
    {PosBinIndex1, FullBin, _HL, _LK} = generate_binary_slot(lookup, Keys),
    ?assertMatch(<<_BL:24/binary, 127:8/integer>>, PosBinIndex1),
    % SW = os:timestamp(),
    BinToList = binaryslot_tolist(FullBin),
    % io:format(user,
    %             "Indexed list flattened in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]),
    ?assertMatch(Keys, BinToList),
    ?assertMatch(Keys, binaryslot_trimmedlist(FullBin, all, all)).

indexed_list_allindexkeys_nolookup_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(1000)),
                            ?NOLOOK_SLOTSIZE),
    {PosBinIndex1, FullBin, _HL, _LK} = generate_binary_slot(no_lookup, Keys),
    ?assertMatch(<<_BL:24/binary, 127:8/integer>>, PosBinIndex1),
    % SW = os:timestamp(),
    BinToList = binaryslot_tolist(FullBin),
    % io:format(user,
    %             "Indexed list flattened in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]),
    ?assertMatch(Keys, BinToList),
    ?assertMatch(Keys, binaryslot_trimmedlist(FullBin, all, all)).

indexed_list_allindexkeys_trimmed_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(150)), 128),
    {PosBinIndex1, FullBin, _HL, _LK} = generate_binary_slot(lookup, Keys),
    ?assertMatch(<<_BL:24/binary, 127:8/integer>>, PosBinIndex1),
    ?assertMatch(Keys, binaryslot_trimmedlist(FullBin, 
                                                {i, 
                                                    "Bucket", 
                                                    {"t1_int", 0},
                                                    null}, 
                                                {i, 
                                                    "Bucket", 
                                                    {"t1_int", 99999},
                                                    null})),
    
    {SK1, _} = lists:nth(10, Keys),
    {EK1, _} = lists:nth(100, Keys),
    R1 = lists:sublist(Keys, 10, 91),
    O1 = binaryslot_trimmedlist(FullBin, SK1, EK1),
    ?assertMatch(91, length(O1)),
    ?assertMatch(R1, O1),

    {SK2, _} = lists:nth(10, Keys),
    {EK2, _} = lists:nth(20, Keys),
    R2 = lists:sublist(Keys, 10, 11),
    O2 = binaryslot_trimmedlist(FullBin, SK2, EK2),
    ?assertMatch(11, length(O2)),
    ?assertMatch(R2, O2),

    {SK3, _} = lists:nth(127, Keys),
    {EK3, _} = lists:nth(128, Keys),
    R3 = lists:sublist(Keys, 127, 2),
    O3 = binaryslot_trimmedlist(FullBin, SK3, EK3),
    ?assertMatch(2, length(O3)),
    ?assertMatch(R3, O3).


indexed_list_mixedkeys_bitflip_test() ->
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, 50, 1, 4)),
    KVL1 = lists:sublist(KVL0, 33),
    Keys = lists:ukeysort(1, generate_indexkeys(60) ++ KVL1),
    {_PosBinIndex1, FullBin, _HL, LK} = generate_binary_slot(lookup, Keys),
    ?assertMatch(LK, element(1, lists:last(Keys))),
    L = byte_size(FullBin),
    Byte1 = leveled_rand:uniform(L),
    <<PreB1:Byte1/binary, A:8/integer, PostByte1/binary>> = FullBin,
    FullBin0 = 
        case A of 
            0 ->
                <<PreB1:Byte1/binary, 255:8/integer, PostByte1/binary>>;
            _ ->
                <<PreB1:Byte1/binary, 0:8/integer, PostByte1/binary>>
        end,
    
    {TestK1, _TestV1} = lists:nth(20, KVL1),
    MH1 = leveled_codec:magic_hash(TestK1),

    test_binary_slot(FullBin0, TestK1, MH1, not_present),
    ToList = binaryslot_tolist(FullBin0),
    ?assertMatch([], ToList),

    {SK1, _} = lists:nth(10, Keys),
    {EK1, _} = lists:nth(50, Keys),
    O1 = binaryslot_trimmedlist(FullBin0, SK1, EK1),
    ?assertMatch(0, length(O1)),
    ?assertMatch([], O1).



test_binary_slot(FullBin, Key, Hash, ExpectedValue) ->
    % SW = os:timestamp(),
    {ReturnedValue, _BLs, _Idx} = binaryslot_get(FullBin, Key, Hash),
    ?assertMatch(ExpectedValue, ReturnedValue).
    % io:format(user, "Fetch success in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]).

    

merge_test() ->
    N = 3000,
    KVL1 = lists:ukeysort(1, generate_randomkeys(N + 1, N, 1, 20)),
    KVL2 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 20)),
    KVL3 = lists:ukeymerge(1, KVL1, KVL2),
    SW0 = os:timestamp(),
    {ok, P1, {FK1, LK1}} = sst_new("../test/", "level1_src", 1, KVL1, 6000),
    {ok, P2, {FK2, LK2}} = sst_new("../test/", "level2_src", 2, KVL2, 3000),
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
    NewR = sst_new("../test/", "level2_merge", ML1, ML2, false, 2, N * 2),
    {ok, P3, {{Rem1, Rem2}, FK3, LK3}} = NewR,
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
    ok = file:delete("../test/level1_src.sst"),
    ok = file:delete("../test/level2_src.sst"),
    ok = file:delete("../test/level2_merge.sst").
    

simple_persisted_range_test() ->
    {RP, Filename} = {"../test/", "simple_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 16, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}} = sst_new(RP,
                                                Filename,
                                                1,
                                                KVList1,
                                                length(KVList1)),
    
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
    {ok,
        P1,
        {{Rem1, Rem2},
        SK,
        EK}} = sst_new("../test/", "range1_src", IK1, IK2, false, 1, 9999),
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
    {RP, Filename} = {"../test/", "simple_slotsize_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 2, 1, 20),
    KVList1 = lists:sublist(lists:ukeysort(1, KVList0),
                            ?LOOK_SLOTSIZE),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}} = sst_new(RP,
                                                Filename,
                                                1,
                                                KVList1,
                                                length(KVList1)),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(Pid, K))
                        end,
                    KVList1),
    ok = sst_close(Pid),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).

simple_persisted_test() ->
    {RP, Filename} = {"../test/", "simple_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 32, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}} = sst_new(RP,
                                                Filename,
                                                1,
                                                KVList1,
                                                length(KVList1)),
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
                    [{K, leveled_codec:magic_hash(K), V}|Acc];
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
    
    ok = sst_close(Pid),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).

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
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    ok = gen_fsm:send_all_state_event(Pid, nonsense),
    ?assertMatch({next_state, reader, #state{}}, handle_info(nonsense,
                                                                reader,
                                                                #state{})),
    ?assertMatch({ok, reader, #state{}}, code_change(nonsense,
                                                        reader,
                                                        #state{},
                                                        nonsense)),
    ?assertMatch({reply, undefined, reader, #state{}},
                    handle_sync_event("hello", self(), reader, #state{})).

-endif.
