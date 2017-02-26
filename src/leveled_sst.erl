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
-define(SLOT_SIZE, 128). % This is not configurable
-define(COMPRESSION_LEVEL, 1).
-define(BINARY_SETTINGS, [{compressed, ?COMPRESSION_LEVEL}]).
% -define(LEVEL_BLOOM_BITS, [{0, 8}, {1, 10}, {2, 8}, {default, 6}]).
-define(MERGE_SCANWIDTH, 16).
-define(INDEX_MARKER_WIDTH, 16).
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

-export([sst_new/4,
            sst_new/6,
            sst_newlevelzero/5,
            sst_open/1,
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
                        index :: tuple(), 
                        size :: integer(),
                        max_sqn :: integer()}).

-record(state,      {summary,
                        handle :: file:fd(),
                        sst_timings :: tuple(),
                        penciller :: pid(),
                        filename,
                        blockindex_cache}).


%%%============================================================================
%%% API
%%%============================================================================

sst_open(Filename) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    case gen_fsm:sync_send_event(Pid, {sst_open, Filename}, infinity) of
        {ok, {SK, EK}} ->
            {ok, Pid, {SK, EK}}
    end.

sst_new(Filename, Level, KVList, MaxSQN) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    case gen_fsm:sync_send_event(Pid,
                                    {sst_new,
                                        Filename,
                                        Level,
                                        KVList,
                                        MaxSQN},
                                    infinity) of
        {ok, {SK, EK}} ->
            {ok, Pid, {SK, EK}}
    end.

sst_new(Filename, KL1, KL2, IsBasement, Level, MaxSQN) ->
    {{Rem1, Rem2}, MergedList} = merge_lists(KL1, KL2, {IsBasement, Level}),
    case MergedList of
        [] ->
            empty;
        _ ->
            {ok, Pid} = gen_fsm:start(?MODULE, [], []),
            case gen_fsm:sync_send_event(Pid,
                                            {sst_new,
                                                Filename,
                                                Level,
                                                MergedList,
                                                MaxSQN},
                                            infinity) of
                {ok, {SK, EK}} ->
                    {ok, Pid, {{Rem1, Rem2}, SK, EK}}
            end
    end.

sst_newlevelzero(Filename, Slots, FetchFun, Penciller, MaxSQN) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    gen_fsm:send_event(Pid,
                        {sst_newlevelzero,
                            Filename,
                            Slots,
                            FetchFun,
                            Penciller,
                            MaxSQN}),
    {ok, Pid, noreply}.

sst_get(Pid, LedgerKey) ->
    sst_get(Pid, LedgerKey, leveled_codec:magic_hash(LedgerKey)).

sst_get(Pid, LedgerKey, Hash) ->
    gen_fsm:sync_send_event(Pid, {get_kv, LedgerKey, Hash}, infinity).

sst_getkvrange(Pid, StartKey, EndKey, ScanWidth) ->
    gen_fsm:sync_send_event(Pid,
                            {get_kvrange, StartKey, EndKey, ScanWidth},
                            infinity).

sst_getslots(Pid, SlotList) ->
    gen_fsm:sync_send_event(Pid, {get_slots, SlotList}, infinity).

sst_getmaxsequencenumber(Pid) ->
    gen_fsm:sync_send_event(Pid, get_maxsequencenumber, infinity).

sst_setfordelete(Pid, Penciller) ->
    gen_fsm:sync_send_event(Pid, {set_for_delete, Penciller}, infinity).

sst_clear(Pid) ->
    gen_fsm:sync_send_event(Pid, {set_for_delete, false}, infinity),
    gen_fsm:sync_send_event(Pid, close, 1000).

sst_deleteconfirmed(Pid) ->
    gen_fsm:send_event(Pid, close).

sst_checkready(Pid) ->
    %% Only used in test
    gen_fsm:sync_send_event(Pid, background_complete, 100).


sst_close(Pid) ->
    gen_fsm:sync_send_event(Pid, close, 2000).

%% Used in unit tests to force the printing of timings
sst_printtimings(Pid) ->
    gen_fsm:sync_send_event(Pid, print_timings, 1000).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, starting, #state{}}.

starting({sst_open, Filename}, _From, State) ->
    UpdState = read_file(Filename, State),
    Summary = UpdState#state.summary,
    {reply,
        {ok, {Summary#summary.first_key, Summary#summary.last_key}},
        reader,
        UpdState};
starting({sst_new, Filename, Level, KVList, MaxSQN}, _From, State) ->
    SW = os:timestamp(),
    {FirstKey, 
        Length, 
        SlotIndex, 
        BlockIndex, 
        SlotsBin} = build_all_slots(KVList),
    SummaryBin = build_table_summary(SlotIndex,
                                        Level,
                                        FirstKey,
                                        Length,
                                        MaxSQN),
    ActualFilename = write_file(Filename, SummaryBin, SlotsBin),
    UpdState = read_file(ActualFilename, State),
    Summary = UpdState#state.summary,
    leveled_log:log_timer("SST08",
                            [ActualFilename, Level, Summary#summary.max_sqn],
                            SW),
    {reply,
        {ok, {Summary#summary.first_key, Summary#summary.last_key}},
        reader,
        UpdState#state{blockindex_cache = BlockIndex}}.

starting({sst_newlevelzero, Filename, Slots, FetchFun, Penciller, MaxSQN},
                                                                    State) ->
    SW = os:timestamp(),
    KVList = leveled_pmem:to_list(Slots, FetchFun),
    {FirstKey, 
        Length, 
        SlotIndex, 
        BlockIndex, 
        SlotsBin} = build_all_slots(KVList),
    SummaryBin = build_table_summary(SlotIndex,
                                        0,
                                        FirstKey,
                                        Length,
                                        MaxSQN),
    ActualFilename = write_file(Filename, SummaryBin, SlotsBin),
    UpdState = read_file(ActualFilename, State),
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
    {reply,
        fetch_range(StartKey, EndKey, ScanWidth, State),
        reader,
        State};
reader({get_slots, SlotList}, _From, State) ->
    SlotBins = read_slots(State#state.handle, SlotList),
    FetchFun =
        fun({SlotBin, SK, EK}, Acc) ->
            Acc ++ binaryslot_trimmedlist(SlotBin, SK, EK)
        end,
    {reply, lists:foldl(FetchFun, [], SlotBins), reader, State};
reader(get_maxsequencenumber, _From, State) ->
    Summary = State#state.summary,
    {reply, Summary#summary.max_sqn, reader, State};
reader(print_timings, _From, State) ->
    io:format(user, "Timings of ~w~n", [State#state.sst_timings]),
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
    {reply,
        fetch_range(StartKey, EndKey, ScanWidth, State),
        delete_pending,
        State,
        ?DELETE_TIMEOUT};
delete_pending({get_slots, SlotList}, _From, State) ->
    SlotBins = read_slots(State#state.handle, SlotList),
    FetchFun =
        fun({SlotBin, SK, EK}, Acc) ->
            Acc ++ binaryslot_trimmedlist(SlotBin, SK, EK)
        end,
    {reply, 
        lists:foldl(FetchFun, [], SlotBins),
        delete_pending,
        State,
        ?DELETE_TIMEOUT};
delete_pending(close, _From, State) ->
    leveled_log:log("SST07", [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = file:delete(State#state.filename),
    {stop, normal, ok, State}.

delete_pending(timeout, State) ->
    ok = leveled_penciller:pcl_confirmdelete(State#state.penciller,
                                               State#state.filename,
                                               self()),
    {next_state, delete_pending, State, ?DELETE_TIMEOUT};
delete_pending(close, State) ->
    leveled_log:log("SST07", [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = file:delete(State#state.filename),
    {stop, normal, State}.

handle_sync_event(_Msg, _From, StateName, State) ->
    {reply, undefined, StateName, State}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

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
                    {Result, BlockIdx} = binaryslot_get(SlotBin, 
                                                        LedgerKey, 
                                                        Hash, 
                                                        none),
                    BlockIndexCache = array:set(SlotID - 1, 
                                                BlockIdx,
                                                State#state.blockindex_cache),
                    {Result, 
                        slot_fetch, 
                        Slot#slot_index_value.slot_id,
                        State#state{blockindex_cache = BlockIndexCache}};
                _ ->
                    PosList = find_pos(CachedBlockIdx, 
                                        double_hash(Hash, LedgerKey), 
                                        [], 
                                        0),
                    case PosList of 
                        [] ->
                            {not_present, slot_bloom,  SlotID, State};
                        _ ->
                            SlotBin = read_slot(State#state.handle, Slot),
                            Result = binaryslot_get(SlotBin, 
                                                    LedgerKey, 
                                                    Hash, 
                                                    {true, PosList}),
                            {element(1, Result), slot_fetch, SlotID, State}
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
            0 ->
                [];
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

    FetchFun =
        fun({SlotBin, SK, EK}, Acc) ->
            Acc ++ binaryslot_trimmedlist(SlotBin, SK, EK)
        end,
    lists:foldl(FetchFun, [], SlotsToFetchBinList) ++ SlotsToPoint.


write_file(Filename, SummaryBin, SlotsBin) ->
    SummaryLength = byte_size(SummaryBin),
    SlotsLength = byte_size(SlotsBin),
    {PendingName, FinalName} = generate_filenames(Filename),
    ok = file:write_file(PendingName,
                            <<SlotsLength:32/integer,
                                SummaryLength:32/integer,    
                                SlotsBin/binary,
                                SummaryBin/binary>>,
                            [raw]),
    case filelib:is_file(FinalName) of
        true ->
            AltName = filename:join(filename:dirname(FinalName),
                                    filename:basename(FinalName))
                        ++ ?DISCARD_EXT,
            leveled_log:log("SST05", [FinalName, AltName]),
            ok = file:rename(FinalName, AltName);
        false ->
            ok
    end,
    file:rename(PendingName, FinalName),
    FinalName.

read_file(Filename, State) ->
    {Handle, SummaryBin} = open_reader(Filename),
    {Summary, SlotList} = read_table_summary(SummaryBin),
    SlotCount = length(SlotList),
    BlockIndexCache = array:new([{size, SlotCount}, {default, none}]),
    UpdState = State#state{blockindex_cache = BlockIndexCache},
    SlotIndex = from_list(SlotList),
    UpdSummary = Summary#summary{index = SlotIndex},
    leveled_log:log("SST03", [Filename,
                                Summary#summary.size,
                                SlotCount,
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

build_table_summary(SlotList, _Level, FirstKey, L, MaxSQN) ->
    [{LastKey, _LastV}|_Rest] = SlotList,
    Summary = #summary{first_key = FirstKey,
                        last_key = LastKey,
                        size = L,
                        max_sqn = MaxSQN},
    SummBin = term_to_binary({Summary, lists:reverse(SlotList)},
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

build_all_slots(KVList) ->
    L = length(KVList),
    % The length is not a constant time command and the list may be large,
    % but otherwise length must be called each iteration to avoid exception
    % on split or sublist
    [{FirstKey, _FirstV}|_Rest] = KVList,
    SlotCount = L div ?SLOT_SIZE + 1,
    BuildResponse = build_all_slots(KVList,
                                    SlotCount,
                                    8,
                                    1,
                                    [],
                                    array:new([{size, SlotCount}, 
                                                {default, none}]),
                                    <<>>),
    {SlotIndex, BlockIndex, SlotsBin} = BuildResponse,
    {FirstKey, L, SlotIndex, BlockIndex, SlotsBin}.

build_all_slots([], _SC, _Pos, _SlotID, SlotIdx, BlockIdxA, SlotsBin) ->
    {SlotIdx, BlockIdxA, SlotsBin};
build_all_slots(KVL, SC, Pos, SlotID, SlotIdx, BlockIdxA, SlotsBin) ->
    {SlotList, KVRem} =
        case SC of
            1 ->
                {lists:sublist(KVL, ?SLOT_SIZE), []};
            _N ->
                lists:split(?SLOT_SIZE, KVL)
        end,
    {LastKey, _V} = lists:last(SlotList),
    {BlockIndex, SlotBin, HashList} = generate_binary_slot(SlotList),
    Length = byte_size(SlotBin),
    Bloom = leveled_tinybloom:create_bloom(HashList),
    SlotIndexV = #slot_index_value{slot_id = SlotID,
                                    start_position = Pos,
                                    length = Length,
                                    bloom = Bloom},
    build_all_slots(KVRem,
                    SC - 1,
                    Pos + Length,
                    SlotID + 1,
                    [{LastKey, SlotIndexV}|SlotIdx],
                    array:set(SlotID - 1, BlockIndex, BlockIdxA),
                    <<SlotsBin/binary, SlotBin/binary>>).

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
    case EK of
        EndKey ->
            {lists:map(MapFun, SlotList), false};
        _ ->
            {lists:map(MapFun, SlotList), true}
    end.


%%%============================================================================
%%% Slot Implementation
%%%============================================================================

%% Implementing a slot has gone through numerous iterations.  One of the most
%% critical considerations has been the cost of the binary_to_term and 
%% term_to_binary calls for different sizes of slots and different data types.
%%
%% Microbenchmarking indicated that flat lists were the fastest.  However, the 
%% lists need scanning at query time - and so give longer lookups.  Bigger slots
%% did better at term_to_binary time.  However term_to_binary is an often
%% repeated task, and this is better with smaller slots.  
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


generate_binary_slot(KVL) ->
    
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

    {PosBinIndex0, NHC, HashL} = lists:foldr(HashFoldFun, {<<>>, 0, []}, KVL),
    PosBinIndex1 = 
        case NHC of
            0 ->
                PosBinIndex0;
            _ ->
                N = NHC - 1,
                <<0:1/integer, N:7/integer, PosBinIndex0/binary>>
        end,


    {B1, B2, B3, B4} = 
        case length(KVL) of 
            L when L =< 32 ->
                {term_to_binary(KVL, ?BINARY_SETTINGS),
                    <<0:0>>, 
                    <<0:0>>, 
                    <<0:0>>};
            L when L =< 64 ->
                {KVLA_32, KVLB_32} = lists:split(32, KVL),
                {term_to_binary(KVLA_32, ?BINARY_SETTINGS),
                    term_to_binary(KVLB_32, ?BINARY_SETTINGS),
                    <<0:0>>, 
                    <<0:0>>};
            L when L =< 96 ->
                {KVLA_32, KVLB_64} = lists:split(32, KVL),
                {KVLB_32, KVLC_32} = lists:split(32, KVLB_64),
                {term_to_binary(KVLA_32, ?BINARY_SETTINGS),
                    term_to_binary(KVLB_32, ?BINARY_SETTINGS),
                    term_to_binary(KVLC_32, ?BINARY_SETTINGS),
                    <<0:0>>};
            L when L =< 128 ->
                {KVLA_32, KVLB_96} = lists:split(32, KVL),
                {KVLB_32, KVLC_64} = lists:split(32, KVLB_96),
                {KVLC_32, KVLD_32} = lists:split(32, KVLC_64),
                {term_to_binary(KVLA_32, ?BINARY_SETTINGS),
                    term_to_binary(KVLB_32, ?BINARY_SETTINGS),
                    term_to_binary(KVLC_32, ?BINARY_SETTINGS),
                    term_to_binary(KVLD_32, ?BINARY_SETTINGS)}
        end,

    B1P = byte_size(PosBinIndex1),
    B1L = byte_size(B1),
    B2L = byte_size(B2),
    B3L = byte_size(B3),
    B4L = byte_size(B4),
    Lengths = <<B1P:32/integer, 
                B1L:32/integer, 
                B2L:32/integer, 
                B3L:32/integer, 
                B4L:32/integer>>,
    SlotBin = <<Lengths/binary, 
                PosBinIndex1/binary, 
                B1/binary, B2/binary, B3/binary, B4/binary>>,
    CRC32 = erlang:crc32(SlotBin),
    FullBin = <<CRC32:32/integer, SlotBin/binary>>,

    {PosBinIndex1, FullBin, HashL}.


binaryslot_get(FullBin, Key, Hash, CachedPosLookup) ->
    case crc_check_slot(FullBin) of 
        {Lengths, Rest} ->
            B1P = element(1, Lengths),
            case CachedPosLookup of 
                {true, PosList} ->
                    <<_PosBinIndex:B1P/binary, Blocks/binary>> = Rest,
                    {fetch_value(PosList, Lengths, Blocks, Key), none};
                none ->
                    <<PosBinIndex:B1P/binary, Blocks/binary>> = Rest,
                    PosList = find_pos(PosBinIndex, 
                                        double_hash(Hash, Key), 
                                        [], 
                                        0),
                    {fetch_value(PosList, Lengths, Blocks, Key), PosBinIndex}
            end;
        crc_wonky ->
            {not_present, none}
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
            {Lengths, RestBin} ->
                {B1P, B1L, B2L, B3L, B4L} = Lengths,
                <<_PosBinIndex:B1P/binary, Blocks/binary>> = RestBin,
                lists:foldl(BlockFetchFun, {[], Blocks}, [B1L, B2L, B3L, B4L]);
            crc_wonky ->
                {[], <<>>}
        end,
    Out.


binaryslot_trimmedlist(FullBin, all, all) ->
    binaryslot_tolist(FullBin);
binaryslot_trimmedlist(FullBin, StartKey, EndKey) ->
    LTrimFun = fun({K, _V}) -> K < StartKey end,
    RTrimFun = fun({K, _V}) -> not leveled_codec:endkey_passed(EndKey, K) end,
    BlockFetchFun = 
        fun(Length, {Acc, Bin}) ->
            case Length of 
                0 ->
                    {Acc, Bin};
                _ ->
                    <<Block:Length/binary, Rest/binary>> = Bin,
                    BlockList = binary_to_term(Block),
                    {FirstKey, _FV} = lists:nth(1, BlockList),
                    {LastKey, _LV} = lists:last(BlockList),
                    TrimBools = trim_booleans(FirstKey, LastKey, 
                                                StartKey, EndKey),
                    case TrimBools of 
                        {true, _, _, _} ->
                            {Acc, Rest};
                        {false, true, _, _} ->
                            {Acc ++ BlockList, Rest};
                        {false, false, true, false} ->
                            {_LDrop, RKeep} = lists:splitwith(LTrimFun, 
                                                                BlockList),
                            {Acc ++ RKeep, Rest};
                        {false, false, false, true} ->
                            {LKeep, _RDrop} = lists:splitwith(RTrimFun, 
                                                                BlockList),
                            {Acc ++ LKeep, Rest};
                        {false, false, true, true} ->
                            {_LDrop, RKeep} = lists:splitwith(LTrimFun, 
                                                                BlockList),
                            {LKeep, _RDrop} = lists:splitwith(RTrimFun, RKeep),
                            {Acc ++ LKeep, Rest}
                    end

            end
        end,

    {Out, _Rem} = 
        case crc_check_slot(FullBin) of 
            {Lengths, RestBin} ->
                {B1P, B1L, B2L, B3L, B4L} = Lengths,
                <<_PosBinIndex:B1P/binary, Blocks/binary>> = RestBin,
                lists:foldl(BlockFetchFun, {[], Blocks}, [B1L, B2L, B3L, B4L]);
            crc_wonky ->
                {[], <<>>}
        end,
    Out.


trim_booleans(FirstKey, _LastKey, StartKey, all) ->
    FirstKeyPassedStart = FirstKey > StartKey,
    case FirstKeyPassedStart of 
        true ->
            {false, true, false, false};
        false ->
            {false, false, true, false}
    end;
trim_booleans(_FirstKey, LastKey, all, EndKey) ->
    LastKeyPassedEnd = leveled_codec:endkey_passed(EndKey, LastKey),
    case LastKeyPassedEnd of 
        true ->
            {false, false, false, true};
        false ->
            {false, true, false, false}
        end;
trim_booleans(FirstKey, LastKey, StartKey, EndKey) ->
    FirstKeyPassedStart = FirstKey > StartKey,
    PreRange = LastKey < StartKey,
    PostRange = leveled_codec:endkey_passed(EndKey, FirstKey),
    OutOfRange = PreRange or PostRange,
    LastKeyPassedEnd = leveled_codec:endkey_passed(EndKey, LastKey),
    case OutOfRange of 
        true ->
            {true, false, false, false};
        false ->
            case {FirstKeyPassedStart, LastKeyPassedEnd} of 
                {true, false} ->
                    {false, true, false, false};
                {false, false} ->
                    {false, false, true, false};
                {true, true} ->
                    {false, false, false, true};
                {false, true} ->
                    {false, false, true, true}
            end 
    end.




crc_check_slot(FullBin) ->
    <<CRC32:32/integer, SlotBin/binary>> = FullBin,
    case erlang:crc32(SlotBin) of 
        CRC32 ->
            <<B1P:32/integer, 
                B1L:32/integer, 
                B2L:32/integer, 
                B3L:32/integer, 
                B4L:32/integer,
                Rest/binary>> = SlotBin,
            Lengths = {B1P, B1L, B2L, B3L, B4L},
            {Lengths, Rest};
        _ ->
            leveled_log:log("SST09", []),
            crc_wonky
    end.

double_hash(Hash, Key) ->
    H2 = erlang:phash2(Key),
    (Hash bxor H2) band 32767.

fetch_value([], _Lengths, _Blocks, _Key) ->
    not_present;
fetch_value([Pos|Rest], Lengths, Blocks, Key) ->
    BlockNumber = (Pos div 32) + 1,
    BlockPos = (Pos rem 32) + 1,
    BlockL = 
        case BlockNumber of 
            1 ->
                B1L = element(2, Lengths),
                <<Block:B1L/binary, _Rest/binary>> = Blocks,
                binary_to_term(Block);
            2 ->
                B1L = element(2, Lengths),
                B2L = element(3, Lengths),
                <<_Pass:B1L/binary, Block:B2L/binary, _Rest/binary>> = Blocks,
                binary_to_term(Block);
            3 ->
                PreL = element(2, Lengths) + element(3, Lengths),
                B3L = element(4, Lengths),
                <<_Pass:PreL/binary, Block:B3L/binary, _Rest/binary>> = Blocks,
                binary_to_term(Block);
            4 ->
                {_B1P, B1L, B2L, B3L, B4L} = Lengths,
                PreL = B1L + B2L + B3L,
                <<_Pass:PreL/binary, Block:B4L/binary>> = Blocks,
                binary_to_term(Block)
        end,

    {K, V} = lists:nth(BlockPos, BlockL),
    case K of 
        Key ->
            {K, V};
        _ ->
            fetch_value(Rest, Lengths, Blocks, Key)
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

%% functions for merging two KV lists with pointers

%% Compare the keys at the head of the list, and either skip that "best" key or
%% identify as the next key.
%%
%% The logic needs to change if the file is in the basement level, as keys with
%% expired timestamps need not be written at this level
%%
%% The best key is considered to be the lowest key in erlang term order.  If
%% there are matching keys then the highest sequence number must be chosen and
%% any lower sequence numbers should be compacted out of existence

merge_lists(KeyList1, KeyList2, LevelInfo) ->
    merge_lists(KeyList1, KeyList2, LevelInfo, [], ?MAX_SLOTS * ?SLOT_SIZE).

merge_lists([], [], _LevelR, MergedList, _MaxSize) ->
    {{[], []}, lists:reverse(MergedList)};
merge_lists(Rem1, Rem2, _LevelR, MergedList, 0) ->
    {{Rem1, Rem2}, lists:reverse(MergedList)};
merge_lists(KeyList1, KeyList2, {IsBasement, TS}, MergedList, MaxSize) ->
    case key_dominates(KeyList1, KeyList2, {IsBasement, TS}) of
        {{next_key, TopKey}, Rem1, Rem2} ->
            merge_lists(Rem1,
                        Rem2,
                        {IsBasement, TS},
                        [TopKey|MergedList],
                        MaxSize - 1);
        {skipped_key, Rem1, Rem2} ->
            merge_lists(Rem1, Rem2, {IsBasement, TS}, MergedList, MaxSize)
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
    BRand = random:uniform(BRange),
    BNumber = string:right(integer_to_list(BucketLow + BRand), 4, $0),
    KNumber = string:right(integer_to_list(random:uniform(1000)), 6, $0),
    LedgerKey = leveled_codec:to_ledgerkey("Bucket" ++ BNumber,
                                            "Key" ++ KNumber,
                                            o),
    {_B, _K, KV, _H} = leveled_codec:generate_ledgerkv(LedgerKey,
                                                        Seqn,
                                                        crypto:rand_bytes(64),
                                                        64,
                                                        infinity),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [KV|Acc],
                        BucketLow,
                        BRange).


generate_indexkeys(Count) ->
    generate_indexkeys(Count, []).

generate_indexkeys(0, IndexList) ->
    IndexList;
generate_indexkeys(Count, IndexList) ->
    IndexSpecs = [{add, "t1_int", random:uniform(80000)}],
    Changes = leveled_codec:convert_indexspecs(IndexSpecs, 
                                                "Bucket", 
                                                "Key" ++ integer_to_list(Count), 
                                                Count, 
                                                infinity),
    generate_indexkeys(Count - 1, IndexList ++ Changes).


indexed_list_test() ->
    io:format(user, "~nIndexed list timing test:~n", []),
    N = 150,
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 4)),
    KVL1 = lists:sublist(KVL0, 128),

    SW0 = os:timestamp(),

    {_PosBinIndex1, FullBin, _HL} = generate_binary_slot(KVL1),
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

    {_PosBinIndex1, FullBin, _HL} = generate_binary_slot(Keys),

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
    {_PosBinIndex1, FullBin, _HL} = generate_binary_slot(Keys),
    lists:foreach(fun({K, V}) ->
                        MH = leveled_codec:magic_hash(K),
                        test_binary_slot(FullBin, K, MH, {K, V})
                        end,
                    KVL1).

indexed_list_allindexkeys_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(150)), 128),
    {PosBinIndex1, FullBin, _HL} = generate_binary_slot(Keys),
    ?assertMatch(<<127:8/integer>>, PosBinIndex1),
    % SW = os:timestamp(),
    BinToList = binaryslot_tolist(FullBin),
    % io:format(user,
    %             "Indexed list flattened in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]),
    ?assertMatch(Keys, BinToList),
    ?assertMatch(Keys, binaryslot_trimmedlist(FullBin, all, all)).


indexed_list_allindexkeys_trimmed_test() ->
    Keys = lists:sublist(lists:ukeysort(1, generate_indexkeys(150)), 128),
    {PosBinIndex1, FullBin, _HL} = generate_binary_slot(Keys),
    ?assertMatch(<<127:8/integer>>, PosBinIndex1),
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
    {_PosBinIndex1, FullBin, _HL} = generate_binary_slot(Keys),
    L = byte_size(FullBin),
    Byte1 = random:uniform(L),
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
    {ReturnedValue, _} = binaryslot_get(FullBin, Key, Hash, none),
    ?assertMatch(ExpectedValue, ReturnedValue).
    % io:format(user, "Fetch success in ~w microseconds ~n",
    %             [timer:now_diff(os:timestamp(), SW)]).

    

merge_test() ->
    N = 3000,
    KVL1 = lists:ukeysort(1, generate_randomkeys(N + 1, N, 1, 20)),
    KVL2 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 20)),
    KVL3 = lists:ukeymerge(1, KVL1, KVL2),
    SW0 = os:timestamp(),
    {ok, P1, {FK1, LK1}} = sst_new("../test/level1_src", 1, KVL1, 6000),
    {ok, P2, {FK2, LK2}} = sst_new("../test/level2_src", 2, KVL2, 3000),
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
    {ok, P3, {{Rem1, Rem2}, FK3, LK3}} = sst_new("../test/level2_merge",
                                                    ML1,
                                                    ML2,
                                                    false,
                                                    2,
                                                    N * 2),
    ?assertMatch([], Rem1),
    ?assertMatch([], Rem2),
    ?assertMatch(true, FK3 == min(FK1, FK2)),
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
    Filename = "../test/simple_test",
    KVList0 = generate_randomkeys(1, ?SLOT_SIZE * 16, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}} = sst_new(Filename,
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
    
    

simple_persisted_test() ->
    Filename = "../test/simple_test",
    KVList0 = generate_randomkeys(1, ?SLOT_SIZE * 32, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}} = sst_new(Filename,
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
    KVList2 = generate_randomkeys(1, ?SLOT_SIZE * 32, 1, 20),
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
                            io:format("Get slot ~w with Acc at ~w~n", 
                                        [S, length(Acc)]),
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
    ok = file:delete(Filename ++ ".sst").

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