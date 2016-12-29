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
%% keys and values in size.  The slots are each themselves a gb_tree.  The
%% gb_tree is slightly slower than the skiplist at fetch time, and doesn't
%% support directly the useful to_range function.  However the from_orddict
%% capability is much faster than from_sortedlist in skiplist, saving on CPU
%% at sst build time:
%%
%% Skiplist:
%% build and serialise slot 3233 microseconds
%% de-serialise and check * 128 - 14669 microseconds
%% flatten back to list - 164 microseconds
%%
%% GBTree:
%% build and serialise tree 1433 microseconds
%% de-serialise and check * 128 - 15263 microseconds
%% flatten back to list - 175 microseconds
%%
%% The performance advantage at lookup time is no negligible as the time to
%% de-deserialise for each check is dominant.  This time grows linearly with
%% the size of the slot, wherease the serialisation time is relatively constant
%% with growth.  So bigger slots would be quicker to build, but the penalty for
%% that speed is too high at lookup time.
%%
%% -------- Blooms ---------
%%
%% There are two different tiny blooms for each table.  One is split by the
%% first byte of the hash, and consists of two hashes (derived from the
%% remainder of the hash).  This is the top bloom, and the size vaires by
%% level.
%% Level 0 has 8 bits per key - 0.05 fpr
%% Level 1 has 6 bits per key - 0.08 fpr
%% Other Levels have 4 bits per key - 0.15 fpr
%%
%% If this level is passed, then each slot has its own bloom based on the
%% same hash, but now split into three hashes and having a fixed 8 bit per
%% key size at all levels.
%% Slot Bloom has 8 bits per key - 0.03 fpr
%%
%% All blooms are based on the DJ Bernstein magic hash which proved to give
%% the predicted fpr in tests (unlike phash2 which has significantly higher
%% fpr).  Due to the cost of producing the magic hash, it is read from the
%% value not reproduced each time. If the value is set to no_lookup no bloom
%% entry is added, and if all hashes are no_lookup in the slot then no bloom
%% is produced.


-module(leveled_sst).

-behaviour(gen_fsm).

-include("include/leveled.hrl").

-define(MAX_SLOTS, 256).
-define(SLOT_SIZE, 128).
-define(COMPRESSION_LEVEL, 1).
-define(LEVEL_BLOOM_SLOTS, [{0, 64}, {1, 48}, {default, 32}]).
-define(MERGE_SCANWIDTH, 16).
-define(DISCARD_EXT, ".discarded").
-define(DELETE_TIMEOUT, 10000).

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
                            bloom,
                            start_position :: integer(),
                            length :: integer()}).

-record(summary,    {first_key :: tuple(),
                        last_key :: tuple(),
                        index :: list(), % leveled_skiplist
                        bloom :: tuple(), % leveled_tinybloom
                        size :: integer(),
                        max_sqn :: integer()}).

-record(state,      {summary,
                        handle :: file:fd(),
                        sst_timings :: tuple(),
                        slot_lengths :: list(),
                        penciller :: pid(),
                        filename,
                        cache}).


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
    {FirstKey, L, SlotIndex, AllHashes, SlotsBin} = build_all_slots(KVList),
    SummaryBin = build_table_summary(SlotIndex,
                                        AllHashes,
                                        Level,
                                        FirstKey,
                                        L,
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
        UpdState}.

starting({sst_newlevelzero, Filename, Slots, FetchFun, Penciller, MaxSQN},
                                                                    State) ->
    SW = os:timestamp(),
    KVList = leveled_pmem:to_list(Slots, FetchFun),
    {FirstKey, L, SlotIndex, AllHashes, SlotsBin} = build_all_slots(KVList),
    SummaryBin = build_table_summary(SlotIndex,
                                        AllHashes,
                                        0,
                                        FirstKey,
                                        L,
                                        MaxSQN),
    ActualFilename = write_file(Filename, SummaryBin, SlotsBin),
    UpdState = read_file(ActualFilename, State),
    Summary = UpdState#state.summary,
    leveled_log:log_timer("SST08",
                            [ActualFilename, 0, Summary#summary.max_sqn],
                            SW),
    case Penciller of
        undefined ->
            {next_state, reader, UpdState};
        _ ->
            leveled_penciller:pcl_confirml0complete(Penciller,
                                                    UpdState#state.filename,
                                                    Summary#summary.first_key,
                                                    Summary#summary.last_key),
            {next_state, reader, UpdState}
    end.


reader({get_kv, LedgerKey, Hash}, _From, State) ->
    SW = os:timestamp(),
    {Result, Stage, SlotID} = fetch(LedgerKey, Hash, State),
    UpdTimings = leveled_log:sst_timing(State#state.sst_timings, SW, Stage),
    case {Result, Stage} of
        {not_present, slot_crc_wonky} ->
            leveled_log:log("SST02", [State#state.filename, SlotID]),
            {reply, Result, reader, State#state{sst_timings = UpdTimings}};
        {not_present, _} ->
            {reply, Result, reader, State#state{sst_timings = UpdTimings}};
        {KV, slot_lookup_hit} ->
            UpdCache = array:set(SlotID, KV, State#state.cache),
            {reply, Result, reader, State#state{cache = UpdCache,
                                                sst_timings = UpdTimings}};
        _ ->
            {reply, Result, reader, State#state{sst_timings = UpdTimings}}
    end;
reader({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    {reply,
        fetch_range(StartKey, EndKey, ScanWidth, State),
        reader,
        State};
reader({get_slots, SlotList}, _From, State) ->
    SlotBins = read_slots(State#state.handle, SlotList),
    FoldFun =
        fun({SlotBin, SK, EK}, Acc) ->
            Acc ++ trim_slot(SlotBin, SK, EK) end,
    {reply, lists:foldl(FoldFun, [], SlotBins), reader, State};
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
    {Result, Stage, SlotID} = fetch(LedgerKey, Hash, State),
    case {Result, Stage} of
        {not_present, slot_crc_wonky} ->
            leveled_log:log("SST02", [State#state.filename, SlotID]),
            {reply, Result, delete_pending, State, ?DELETE_TIMEOUT};
        {not_present, _} ->
            {reply, Result, delete_pending, State, ?DELETE_TIMEOUT};
        {KV, slot_lookup_hit} ->
            UpdCache = array:set(SlotID, KV, State#state.cache),
            UpdState = State#state{cache = UpdCache},
            {reply, Result, delete_pending, UpdState, ?DELETE_TIMEOUT};
        _ ->
            {reply, Result, delete_pending, State, ?DELETE_TIMEOUT}
    end;
delete_pending({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    {reply,
        fetch_range(StartKey, EndKey, ScanWidth, State),
        delete_pending,
        State,
        ?DELETE_TIMEOUT};
delete_pending({get_slots, SlotList}, _From, State) ->
    SlotBins = read_slots(State#state.handle, SlotList),
    FoldFun =
        fun({SlotBin, SK, EK}, Acc) ->
            Acc ++ trim_slot(SlotBin, SK, EK) end,
    {reply,
        lists:foldl(FoldFun, [], SlotBins),
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
                                               State#state.filename),
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
    case leveled_tinybloom:check({hash, Hash},
                                    Summary#summary.bloom) of
        false ->
            {not_present, summary_bloom, null};
        true ->
            Slot = lookup_slot(LedgerKey, Summary#summary.index),
            SlotBloom = Slot#slot_index_value.bloom,
            case is_check_slot_required({hash, Hash}, LedgerKey, SlotBloom) of
                false ->
                    {not_present, slot_bloom, null};
                true ->
                    CacheEntry = array:get(Slot#slot_index_value.slot_id,
                                    State#state.cache),
                    case CacheEntry of
                        {LedgerKey, CachedValue} ->
                            {{LedgerKey, CachedValue}, cache_entry, null};
                        _ ->
                            SlotLook = lookup_in_slot(LedgerKey,
                                                        {pointer,
                                                            State#state.handle,
                                                            Slot}),
                            case SlotLook of
                                crc_wonky ->
                                    {not_present,
                                        slot_crc_wonky,
                                        Slot#slot_index_value.slot_id};    
                                none ->
                                    {not_present,
                                        slot_lookup_miss,
                                        null};
                                {value, V} ->
                                    {{LedgerKey, V},
                                        slot_lookup_hit,
                                        Slot#slot_index_value.slot_id}
                            end
                    end
            end
    end.

fetch_range(StartKey, EndKey, ScanWidth, State) ->
    Summary = State#state.summary,
    Handle = State#state.handle,
    {Slots, LTrim, RTrim} = lookup_slots(StartKey,
                                            EndKey,
                                            Summary#summary.index),
    Self = self(),
    SL = length(Slots),
    ExpandedSlots = 
        case SL of
            0 ->
                [];
            1 ->
                [Slot] = Slots,
                case {LTrim, RTrim} of
                    {true, true} ->
                        [{pointer, Self, Slot, StartKey, EndKey}];
                    {true, false} ->
                        [{pointer, Self, Slot, StartKey, all}];
                    {false, true} ->
                        [{pointer, Self, Slot, all, EndKey}];
                    {false, false} ->
                        [{pointer, Self, Slot, all, all}]
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
                case {LTrim, RTrim} of
                    {true, true} ->
                        [{pointer, Self, LSlot, StartKey, all}] ++
                            MidSlotPointers ++
                            [{pointer, Self, RSlot, all, EndKey}];
                    {true, false} ->
                        [{pointer, Self, LSlot, StartKey, all}] ++
                            MidSlotPointers ++
                            [{pointer, Self, RSlot, all, all}];
                    {false, true} ->
                        [{pointer, Self, LSlot, all, all}] ++
                            MidSlotPointers ++
                            [{pointer, Self, RSlot, all, EndKey}];
                    {false, false} ->
                        [{pointer, Self, LSlot, all, all}] ++
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
    FetchFun =
        fun({pointer, _Self, S, SK, EK}, Acc) ->
            Acc ++ trim_slot({pointer, Handle, S}, SK, EK) end,
    lists:foldl(FetchFun, [], SlotsToFetch) ++ SlotsToPoint.


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
    Summary = read_table_summary(SummaryBin),
    SlotLengthFetchFun =
        fun({_K, V}, Acc) ->
                [{V#slot_index_value.slot_id,
                    V#slot_index_value.length}|Acc]
        end,
    SlotLengths = lists:foldr(SlotLengthFetchFun, [], Summary#summary.index),
    SlotCount = length(SlotLengths),
    SkipL = leveled_skiplist:from_sortedlist(Summary#summary.index),
    UpdSummary = Summary#summary{index = SkipL},
    leveled_log:log("SST03", [Filename,
                                Summary#summary.size,
                                SlotCount,
                                Summary#summary.max_sqn]),
    State#state{summary = UpdSummary,
                slot_lengths = SlotLengths,
                handle = Handle,
                filename = Filename,
                cache = array:new({size, SlotCount + 1})}.

open_reader(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    {ok, Lengths} = file:pread(Handle, 0, 8),
    <<SlotsLength:32/integer, SummaryLength:32/integer>> = Lengths,
    {ok, SummaryBin} = file:pread(Handle, SlotsLength + 8, SummaryLength),
    {Handle, SummaryBin}.

build_table_summary(SlotIndex, AllHashes, Level, FirstKey, L, MaxSQN) ->
    BloomSlots =
        case lists:keyfind(Level, 1, ?LEVEL_BLOOM_SLOTS) of
            {Level, N} ->
                N;
            false ->
                element(2, lists:keyfind(default, 1, ?LEVEL_BLOOM_SLOTS))
        end,
    BloomAddFun =
        fun({H, _K}, Bloom) -> leveled_tinybloom:enter(H, Bloom) end,
    Bloom = lists:foldr(BloomAddFun,
                            leveled_tinybloom:empty(BloomSlots),
                            AllHashes),
    [{LastKey, _LastV}|_Rest] = SlotIndex,
    Summary = #summary{first_key = FirstKey,
                        last_key = LastKey,
                        size = L,
                        index = lists:reverse(SlotIndex),
                        bloom = Bloom,
                        max_sqn = MaxSQN},
    SummBin = term_to_binary(Summary, [{compressed, ?COMPRESSION_LEVEL}]),
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
    SlotCount = L div ?SLOT_SIZE,
    {SlotIndex, AllHashes, SlotsBin} = build_all_slots(KVList,
                                                        SlotCount,
                                                        8,
                                                        [],
                                                        1,
                                                        [],
                                                        <<>>),
    {FirstKey, L, SlotIndex, AllHashes, SlotsBin}.

build_all_slots([], _Count, _Start, AllHashes, _SlotID, SlotIndex, SlotsBin) ->
    {SlotIndex, AllHashes, SlotsBin};
build_all_slots(KVL, Count, Start, AllHashes, SlotID, SlotIndex, SlotsBin) ->
    {SlotList, KVRem} =
        case Count of
            0 ->
                {lists:sublist(KVL, ?SLOT_SIZE), []};
            _N ->
                lists:split(?SLOT_SIZE, KVL)
        end,
    {LastKey, _V} = lists:last(SlotList),
    ExtractHashFun =
        fun({K, V}, Acc) ->
            {_SQN, H} = leveled_codec:strip_to_seqnhashonly({K, V}),
            case H of
                no_lookup ->
                    Acc;
                H ->
                    [{{hash, H}, K}|Acc]
            end
            end,
    HashList = lists:foldr(ExtractHashFun, [], SlotList),
    {SlotBin, Bloom} = build_slot(SlotList, HashList),
    SlotCRC = erlang:crc32(SlotBin),
    Length = byte_size(SlotBin) + 4,
    SlotIndexV = #slot_index_value{slot_id = SlotID,
                                    bloom = Bloom,
                                    start_position = Start,
                                    length = Length},
    build_all_slots(KVRem,
                    Count - 1,
                    Start + Length,
                    HashList ++ AllHashes,
                    SlotID + 1,
                    [{LastKey, SlotIndexV}|SlotIndex],
                    <<SlotsBin/binary, SlotCRC:32/integer, SlotBin/binary>>).


build_slot(KVList, HashList) ->
    Tree = gb_trees:from_orddict(KVList),
    BloomAddFun =
        fun({H, K}, Bloom) -> leveled_tinybloom:tiny_enter(H, K, Bloom) end,
    Bloom = lists:foldr(BloomAddFun,
                        leveled_tinybloom:tiny_empty(),
                        HashList),
    SlotBin = term_to_binary(Tree, [{compressed, ?COMPRESSION_LEVEL}]),
    {SlotBin, Bloom}.

is_check_slot_required(_Hash, _Key, none) ->
    true;
is_check_slot_required(Hash, Key, Bloom) ->
    leveled_tinybloom:tiny_check(Hash, Key, Bloom).

%% Returns a section from the summary index and two booleans to indicate if
%% the first slot needs trimming, or the last slot
lookup_slots(StartKey, EndKey, SkipList) ->
    SlotsOnlyFun = fun({_K, V}) -> V end,
    {KSL, LTrim, RTrim} = lookup_slots_int(StartKey, EndKey, SkipList),
    {lists:map(SlotsOnlyFun, KSL), LTrim, RTrim}.

lookup_slots_int(all, all, SkipList) ->
    {leveled_skiplist:to_list(SkipList), false, false};
lookup_slots_int(StartKey, all, SkipList) ->
    L = leveled_skiplist:to_list(SkipList),
    LTrimFun = fun({K, _V}) -> K < StartKey end,
    {_LDrop, RKeep0} = lists:splitwith(LTrimFun, L),
    [{FirstKey, _V}|_Rest] = RKeep0,
    LTrim = FirstKey < StartKey,
    {RKeep0, LTrim, false};
lookup_slots_int(StartKey, EndKey, SkipList) ->
    case leveled_skiplist:to_range(SkipList, StartKey, EndKey) of
        [] ->
            BestKey = leveled_skiplist:key_above(SkipList, StartKey),
            {[BestKey], true, true};
        L0 ->
            {LastKey, _LastVal} = lists:last(L0),
            case LastKey of
                EndKey ->
                    {L0, true, false};
                _ ->
                    LTail = leveled_skiplist:key_above_notequals(SkipList,
                                                                    LastKey),
                    case LTail of
                        false ->
                            {L0, true, false};
                        _ ->
                            {L0 ++ [LTail], true, true}
                    end
            end
    end.
        

lookup_slot(Key, SkipList) ->
    {_Mark, Slot} = leveled_skiplist:key_above(SkipList, Key),
    Slot.

lookup_in_slot(Key, {pointer, Handle, Slot}) ->
    SlotBin = read_slot(Handle, Slot),
    case SlotBin of
        crc_wonky ->
            crc_wonky;
        _ ->
            lookup_in_slot(Key, SlotBin)
    end;
lookup_in_slot(Key, SlotBin) ->
    Tree = binary_to_term(SlotBin),
    gb_trees:lookup(Key, Tree).

read_slot(Handle, Slot) ->
    {ok, SlotBin} = file:pread(Handle,
                                Slot#slot_index_value.start_position,
                                Slot#slot_index_value.length),
    <<SlotCRC:32/integer, SlotNoCRC/binary>> = SlotBin,
    case erlang:crc32(SlotNoCRC) of
        SlotCRC ->
            SlotNoCRC;
        _ ->
            crc_wonky
    end.

read_slots(Handle, SlotList) ->
    [{pointer, FirstSlot, _SK1, _EK1}|_Rest] = SlotList,
    {pointer, LastSlot, _SKL, _EKL} = lists:last(SlotList),
    StartPos = FirstSlot#slot_index_value.start_position,
    Length = LastSlot#slot_index_value.start_position
                + LastSlot#slot_index_value.length
                - StartPos,
    {ok, MultiSlotBin} = file:pread(Handle, StartPos, Length),
    read_off_binary(MultiSlotBin, SlotList, []).

read_off_binary(<<>>, [], SplitBins) ->
    SplitBins;
read_off_binary(MultiSlotBin, [TopSlot|Rest], SplitBins) ->
    {pointer, Slot, SK, EK} = TopSlot,
    Length = Slot#slot_index_value.length - 4,
    <<SlotCRC:32/integer,
        SlotBin:Length/binary,
        RestBin/binary>> = MultiSlotBin,
    case erlang:crc32(SlotBin) of
        SlotCRC ->
            read_off_binary(RestBin,
                            Rest,
                            SplitBins ++ [{SlotBin, SK, EK}]);
        _ ->
            read_off_binary(RestBin,
                            Rest,
                            SplitBins ++ [])
    end.


trim_slot({pointer, Handle, Slot}, all, all) ->
    case read_slot(Handle, Slot) of
        crc_wonky ->
            [];
        SlotBin ->
            trim_slot(SlotBin, all, all)
    end;
trim_slot(SlotBinary, all, all) ->
    Tree = binary_to_term(SlotBinary),
    gb_trees:to_list(Tree);
trim_slot({pointer, Handle, Slot}, StartKey, EndKey) ->
    case read_slot(Handle, Slot) of
        crc_wonky ->
            [];
        SlotBin ->
            trim_slot(SlotBin, StartKey, EndKey)
    end;
trim_slot(SlotBinary, StartKey, EndKey) ->
    Tree = binary_to_term(SlotBinary),
    L = gb_trees:to_list(Tree),
    LTrimFun = fun({K, _V}) ->
                        K < StartKey end,
    RTrimFun = fun({K, _V}) -> 
                        not leveled_codec:endkey_passed(EndKey, K) end,
    LTrimL =
        case StartKey of
            all ->
                L;
            _ ->
                {_LDrop, RKeep} = lists:splitwith(LTrimFun, L),
                RKeep
        end,
    RTrimL =
        case EndKey of
            all ->
                LTrimL;
            _ ->
                {LKeep, _RDrop} = lists:splitwith(RTrimFun, LTrimL),
                LKeep
        end,
    RTrimL.


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
maybe_expand_pointer([{next, SSTPid, StartKey}|Tail]) ->
    expand_list_by_pointer({next, SSTPid, StartKey, all},
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
expand_list_by_pointer({next, SSTPid, StartKey, EndKey}, Tail, Width) ->
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
    BNumber =
        case BRange of
            0 ->
                string:right(integer_to_list(BucketLow), 4, $0);
            _ ->
                BRand = random:uniform(BRange),
                string:right(integer_to_list(BucketLow + BRand), 4, $0)
        end,
    KNumber = string:right(integer_to_list(random:uniform(1000)), 6, $0),
    LedgerKey = leveled_codec:to_ledgerkey("Bucket" ++ BNumber,
                                            "Key" ++ KNumber,
                                            o),
    {_B, _K, KV} = leveled_codec:generate_ledgerkv(LedgerKey,
                                                    Seqn,
                                                    crypto:rand_bytes(64),
                                                    64,
                                                    infinity),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [KV|Acc],
                        BucketLow,
                        BRange).


experimental_test() ->
    io:format(user, "~nExperimental timing test:~n", []),
    N = 128,
    KVL1 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 2)),
    ExtractHashFun =
        fun({K, V}) ->
            {_SQN, H} = leveled_codec:strip_to_seqnhashonly({K, V}),
            {{hash, H}, K} end,
    HashList = lists:map(ExtractHashFun, KVL1),
    
    SWA0 = os:timestamp(),
    Tree = gb_trees:from_orddict(KVL1),
    BloomAddFun =
        fun({H, K}, Bloom) -> leveled_tinybloom:tiny_enter(H, K, Bloom) end,
    _Bloom = lists:foldr(BloomAddFun,
                        leveled_tinybloom:tiny_empty(),
                        HashList),
    SlotBin = term_to_binary(Tree, [{compressed, ?COMPRESSION_LEVEL}]),
    io:format(user,
                "Created slot in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWA0)]),
    
    % {TestK1, TestV1} = lists:nth(16, KVL1),
    {TestK2, TestV2} = lists:nth(64, KVL1),
    % {TestK3, TestV3} = lists:nth(96, KVL1),
    SWA1 = os:timestamp(),
    Slot0 = binary_to_term(SlotBin),
    {value, TestV2} = gb_trees:lookup(TestK2, Slot0),
    io:format(user,
                "Looked in slot in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWA1)]).
    
    
    

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
    ML1 = [{next, P1, FK1}],
    ML2 = [{next, P2, FK2}],
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

simple_slotbin_test() ->
    KVList0 = generate_randomkeys(1, ?SLOT_SIZE * 2, 1, 4),
    KVList1 = lists:sublist(lists:ukeysort(1, KVList0), 1, ?SLOT_SIZE),
    ExtractHashFun =
        fun({K, V}) ->
            {_SQN, H} = leveled_codec:strip_to_seqnhashonly({K, V}),
            {{hash, H}, K} end,
    HashList = lists:map(ExtractHashFun, KVList1),
    SW0 = os:timestamp(),
    {SlotBin0, Bloom0} = build_slot(KVList1, HashList),
    io:format(user, "Slot built in ~w microseconds with size ~w~n",
                [timer:now_diff(os:timestamp(), SW0), byte_size(SlotBin0)]),
    
    SW1 = os:timestamp(),
    lists:foreach(fun({H, K}) -> ?assertMatch(true,
                                            is_check_slot_required(H,
                                                                    K,
                                                                    Bloom0))
                                            end,
                    HashList),
    lists:foreach(fun({K, V}) ->
                            ?assertMatch({value, V},
                                            lookup_in_slot(K, SlotBin0))
                                            end,
                    KVList1),
    io:format(user, "Slot checked for all keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW1)]),
    SW2 = os:timestamp(),
    ?assertMatch(KVList1, trim_slot(SlotBin0, all, all)),
    io:format(user, "Slot flattened in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW2)]).
    

simple_slotbinsummary_test() ->
    KVList0 = generate_randomkeys(1, ?SLOT_SIZE * 16, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _V}|_Rest] = KVList1,
    {FirstKey, _L, SlotIndex, AllHashes, SlotsBin} = build_all_slots(KVList1),
    SummaryBin = build_table_summary(SlotIndex,
                                        AllHashes,
                                        2,
                                        FirstKey,
                                        length(KVList1),
                                        undefined),
    Summary = read_table_summary(SummaryBin),
    SummaryIndex = leveled_skiplist:from_sortedlist(Summary#summary.index),
    FetchFun =
        fun({Key, Value}) ->
            Slot = lookup_slot(Key, SummaryIndex),
            StartPos = Slot#slot_index_value.start_position,
            Length = Slot#slot_index_value.length,
            io:format("lookup slot id ~w from ~w length ~w~n",
                        [Slot#slot_index_value.slot_id, StartPos, Length]),
            <<_Pre:StartPos/binary,
                SlotBin:Length/binary,
                _Post/binary>> = <<0:64/integer, SlotsBin/binary>>,
            <<SlotCRC:32/integer, SlotBinNoCRC/binary>> = SlotBin,
            ?assertMatch(SlotCRC, erlang:crc32(SlotBinNoCRC)),
            {value, V} = lookup_in_slot(Key, SlotBinNoCRC),
            ?assertMatch(Value, V)
            end,
    SW = os:timestamp(),
    lists:foreach(FetchFun, KVList1),
    io:format(user,
                "Checking for ~w keys in slots took ~w microseconds~n",
                [length(KVList1), timer:now_diff(os:timestamp(), SW)]).

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
                            Acc ++ sst_getslots(P, [{pointer, S, SK, EK}]);
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



-endif.