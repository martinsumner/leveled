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

-behaviour(gen_statem).

-include("include/leveled.hrl").

-define(LOOK_SLOTSIZE, 128). % Maximum of 128
-define(LOOK_BLOCKSIZE, {24, 32}). % 4x + y = ?LOOK_SLOTSIZE
-define(NOLOOK_SLOTSIZE, 256).
-define(NOLOOK_BLOCKSIZE, {56, 32}). % 4x + y = ?NOLOOK_SLOTSIZE
-define(COMPRESSION_FACTOR, 1).
    % When using native compression - how hard should the compression code
    % try to reduce the size of the compressed output. 1 Is to imply minimal
    % effort, 6 is default in OTP:
    % https://www.erlang.org/doc/man/erlang.html#term_to_binary-2
-define(BINARY_SETTINGS, [{compressed, ?COMPRESSION_FACTOR}]).
-define(MERGE_SCANWIDTH, 16).
-define(DISCARD_EXT, ".discarded").
-define(DELETE_TIMEOUT, 10000).
-define(TREE_TYPE, idxt).
-define(TREE_SIZE, 16).
-define(TIMING_SAMPLECOUNTDOWN, 20000).
-define(TIMING_SAMPLESIZE, 100).
-define(BLOCK_LENGTHS_LENGTH, 20).
-define(LMD_LENGTH, 4).
-define(FLIPPER32, 4294967295).
-define(DOUBLESIZE_LEVEL, 3).
-define(INDEX_MODDATE, true).
-define(TOMB_COUNT, true).
-define(USE_SET_FOR_SPEED, 64).
-define(STARTUP_TIMEOUT, 10000).

-ifdef(TEST).
-define(HIBERNATE_TIMEOUT, 5000).
-else.
-define(HIBERNATE_TIMEOUT, 60000).
-endif.

-define(START_OPTS, [{hibernate_after, ?HIBERNATE_TIMEOUT}]).

-export([init/1,
         callback_mode/0,
         terminate/3,
         code_change/4,
         format_status/2]).

%% states
-export([starting/3,
         reader/3,
         delete_pending/3]).

-export([sst_new/6,
         sst_newmerge/8,
         sst_newlevelzero/7,
         sst_open/4,
         sst_get/2,
         sst_get/3,
         sst_getsqn/3,
         sst_expandpointer/5,
         sst_getmaxsequencenumber/1,
         sst_setfordelete/2,
         sst_clear/1,
         sst_checkready/1,
         sst_switchlevels/2,
         sst_deleteconfirmed/1,
         sst_gettombcount/1,
         sst_close/1]).

-export([sst_newmerge/10]).

-export([tune_seglist/1, extract_hash/1, member_check/2]).

-export([in_range/3]).

-record(slot_index_value, {slot_id :: integer(),
                            start_position :: integer(),
                            length :: integer()}).

-record(summary,
            {first_key :: tuple(),
            last_key :: tuple(),
            index :: tuple() | undefined,
            size :: integer(),
            max_sqn :: integer()}).
    %% DO NOT CHANGE
    %% The summary record is persisted as part of the sile format
    %% Any chnage to this record will mean the change cannot be rolled back

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
        :: slot_pointer()|sst_pointer()|sst_closed_pointer().
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
-type binary_slot()
        :: {binary(), binary(), list(integer()), leveled_codec:ledger_key()}.
-type sst_summary()
        :: #summary{}.
-type blockindex_cache()
        :: {non_neg_integer(), array:array(), non_neg_integer()}.
-type fetch_cache()
        :: array:array()|no_cache.
-type cache_size()
        :: no_cache|4|32|64.
-type cache_hash()
        :: no_cache|non_neg_integer().
-type level()
        :: non_neg_integer().
-type summary_filter()
        :: fun((leveled_codec:ledger_key()) -> any()).

%% yield_blockquery is used to determine if the work necessary to process a
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
            blockindex_cache ::
                blockindex_cache() | undefined | redacted,
            compression_method = native :: press_method(),
            index_moddate = ?INDEX_MODDATE :: boolean(),
            starting_pid :: pid()|undefined,
            fetch_cache = no_cache :: fetch_cache() | redacted,
            new_slots :: list()|undefined,
            deferred_startup_tuple :: tuple()|undefined,
            level :: level()|undefined,
            tomb_count = not_counted
                    :: non_neg_integer()|not_counted,
            high_modified_date :: non_neg_integer()|undefined,
            filter_fun :: summary_filter() | undefined,
            monitor = {no_monitor, 0} :: leveled_monitor:monitor()}).

-record(build_timings,
        {slot_hashlist = 0 :: integer(),
            slot_serialise = 0 :: integer(),
            slot_finish = 0 :: integer(),
            fold_toslot = 0 :: integer()}).

-type sst_state() :: #state{}.
-type build_timings() :: no_timing|#build_timings{}.

-export_type([expandable_pointer/0, press_method/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec sst_open(string(), string(), sst_options(), level())
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
    {ok, Pid} = gen_statem:start_link(?MODULE, [], ?START_OPTS),
    case gen_statem:call(
            Pid, {sst_open, RootPath, Filename, OptsSST, Level}, infinity) of
        {ok, {SK, EK}, Bloom} ->
            {ok, Pid, {SK, EK}, Bloom}
    end.

-spec sst_new(string(), string(), level(),
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
    sst_new(
        RootPath, Filename, Level, KVList, MaxSQN, OptsSST, ?INDEX_MODDATE).

sst_new(RootPath, Filename, Level, KVList, MaxSQN, OptsSST, IndexModDate) ->
    {ok, Pid} = gen_statem:start_link(?MODULE, [], ?START_OPTS),
    OptsSST0 = update_options(OptsSST, Level),
    {[], [], SlotList, FK, _CountOfTombs}  =
        merge_lists(KVList, OptsSST0, IndexModDate),
    case gen_statem:call(Pid, {sst_new,
                               RootPath,
                               Filename,
                               Level,
                               {SlotList, FK},
                               MaxSQN,
                               OptsSST0,
                               IndexModDate,
                               not_counted,
                               self()},
                         infinity) of
        {ok, {SK, EK}, Bloom} ->
            {ok, Pid, {SK, EK}, Bloom}
    end.

-spec sst_newmerge(string(), string(),
                    list(leveled_codec:ledger_kv()|sst_pointer()),
                    list(leveled_codec:ledger_kv()|sst_pointer()),
                    boolean(), level(),
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
%% so that the remainder can be  used in the next file in the merge.  It might
%% be that the merge_lists returns nothing (for example when a basement file is
%% all tombstones) - and the atom empty is returned in this case so that the
%% file is not added to the manifest.
sst_newmerge(RootPath, Filename,
        KVL1, KVL2, IsBasement, Level,
        MaxSQN, OptsSST) when Level > 0 ->
    sst_newmerge(RootPath, Filename,
        KVL1, KVL2, IsBasement, Level,
        MaxSQN, OptsSST, ?INDEX_MODDATE, ?TOMB_COUNT).

sst_newmerge(RootPath, Filename,
        KVL1, KVL2, IsBasement, Level,
        MaxSQN, OptsSST, IndexModDate, TombCount) ->
    OptsSST0 = update_options(OptsSST, Level),
    {Rem1, Rem2, SlotList, FK, CountOfTombs} = 
        merge_lists(
            KVL1,
            KVL2,
            {IsBasement, Level},
            OptsSST0,
            IndexModDate,
            TombCount),
    case SlotList of
        [] ->
            empty;
        _ ->
            {ok, Pid} = gen_statem:start_link(?MODULE, [], ?START_OPTS),
            case gen_statem:call(Pid, {sst_new,
                                       RootPath,
                                       Filename,
                                       Level,
                                       {SlotList, FK},
                                       MaxSQN,
                                       OptsSST0,
                                       IndexModDate,
                                       CountOfTombs,
                                       self()},
                                 infinity) of
                {ok, {SK, EK}, Bloom} ->
                    {ok, Pid, {{Rem1, Rem2}, SK, EK}, Bloom}
            end
    end.

-spec sst_newlevelzero(string(), string(),
                            integer(),
                            fun((pos_integer(),
                                    leveled_penciller:levelzero_returnfun())
                                -> ok)|
                                list(),
                            pid()|undefined,
                            integer(),
                            sst_options()) ->
                                        {ok, pid(), noreply}.
%% @doc
%% Start a new file at level zero.  At this level the file size is not fixed -
%% it will be as big as the input.  Also the KVList is not passed in, it is
%% fetched slot by slot using the FetchFun
sst_newlevelzero(RootPath, Filename,
                    Slots, Fetcher, Penciller,
                    MaxSQN, OptsSST) ->
    OptsSST0 = update_options(OptsSST, 0),
    {ok, Pid} = gen_statem:start_link(?MODULE, [], ?START_OPTS),
    %% Initiate the file into the "starting" state
    ok = gen_statem:call(Pid, {sst_newlevelzero,
                               RootPath,
                               Filename,
                               Penciller,
                               MaxSQN,
                               OptsSST0,
                               ?INDEX_MODDATE},
                         infinity),
    ok =
        case Fetcher of
            FetchSlots when is_list(Fetcher) ->
               gen_statem:cast(Pid, {complete_l0startup, FetchSlots});
            _ ->
                % Fetcher is a function
                gen_statem:cast(Pid, {sst_returnslot, none, Fetcher, Slots})
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
    gen_statem:call(Pid, {get_kv, LedgerKey, Hash, undefined}, infinity).

-spec sst_getsqn(pid(),
    leveled_codec:ledger_key(),
    leveled_codec:segment_hash()) -> leveled_codec:sqn()|not_present.
%% @doc
%% Return a SQN for the key or not_present if the key is not in
%% the store (with the magic hash precalculated).
sst_getsqn(Pid, LedgerKey, Hash) ->
    gen_statem:call(Pid, {get_kv, LedgerKey, Hash, fun sqn_only/1}, infinity).

-spec sst_getmaxsequencenumber(pid()) -> integer().
%% @doc
%% Get the maximume sequence number for this SST file
sst_getmaxsequencenumber(Pid) ->
    gen_statem:call(Pid, get_maxsequencenumber, infinity).

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
    gen_statem:call(Pid, {set_for_delete, Penciller}, infinity).

-spec sst_gettombcount(pid()) -> non_neg_integer()|not_counted.
%% @doc
%% Get the count of tomb stones in this SST file, returning not_counted if this
%% file was created with a version which did not support tombstone counting, or
%% could also be because the file is L0 (which aren't counted as being chosen
%% for merge is inevitable)
sst_gettombcount(Pid) ->
    gen_statem:call(Pid, get_tomb_count, infinity).

-spec sst_clear(pid()) -> ok.
%% @doc
%% For this file to be closed and deleted
sst_clear(Pid) ->
    gen_statem:call(Pid, {set_for_delete, false}, infinity),
    gen_statem:call(Pid, close).

-spec sst_deleteconfirmed(pid()) -> ok.
%% @doc
%% Allows a penciller to confirm to a SST file that it can be cleared, as it
%% is no longer in use
sst_deleteconfirmed(Pid) ->
    gen_statem:cast(Pid, close).

-spec sst_checkready(pid()) -> {ok, string(),
                                leveled_codec:ledger_key(),
                                leveled_codec:ledger_key()}.
%% @doc
%% If a file has been set to be built, check that it has been built.  Returns
%% the filename and the {startKey, EndKey} for the manifest.
sst_checkready(Pid) ->
    %% Only used in test
    gen_statem:call(Pid, background_complete).

-spec sst_switchlevels(pid(), pos_integer()) -> ok.
%% @doc
%% Notify the SST file that it is now working at a new level
%% This simply prompts a GC on the PID now (as this may now be a long-lived
%% file, so don't want all the startup state to be held on memory - want to
%% proactively drop it
sst_switchlevels(Pid, NewLevel) ->
    gen_statem:cast(Pid, {switch_levels, NewLevel}).

-spec sst_close(pid()) -> ok.
%% @doc
%% Close the file
sst_close(Pid) ->
    gen_statem:call(Pid, close).



%%%============================================================================
%%% gen_statem callbacks
%%%============================================================================

callback_mode() ->
    state_functions.

init([]) ->
    {ok, starting, #state{}}.

starting({call, From},
            {sst_open, RootPath, Filename, OptsSST, Level},
            State) ->
    leveled_log:save(OptsSST#sst_options.log_options),
    Monitor = OptsSST#sst_options.monitor,
    {UpdState, Bloom} =
        read_file(Filename,
                    State#state{root_path=RootPath},
                    OptsSST#sst_options.pagecache_level >= Level),
    Summary = UpdState#state.summary,
    {next_state,
        reader,
        UpdState#state{
            level = Level, fetch_cache = new_cache(Level), monitor = Monitor},
        [{reply, From,
            {ok,
                {Summary#summary.first_key, Summary#summary.last_key},
                Bloom}
        }]};
starting({call, From},
            {sst_new,
                RootPath, Filename, Level,
                {SlotList, FirstKey}, MaxSQN,
                OptsSST, IdxModDate, CountOfTombs, StartingPID},
            State) ->
    SW = os:timestamp(),
    leveled_log:save(OptsSST#sst_options.log_options),
    Monitor = OptsSST#sst_options.monitor,
    PressMethod = OptsSST#sst_options.press_method,
    {Length, SlotIndex, BlockEntries, SlotsBin, Bloom} =
        build_all_slots(SlotList),
    {_, BlockIndex, HighModDate} =
        update_blockindex_cache(true,
                                BlockEntries,
                                new_blockindex_cache(Length),
                                undefined,
                                IdxModDate),
    SummaryBin =
        build_table_summary(SlotIndex, Level, FirstKey, Length,
                            MaxSQN, Bloom, CountOfTombs),
    ActualFilename =
        write_file(RootPath, Filename, SummaryBin, SlotsBin,
                    PressMethod, IdxModDate, CountOfTombs),
    YBQ = Level =< 2,
    {UpdState, Bloom} =
        read_file(ActualFilename,
                    State#state{root_path=RootPath, yield_blockquery=YBQ},
                    OptsSST#sst_options.pagecache_level >= Level),
    Summary = UpdState#state.summary,
    leveled_log:log_timer(
        sst08, [ActualFilename, Level, Summary#summary.max_sqn], SW),
    erlang:send_after(?STARTUP_TIMEOUT, self(), start_complete),
    {next_state,
        reader,
        UpdState#state{
            blockindex_cache = BlockIndex,
            high_modified_date = HighModDate,
            starting_pid = StartingPID,
            level = Level,
            fetch_cache = new_cache(Level),
            monitor = Monitor},
        [{reply,
            From,
            {ok, {Summary#summary.first_key, Summary#summary.last_key}, Bloom}
        }]};
starting({call, From}, {sst_newlevelzero, RootPath, Filename,
                    Penciller, MaxSQN,
                    OptsSST, IdxModDate}, State) ->
    DeferredStartupTuple =
        {RootPath, Filename, Penciller, MaxSQN, OptsSST,
            IdxModDate},
    {next_state, starting,
        State#state{
            deferred_startup_tuple = DeferredStartupTuple,
            level = 0,
            fetch_cache = new_cache(0)},
        [{reply, From, ok}]};
starting({call, From}, close, State) ->
    %% No file should have been created, so nothing to close.
    {stop_and_reply, normal, [{reply, From, ok}], State};

starting(cast, {complete_l0startup, Slots}, State) ->
    {keep_state,
        State#state{new_slots = Slots},
        [{next_event, cast, complete_l0startup}]};
starting(cast, complete_l0startup, State) ->
    {RootPath, Filename, Penciller, MaxSQN, OptsSST, IdxModDate} =
        State#state.deferred_startup_tuple,
    SW0 = os:timestamp(),
    FetchedSlots = State#state.new_slots,
    leveled_log:save(OptsSST#sst_options.log_options),
    Monitor = OptsSST#sst_options.monitor,
    PressMethod = OptsSST#sst_options.press_method,
    FetchFun = fun(Slot) -> lists:nth(Slot, FetchedSlots) end,
    KVList = leveled_pmem:to_list(length(FetchedSlots), FetchFun),
    Time0 = timer:now_diff(os:timestamp(), SW0),

    SW1 = os:timestamp(),
    {[], [], SlotList, FirstKey, _CountOfTombs} =
        merge_lists(KVList, OptsSST, IdxModDate),
    Time1 = timer:now_diff(os:timestamp(), SW1),

    SW2 = os:timestamp(),
    {SlotCount, SlotIndex, BlockEntries, SlotsBin,Bloom} =
        build_all_slots(SlotList),
    {_, BlockIndex, HighModDate} =
        update_blockindex_cache(true,
                                BlockEntries,
                                new_blockindex_cache(SlotCount),
                                undefined,
                                IdxModDate),
    Time2 = timer:now_diff(os:timestamp(), SW2),

    SW3 = os:timestamp(),
    SummaryBin =
        build_table_summary(SlotIndex, 0, FirstKey, SlotCount,
                            MaxSQN, Bloom, not_counted),
    Time3 = timer:now_diff(os:timestamp(), SW3),

    SW4 = os:timestamp(),
    ActualFilename =
        write_file(RootPath, Filename, SummaryBin, SlotsBin,
                    PressMethod, IdxModDate, not_counted),
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

    leveled_log:log_timer(
        sst08, [ActualFilename, 0, Summary#summary.max_sqn], SW0),
    leveled_log:log(sst11, [Time0, Time1, Time2, Time3, Time4]),

    case Penciller of
        undefined ->
            ok;
        _ ->
            leveled_penciller:pcl_confirml0complete(
                Penciller,
                UpdState#state.filename,
                Summary#summary.first_key,
                Summary#summary.last_key,
                Bloom),
            ok
    end,
    {next_state,
        reader,
        UpdState#state{
            blockindex_cache = BlockIndex,
            high_modified_date = HighModDate,
            monitor = Monitor}};
starting(cast, {sst_returnslot, FetchedSlot, FetchFun, SlotCount}, State) ->
    FetchedSlots =
        case FetchedSlot of
            none ->
                [];
            _ ->
                [FetchedSlot|State#state.new_slots]
        end,
    case length(FetchedSlots) == SlotCount of
        true ->
            {keep_state,
                %% Reverse the slots so that they are back in the expected
                %% order
                State#state{new_slots = lists:reverse(FetchedSlots)},
            [{next_event, cast, complete_l0startup}]};
        false ->
            Self = self(),
            ReturnFun =
                fun(NextSlot) ->
                    gen_statem:cast(Self,
                                        {sst_returnslot, NextSlot,
                                            FetchFun, SlotCount})
                end,
            FetchFun(length(FetchedSlots) + 1, ReturnFun),
            {keep_state,
                State#state{new_slots = FetchedSlots}}
    end.


reader({call, From}, {get_kv, LedgerKey, Hash, Filter}, State) ->
    % Get a KV value and potentially take sample timings
    Monitor =
        case Filter of
            undefined ->
                State#state.monitor;
            _ ->
                {no_monitor, 0}
        end,
    {KeyValue, BIC, HMD, FC} = 
        fetch(
            LedgerKey, Hash,
            State#state.summary,
            State#state.compression_method,
            State#state.high_modified_date,
            State#state.index_moddate,
            State#state.filter_fun,
            State#state.blockindex_cache,
            State#state.fetch_cache,
            State#state.handle,
            State#state.level,
            Monitor),
    Result =
        case Filter of
            undefined ->
                KeyValue;
            F ->
                F(KeyValue)
        end,
    case {BIC, HMD, FC} of
        {no_update, no_update, no_update} ->
            {keep_state_and_data, [{reply, From, Result}]};
        {no_update, no_update, FC} ->
            {keep_state,
                State#state{fetch_cache = FC},
                [{reply, From, Result}]};
        {BIC, undefined, no_update} ->
            {keep_state,
                State#state{blockindex_cache = BIC},
                [{reply, From, Result}]};
        {BIC, HMD, no_update} ->
            {keep_state,
                State#state{blockindex_cache = BIC, high_modified_date = HMD},
                [hibernate, {reply, From, Result}]}
    end;
reader({call, From},
        {get_kvrange, StartKey, EndKey, ScanWidth, SegList, LowLastMod},
        State) ->
    ReadNeeded =
        check_modified(State#state.high_modified_date,
                        LowLastMod,
                        State#state.index_moddate),
    {NeedBlockIdx, SlotsToFetchBinList, SlotsToPoint} =
        case ReadNeeded of
            true ->
                fetch_range(StartKey, EndKey, ScanWidth,
                            SegList, LowLastMod,
                            State);
            false ->
                {false, [], []}
        end,
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,

    case State#state.yield_blockquery of
        true ->
            {keep_state_and_data,
                [{reply,
                    From,
                    {yield,
                        SlotsToFetchBinList,
                        SlotsToPoint,
                        PressMethod,
                        IdxModDate}
                }]};
        false ->
            {L, FoundBIC} =
                binaryslot_reader(
                    SlotsToFetchBinList, PressMethod, IdxModDate, SegList),
            {UpdateCache, BlockIdxC0, HighModDate} =
                update_blockindex_cache(NeedBlockIdx,
                                        FoundBIC,
                                        State#state.blockindex_cache,
                                        State#state.high_modified_date,
                                        State#state.index_moddate),
            case UpdateCache of
                true ->
                    {keep_state,
                        State#state{
                            blockindex_cache = BlockIdxC0,
                            high_modified_date = HighModDate},
                        [{reply, From, L ++ SlotsToPoint}]};
                false ->
                    {keep_state_and_data,
                        [hibernate, {reply, From, L ++ SlotsToPoint}]}
            end
    end;
reader({call, From}, {get_slots, SlotList, SegList, LowLastMod}, State) ->
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    {NeedBlockIdx, SlotBins} =
        read_slots(State#state.handle,
                        SlotList,
                        {SegList,
                            LowLastMod,
                            State#state.blockindex_cache},
                        State#state.compression_method,
                        State#state.index_moddate),
    {keep_state_and_data,
        [{reply, From, {NeedBlockIdx, SlotBins, PressMethod, IdxModDate}}]};
reader({call, From}, get_maxsequencenumber, State) ->
    Summary = State#state.summary,
    {keep_state_and_data,
     [{reply, From, Summary#summary.max_sqn}]};
reader({call, From}, {set_for_delete, Penciller}, State) ->
    leveled_log:log(sst06, [State#state.filename]),
    {next_state,
        delete_pending,
        State#state{penciller=Penciller},
        [{reply, From,ok}, ?DELETE_TIMEOUT]};
reader({call, From}, background_complete, State) ->
    Summary = State#state.summary,
    {keep_state_and_data,
        [{reply,
            From,
            {ok,
                State#state.filename,
                Summary#summary.first_key,
                Summary#summary.last_key}
        }]};
reader({call, From}, get_tomb_count, State) ->
    {keep_state_and_data,
     [{reply, From, State#state.tomb_count}]};
reader({call, From}, close, State) ->
    ok = file:close(State#state.handle),
    {stop_and_reply, normal, [{reply, From, ok}], State};

reader(cast, {switch_levels, NewLevel}, State) ->
    FreshCache = new_cache(NewLevel),
    {keep_state,
        State#state{level = NewLevel, fetch_cache = FreshCache},
        [hibernate]};
reader(info, {update_blockindex_cache, BIC}, State) ->
    handle_update_blockindex_cache(BIC, State);
reader(info, bic_complete, State) ->
    % The block index cache is complete, so the memory footprint should be
    % relatively stable from this point.  Hibernate to help minimise
    % fragmentation
    leveled_log:log(sst14, [State#state.filename]),
    {keep_state_and_data, [hibernate]};
reader(info, start_complete, State) ->
    % The SST file will be started by a clerk, but the clerk may be shut down
    % prior to the manifest being updated about the existence of this SST file.
    % If there is no activity after startup, check the clerk is still alive and
    % otherwise assume this file is part of a closed store and shut down.
    % If the clerk has crashed, the penciller will restart at the latest
    % manifest, and so this file sill be restarted if and only if it is still
    % part of the store
    case is_process_alive(State#state.starting_pid) of
        true ->
            {keep_state_and_data, []};
        false ->
            {stop, normal}
    end.


delete_pending({call, From}, {get_kv, LedgerKey, Hash, Filter}, State) ->
    {KeyValue, _BIC, _HMD, _FC} = 
        fetch(
            LedgerKey, Hash,
            State#state.summary,
            State#state.compression_method,
            State#state.high_modified_date,
            State#state.index_moddate,
            State#state.filter_fun,
            State#state.blockindex_cache,
            State#state.fetch_cache,
            State#state.handle,
            State#state.level,
            {no_monitor, 0}),
    Result =
        case Filter of
            undefined ->
                KeyValue;
            F ->
                F(KeyValue)
        end,
    {keep_state_and_data, [{reply, From, Result}, ?DELETE_TIMEOUT]};
delete_pending(
        {call, From},
        {get_kvrange, StartKey, EndKey, ScanWidth, SegList, LowLastMod},
        State) ->
    {_NeedBlockIdx, SlotsToFetchBinList, SlotsToPoint} =
        fetch_range(StartKey, EndKey, ScanWidth, SegList, LowLastMod, State),
    % Always yield as about to clear and de-reference
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    {keep_state_and_data,
        [{reply, From,
            {yield,
            SlotsToFetchBinList,
            SlotsToPoint,
            PressMethod,
            IdxModDate}},
        ?DELETE_TIMEOUT]};
delete_pending(
        {call, From},
        {get_slots, SlotList, SegList, LowLastMod},
        State) ->
    PressMethod = State#state.compression_method,
    IdxModDate = State#state.index_moddate,
    {_NeedBlockIdx, SlotBins} =
        read_slots(State#state.handle,
                    SlotList,
                    {SegList, LowLastMod, State#state.blockindex_cache},
                    PressMethod,
                    IdxModDate),
    {keep_state_and_data,
        [{reply, From, {false, SlotBins, PressMethod, IdxModDate}},
            ?DELETE_TIMEOUT]};
delete_pending({call, From}, close, State) ->
    leveled_log:log(sst07, [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = 
        file:delete(
            filename:join(State#state.root_path, State#state.filename)),
    {stop_and_reply, normal, [{reply, From, ok}], State};
delete_pending(cast, close, State) ->
    leveled_log:log(sst07, [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = 
        file:delete(
            filename:join(State#state.root_path, State#state.filename)),
    {stop, normal, State};

delete_pending(info, _Event, _State) ->
    % Ignore messages when pending delete. The message may have interrupted
    % the delete timeout, so timeout straight away
    {keep_state_and_data, [0]};
delete_pending(timeout, _, State) ->
    case State#state.penciller of
        false ->
            ok = leveled_sst:sst_deleteconfirmed(self());
        PCL ->
            FN = State#state.filename,
            ok = leveled_penciller:pcl_confirmdelete(PCL, FN, self())
        end,
    % If the next thing is another timeout - may be long-running snapshot, so
    % back-off
    {keep_state_and_data, [leveled_rand:uniform(10) * ?DELETE_TIMEOUT]}.

handle_update_blockindex_cache(BIC, State) ->
    {_, BlockIndexCache, HighModDate} =
        update_blockindex_cache(true,
                                BIC,
                                State#state.blockindex_cache,
                                State#state.high_modified_date,
                                State#state.index_moddate),
    {keep_state,
        State#state{
            blockindex_cache = BlockIndexCache,
            high_modified_date = HighModDate}}.
    

terminate(normal, delete_pending, _State) ->
    ok;
terminate(Reason, _StateName, State) ->
    leveled_log:log(sst04, [Reason, State#state.filename]).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(normal, [_PDict, _, State]) ->
    State;
format_status(terminate, [_PDict, _, State]) ->
    State#state{
        blockindex_cache = redacted, fetch_cache = redacted}.


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
%% By default will not have a segment filter, or a low last_modified_date, but
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
    leveled_log:log(sst10, [SSTPid, is_process_alive(SSTPid)]),
    ExpPointer = sst_getfilteredrange(SSTPid,
                                        StartKey,
                                        EndKey,
                                        Width,
                                        SegList,
                                        LowLastMod),
    ExpPointer ++ Tail.


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
    case gen_statem:call(Pid, {get_kvrange,
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
    {NeedBlockIdx, SlotBins, PressMethod, IdxModDate} =
        gen_statem:call(
            Pid, {get_slots, SlotList, SegL0, LowLastMod}, infinity),
    {L, BIC} = binaryslot_reader(SlotBins, PressMethod, IdxModDate, SegL0),
    case NeedBlockIdx of
        true ->
            erlang:send(Pid, {update_blockindex_cache, BIC});
        false ->
            ok
    end,
    L.


-spec find_pos(binary(),
                non_neg_integer()|
                    {list, list(non_neg_integer())}|
                    {sets, sets:set(non_neg_integer())},
                list(non_neg_integer()),
                non_neg_integer()) -> list(non_neg_integer()).
%% @doc
%% Find a list of positions where there is an element with a matching segment
%% ID to the expected segments (which can either be a single segment, a list of
%% segments or a set of segments depending on size.
find_pos(<<1:1/integer, PotentialHit:15/integer, T/binary>>,
                                        Checker, PosList, Count) ->
    case member_check(PotentialHit, Checker) of
        true ->
            find_pos(T, Checker, PosList ++ [Count], Count + 1);
        false ->
            find_pos(T, Checker, PosList, Count + 1)
    end;
find_pos(<<0:1/integer, NHC:7/integer, T/binary>>, Checker, PosList, Count) ->
    find_pos(T, Checker, PosList, Count + NHC + 1);
find_pos(_BinRem, _Hash, PosList, _Count) ->
    %% Expect this to be <<>> - i.e. at end of binary, but if there is
    %% corruption, could be some other value - so return as well in this
    %% case
    PosList.


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

-spec sqn_only(leveled_codec:ledger_kv()|not_present)
        -> leveled_codec:sqn()|not_present.
sqn_only(not_present) ->
    not_present;
sqn_only(KV) ->
    leveled_codec:strip_to_seqonly(KV).

extract_hash({SegHash, _ExtraHash}) when is_integer(SegHash) ->
    tune_hash(SegHash);
extract_hash(NotHash) ->
    NotHash.


-spec new_cache(level()) -> fetch_cache().
new_cache(Level) ->
    case cache_size(Level) of
        no_cache ->
            no_cache;
        CacheSize ->
            array:new([{size, CacheSize}])
    end.

-spec cache_hash(leveled_codec:segment_hash(), non_neg_integer()) ->
    cache_hash().
cache_hash({_SegHash, ExtraHash}, Level) when is_integer(ExtraHash) ->
    case cache_size(Level) of
        no_cache -> no_cache;
        CH -> ExtraHash band (CH - 1)
    end.

%% @doc
%% The lower the level, the bigger the memory cost of supporting the cache,
%% as each level has more files than the previous level.  Load tests with
%% any sort of pareto distribution show far better cost/benefit ratios for
%% cache at higher levels.
-spec cache_size(level()) -> cache_size().
cache_size(N) when N < 3 ->
    64;
cache_size(3) ->
    32;
cache_size(4) ->
    16;
cache_size(5) ->
    4;
cache_size(6) ->
    4;
cache_size(_LowerLevel) ->
    no_cache.

-spec fetch_from_cache(
    cache_hash(),
    fetch_cache()) -> undefined|leveled_codec:ledger_kv().
fetch_from_cache(_CacheHash, no_cache) ->
    undefined;
fetch_from_cache(CacheHash, Cache) ->
    array:get(CacheHash, Cache).

-spec add_to_cache(
    non_neg_integer(),
    leveled_codec:ledger_kv(),
    fetch_cache()) -> fetch_cache().
add_to_cache(_CacheHash, _KV, no_cache) ->
    no_cache;
add_to_cache(CacheHash, KV, FetchCache) ->
    array:set(CacheHash, KV, FetchCache).


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


-spec update_options(sst_options(), non_neg_integer()) -> sst_options().
update_options(OptsSST, Level) ->
    CompressLevel = OptsSST#sst_options.press_level,
    PressMethod0 =
        compress_level(Level, CompressLevel, OptsSST#sst_options.press_method),
    MaxSlots0 =
        maxslots_level(Level, OptsSST#sst_options.max_sstslots),
    OptsSST#sst_options{press_method = PressMethod0, max_sstslots = MaxSlots0}.

-spec new_blockindex_cache(pos_integer()) -> blockindex_cache().
new_blockindex_cache(Size) ->
    {0, array:new([{size, Size}, {default, none}]), 0}.

-spec updatebic_foldfun(boolean()) ->
        fun(({integer(), binary()}, blockindex_cache()) -> blockindex_cache()).
updatebic_foldfun(HMDRequired) ->
    fun(CacheEntry, {AccCount, Cache, AccHMD}) ->
        case CacheEntry of
            {ID, Header} when is_binary(Header) ->
                case array:get(ID - 1, Cache) of
                    none ->
                        H0 = binary:copy(Header),
                        AccHMD0 =
                            case HMDRequired of
                                true ->
                                    max(AccHMD,
                                        element(2, extract_header(H0, true)));
                                false ->
                                    AccHMD
                                end,
                        {AccCount + 1, array:set(ID - 1, H0, Cache), AccHMD0};
                    _ ->
                        {AccCount, Cache, AccHMD}
                end;
            _ ->
                {AccCount, Cache, AccHMD}
        end
    end.

-spec update_blockindex_cache(
        boolean(), list({integer(), binary()}),
        blockindex_cache(), non_neg_integer()|undefined,
        boolean()) ->
            {boolean(), blockindex_cache(), non_neg_integer()|undefined}.
update_blockindex_cache(true, Entries, BIC, HighModDate, IdxModDate) ->
    case {element(1, BIC), array:size(element(2, BIC))} of
        {N, N} ->
            {false, BIC, HighModDate};
        {N, S} when N < S ->
            FoldFun =
                case {HighModDate, IdxModDate} of
                    {undefined, true} ->
                        updatebic_foldfun(true);
                    _ ->
                        updatebic_foldfun(false)
                end,
            BIC0 = lists:foldl(FoldFun, BIC, Entries),
            case {element(1, BIC0), IdxModDate} of
                {N, _} ->
                    {false, BIC, HighModDate};
                {S, true} ->
                    erlang:send(self(), bic_complete),
                    {true, BIC0, element(3, BIC0)};
                _ ->
                    {true, BIC0, undefined}
            end
    end;
update_blockindex_cache(_Needed, _Entries, BIC, HighModDate, _IdxModDate) ->
    {false, BIC, HighModDate}.

-spec check_modified(non_neg_integer()|undefined,
                        non_neg_integer(),
                        boolean())  -> boolean().
check_modified(HighLastModifiedInSST, LowModDate, true)
                when is_integer(HighLastModifiedInSST) ->
    LowModDate =< HighLastModifiedInSST;
check_modified(_, _, _) ->
    true.

-spec fetch(
    leveled_codec:ledger_key(),
    leveled_codec:segment_hash(),
    sst_summary(),
    press_method(),
    non_neg_integer()|undefined,
    boolean(),
    summary_filter(),
    blockindex_cache(),
    fetch_cache(),
    file:fd(),
    leveled_pmanifest:lsm_level(),
    leveled_monitor:monitor())
        -> {not_present|leveled_codec:ledger_kv(),
            blockindex_cache()|no_update,
            non_neg_integer()|undefined|no_update,
            fetch_cache()|no_update}.
%% @doc
%%
%% Fetch a key from the store, potentially taking timings.  Result should be
%% not_present if the key is not in the store.
fetch(LedgerKey, Hash,
        Summary,
        PressMethod, HighModDate, IndexModDate, FilterFun, BIC, FetchCache,
        Handle, Level, Monitor) ->
    SW0 = leveled_monitor:maybe_time(Monitor),
    Slot =
        lookup_slot(LedgerKey, Summary#summary.index, FilterFun),
    SlotID = Slot#slot_index_value.slot_id,
    CachedBlockIdx = array:get(SlotID - 1, element(2, BIC)),

    case extract_header(CachedBlockIdx, IndexModDate) of
        none ->
            SlotBin = read_slot(Handle, Slot),
            {Result, Header} =
                binaryslot_get(
                    SlotBin, LedgerKey, Hash, PressMethod, IndexModDate),
            {_UpdateState, BIC0, HMD0} =
                update_blockindex_cache(true,
                                        [{SlotID, Header}],
                                        BIC,
                                        HighModDate,
                                        IndexModDate),
            case Result of
                not_present ->
                    maybelog_fetch_timing(
                        Monitor, Level, not_found, SW0);
                _ ->
                    maybelog_fetch_timing(
                        Monitor, Level, slot_noncachedblock, SW0)
            end,
            {Result, BIC0, HMD0, no_update};
        {BlockLengths, _LMD, PosBin} ->
            PosList = find_pos(PosBin, extract_hash(Hash), [], 0),
            case PosList of
                [] ->
                    maybelog_fetch_timing(Monitor, Level, not_found, SW0),
                    {not_present, no_update, no_update, no_update};
                _ ->
                    CacheHash = cache_hash(Hash, Level),
                    case fetch_from_cache(CacheHash, FetchCache) of
                        {LedgerKey, V} ->
                            maybelog_fetch_timing(
                                Monitor, Level, fetch_cache, SW0),
                            {{LedgerKey, V}, no_update, no_update, no_update};
                        _ ->
                            StartPos = Slot#slot_index_value.start_position,
                            Result =
                                check_blocks(PosList,
                                                {Handle, StartPos},
                                                BlockLengths,
                                                byte_size(PosBin),
                                                LedgerKey,
                                                PressMethod,
                                                IndexModDate,
                                                not_present),
                            case Result of
                                not_present ->
                                    maybelog_fetch_timing(
                                        Monitor, Level, not_found, SW0),
                                    {not_present,
                                        no_update, no_update, no_update};
                                _ ->
                                    FetchCache0 =
                                        add_to_cache(
                                            CacheHash, Result, FetchCache),
                                    maybelog_fetch_timing(
                                        Monitor, Level, slot_cachedblock, SW0),
                                    {Result,
                                        no_update, no_update, FetchCache0}
                            end
                    end
            end
    end.


-spec fetch_range(tuple(), tuple(), integer(),
                    leveled_codec:segment_list(), non_neg_integer(),
                    sst_state()) ->
                        {boolean(), list(), list()}.
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
    {Slots, RTrim} =
        lookup_slots(
            StartKey,
            EndKey,
            Summary#summary.index,
            State#state.filter_fun),
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

    {NeededBlockIdx, SlotsToFetchBinList} =
        read_slots(Handle,
                    SlotsToFetch,
                    {SegList, LowLastMod, State#state.blockindex_cache},
                    State#state.compression_method,
                    State#state.index_moddate),
    {NeededBlockIdx, SlotsToFetchBinList, SlotsToPoint}.

-spec compress_level(
    non_neg_integer(), non_neg_integer(), press_method()) -> press_method().
%% @doc
%% disable compression at higher levels for improved performance
compress_level(
        Level, LevelToCompress, _PressMethod) when Level < LevelToCompress ->
    none;
compress_level(_Level, _LevelToCompress, PressMethod) ->
    PressMethod.

-spec maxslots_level(level(), pos_integer()) ->  pos_integer().
maxslots_level(Level, MaxSlotCount) when Level < ?DOUBLESIZE_LEVEL ->
    MaxSlotCount;
maxslots_level(_Level, MaxSlotCount) ->
    2 * MaxSlotCount.

write_file(RootPath, Filename, SummaryBin, SlotsBin,
            PressMethod, IdxModDate, CountOfTombs) ->
    SummaryLength = byte_size(SummaryBin),
    SlotsLength = byte_size(SlotsBin),
    {PendingName, FinalName} = generate_filenames(Filename),
    FileVersion = gen_fileversion(PressMethod, IdxModDate, CountOfTombs),
    case filelib:is_file(filename:join(RootPath, FinalName)) of
        true ->
            AltName = filename:join(RootPath, filename:basename(FinalName))
                        ++ ?DISCARD_EXT,
            leveled_log:log(sst05, [FinalName, AltName]),
            ok = file:rename(filename:join(RootPath, FinalName), AltName);
        false ->
            ok
    end,

    ok = leveled_util:safe_rename(filename:join(RootPath, PendingName),
                                    filename:join(RootPath, FinalName),
                                    <<FileVersion:8/integer,
                                        SlotsLength:32/integer,
                                        SummaryLength:32/integer,
                                        SlotsBin/binary,
                                        SummaryBin/binary>>,
                                        false),
    FinalName.

read_file(Filename, State, LoadPageCache) ->
    {Handle, FileVersion, SummaryBin} =
        open_reader(
            filename:join(State#state.root_path, Filename),
            LoadPageCache),
    UpdState0 = imp_fileversion(FileVersion, State),
    {Summary, Bloom, SlotList, TombCount} =
        read_table_summary(SummaryBin, UpdState0#state.tomb_count),
    BlockIndexCache = new_blockindex_cache(Summary#summary.size),
    UpdState1 = UpdState0#state{blockindex_cache = BlockIndexCache},
    {SlotIndex, FilterFun} =
        from_list(
            SlotList, Summary#summary.first_key, Summary#summary.last_key),
    UpdSummary = Summary#summary{index = SlotIndex},
    leveled_log:log(
        sst03, [Filename, Summary#summary.size, Summary#summary.max_sqn]),
    {UpdState1#state{summary = UpdSummary,
                        handle = Handle,
                        filename = Filename,
                        tomb_count = TombCount,
                        filter_fun = FilterFun},
        Bloom}.

gen_fileversion(PressMethod, IdxModDate, CountOfTombs) ->
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
    Bit3 =
        case CountOfTombs of
            not_counted ->
                0;
            _ ->
                4
        end,
    Bit1 + Bit2 + Bit3.

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
    case VersionInt band 4 of
        0 ->
            UpdState1;
        4 ->
            UpdState1#state{tomb_count = 0}
    end.

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

build_table_summary(SlotIndex, _Level, FirstKey,
                        SlotCount, MaxSQN, Bloom, CountOfTombs) ->
    [{LastKey, _LastV}|_Rest] = SlotIndex,
    Summary = #summary{first_key = FirstKey,
                        last_key = LastKey,
                        size = SlotCount,
                        max_sqn = MaxSQN},
    SummBin0 =
        term_to_binary({Summary, Bloom, lists:reverse(SlotIndex)},
                        ?BINARY_SETTINGS),

    SummBin =
        case CountOfTombs of
            not_counted ->
                SummBin0;
            I ->
                <<I:32/integer, SummBin0/binary>>
        end,

    SummCRC = hmac(SummBin),
    <<SummCRC:32/integer, SummBin/binary>>.

-spec read_table_summary(binary(), not_counted|non_neg_integer()) ->
                            {sst_summary(),
                                leveled_ebloom:bloom(),
                                list(tuple()),
                                not_counted|non_neg_integer()}.
%% @doc
%% Read the table summary - format varies depending on file version (presence
%% of tomb count)
read_table_summary(BinWithCheck, TombCount) ->
    <<SummCRC:32/integer, SummBin/binary>> = BinWithCheck,
    CRCCheck = hmac(SummBin),
    if
        CRCCheck == SummCRC ->
            % If not might it might be possible to rebuild from all the slots
            case TombCount of
                not_counted ->
                    erlang:append_element(binary_to_term(SummBin),
                                            not_counted);
                _ ->
                    <<I:32/integer, SummBin0/binary>> = SummBin,
                    erlang:append_element(binary_to_term(SummBin0), I)
            end
    end.


build_all_slots(SlotList) ->
    SlotCount = length(SlotList),
    {SlotIndex, BlockIndex, SlotsBin, HashLists} =
        build_all_slots(SlotList,
                            9,
                            1,
                            [],
                            [],
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
                    [{SlotID, BlockIdx}|BlockIdxAcc],
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
deserialise_block(Bin, PressMethod) when byte_size(Bin) > 4 ->
    BinS = byte_size(Bin) - 4,
    <<TermBin:BinS/binary, CRC32:32/integer>> = Bin,
    case hmac(TermBin) of
        CRC32 ->
            deserialise_checkedblock(TermBin, PressMethod);
        _ ->
            []
    end;
deserialise_block(_Bin, _PM) ->
    [].

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

from_list(SlotList, FirstKey, LastKey) ->
    FilterFun = get_filterfun(FirstKey, LastKey),
    FilteredList =
        lists:map(fun({K, S}) -> {FilterFun(K), S} end, SlotList),
    {leveled_tree:from_orderedlist(FilteredList, ?TREE_TYPE, ?TREE_SIZE),
        FilterFun}.

-spec get_filterfun(
    leveled_codec:ledger_key(), leveled_codec:ledger_key()) ->
        fun((leveled_codec:ledger_key())
            -> leveled_codec:ledger_key()|leveled_codec:slimmed_key()).
get_filterfun(
        {?IDX_TAG, B, {Field, FT}, FK}, {?IDX_TAG, B, {Field, LT}, LK})
            when is_binary(Field),
            is_binary(FT), is_binary(FK), is_binary(LT), is_binary(LK) ->
    case {binary:longest_common_prefix([FT, LT]), byte_size(FT)} of
        {N, M} when N > 0, M >= N ->
            <<Prefix:N/binary, _Rest/binary>> = FT,
            term_prefix_filter(N, Prefix);
        _ ->
            fun term_filter/1
    end;
get_filterfun(
        {Tag, B, FK, null}, {Tag, B, LK, null})
            when is_binary(FK), is_binary(LK) ->
    case {binary:longest_common_prefix([FK, LK]), byte_size(FK)} of
        {N, M} when N > 0, M >= N ->
            <<Prefix:N/binary, _Rest/binary>> = FK,
            key_prefix_filter(N, Prefix);
        _ ->
            fun key_filter/1

    end;
get_filterfun(_FirstKey, _LastKey) ->
    fun null_filter/1.

-spec null_filter(leveled_codec:ledger_key()) -> leveled_codec:ledger_key().
null_filter(Key) -> Key.

-spec key_filter(leveled_codec:ledger_key()) -> leveled_codec:slimmed_key().
key_filter({_Tag, _Bucket, Key, null}) -> Key.

-spec term_filter(leveled_codec:ledger_key()) -> leveled_codec:slimmed_key().
term_filter({_Tag, _Bucket, {_Field, Term}, Key}) -> {Term, Key}.

-spec key_prefix_filter(
    pos_integer(), binary()) ->
        fun((leveled_codec:ledger_key()) -> leveled_codec:slimmed_key()).
key_prefix_filter(N, Prefix) ->
    fun({_Tag, _Bucket, Key, null}) ->
        case Key of
            <<Prefix:N/binary, Suffix/binary>> ->
                Suffix;
            _ ->
                null
        end
    end.

-spec term_prefix_filter(
    pos_integer(), binary()) ->
        fun((leveled_codec:ledger_key()) -> leveled_codec:slimmed_key()).
term_prefix_filter(N, Prefix) ->
    fun({_Tag, _Bucket, {_Field, Term}, Key}) ->
        case Term of
            <<Prefix:N/binary, Suffix/binary>> ->
                {Suffix, Key};
            _ ->
                null
        end
    end.

lookup_slot(Key, Tree, FilterFun) ->
    StartKeyFun =
        fun(_V) ->
            all
        end,
    % The penciller should never ask for presence out of range - so will
    % always return a slot (as we don't compare to StartKey)
    {_LK, Slot} = leveled_tree:search(FilterFun(Key), Tree, StartKeyFun),
    Slot.

lookup_slots(StartKey, EndKey, Tree, FilterFun) ->
    StartKeyFun =
        fun(_V) ->
            all
        end,
    MapFun =
        fun({_LK, Slot}) ->
            Slot
        end,
    FilteredStartKey =
        case StartKey of
            all -> all;
            _ -> FilterFun(StartKey)
        end,
    FilteredEndKey =
        case EndKey of
            all -> all;
            _ -> FilterFun(EndKey)
        end,
    SlotList =
        leveled_tree:search_range(
            FilteredStartKey,
            FilteredEndKey,
            Tree,
            StartKeyFun),
    {EK, _EndSlot} = lists:last(SlotList),
    {lists:map(MapFun, SlotList),
        leveled_codec:endkey_passed(FilteredEndKey, EK)}.


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
                        [H1|HashAcc],
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
                                {binary_slot(),
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
                        %% if false the acc is a list, and if true
                        %% Acc will be initially not_present, and may
                        %% result in a {K, V} tuple
                    press_method(),
                    boolean(),
                    list()|not_present) ->
                        list(leveled_codec:ledger_kv())|
                            not_present|leveled_codec:ledger_kv().
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
    Result = spawn_check_block(BlockPos, BlockBin, PressMethod),
    case {Result, LedgerKeyToCheck} of
        {{K, V}, K} ->
            {K, V};
        {{K, V}, false} ->
            check_blocks(Rest, BlockPointer,
                            BlockLengths, PosBinLength,
                            LedgerKeyToCheck, PressMethod, IdxModDate,
                            [{K, V}|Acc]);
        _ ->
            check_blocks(Rest, BlockPointer,
                            BlockLengths, PosBinLength,
                            LedgerKeyToCheck, PressMethod, IdxModDate,
                            Acc)
    end.

-spec spawn_check_block(non_neg_integer(), binary(), press_method())
        -> not_present|leveled_codec:ledger_kv().
spawn_check_block(BlockPos, BlockBin, PressMethod) ->
    Parent = self(),
    Pid =
        spawn_link(
            fun() -> check_block(Parent, BlockPos, BlockBin, PressMethod) end
        ),
    receive {checked_block, Pid, R} -> R end.

check_block(From, BlockPos, BlockBin, PressMethod) ->
    R = fetchfrom_rawblock(BlockPos, deserialise_block(BlockBin, PressMethod)),
    From ! {checked_block, self(), R}.

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


-type slotbin_fun() ::
    fun(({non_neg_integer(), non_neg_integer(), non_neg_integer(),
        range_endpoint(), range_endpoint()}) ->
            {binary(), non_neg_integer(), range_endpoint(), range_endpoint()}).

-spec binarysplit_mapfun(binary(), integer()) -> slotbin_fun().
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
                    {false|list(), non_neg_integer(), blockindex_cache()},
                    press_method(), boolean()) ->
                        {boolean(), list(binaryslot_element())}.
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
    {false, read_slotlist(SlotList, Handle)};
read_slots(Handle, SlotList, {SegList, LowLastMod, BlockIndexCache},
                PressMethod, IdxModDate) ->
    % List of segments passed so only {K, V} pairs matching those segments
    % should be returned.  This required the {K, V} pair to have been added
    % with the appropriate hash - if the pair were added with no_lookup as
    % the hash value this will fail unexpectedly.
    BinMapFun =
        fun(Pointer, {NeededBlockIdx, Acc}) ->
            {SP, _L, ID, SK, EK} = pointer_mapfun(Pointer),
            CachedHeader = array:get(ID - 1, element(2, BlockIndexCache)),
            case extract_header(CachedHeader, IdxModDate) of
                none ->
                    % If there is an attempt to use the seg list query and the
                    % index block cache isn't cached for any part this may be
                    % slower as each slot will be read in turn
                    {true, Acc ++ read_slotlist([Pointer], Handle)};
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
                            {NeededBlockIdx, Acc};
                        false ->
                            case SegList of
                                false ->
                                    % Need all the slot now
                                    {NeededBlockIdx,
                                        Acc ++
                                            read_slotlist([Pointer], Handle)};
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
                                    {NeededBlockIdx,
                                        Acc ++ lists:filter(FilterFun, KVL)}
                            end
                    end
            end
        end,
    lists:foldl(BinMapFun, {false, []}, SlotList).


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
                                {binary(), non_neg_integer(), binary()}|none.
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
                    case fetchends_rawblock(BlockList) of
                        {_, LastKey} when StartKey > LastKey ->
                            %% This includes the case when LastKey is
                            %% not_present due to corruption in the BlockList
                            %% as tuple is > not_present.
                            {Acc, true};
                        {_, LastKey} ->
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
                            end
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
                blocks_required({StartKey, EndKey},
                                [Block1, Block2, MidBlock, Block4, Block5],
                                PressMethod),
            {Acc, _Continue} =
                lists:foldl(BlockCheckFun, {[], true}, BlocksToCheck),
            {Acc, none};
        {{Header, _Blocks}, SegList} ->
            {BlockLengths, _LMD, BlockIdx} =
                extract_header(Header, IdxModDate),
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


blocks_required({StartKey, EndKey}, [B1, B2, MidBlock, B4, B5], PressMethod) ->
    MidBlockList = deserialise_block(MidBlock, PressMethod),
    filter_blocks_required(fetchends_rawblock(MidBlockList),
                            {StartKey, EndKey},
                            [B1, B2, MidBlockList, B4, B5]).

filter_blocks_required({not_present, not_present}, _RangeKeys, AllBlocks) ->
    AllBlocks;
filter_blocks_required({_MidFirst, MidLast}, {StartKey, _EndKey},
                [_Block1, _Block2, _MidBlockList, Block4, Block5])
                when StartKey > MidLast ->
    [Block4, Block5];
filter_blocks_required({MidFirst, MidLast}, {StartKey, EndKey},
                [_Block1, _Block2, MidBlockList, Block4, Block5])
                when StartKey >= MidFirst ->
    NoneAfter = leveled_codec:endkey_passed(EndKey, MidLast),
    case NoneAfter of
        true ->
            [MidBlockList];
        false ->
            [MidBlockList, Block4, Block5]
    end;
filter_blocks_required({MidFirst, MidLast}, {_StartKey, EndKey},
                [Block1, Block2, MidBlockList, Block4, Block5]) ->
    AllBefore = leveled_codec:endkey_passed(EndKey, MidFirst),
    NoneAfter = leveled_codec:endkey_passed(EndKey, MidLast),
    case {AllBefore, NoneAfter} of
        {true, true} ->
            [Block1, Block2];
        {false, true} ->
            [Block1, Block2, MidBlockList];
        {false, false} ->
            [Block1, Block2, MidBlockList, Block4, Block5]
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
            leveled_log:log(sst09, []),
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
    R = fetchfrom_rawblock(BlockPos, deserialise_block(Block, PressMethod)),
    case R of 
        {K, V} when K == Key ->
            {K, V};
        _ ->
            fetch_value(Rest, BlockLengths, Blocks, Key, PressMethod)
    end.

-spec fetchfrom_rawblock(pos_integer(), list(leveled_codec:ledger_kv())) ->
                                        not_present|leveled_codec:ledger_kv().
%% @doc
%% Fetch from a deserialised block, but accounting for potential corruption
%% in that block which may lead to it returning as an empty list if that
%% corruption is detected by the deserialising function
fetchfrom_rawblock(BlockPos, RawBlock) when BlockPos > length(RawBlock) ->
    %% Capture the slightly more general case than this being an empty list
    %% in case of some other unexpected misalignement that would otherwise
    %% crash the leveled_sst file process
    not_present;
fetchfrom_rawblock(BlockPos, RawBlock) ->
    lists:nth(BlockPos, RawBlock).

-spec fetchends_rawblock(list(leveled_codec:ledger_kv())) ->
                    {not_present, not_present}|
                    {leveled_codec:ledger_key(), leveled_codec:ledger_key()}.
%% @doc
%% Fetch the first and last key from a block, and not_present if the block
%% is empty (rather than crashing)
fetchends_rawblock([]) ->
    {not_present, not_present};
fetchends_rawblock(RawBlock) ->
    {element(1, lists:nth(1, RawBlock)),
        element(1, lists:last(RawBlock))}.


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
%% The outcome of merge_lists/3 and merge_lists/6 should be an list of slots.
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
                    -> {list(), list(), list(binary_slot()),
                        tuple()|null, non_neg_integer()|not_counted}.
%% @doc
%%
%% Merge from a single list (i.e. at Level 0)
merge_lists(KVList1, SSTOpts, IdxModDate) ->
    SlotCount = length(KVList1) div ?LOOK_SLOTSIZE,
    {[],
        [],
        split_lists(KVList1, [],
                    SlotCount, SSTOpts#sst_options.press_method, IdxModDate),
        element(1, lists:nth(1, KVList1)),
        not_counted}.


split_lists([], SlotLists, 0, _PressMethod, _IdxModDate) ->
    lists:reverse(SlotLists);
split_lists(LastPuff, SlotLists, 0, PressMethod, IdxModDate) ->
    {SlotD, _} =
        generate_binary_slot(
            lookup, LastPuff, PressMethod, IdxModDate, no_timing),
    lists:reverse([SlotD|SlotLists]);
split_lists(KVList1, SlotLists, N, PressMethod, IdxModDate) ->
    {Slot, KVListRem} = lists:split(?LOOK_SLOTSIZE, KVList1),
    {SlotD, _} =
        generate_binary_slot(lookup, Slot, PressMethod, IdxModDate, no_timing),
    split_lists(KVListRem, [SlotD|SlotLists], N - 1, PressMethod, IdxModDate).


-spec merge_lists(
    list(expanded_pointer()),
    list(expanded_pointer()),
    {boolean(), non_neg_integer()},
    sst_options(), boolean(), boolean()) ->
            {list(expanded_pointer()),
            list(expanded_pointer()),
                list(binary_slot()),
                leveled_codec:ledger_key()|null,
                non_neg_integer()}.
%% @doc
%% Merge lists when merging across more than one file.  KVLists that are
%% provided may include pointers to fetch more Keys/Values from the source
%% file
merge_lists(
        KVList1, KVList2, LevelInfo, SSTOpts, IndexModDate, SaveTombCount) ->
    InitTombCount =
        case SaveTombCount of true -> 0; false -> not_counted end,
    merge_lists(KVList1, KVList2,
                LevelInfo,
                [], null, 0,
                SSTOpts#sst_options.max_sstslots,
                SSTOpts#sst_options.press_method,
                IndexModDate,
                InitTombCount,
                #build_timings{}).


-spec merge_lists(
    list(expanded_pointer()),
    list(expanded_pointer()),
    {boolean(), non_neg_integer()},
    list(binary_slot()),
    leveled_codec:ledger_key()|null,
    non_neg_integer(),
    non_neg_integer(),
    press_method(),
    boolean(),
    non_neg_integer()|not_counted,
    build_timings()) ->
        {list(expanded_pointer()), list(expanded_pointer()),
            list(binary_slot()), leveled_codec:ledger_key()|null,
            non_neg_integer()|not_counted}.

merge_lists(KVL1, KVL2, LI, SlotList, FirstKey, MaxSlots, MaxSlots,
                                _PressMethod, _IdxModDate, CountOfTombs, T0) ->
    % This SST file is full, move to complete file, and return the
    % remainder
    log_buildtimings(T0, LI),
    {KVL1, KVL2, lists:reverse(SlotList), FirstKey, CountOfTombs};
merge_lists([], [], LI, SlotList, FirstKey, _SlotCount, _MaxSlots,
                                _PressMethod, _IdxModDate, CountOfTombs, T0) ->
    % the source files are empty, complete the file
    log_buildtimings(T0, LI),
    {[], [], lists:reverse(SlotList), FirstKey, CountOfTombs};
merge_lists(KVL1, KVL2, LI, SlotList, FirstKey, SlotCount, MaxSlots,
                                PressMethod, IdxModDate, CountOfTombs, T0) ->
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
                        CountOfTombs,
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
                        count_tombs(KVL, CountOfTombs),
                        T2)
    end.


-spec count_tombs(
    list(leveled_codec:ledger_kv()), non_neg_integer()|not_counted) ->
        non_neg_integer()|not_counted.
%% @doc
%% Count the tombstones in a list of KVs
count_tombs(_KVL, not_counted) ->
    not_counted;
count_tombs(KVL, InitCount) ->
    FoldFun =
        fun(KV, Count) ->
            case leveled_codec:strip_to_statusonly(KV) of
                tomb ->
                    Count + 1;
                _ ->
                    Count
            end
        end,
    lists:foldl(FoldFun, InitCount, KVL).

-spec form_slot(list(expanded_pointer()),
                    list(expanded_pointer()),
                    {boolean(), non_neg_integer()},
                    lookup|no_lookup,
                    non_neg_integer(),
                    list(leveled_codec:ledger_kv()),
                    leveled_codec:ledger_key()|null) ->
                {list(expanded_pointer()), list(expanded_pointer()),
                    {lookup|no_lookup, list(leveled_codec:ledger_kv())},
                    leveled_codec:ledger_key()}.
%% @doc
%% Merge together Key Value lists to provide an ordered slot of KVs
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

-spec update_buildtimings(
    erlang:timestamp(), build_timings(), atom()) -> build_timings().
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
    leveled_log:log(
        sst13,
        [Timings#build_timings.fold_toslot,
            Timings#build_timings.slot_hashlist,
            Timings#build_timings.slot_serialise,
            Timings#build_timings.slot_finish,
            element(1, LI), element(2, LI)]).

-spec maybelog_fetch_timing(
        leveled_monitor:monitor(),
        leveled_pmanifest:lsm_level(),
        leveled_monitor:sst_fetch_type(),
        erlang:timestamp()|no_timing) -> ok.
maybelog_fetch_timing(_Monitor, _Level, _Type, no_timing) ->
    ok;
maybelog_fetch_timing({Pid, _SlotFreq}, Level, Type, SW) ->
    {TS1, _} = leveled_monitor:step_time(SW),
    leveled_monitor:add_stat(Pid, {sst_fetch_update, Level, Type, TS1}).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_AREA, "test/test_area/").

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

-spec sst_getslots(pid(), list(slot_pointer()))
                                        -> list(leveled_codec:ledger_kv()).
%% @doc
%% Get a list of slots by their ID. The slot will be converted from the binary
%% to term form outside of the FSM loop, this is to stop the copying of the 
%% converted term to the calling process.
sst_getslots(Pid, SlotList) ->
    sst_getfilteredslots(Pid, SlotList, false, 0).

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
    sst_newmerge(RootPath, Filename, KVL1, KVL2, IsBasement, Level, MaxSQN,
                    OptsSST, false, false).

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

tombcount_test() ->
    tombcount_tester(1),
    tombcount_tester(2),
    tombcount_tester(3),
    tombcount_tester(4).

tombcount_tester(Level) ->
    N = 1600,
    KL1 = generate_randomkeys(N div 2 + 1, N, 1, 4),
    KL2 = generate_indexkeys(N div 2),
    FlipToTombFun =
        fun({K, V}) ->
            case leveled_rand:uniform(10) of
                X when X > 5 ->
                    {K, setelement(2, V, tomb)};
                _ ->
                    {K, V}
            end
        end,
    KVL1 = lists:map(FlipToTombFun, KL1),
    KVL2 = lists:map(FlipToTombFun, KL2),
    CountTombFun =
        fun({_K, V}, Acc) ->
            case element(2, V) of
                tomb ->
                    Acc + 1;
                _ ->
                    Acc
            end
        end,
    ExpectedCount = lists:foldl(CountTombFun, 0, KVL1 ++ KVL2),

    {RP, Filename} = {?TEST_AREA, "tombcount_test"},
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, SST1, _KD, _BB} = sst_newmerge(RP, Filename,
                                KVL1, KVL2, false, Level,
                                N, OptsSST, false, false),
    ?assertMatch(not_counted, sst_gettombcount(SST1)),
    ok = sst_close(SST1),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")),

    {ok, SST2, _KD1, _BB1} = sst_newmerge(RP, Filename,
                                KVL1, KVL2, false, Level,
                                N, OptsSST, false, true),

    ?assertMatch(ExpectedCount, sst_gettombcount(SST2)),
    ok = sst_close(SST2),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).


form_slot_test() ->
    % If a skip key happens, mustn't switch to loookup by accident as could be
    % over the expected size
    SkippingKV =
        {{o, "B1", "K9999", null}, {9999, tomb, {1234568, 1234567}, {}}},
    Slot =
        [{{o, "B1", "K5", null},
            {5, {active, infinity}, {99234568, 99234567}, {}}}],
    R1 = form_slot([SkippingKV], [],
                    {true, 99999999},
                    no_lookup,
                    ?LOOK_SLOTSIZE + 1,
                    Slot,
                    {o, "B1", "K5", null}),
    ?assertMatch({[], [], {no_lookup, Slot}, {o, "B1", "K5", null}}, R1).

merge_tombstonelist_test() ->
    % Merge lists with nothing but tombstones, and file at basement level
    SkippingKV1 =
        {{o, "B1", "K9995", null}, {9995, tomb, {1234568, 1234567}, {}}},
    SkippingKV2 =
        {{o, "B1", "K9996", null}, {9996, tomb, {1234568, 1234567}, {}}},
    SkippingKV3 =
        {{o, "B1", "K9997", null}, {9997, tomb, {1234568, 1234567}, {}}},
    SkippingKV4 =
        {{o, "B1", "K9998", null}, {9998, tomb, {1234568, 1234567}, {}}},
    SkippingKV5 =
        {{o, "B1", "K9999", null}, {9999, tomb, {1234568, 1234567}, {}}},
    R = merge_lists([SkippingKV1, SkippingKV3, SkippingKV5],
                        [SkippingKV2, SkippingKV4],
                        {true, 9999999},
                        #sst_options{press_method = native,
                                        max_sstslots = 256},
                        ?INDEX_MODDATE,
                        true),

    ?assertMatch({[], [], [], null, 0}, R).

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
    {{HeaderT, FullBinT, HL, LK}, no_timing} =
        generate_binary_slot(lookup, Keys, native, true, no_timing),
    {{HeaderF, FullBinF, HL, LK}, no_timing} =
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


findposfrag_test() ->
    ?assertMatch([], find_pos(<<128:8/integer>>, 1, [], 0)).

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

doublesize_test_() ->
    {timeout, 300, fun doublesize_tester/0}.

doublesize_tester() ->
    io:format(user, "~nPreparing key lists for test~n", []),
    Contents = lists:ukeysort(1, generate_randomkeys(1, 65000, 1, 6)),
    SplitFun =
        fun({K, V}, {L1, L2}) ->
            case length(L1) > length(L2) of
                true ->
                    {L1, [{K, V}|L2]};
                _ ->
                    {[{K, V}|L1], L2}
            end
        end,
    {KVL1, KVL2} = lists:foldr(SplitFun, {[], []}, Contents),

    io:format(user, "Running tests over different sizes:~n", []),

    size_tester(lists:sublist(KVL1, 4000), lists:sublist(KVL2, 4000), 8000),
    size_tester(lists:sublist(KVL1, 16000), lists:sublist(KVL2, 16000), 32000),
    size_tester(lists:sublist(KVL1, 24000), lists:sublist(KVL2, 24000), 48000),
    size_tester(lists:sublist(KVL1, 32000), lists:sublist(KVL2, 32000), 64000).

size_tester(KVL1, KVL2, N) ->
    io:format(user, "~nStarting ... test with ~w keys ~n", [N]),

    {RP, Filename} = {?TEST_AREA, "doublesize_test"},
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, SST1, _KD, _BB} = sst_newmerge(RP, Filename,
                                KVL1, KVL2, false, ?DOUBLESIZE_LEVEL,
                                N, OptsSST, false, false),
    ok = sst_close(SST1),
    {ok, SST2, _SKEK, Bloom} =
        sst_open(RP, Filename ++ ".sst", OptsSST, ?DOUBLESIZE_LEVEL),
    FetchFun =
        fun({K, V}) ->
            {K0, V0} = sst_get(SST2, K),
            ?assertMatch(K, K0),
            ?assertMatch(V, V0)
        end,
    lists:foreach(FetchFun, KVL1 ++ KVL2),

    CheckBloomFun =
        fun({K, _V}) ->
            leveled_ebloom:check_hash(leveled_codec:segment_hash(K), Bloom)
        end,
    KBIn = length(lists:filter(CheckBloomFun, KVL1 ++ KVL2)),
    KBOut =
        length(lists:filter(CheckBloomFun,
            generate_randomkeys(1, 1000, 7, 9))),

    ?assertMatch(N, KBIn),

    io:format(user, "~w false positives in 1000~n", [KBOut]),

    ok = sst_close(SST2),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).


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
    % summary index
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
    io:format("Gap test between ~p and ~p", [GapSKV, GapEKV]),
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

simple_switchcache_test_() ->
    {timeout, 60, fun simple_switchcache_tester/0}.

simple_switchcache_tester() ->
    {RP, Filename} = {?TEST_AREA, "simple_switchcache_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 2, 1, 20),
    KVList1 = lists:sublist(lists:ukeysort(1, KVList0), ?LOOK_SLOTSIZE),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, OpenP4, {FirstKey, LastKey}, _Bloom1} =
        testsst_new(RP, Filename, 4, KVList1, length(KVList1), native),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP4, K))
                        end,
                    KVList1),
    ok = sst_switchlevels(OpenP4, 5),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP4, K))
                        end,
                    KVList1),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP4, K))
                        end,
                    KVList1),
    timer:sleep(?HIBERNATE_TIMEOUT + 10),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP4, K))
                        end,
                    KVList1),
    ok = sst_close(OpenP4),
    OptsSST = #sst_options{press_method=native,
                            log_options=leveled_log:get_opts()},
    {ok, OpenP5, {FirstKey, LastKey}, _Bloom2} =
        sst_open(RP, Filename ++ ".sst", OptsSST, 5),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP5, K))
                        end,
                    KVList1),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP5, K))
                        end,
                    KVList1),
    ok = sst_switchlevels(OpenP5, 6),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP5, K))
                        end,
                    KVList1),
    ok = sst_switchlevels(OpenP5, 7),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP5, K))
                        end,
                    KVList1),
    timer:sleep(?HIBERNATE_TIMEOUT + 10),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sst_get(OpenP5, K))
                        end,
                    KVList1),
    ok = sst_close(OpenP5),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).


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

reader_hibernate_test_() ->
    {timeout, 90, fun reader_hibernate_tester/0}.

reader_hibernate_tester() ->
    {RP, Filename} = {?TEST_AREA, "readerhibernate_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 32, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} =
        testsst_new(RP, Filename, 1, KVList1, length(KVList1), native),
    ?assertMatch({FirstKey, FV}, sst_get(Pid, FirstKey)),
    SQN = leveled_codec:strip_to_seqonly({FirstKey, FV}),
    ?assertMatch(
        SQN,
        sst_getsqn(Pid, FirstKey, leveled_codec:segment_hash(FirstKey))),
    timer:sleep(?HIBERNATE_TIMEOUT + 1000),
    ?assertMatch({FirstKey, FV}, sst_get(Pid, FirstKey)).

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

fetch_status_test() ->
    {RP, Filename} = {?TEST_AREA, "fetchstatus_test"},
    KVList0 = generate_randomkeys(1, ?LOOK_SLOTSIZE * 4, 1, 20),
    KVList1 = lists:ukeysort(1, KVList0),
    [{FirstKey, _FV}|_Rest] = KVList1,
    {LastKey, _LV} = lists:last(KVList1),
    {ok, Pid, {FirstKey, LastKey}, _Bloom} =
        testsst_new(RP, Filename, 1, KVList1, length(KVList1), native),
    {status, Pid, {module, gen_statem}, SItemL} = sys:get_status(Pid),
    S = lists:keyfind(state, 1, lists:nth(5, SItemL)),
    true = is_integer(array:size(S#state.fetch_cache)),
    true = is_integer(array:size(element(2, S#state.blockindex_cache))),
    ST = format_status(terminate, [dict:new(), starting, S]),
    ?assertMatch(redacted, ST#state.blockindex_cache),
    ?assertMatch(redacted, ST#state.fetch_cache),
    ok = sst_close(Pid),
    ok = file:delete(filename:join(RP, Filename ++ ".sst")).

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
    {ok, Pid, {FirstKey, LastKey}, Bloom} =
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
    true = [] == MapFun({FirstKey, "V"}, []), % coverage cheat within MapFun
    KVList3 = lists:foldl(MapFun, [], KVList2),
    SW2 = os:timestamp(),
    lists:foreach(fun({K, H, _V}) ->
                        ?assertMatch(not_present, sst_get(Pid, K, H))
                        end,
                    KVList3),
    io:format(user,
                "Checking for ~w missing keys took ~w microseconds~n",
                [length(KVList3), timer:now_diff(os:timestamp(), SW2)]),
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

    {Eight000Key, V800} = lists:nth(800, KVList1),
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
    ?assertMatch([{Eight000Key, V800}], FetchedListB4),

    B1 = check_binary_references(Pid),

    ok = sst_close(Pid),

    io:format(user, "Reopen SST file~n", []),
    OptsSST = #sst_options{press_method=native,
                            log_options=leveled_log:get_opts()},
    {ok, OpenP, {FirstKey, LastKey}, Bloom} =
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
    ?assertMatch(
        {ok, reader, #state{}},
        code_change(nonsense, reader, #state{}, nonsense)),
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

stopstart_test() ->
    {ok, Pid} = gen_statem:start_link(?MODULE, [], ?START_OPTS),
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

corrupted_block_range_test() ->
    corrupted_block_rangetester(native, 100),
    corrupted_block_rangetester(lz4, 100),
    corrupted_block_rangetester(none, 100).

corrupted_block_rangetester(PressMethod, TestCount) ->
    N = 100,
    KVL1 = lists:ukeysort(1, generate_randomkeys(1, N, 1, 2)),
    RandomRangesFun =
        fun(_X) ->
            SKint = leveled_rand:uniform(90) + 1,
            EKint = min(N, leveled_rand:uniform(N - SKint)),
            SK = element(1, lists:nth(SKint, KVL1)),
            EK = element(1, lists:nth(EKint, KVL1)),
            {SK, EK}
        end,
    RandomRanges = lists:map(RandomRangesFun, lists:seq(1, TestCount)),
    B1 = serialise_block(lists:sublist(KVL1, 1, 20), PressMethod),
    B2 = serialise_block(lists:sublist(KVL1, 21, 20), PressMethod),
    MidBlock = serialise_block(lists:sublist(KVL1, 41, 20), PressMethod),
    B4 = serialise_block(lists:sublist(KVL1, 61, 20), PressMethod),
    B5 = serialise_block(lists:sublist(KVL1, 81, 20), PressMethod),
    CorruptBlockFun =
        fun(Block) ->
            case leveled_rand:uniform(10) < 2 of
                true ->
                    flip_byte(Block, 0 , byte_size(Block));
                false ->
                    Block
            end
        end,

    CheckFun =
        fun({SK, EK}) ->
            InputBlocks =
                lists:map(CorruptBlockFun, [B1, B2, MidBlock, B4, B5]),
            BR = blocks_required({SK, EK}, InputBlocks, PressMethod),
            ?assertMatch(true, length(BR) =< 5),
            BlockListFun =
                fun(B) ->
                    case is_binary(B) of
                        true ->
                            deserialise_block(B, PressMethod);
                        false ->
                            B
                    end
                end,
            BRL = lists:flatten(lists:map(BlockListFun, BR)),
            lists:foreach(fun({_K, _V}) -> ok end, BRL)
    end,
    lists:foreach(CheckFun, RandomRanges).

corrupted_block_fetch_test() ->
    corrupted_block_fetch_tester(native),
    corrupted_block_fetch_tester(lz4),
    corrupted_block_fetch_tester(none).

corrupted_block_fetch_tester(PressMethod) ->
    KC = 120,
    KVL1 = lists:ukeysort(1, generate_randomkeys(1, KC, 1, 2)),

    {{Header, SlotBin, _HashL, _LastKey}, _BT} =
        generate_binary_slot(lookup, KVL1, PressMethod, false, no_timing),
    <<B1L:32/integer,
        B2L:32/integer,
        B3L:32/integer,
        B4L:32/integer,
        B5L:32/integer,
        PosBinIndex/binary>> = Header,
    HS = byte_size(Header),

    <<CheckB1P:32/integer, B1P:32/integer,
        CheckH:32/integer, Header:HS/binary,
        B1:B1L/binary, B2:B2L/binary, B3:B3L/binary,
            B4:B4L/binary, B5:B5L/binary>> = SlotBin,

    CorruptB3 = flip_byte(B3, 0 , B3L),
    CorruptSlotBin =
        <<CheckB1P:32/integer, B1P:32/integer,
            CheckH:32/integer, Header/binary,
            B1/binary, B2/binary, CorruptB3/binary, B4/binary, B5/binary>>,

    CheckFun =
        fun(N, {AccHit, AccMiss}) ->
            PosL = [min(0, leveled_rand:uniform(N - 2)), N - 1],
            {LK, LV} = lists:nth(N, KVL1),
            {BlockLengths, 0, PosBinIndex} =
                extract_header(Header, false),
            R = check_blocks(PosL,
                                CorruptSlotBin,
                                BlockLengths,
                                byte_size(PosBinIndex),
                                LK,
                                PressMethod,
                                false,
                                not_present),
            case R of
                not_present ->
                    {AccHit, AccMiss + 1};
                {LK, LV} ->
                    {AccHit + 1, AccMiss}
            end
        end,
    {_HitCount, MissCount} =
        lists:foldl(CheckFun, {0, 0}, lists:seq(16, length(KVL1))),
    ExpectedMisses = element(2, ?LOOK_BLOCKSIZE),
    ?assertMatch(ExpectedMisses, MissCount).

block_index_cache_test() ->
    {Mega, Sec, _} = os:timestamp(),
    Now = Mega * 1000000 + Sec,
    EntriesTS =
        lists:map(fun(I) ->
                        TS = Now - I + 1,
                        {I, <<0:160/integer, TS:32/integer, 0:32/integer>>}
                    end,
                    lists:seq(1, 8)),
    EntriesNoTS =
        lists:map(fun(I) ->
                        {I, <<0:160/integer, 0:32/integer>>}
                    end,
                    lists:seq(1, 8)),
    HeaderTS = <<0:160/integer, Now:32/integer, 0:32/integer>>,
    HeaderNoTS = <<0:192>>,
    BIC = new_blockindex_cache(8),
    {_, BIC0, undefined} =
        update_blockindex_cache(false, EntriesNoTS, BIC, undefined, false),
    {_, BIC1, undefined} =
        update_blockindex_cache(false, EntriesTS, BIC, undefined, true),
    {_, BIC2, undefined} =
        update_blockindex_cache(true, EntriesNoTS, BIC, undefined, false),
    {ETSP1, ETSP2} = lists:split(6, EntriesTS),
    {_, BIC3, undefined} =
        update_blockindex_cache(true, ETSP1, BIC, undefined, true),
    {_, BIC3, undefined} =
        update_blockindex_cache(true, ETSP1, BIC3, undefined, true),
    {_, BIC4, LMD4} =
        update_blockindex_cache(true, ETSP2, BIC3, undefined, true),
    {_, BIC4, LMD4} =
        update_blockindex_cache(true, ETSP2, BIC4, LMD4, true),

    ?assertMatch(none, array:get(0, element(2, BIC0))),
    ?assertMatch(none, array:get(0, element(2, BIC1))),
    ?assertMatch(HeaderNoTS, array:get(0, element(2, BIC2))),
    ?assertMatch(HeaderTS, array:get(0, element(2, BIC3))),
    ?assertMatch(HeaderTS, array:get(0, element(2, BIC4))),
    ?assertMatch(Now, LMD4).

key_matchesprefix_test() ->
    FileName = "keymatchesprefix_test",
    IndexKeyFun =
        fun(I) ->
            {{?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>,
                    list_to_binary("19601301|"
                        ++ io_lib:format("~6..0w", [I]))},
                list_to_binary(io_lib:format("~6..0w", [I]))},
            {1, {active, infinity}, no_lookup, null}}
        end,
    IndexEntries = lists:map(IndexKeyFun, lists:seq(1, 500)),
    OddIdxKey =
        {{?IDX_TAG,
            {<<"btype">>, <<"bucket">>},
            {<<"dob_bin">>, <<"19601301">>},
            list_to_binary(io_lib:format("~6..0w", [0]))},
        {1, {active, infinity}, no_lookup, null}},
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, P1, {_FK1, _LK1}, _Bloom1} =
        sst_new(
            ?TEST_AREA, FileName, 1, [OddIdxKey|IndexEntries], 6000, OptsSST),
    IdxRange2 =
        sst_getkvrange(
            P1,
            {?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1960">>}, null},
            {?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRange4 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000251">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRangeX =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRangeY =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRangeZ =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000500">>}, null},
            16),
    ?assertMatch(501, length(IdxRange2)),
    ?assertMatch(250, length(IdxRange4)),
    ?assertMatch(501, length(IdxRangeX)),
    ?assertMatch(500, length(IdxRangeY)),
    ?assertMatch(500, length(IdxRangeZ)),
    ok = sst_close(P1),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    ObjectKeyFun =
        fun(I) ->
            {{?RIAK_TAG,
                {<<"btype">>, <<"bucket">>},
                list_to_binary("19601301|"
                    ++ io_lib:format("~6..0w", [I])),
                null},
            {1, {active, infinity}, {0, 0}, null}}
        end,
    ObjectEntries = lists:map(ObjectKeyFun, lists:seq(1, 500)),
    OddObjKey =
        {{?RIAK_TAG,
            {<<"btype">>, <<"bucket">>},
            <<"19601301">>,
            null},
        {1, {active, infinity}, {100, 100}, null}},
    OptsSST =
        #sst_options{press_method=native, log_options=leveled_log:get_opts()},
    {ok, P2, {_FK2, _LK2}, _Bloom2} =
        sst_new(
            ?TEST_AREA, FileName, 1, [OddObjKey|ObjectEntries], 6000, OptsSST),
    ObjRange2 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG,
                {<<"btype">>, <<"bucket">>},
                <<"1960">>, null},
            {?RIAK_TAG,
                {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    ObjRange4 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000251">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    ObjRangeX =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    ObjRangeY =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    ?assertMatch(501, length(ObjRange2)),
    ?assertMatch(250, length(ObjRange4)),
    ?assertMatch(501, length(ObjRangeX)),
    ?assertMatch(500, length(ObjRangeY)),
    ok = sst_close(P2),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")).


range_key_indextermmatch_test() ->
    FileName = "indextermmatch_test",
    IndexKeyFun =
        fun(I) ->
            {{?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>,
                    <<"19601301">>},
                list_to_binary(io_lib:format("~6..0w", [I]))},
            {1, {active, infinity}, no_lookup, null}}
        end,
    IndexEntries = lists:map(IndexKeyFun, lists:seq(1, 500)),
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, P1, {_FK1, _LK1}, _Bloom1} =
        sst_new(?TEST_AREA, FileName, 1, IndexEntries, 6000, OptsSST),

    IdxRange1 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>}, {<<"dob_bin">>, <<"1959">>}, null},
            all,
            16),
    IdxRange2 =
        sst_getkvrange(
            P1,
            {?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1960">>}, null},
            {?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRange3 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, <<"000000">>},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            16),
    IdxRange4 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, <<"000100">>},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            16),
    IdxRange5 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, <<"000100">>},
            16),
    IdxRange6 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, <<"000300">>},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            16),
    IdxRange7 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, <<"000300">>},
            16),
    IdxRange8 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601302">>}, <<"000300">>},
            16),
    IdxRange9 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601300">>}, <<"000100">>},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301">>}, null},
            16),
    ?assertMatch(500, length(IdxRange1)),
    ?assertMatch(500, length(IdxRange2)),
    ?assertMatch(500, length(IdxRange3)),
    ?assertMatch(401, length(IdxRange4)),
    ?assertMatch(100, length(IdxRange5)),
    ?assertMatch(201, length(IdxRange6)),
    ?assertMatch(300, length(IdxRange7)),
    ?assertMatch(500, length(IdxRange8)),
    ?assertMatch(500, length(IdxRange9)),
    ok = sst_close(P1),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")).


range_key_lestthanprefix_test() ->
    FileName = "lessthanprefix_test",
    IndexKeyFun =
        fun(I) ->
            {{?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>,
                    list_to_binary("19601301|"
                        ++ io_lib:format("~6..0w", [I]))},
                list_to_binary(io_lib:format("~6..0w", [I]))},
            {1, {active, infinity}, no_lookup, null}}
        end,
    IndexEntries = lists:map(IndexKeyFun, lists:seq(1, 500)),
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, P1, {_FK1, _LK1}, _Bloom1} =
        sst_new(?TEST_AREA, FileName, 1, IndexEntries, 6000, OptsSST),

    IndexFileStateSize = size_summary(P1),

    IdxRange1 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>}, {<<"dob_bin">>, <<"1959">>}, null},
            all,
            16),
    IdxRange2 =
        sst_getkvrange(
            P1,
            {?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1960">>}, null},
            {?IDX_TAG,
                {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRange3 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1960">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000250">>}, null},
            16),
    IdxRange4 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000251">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRange5 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000250">>}, <<"000251">>},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"1961">>}, null},
            16),
    IdxRange6 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|0002">>}, null},
            16),
    IdxRange7 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|0001">>}, null},
            16),
    IdxRange8 =
        sst_getkvrange(
            P1,
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000000">>}, null},
            {?IDX_TAG, {<<"btype">>, <<"bucket">>},
                {<<"dob_bin">>, <<"19601301|000100">>}, null},
            16),
    ?assertMatch(500, length(IdxRange1)),
    ?assertMatch(500, length(IdxRange2)),
    ?assertMatch(250, length(IdxRange3)),
    ?assertMatch(250, length(IdxRange4)),
    ?assertMatch(250, length(IdxRange5)),
    ?assertMatch(199, length(IdxRange6)),
    ?assertMatch(99, length(IdxRange7)),
    ?assertMatch(100, length(IdxRange8)),
    ok = sst_close(P1),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    ObjectKeyFun =
        fun(I) ->
            {{?RIAK_TAG,
                {<<"btype">>, <<"bucket">>},
                list_to_binary("19601301|"
                    ++ io_lib:format("~6..0w", [I])),
                null},
            {1, {active, infinity}, {0, 0}, null}}
        end,
    ObjectEntries = lists:map(ObjectKeyFun, lists:seq(1, 500)),
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, P2, {_FK2, _LK2}, _Bloom2} =
        sst_new(?TEST_AREA, FileName, 1, ObjectEntries, 6000, OptsSST),

    ObjectFileStateSize = size_summary(P2),

    ObjRange1 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>}, <<"1959">>, null},
            all,
            16),
    ObjRange2 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG,
                {<<"btype">>, <<"bucket">>},
                <<"1960">>, null},
            {?RIAK_TAG,
                {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    ObjRange3 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"1960">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000250">>, null},
            16),
    ObjRange4 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000251">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    ObjRange6 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|0002">>, null},
            16),
    ObjRange7 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|0001">>, null},
            16),
    ObjRange8 =
        sst_getkvrange(
            P2,
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000000">>, null},
            {?RIAK_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000100">>, null},
            16),

    ?assertMatch(500, length(ObjRange1)),
    ?assertMatch(500, length(ObjRange2)),
    ?assertMatch(250, length(ObjRange3)),
    ?assertMatch(250, length(ObjRange4)),
    ?assertMatch(199, length(ObjRange6)),
    ?assertMatch(99, length(ObjRange7)),
    ?assertMatch(100, length(ObjRange8)),
    ok = sst_close(P2),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    HeadKeyFun =
        fun(I) ->
            {{?HEAD_TAG,
                {<<"btype">>, <<"bucket">>},
                list_to_binary("19601301|"
                    ++ io_lib:format("~6..0w", [I])),
                null},
            {1, {active, infinity}, {0, 0}, null, undefined}}
        end,
    HeadEntries = lists:map(HeadKeyFun, lists:seq(1, 500)),
    {ok, P3, {_FK3, _LK3}, _Bloom3} =
        sst_new(?TEST_AREA, FileName, 1, HeadEntries, 6000, OptsSST),

    HeadFileStateSize =  size_summary(P3),

    HeadRange1 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>}, <<"1959">>, null},
            all,
            16),
    HeadRange2 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG,
                {<<"btype">>, <<"abucket">>},
                <<"1962">>, null},
            {?HEAD_TAG,
                {<<"btype">>, <<"zbucket">>},
                <<"1960">>, null},
            16),
    HeadRange3 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"1960">>, null},
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000250">>, null},
            16),
    HeadRange4 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000251">>, null},
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"1961">>, null},
            16),
    HeadRange6 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000">>, null},
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|0002">>, null},
            16),
    HeadRange7 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000">>, null},
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|0001">>, null},
            16),
    HeadRange8 =
        sst_getkvrange(
            P3,
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000000">>, null},
            {?HEAD_TAG, {<<"btype">>, <<"bucket">>},
                <<"19601301|000100">>, null},
            16),

    ?assertMatch(500, length(HeadRange1)),
    ?assertMatch(500, length(HeadRange2)),
    ?assertMatch(250, length(HeadRange3)),
    ?assertMatch(250, length(HeadRange4)),
    ?assertMatch(199, length(HeadRange6)),
    ?assertMatch(99, length(HeadRange7)),
    ?assertMatch(100, length(HeadRange8)),
    ok = sst_close(P3),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    [_HdO|RestObjectEntries] = ObjectEntries,
    [_HdI|RestIndexEntries] = IndexEntries,
    [_Hdh|RestHeadEntries] = HeadEntries,

    {ok, P4, {_FK4, _LK4}, _Bloom4} =
        sst_new(
            ?TEST_AREA,
            FileName, 1,
            [HeadKeyFun(9999)|RestIndexEntries],
            6000, OptsSST),
    print_compare_size("Index", IndexFileStateSize, size_summary(P4)),
    ok = sst_close(P4),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    {ok, P5, {_FK5, _LK5}, _Bloom5} =
    sst_new(
        ?TEST_AREA,
        FileName, 1,
        [HeadKeyFun(9999)|RestObjectEntries],
        6000, OptsSST),
    print_compare_size("Object", ObjectFileStateSize, size_summary(P5)),
    ok = sst_close(P5),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    {ok, P6, {_FK6, _LK6}, _Bloom6} =
    sst_new(
        ?TEST_AREA,
        FileName, 1,
        RestHeadEntries ++ [IndexKeyFun(1)],
        6000, OptsSST),
    print_compare_size("Head", HeadFileStateSize, size_summary(P6)),
    ok = sst_close(P6),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")).

size_summary(P) ->
    Summary = element(2, element(2, sys:get_state(P))),
    true = is_record(Summary, summary),
    erts_debug:flat_size(Summary).

print_compare_size(Type, OptimisedSize, UnoptimisedSize) ->
    io:format(
        user,
        "~n~s State optimised to ~w bytes unoptimised ~w bytes~n",
        [Type, OptimisedSize * 8, UnoptimisedSize * 8]),
    % Reduced by at least a quarter
    ?assert(OptimisedSize < (UnoptimisedSize - (UnoptimisedSize div 4))).


single_key_test() ->
    FileName = "single_key_test",
    Field = <<"t1_bin">>,
    LK = leveled_codec:to_ledgerkey(<<"Bucket0">>, <<"Key0">>, ?STD_TAG),
    Chunk = leveled_rand:rand_bytes(16),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, 1, Chunk, 16, infinity),
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},
    {ok, P1, {LK, LK}, _Bloom1} =
        sst_new(?TEST_AREA, FileName, 1, [{LK, MV}], 6000, OptsSST),
    ?assertMatch({LK, MV}, sst_get(P1, LK)),
    ok = sst_close(P1),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    IndexSpecs = [{add, Field, <<"20220101">>}],
    [{IdxK, IdxV}] =
        leveled_codec:idx_indexspecs(IndexSpecs,
                                    <<"Bucket">>,
                                    <<"Key">>,
                                    1,
                                    infinity),
    {ok, P2, {IdxK, IdxK}, _Bloom2} =
        sst_new(?TEST_AREA, FileName, 1, [{IdxK, IdxV}], 6000, OptsSST),
    ?assertMatch(
        [{IdxK, IdxV}],
        sst_getkvrange(
            P2,
            {?IDX_TAG, <<"Bucket">>, {Field, <<"20220100">>}, null},
            all,
            16)),
    ?assertMatch(
        [{IdxK, IdxV}],
        sst_getkvrange(
            P2,
            {?IDX_TAG, <<"Bucket">>, {Field, <<"20220100">>}, null},
            {?IDX_TAG, <<"Bucket">>, {Field, <<"20220101">>}, null},
            16)),
    ?assertMatch(
        [{IdxK, IdxV}],
        sst_getkvrange(
            P2,
            {?IDX_TAG, <<"Bucket">>, {Field, <<"20220101">>}, null},
            {?IDX_TAG, <<"Bucket">>, {Field, <<"20220101">>}, null},
            16)),
    ok = sst_close(P2),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")).

strange_range_test() ->
    FileName = "strange_range_test",
    V = leveled_head:riak_metadata_to_binary(
        term_to_binary([{"actor1", 1}]),
        <<1:32/integer, 0:32/integer, 0:32/integer>>),
    OptsSST =
        #sst_options{press_method=native,
                        log_options=leveled_log:get_opts()},

    FK = leveled_codec:to_ledgerkey({<<"T0">>, <<"B0">>}, <<"K0">>, ?RIAK_TAG),
    LK = leveled_codec:to_ledgerkey({<<"T0">>, <<"B0">>}, <<"K02">>, ?RIAK_TAG),
    EK = leveled_codec:to_ledgerkey({<<"T0">>, <<"B0">>}, <<"K0299">>, ?RIAK_TAG),

    KL1 =
        lists:map(
            fun(I) ->
                leveled_codec:to_ledgerkey(
                    {<<"T0">>, <<"B0">>},
                    list_to_binary("K00" ++ integer_to_list(I)),
                    ?RIAK_TAG)
            end,
            lists:seq(1, 300)),
    KL2 =
        lists:map(
            fun(I) ->
                leveled_codec:to_ledgerkey(
                    {<<"T0">>, <<"B0">>},
                    list_to_binary("K02" ++ integer_to_list(I)),
                    ?RIAK_TAG)
            end,
            lists:seq(1, 300)),

    GenerateValue =
        fun(K) ->
            element(
                3, leveled_codec:generate_ledgerkv(K, 1, V, 16, infinity))
        end,

    KVL =
        lists:ukeysort(
            1,
            lists:map(
                fun(K) -> {K, GenerateValue(K)} end,
                [FK] ++ KL1 ++ [LK] ++ KL2)),

    {ok, P1, {FK, EK}, _Bloom1} =
            sst_new(?TEST_AREA, FileName, 1, KVL, 6000, OptsSST),

    ?assertMatch(LK, element(1, sst_get(P1, LK))),
    ?assertMatch(FK, element(1, sst_get(P1, FK))),
    ok = sst_close(P1),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")),

    IndexSpecs =
        lists:map(
            fun(I) -> {add, <<"t1_bin">>, integer_to_binary(I)} end,
            lists:seq(1, 500)),
    IdxKVL =
        leveled_codec:idx_indexspecs(IndexSpecs,
                                    <<"Bucket">>,
                                    <<"Key">>,
                                    1,
                                    infinity),
    {ok, P2, {_FIdxK, _EIdxK}, _Bloom2} =
        sst_new(
            ?TEST_AREA, FileName, 1, lists:ukeysort(1, IdxKVL), 6000, OptsSST),
    [{IdxK1, _IdxV1}, {IdxK2, _IdxV2}] =
        sst_getkvrange(
            P2,
            {?IDX_TAG, <<"Bucket">>, {<<"t1_bin">>, <<"1">>}, null},
            {?IDX_TAG, <<"Bucket">>, {<<"t1_bin">>, <<"10">>}, null},
            16),
    ?assertMatch(
        {?IDX_TAG, <<"Bucket">>, {<<"t1_bin">>, <<"1">>}, <<"Key">>},
        IdxK1
    ),
    ?assertMatch(
        {?IDX_TAG, <<"Bucket">>, {<<"t1_bin">>, <<"10">>}, <<"Key">>},
        IdxK2
    ),
    ok = sst_close(P2),
    ok = file:delete(filename:join(?TEST_AREA, FileName ++ ".sst")).


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
