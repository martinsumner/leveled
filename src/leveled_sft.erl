%% This module provides functions for managing sft files - a modified version
%% of sst files, to be used in leveleddb.
%%
%% sft files are segment filtered tables in that they are guarded by a quick
%% access filter that checks for the presence of key by segment id, with the
%% segment id being a hash in the range 0 - 1024 * 1024
%%
%% This filter has a dual purpose
%% - a memory efficient way of discovering non-presence with low false positive
%% rate
%% - to make searching for all keys by hashtree segment more efficient (a
%% specific change to optimise behaviour for use with the incremental refresh)
%% of riak hashtrees
%%
%% All keys are not equal in sft files, keys are only expected in a specific
%% series of formats
%% - {Tag, Bucket, Key, SubKey|null} - Object Keys
%% - {i, Bucket, {IndexName, IndexTerm}, Key} - Postings
%% The {Bucket, Key} part of all types of keys are hashed for segment filters.
%% For Postings the {Bucket, IndexName, IndexTerm} is also hashed.  This
%% causes a false positive on lookup of a segment, but allows for the presence
%% of specific index terms to be checked
%%
%% The objects stored are a tuple of {Key, SequenceNumber, State, Value}, where
%% Key - as above
%% SequenceNumber - monotonically increasing counter of addition to the nursery
%% log
%% State - {active|tomb, ExpiryTimestamp | infinity}
%% Value - null (all postings) | [Object Metadata] (all object keys)
%% Keys should be unique in files.  If more than two keys are candidate for
%% the same file the highest sequence number should be chosen.  If the file
%% is at the basemenet level of a leveleddb database the objects with an
%% ExpiryTimestamp in the past should not be written, but at all other levels
%% keys should not be ignored because of a timestamp in the past.
%% tomb objects are written for deletions, and these tombstones may have an
%% Expirytimestamp which in effect is the time when the tombstone should be
%% reaped.
%%
%% sft files are broken into the following sections:
%% - Header (fixed width 80 bytes - containing pointers and metadata)
%% - Blocks (variable length)
%% - Slot Filter (variable length)
%% - Slot Index (variable length)
%% - Table Summary (variable length)
%% Each section should contain at the footer of the section a 4-byte CRC which
%% is to be checked only on the opening of the file
%%
%% The keys in the sft file are placed into the file in erlang term order.
%% There will normally be 256 slots of keys.  The Slot Index is a gb_tree
%% acting as a helper to find the right slot to check when searching for a key
%% or range of keys.
%% The Key in the Slot Index is the Key at the start of the Slot.
%% The Value in the Slot Index is a record indicating:
%% - The starting position of the Slot within the Blocks (relative to the
%% starting position of the Blocks)
%% - The (relative) starting position of the Slot Filter for this Slot
%% - The number of blocks within the Slot
%% - The length of each of the Blocks within the Slot
%%
%% When checking for a Key in the sft file, the key should be hashed to the
%% segment, then the key should be looked-up in the Slot Index.  The segment
%% ID can then be checked against the Slot Filter which will either return
%% not_present or [BlockIDs]
%% If a list of BlockIDs (normally of length 1) is returned the block should
%% be fetched using the starting position and length of the Block to find the
%% actual key (or not if the Slot Filter had returned a false positive)
%%
%% There will exist a Slot Filter for each entry in the Slot Index
%% The Slot Filter starts with some fixed length metadata
%% - 1 byte stating the expected number of keys in the block
%% - 1 byte stating the number of complete (i.e. containing the expected
%% number of keys) Blocks in the Slot
%% - 1 byte stating the number of keys in any incomplete Block (there can
%% only be 1 incomplete Block per Slot and it must be the last block)
%% - 3 bytes stating the largest segment ID in the Slot
%% - 1 byte stating the exponent used in the rice-encoding of the filter
%% The Filter itself is a rice-encoded list of Integers representing the
%% differences between the Segment IDs in the Slot with each entry being
%% appended by the minimal number of bits to represent the Block ID in which
%% an entry for that segment can be found.  Where a segment exists more than
%% once then a 0 length will be used.
%% To use the filter code should roll over the filter incrementing the Segment
%% ID by each difference, and counting the keys by Block ID.  This should
%% return one of:
%% mismatch - the final Segment Count didn't meet the largest Segment ID or
%% the per-block key counts don't add-up.  There could have been a bit-flip,
%% so don't rely on the filter
%% no_match - everything added up but the counter never equalled the queried
%% Segment ID
%% {match, [BlockIDs]} - everything added up and the Segment may be
%% represented in the given blocks
%%
%% The makeup of a block
%% - A block is a list of 32 {Key, Value} pairs in Erlang term order
%% - The block is stored using standard compression in term_to_binary
%% May be improved by use of lz4 or schema-based binary_to_term
%%
%% The Table Summary may contain multiple summaries
%% The standard table summary contains:
%% - a count of keys by bucket and type of key (posting or object key)
%% - the total size of objects referred to by object keys
%% - the number of postings by index name
%% - the number of tombstones within the file
%% - the highest and lowest sequence number in the file
%% Summaries could be used for other summaries of table content in the future,
%% perhaps application-specific bloom filters

%% The 56-byte header is made up of
%% - 1 byte version (major 5 bits, minor 3 bits) - default 0.1
%% - 1 byte options (currently undefined)
%% - 1 byte Block Size - the expected number of keys in each block
%% - 1 byte Block Count - the expected number of blocks in each slot
%% - 2 byte Slot Count - the maximum number of slots in the file
%% - 6 bytes - spare
%% - 4 bytes - Blocks length
%% - 4 bytes - Slot Index length
%% - 4 bytes - Slot Filter length
%% - 4 bytes - Table summary length
%% - 24 bytes - spare
%% - 4 bytes - CRC32
%% 
%% The file body is written in the same order of events as the header (i.e.
%% Blocks first)
%%
%% Once open the file can be in the following states
%% - writing, the file is still being created
%% - available, the file may be read, but never again must be modified
%% - pending_deletion, the file can be closed and deleted once all outstanding
%% Snapshots have been started beyond a certain sequence number
%%
%% Level managers should only be aware of files in the available state.
%% Iterators may be aware of files in either available or pending_delete.
%% Level maintainers should control the file exclusively when in the writing
%% state, and send the event to trigger pending_delete with the a sequence
%% number equal to or higher than the number at the point it was no longer
%% active at any level.
%%
%% The format of the file is intended to support quick lookups, whilst 
%% allowing for a new file to be written incrementally (so that all keys and
%% values need not be retained in memory) - perhaps n blocks at a time


-module(leveled_sft).

-behaviour(gen_fsm).
-include("include/leveled.hrl").

-export([init/1,
        handle_sync_event/4,
        handle_event/3,
        handle_info/3,
        terminate/3,
        code_change/4,
        starting/2,
        starting/3,
        reader/3,
        delete_pending/3,
        delete_pending/2]).

-export([sft_new/4,
        sft_newfroml0cache/4,
        sft_open/1,
        sft_get/2,
        sft_get/3,
        sft_getkvrange/4,
        sft_close/1,
        sft_clear/1,
        sft_checkready/1,
        sft_setfordelete/2,
        sft_deleteconfirmed/1,
        sft_getmaxsequencenumber/1]).

-export([generate_randomkeys/1]).

-include_lib("eunit/include/eunit.hrl").


-define(WORD_SIZE, 4).
-define(DWORD_SIZE, 8).
-define(CURRENT_VERSION, {0,1}).
-define(SLOT_COUNT, 256).
-define(SLOT_GROUPWRITE_COUNT, 16).
-define(BLOCK_SIZE, 32).
-define(BLOCK_COUNT, 4).
-define(FOOTERPOS_HEADERPOS, 2).
-define(MAX_SEG_HASH, 1048576).
-define(DIVISOR_BITS, 13).
-define(DIVISOR, 8092).
-define(COMPRESSION_LEVEL, 1).
-define(HEADER_LEN, 56).
-define(ITERATOR_SCANWIDTH, 1).
-define(MERGE_SCANWIDTH, 32).
-define(BLOOM_WIDTH, 48).
-define(DELETE_TIMEOUT, 10000).
-define(MAX_KEYS, ?SLOT_COUNT * ?BLOCK_COUNT * ?BLOCK_SIZE).
-define(DISCARD_EXT, ".discarded").
-define(WRITE_OPS, [binary, raw, read, write, delayed_write]).
-define(READ_OPS, [binary, raw, read]).

-record(state, {version = ?CURRENT_VERSION :: tuple(),
                slot_index :: list(),
                next_position :: integer(),
				smallest_sqn :: integer(),
				highest_sqn :: integer(),
                smallest_key :: string(),
                highest_key :: string(),
                slots_pointer :: integer(),
                index_pointer :: integer(),
                filter_pointer :: integer(),
                summ_pointer :: integer(),
                summ_length :: integer(),
                filename = "not set" :: string(),
                handle :: file:fd(),
                background_complete = false :: boolean(),
                oversized_file = false :: boolean(),
                penciller :: pid(),
                bloom}).

%% Helper object when writing a file to keep track of various accumulators
-record(writer, {slot_index = [] :: list(),
                    slot_binary = <<>> :: binary(),
                    bloom = leveled_tinybloom:empty(?BLOOM_WIDTH),
                    min_sqn = infinity :: integer()|infinity,
                    max_sqn = 0 :: integer(),
                    last_key = {last, null}}).

%%%============================================================================
%%% API
%%%============================================================================


sft_new(Filename, KL1, KL2, LevelInfo) ->
    LevelR = case is_integer(LevelInfo) of
                    true ->
                        #level{level=LevelInfo};
                    _ ->
                        if
                            is_record(LevelInfo, level) ->
                                LevelInfo
                        end
                end,
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    Reply = gen_fsm:sync_send_event(Pid,
                                    {sft_new, Filename, KL1, KL2, LevelR},
                                    infinity),
    {ok, Pid, Reply}.

sft_newfroml0cache(Filename, Slots, FetchFun, Options) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    case Options#sft_options.wait of
        true ->
            KL1 = leveled_pmem:to_list(Slots, FetchFun),
            Reply = gen_fsm:sync_send_event(Pid,
                                            {sft_new,
                                                Filename,
                                                KL1,
                                                [],
                                                #level{level=0}},
                                            infinity),
            {ok, Pid, Reply};
        false ->
            gen_fsm:send_event(Pid,
                                {sft_newfroml0cache,
                                    Filename,
                                    Slots,
                                    FetchFun,
                                    Options#sft_options.penciller}),
            {ok, Pid, noreply}
    end.

sft_open(Filename) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    case gen_fsm:sync_send_event(Pid, {sft_open, Filename}, infinity) of
        {ok, {SK, EK}} ->
            {ok, Pid, {SK, EK}}
    end.

sft_setfordelete(Pid, Penciller) ->
    gen_fsm:sync_send_event(Pid, {set_for_delete, Penciller}, infinity).

sft_get(Pid, Key, Hash) ->
    gen_fsm:sync_send_event(Pid, {get_kv, Key, Hash}, infinity).

sft_get(Pid, Key) ->
    sft_get(Pid, Key, leveled_codec:magic_hash(Key)).

sft_getkvrange(Pid, StartKey, EndKey, ScanWidth) ->
    gen_fsm:sync_send_event(Pid,
                            {get_kvrange, StartKey, EndKey, ScanWidth},
                            infinity).

sft_clear(Pid) ->
    gen_fsm:sync_send_event(Pid, {set_for_delete, false}, infinity),
    gen_fsm:sync_send_event(Pid, close, 1000).

sft_close(Pid) ->
    gen_fsm:sync_send_event(Pid, close, 1000).

sft_deleteconfirmed(Pid) ->
    gen_fsm:send_event(Pid, close).

sft_checkready(Pid) ->
    gen_fsm:sync_send_event(Pid, background_complete, 20).

sft_getmaxsequencenumber(Pid) ->
    gen_fsm:sync_send_event(Pid, get_maxsqn, infinity).



%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, starting, #state{}}.

starting({sft_new, Filename, KL1, [], _LevelR=#level{level=L}}, _From, _State)
                                                                when L == 0 ->
    {ok, State} = create_levelzero(KL1, Filename),
    {reply,
        {{[], []}, State#state.smallest_key, State#state.highest_key},
        reader,
        State};
starting({sft_new, Filename, KL1, KL2, LevelR}, _From, _State) ->
    case create_file(Filename) of
        {Handle, FileMD} ->
            {ReadHandle, UpdFileMD, KeyRemainders} = complete_file(Handle,
                                                                    FileMD,
                                                                    KL1, KL2,
                                                                    LevelR),
            {reply,
                {KeyRemainders,
                    UpdFileMD#state.smallest_key,
                    UpdFileMD#state.highest_key},
                reader,
                UpdFileMD#state{handle=ReadHandle, filename=Filename}}
    end;
starting({sft_open, Filename}, _From, _State) ->
    {_Handle, FileMD} = open_file(#state{filename=Filename}),
    leveled_log:log("SFT01", [Filename]),
    {reply,
        {ok, {FileMD#state.smallest_key, FileMD#state.highest_key}},
        reader,
        FileMD}.

starting({sft_newfroml0cache, Filename, Slots, FetchFun, PCL}, _State) ->
    SW = os:timestamp(),
    Inp1 = leveled_pmem:to_list(Slots, FetchFun),
    {ok, State} = create_levelzero(Inp1, Filename),
    leveled_log:log_timer("SFT03", [Filename], SW),
    case PCL of
        undefined ->
            {next_state, reader, State};
        _ ->
            leveled_penciller:pcl_confirml0complete(PCL,
                                                    State#state.filename,
                                                    State#state.smallest_key,
                                                    State#state.highest_key),
            {next_state, reader, State}
    end.


reader({get_kv, Key, Hash}, _From, State) ->
    Reply =
        case leveled_tinybloom:check({hash, Hash}, State#state.bloom) of
            false ->
                not_present;
            true ->
                fetch_keyvalue(State#state.handle, State, Key)
        end,
    {reply, Reply, reader, State};
reader({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    Reply = pointer_append_queryresults(fetch_range_kv(State#state.handle,
                                                        State,
                                                        StartKey,
                                                        EndKey,
                                                        ScanWidth),
                                            self()),
    {reply, Reply, reader, State};
reader(get_maxsqn, _From, State) ->
    {reply, State#state.highest_sqn, reader, State};
reader({set_for_delete, Penciller}, _From, State) ->
    leveled_log:log("SFT02", [State#state.filename]),
    {reply,
        ok,
        delete_pending,
        State#state{penciller=Penciller},
        ?DELETE_TIMEOUT};
reader(background_complete, _From, State) ->
    if
        State#state.background_complete == true ->
            {reply,
                {ok,
                    State#state.filename,
                    State#state.smallest_key,
                    State#state.highest_key},
                reader,
                State}
    end;
reader(close, _From, State) ->
    ok = file:close(State#state.handle),
    {stop, normal, ok, State}.

delete_pending({get_kv, Key, Hash}, _From, State) ->
    Reply =
        case leveled_tinybloom:check({hash, Hash}, State#state.bloom) of
            false ->
                not_present;
            true ->
                fetch_keyvalue(State#state.handle, State, Key)
        end,
    {reply, Reply, delete_pending, State, ?DELETE_TIMEOUT};
delete_pending({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    Reply = pointer_append_queryresults(fetch_range_kv(State#state.handle,
                                                        State,
                                                        StartKey,
                                                        EndKey,
                                                        ScanWidth),
                                            self()),
    {reply, Reply, delete_pending, State, ?DELETE_TIMEOUT};
delete_pending(close, _From, State) ->
    leveled_log:log("SFT06", [State#state.filename]),
    ok = file:close(State#state.handle),
    ok = file:delete(State#state.filename),
    {stop, normal, ok, State}.

delete_pending(timeout, State) ->
    leveled_log:log("SFT05", [timeout, State#state.filename]),
    ok = leveled_penciller:pcl_confirmdelete(State#state.penciller,
                                               State#state.filename),
    {next_state, delete_pending, State, ?DELETE_TIMEOUT};
delete_pending(close, State) ->
    leveled_log:log("SFT06", [State#state.filename]),
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
    leveled_log:log("SFT05", [Reason, State#state.filename]).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.



%%%============================================================================
%%% Internal functions
%%%============================================================================


create_levelzero(ListForFile, Filename) ->
    {TmpFilename, PrmFilename} = generate_filenames(Filename),
    {Handle, FileMD} = create_file(TmpFilename),
    InputSize = length(ListForFile),
    leveled_log:log("SFT07", [InputSize]),
    Rename = {true, TmpFilename, PrmFilename},
    {ReadHandle,
        UpdFileMD,
        {[], []}} = complete_file(Handle, FileMD,
                                    ListForFile, [],
                                    #level{level=0}, Rename),
    {ok,
        UpdFileMD#state{handle=ReadHandle,
                        filename=PrmFilename,
                        background_complete=true,
                        oversized_file=InputSize>?MAX_KEYS}}.


generate_filenames(RootFilename) ->
    Ext = filename:extension(RootFilename),
    Components = filename:split(RootFilename),
    case Ext of
        [] ->
            {filename:join(Components) ++ ".pnd",
                filename:join(Components) ++ ".sft"};
        Ext ->
            %% This seems unnecessarily hard
            DN = filename:dirname(RootFilename),
            FP = lists:last(Components),
            FP_NOEXT = lists:sublist(FP, 1, 1 + length(FP) - length(Ext)),
            {DN ++ "/" ++ FP_NOEXT ++ "pnd", DN ++ "/" ++ FP_NOEXT ++ "sft"}
    end.    


%% Start a bare file with an initial header and no further details
%% Return the {Handle, metadata record}
create_file(FileName) when is_list(FileName) ->
    leveled_log:log("SFT01", [FileName]),
    ok = filelib:ensure_dir(FileName),
    {ok, Handle} = file:open(FileName, ?WRITE_OPS),
    Header = create_header(initial),
    {ok, _} = file:position(Handle, bof),
    ok = file:write(Handle, Header),
    {ok, StartPos} = file:position(Handle, cur),
    FileMD = #state{next_position=StartPos, filename=FileName},
    {Handle, FileMD}.


create_header(initial) ->
	{Major, Minor} = ?CURRENT_VERSION, 
	Version = <<Major:5, Minor:3>>,
    %% Not thought of any options - options are ignored
	Options = <<0:8>>, 
    %% Settings are currently ignored
    {BlSize, BlCount, SlCount} = {?BLOCK_COUNT, ?BLOCK_SIZE, ?SLOT_COUNT},
    Settings = <<BlSize:8, BlCount:8, SlCount:16>>,
    {SpareO, SpareL} = {<<0:48>>, <<0:192>>},
	Lengths = <<0:32, 0:32, 0:32, 0:32>>,
    H1 = <<Version/binary, Options/binary, Settings/binary, SpareO/binary,
    Lengths/binary, SpareL/binary>>,
    CRC32 = erlang:crc32(H1),
	<<H1/binary, CRC32:32/integer>>.

%% Open a file returning a handle and metadata which can be used in fetch and
%% iterator requests
%% The handle should be read-only as these are immutable files, a file cannot
%% be opened for writing keys, it can only be created to write keys

open_file(FileMD) ->
    Filename = FileMD#state.filename,
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    {ok, HeaderLengths} = file:pread(Handle, 12, 16),
    <<Blen:32/integer,
        Ilen:32/integer,
        Flen:32/integer,
        Slen:32/integer>> = HeaderLengths,
    {ok, <<SummaryCRC:32/integer, SummaryBin/binary>>} =
        file:pread(Handle, ?HEADER_LEN + Blen + Ilen + Flen, Slen),
    {{LowSQN, HighSQN}, {LowKey, HighKey}, Bloom} = 
        case erlang:crc32(SummaryBin) of
            SummaryCRC ->
                binary_to_term(SummaryBin)
        end,
    {ok, SlotIndexBin} = file:pread(Handle, ?HEADER_LEN + Blen, Ilen),
    SlotIndex = binary_to_term(SlotIndexBin),
    {Handle, FileMD#state{slot_index=SlotIndex,
                           smallest_sqn=LowSQN,
                           highest_sqn=HighSQN,
                           smallest_key=LowKey,
                           highest_key=HighKey,
                           slots_pointer=?HEADER_LEN,
                           index_pointer=?HEADER_LEN + Blen,
                           filter_pointer=?HEADER_LEN + Blen + Ilen,
                           summ_pointer=?HEADER_LEN + Blen + Ilen + Flen,
                           summ_length=Slen,
                           handle=Handle,
                           bloom=Bloom}}.
    
%% Take a file handle with a previously created header and complete it based on
%% the two key lists KL1 and KL2
complete_file(Handle, FileMD, KL1, KL2, LevelR) ->
    complete_file(Handle, FileMD, KL1, KL2, LevelR, false).

complete_file(Handle, FileMD, KL1, KL2, LevelR, Rename) ->
    {ok, KeyRemainders} = write_keys(Handle,
                                        maybe_expand_pointer(KL1),
                                        maybe_expand_pointer(KL2),
                                        LevelR,
                                        fun sftwrite_function/2,
                                        #writer{}),
    {ReadHandle, UpdFileMD} = case Rename of
        false ->
            open_file(FileMD);
        {true, OldName, NewName} ->
            ok = rename_file(OldName, NewName),
            open_file(FileMD#state{filename=NewName})
    end,    
    {ReadHandle, UpdFileMD, KeyRemainders}.

rename_file(OldName, NewName) ->
    leveled_log:log("SFT08", [OldName, NewName]),
    case filelib:is_file(NewName) of
        true ->
            leveled_log:log("SFT09", [NewName]),
            AltName = filename:join(filename:dirname(NewName),
                                    filename:basename(NewName))
                        ++ ?DISCARD_EXT,
            leveled_log:log("SFT10", [NewName, AltName]),
            ok = file:rename(NewName, AltName);
        false ->
            ok
    end,
    file:rename(OldName, NewName).


%% Fetch a Key and Value from a file, returns
%% {value, KV} or not_present
%% The key must be pre-checked to ensure it is in the valid range for the file
%% A key out of range may fail

fetch_keyvalue(Handle, FileMD, Key) ->
    case get_nearestkey(FileMD#state.slot_index, Key) of
        not_found ->
            not_present;
        {_NearestKey, {FilterLen, PointerF}, {LengthList, PointerB}} -> 
            FilterPointer = PointerF + FileMD#state.filter_pointer,
            {ok, SegFilter} = file:pread(Handle,
                                            FilterPointer,
                                            FilterLen),
            SegID = hash_for_segmentid({keyonly, Key}),
            case check_for_segments(SegFilter, [SegID], true) of
                {maybe_present, BlockList} ->
                    BlockPointer = PointerB + FileMD#state.slots_pointer,
                    fetch_keyvalue_fromblock(BlockList,
                                                Key,
                                                LengthList,
                                                Handle,
                                                BlockPointer);
                not_present ->
                    not_present;
                error_so_maybe_present ->
                    BlockPointer = PointerB + FileMD#state.slots_pointer,
                    fetch_keyvalue_fromblock(lists:seq(0,length(LengthList)),
                                                Key,
                                                LengthList,
                                                Handle,
                                                BlockPointer)
            end
    end.

%% Fetches a range of keys returning a list of {Key, SeqN} tuples
fetch_range_keysonly(Handle, FileMD, StartKey, EndKey) ->
    fetch_range(Handle, FileMD, StartKey, EndKey, fun acc_list_keysonly/2).

fetch_range_keysonly(Handle, FileMD, StartKey, EndKey, ScanWidth) ->
    fetch_range(Handle, FileMD, StartKey, EndKey, fun acc_list_keysonly/2,
                    ScanWidth).

%% Fetches a range of keys returning the full tuple, including value
fetch_range_kv(Handle, FileMD, StartKey, EndKey, ScanWidth) ->
    fetch_range(Handle, FileMD, StartKey, EndKey, fun acc_list_kv/2,
                    ScanWidth).

acc_list_keysonly(null, empty) ->
    [];
acc_list_keysonly(null, RList) ->
    RList;
acc_list_keysonly(R, RList) when is_list(R) ->
    lists:foldl(fun acc_list_keysonly/2, RList, R);
acc_list_keysonly(R, RList) ->
    lists:append(RList, [leveled_codec:strip_to_keyseqstatusonly(R)]).

acc_list_kv(null, empty) ->
    [];
acc_list_kv(null, RList) ->
    RList;
acc_list_kv(R, RList) when is_list(R) ->
    RList ++ R;
acc_list_kv(R, RList) ->
    lists:append(RList, [R]).

%% Iterate keys, returning a batch of keys & values in a range
%% - the iterator can have a ScanWidth which is how many slots should be
%% scanned by the iterator before returning a result
%% - batches can be ended with a pointer to indicate there are potentially
%% further values in the range
%% - a list of functions can be provided, which should either return true
%% or false, and these can be used to filter the results from the query,
%% for example to ignore keys above a certain sequence number, to ignore
%% keys not matching a certain regular expression, or to ignore keys not
%% a member of a particular partition
%% - An Accumulator and an Accumulator function can be passed.  The function
%% needs to handle being passed (KV, Acc) to add the current result to the
%% Accumulator.  The functional should handle KV=null, Acc=empty to initiate
%% the accumulator, and KV=null to leave the Accumulator unchanged.
%% Flexibility with accumulators is such that keys-only can be returned rather
%% than keys and values, or other entirely different accumulators can be
%% used - e.g. counters, hash-lists to build bloom filters etc

fetch_range(Handle, FileMD, StartKey, EndKey, AccFun) ->
    fetch_range(Handle, FileMD, StartKey, EndKey, AccFun, ?ITERATOR_SCANWIDTH).
    
fetch_range(Handle, FileMD, StartKey, EndKey, AccFun, ScanWidth) ->
    fetch_range(Handle, FileMD, StartKey, EndKey, AccFun, ScanWidth, empty).

fetch_range(_Handle, _FileMD, StartKey, _EndKey, _AccFun, 0, Acc) ->
    {partial, Acc, StartKey};
fetch_range(Handle, FileMD, StartKey, EndKey, AccFun, ScanWidth, Acc) ->
    %% get_nearestkey gets the last key in the index <= StartKey, or the next
    %% key along if {next, StartKey} is passed
    case get_nearestkey(FileMD#state.slot_index, StartKey) of
        {NearestKey, _Filter, {LengthList, PointerB}} ->
            fetch_range(Handle, FileMD, StartKey, NearestKey, EndKey,
                            AccFun, ScanWidth,
                            LengthList,
                            0,
                            PointerB + FileMD#state.slots_pointer,
                            AccFun(null, Acc));
        not_found ->
            {complete, AccFun(null, Acc)}
    end.

fetch_range(Handle, FileMD, _StartKey, NearestKey, EndKey,
                        AccFun, ScanWidth,
                        LengthList,
                        BlockNumber,
                        _Pointer,
                        Acc)
                        when length(LengthList) == BlockNumber ->
    %% Reached the end of the slot.  Move the start key on one to scan a new slot
    fetch_range(Handle, FileMD, {next, NearestKey}, EndKey,
                        AccFun, ScanWidth - 1,
                        Acc);
fetch_range(Handle, FileMD, StartKey, NearestKey, EndKey,
                        AccFun, ScanWidth,
                        LengthList,
                        BlockNumber,
                        Pointer,
                        Acc) ->
    Block = fetch_block(Handle, LengthList, BlockNumber, Pointer),
    Results = 
        case maybe_scan_entire_block(Block, StartKey, EndKey) of
            true ->
                {partial, AccFun(Block, Acc), StartKey};
            false ->
                scan_block(Block, StartKey, EndKey, AccFun, Acc)
        end,
    case Results of
        {partial, Acc1, StartKey} ->
            %% Move on to the next block
            fetch_range(Handle, FileMD, StartKey, NearestKey, EndKey,
                        AccFun, ScanWidth,
                        LengthList,
                        BlockNumber +  1,
                        Pointer,
                        Acc1);
        {complete, Acc1} ->
            {complete, Acc1}
    end.

scan_block([], StartKey, _EndKey, _AccFun, Acc) ->
    {partial, Acc, StartKey};
scan_block([HeadKV|T], StartKey, EndKey, AccFun, Acc) ->
    K = leveled_codec:strip_to_keyonly(HeadKV),
    case {StartKey > K, leveled_codec:endkey_passed(EndKey, K)} of
        {true, _} when StartKey /= all ->
            scan_block(T, StartKey, EndKey, AccFun, Acc);
        {_, true} when EndKey /= all ->
            {complete, Acc};
        _ ->
            scan_block(T, StartKey, EndKey, AccFun, AccFun(HeadKV, Acc))
    end.


maybe_scan_entire_block([], _, _) ->
    true;
maybe_scan_entire_block(_Block, all, all) ->
    true;
maybe_scan_entire_block(Block, StartKey, all) ->
    [FirstKey|_Tail] = Block,
    leveled_codec:strip_to_keyonly(FirstKey) >= StartKey;
maybe_scan_entire_block(Block, StartKey, EndKey) ->
    [FirstKey|_Tail] = Block,
    LastKey = leveled_codec:strip_to_keyonly(lists:last(Block)),
    FromStart = leveled_codec:strip_to_keyonly(FirstKey) > StartKey,
    ToEnd = leveled_codec:endkey_passed(EndKey, LastKey),
    case {FromStart, ToEnd} of
        {true, true} ->
            true;
        _ ->
            false
    end.

fetch_keyvalue_fromblock([], _Key, _LengthList, _Handle, _StartOfSlot) ->
    not_present;
fetch_keyvalue_fromblock([BlockNmb|T], Key, LengthList, Handle, StartOfSlot) ->
    BlockToCheck = fetch_block(Handle, LengthList, BlockNmb, StartOfSlot),
    Result = lists:keyfind(Key, 1, BlockToCheck),
    case Result of
        false ->
            fetch_keyvalue_fromblock(T, Key, LengthList, Handle, StartOfSlot);
        KV ->
            KV
    end.
    
fetch_block(Handle, LengthList, BlockNmb, StartOfSlot) ->
    Start = lists:sum(lists:sublist(LengthList, BlockNmb)),
    Length = lists:nth(BlockNmb + 1, LengthList),
    {ok, BlockToCheckBin} = file:pread(Handle, Start + StartOfSlot, Length),
    binary_to_term(BlockToCheckBin).

%% Need to deal with either Key or {next, Key}
get_nearestkey([H|_Tail], all) ->
    H;
get_nearestkey(KVList, Key) ->
    case Key of
        {next, K} ->
            get_nextkeyaftermatch(KVList, K, not_found);
        _ ->
            get_firstkeytomatch(KVList, Key, not_found)
    end.

get_firstkeytomatch([], _KeyToFind, PrevV) ->
    PrevV;
get_firstkeytomatch([{K, FilterInfo, SlotInfo}|_T], KeyToFind, PrevV)
                                                    when K > KeyToFind ->
    case PrevV of
        not_found ->
            {K, FilterInfo, SlotInfo};
        _ ->
            PrevV
    end;
get_firstkeytomatch([{K, FilterInfo, SlotInfo}|T], KeyToFind, _PrevV) ->
    get_firstkeytomatch(T, KeyToFind, {K, FilterInfo, SlotInfo}).

get_nextkeyaftermatch([], _KeyToFind, _PrevV) ->
    not_found;
get_nextkeyaftermatch([{K, FilterInfo, SlotInfo}|T], KeyToFind, PrevV)
                                                    when K >= KeyToFind ->
    case PrevV of
        not_found ->
            get_nextkeyaftermatch(T, KeyToFind, next);
        next ->
            {K, FilterInfo, SlotInfo}
    end;
get_nextkeyaftermatch([_KTuple|T], KeyToFind, PrevV) ->
    get_nextkeyaftermatch(T, KeyToFind, PrevV).


%% Take a file handle at the sart position (after creating the header) and then
%% write the Key lists to the file slot by slot.
%%
%% Slots are created then written in bulk to impove I/O efficiency.  Slots will
%% be written in groups

write_keys(Handle, KL1, KL2, LevelR, WriteFun, WriteState) ->
    write_keys(Handle, KL1, KL2, LevelR, WriteFun, WriteState, {0, 0, []}).

write_keys(Handle, KL1, KL2, LevelR, WriteFun, WState,
                                    {SlotC, SlotT, SlotLists})
                                    when SlotC =:= ?SLOT_GROUPWRITE_COUNT ->
    WState0 = lists:foldl(fun finalise_slot/2, WState, SlotLists),
    Handle0 = WriteFun(slots, {Handle, WState0#writer.slot_binary}),
    case maxslots_bylevel(SlotT, LevelR#level.level) of
        reached ->
            {complete_keywrite(Handle0, WState0, WriteFun), {KL1, KL2}};
        continue ->
            write_keys(Handle0, KL1, KL2, LevelR, WriteFun,
                        WState0#writer{slot_binary = <<>>}, {0, SlotT, []})
    end;
write_keys(Handle, KL1, KL2, LevelR, WriteFun, WState,
                                                {SlotC, SlotT, SlotLists}) ->
    {Status, BlockKeyLists} = create_slot(KL1, KL2, LevelR),
    case Status of
        S when S == complete; S == partial ->
            WState0 =
                case BlockKeyLists of
                    [[]] ->
                        WState;
                    _ ->
                        lists:foldl(fun finalise_slot/2,
                                        WState,
                                        SlotLists ++ [BlockKeyLists])
                end,
            Handle0 = WriteFun(slots, {Handle, WState0#writer.slot_binary}),
            {complete_keywrite(Handle0, WState0, WriteFun), {[], []}};
        {full, KL1Rem, KL2Rem} ->
            write_keys(Handle, KL1Rem, KL2Rem, LevelR, WriteFun, WState,
                        {SlotC + 1, SlotT + 1, SlotLists ++ [BlockKeyLists]})
    end.
    

complete_keywrite(Handle, WriteState, WriteFun) ->
    FirstKey =
        case length(WriteState#writer.slot_index) of
            0 ->
                null;
            _ ->
                element(1, lists:nth(1, WriteState#writer.slot_index))
        end,
    ConvSlotIndex = convert_slotindex(WriteState#writer.slot_index),
    WriteFun(finalise, {Handle,
                        ConvSlotIndex,
                        {{WriteState#writer.min_sqn, WriteState#writer.max_sqn},
                            {FirstKey, WriteState#writer.last_key},
                            WriteState#writer.bloom}}).

%% Take a slot index, and remove the SegFilters replacing with pointers
%% Return a tuple of the accumulated slot filters, and a pointer-based
%% slot-index

convert_slotindex(SlotIndex) ->
    SlotFun = fun({LowKey, SegFilter, LengthList},
                    {FilterAcc, SlotIndexAcc, PointerF, PointerB}) ->
                    FilterOut = serialise_segment_filter(SegFilter),
                    FilterLen = byte_size(FilterOut),
                    {<<FilterAcc/binary, FilterOut/binary>>,
                        lists:append(SlotIndexAcc, [{LowKey,
                                                    {FilterLen, PointerF},
                                                    {LengthList, PointerB}}]),
                        PointerF + FilterLen,
                        PointerB + lists:sum(LengthList)} end,
    {SlotFilters, PointerIndex, _FLength, _BLength} =
            lists:foldl(SlotFun, {<<>>, [], 0, 0}, SlotIndex),
    {SlotFilters, PointerIndex}.

sftwrite_function(slots, {Handle, SerialisedSlots}) ->
    ok = file:write(Handle, SerialisedSlots),
    Handle;
sftwrite_function(finalise,
                    {Handle,
                        {SlotFilters, PointerIndex},
                        {SNExtremes, KeyExtremes, Bloom}}) ->
    {ok, Position} = file:position(Handle, cur),
    
    BlocksLength = Position - ?HEADER_LEN,
    Index = term_to_binary(PointerIndex),
    IndexLength = byte_size(Index),
    FilterLength = byte_size(SlotFilters),
    Summary = term_to_binary({SNExtremes, KeyExtremes, Bloom}),
    SummaryCRC = erlang:crc32(Summary),
    SummaryLength = byte_size(Summary) + 4,
    %% Write Index, Filter and Summary
    ok = file:write(Handle, <<Index/binary,
                                SlotFilters/binary,
                                SummaryCRC:32/integer,
                                Summary/binary>>),
    %% Write Lengths into header
    ok = file:pwrite(Handle, 12, <<BlocksLength:32/integer,
                                    IndexLength:32/integer,
                                    FilterLength:32/integer,
                                    SummaryLength:32/integer>>),
    {ok, _Position} = file:position(Handle, bof),
    ok = file:advise(Handle,
                        BlocksLength + IndexLength,
                        FilterLength,
                        will_need),
    file:close(Handle).

%% Level 0 files are of variable (infinite) size to avoid issues with having
%% any remainders when flushing from memory
maxslots_bylevel(_SlotTotal, 0) ->
    continue;
maxslots_bylevel(SlotTotal, _Level) ->
    case SlotTotal of
        ?SLOT_COUNT ->
            reached;
        X when X < ?SLOT_COUNT ->
            continue
    end.



%% Take two potentially overlapping lists of keys and produce a block size
%% list of keys in the correct order.  Outputs:
%% - Status of
%% - - all_complete (no more keys and block is complete)
%% - - partial (no more keys and block is not complete)
%% - - {block_full, Rem1, Rem2} the block is complete but there is a remainder
%% of keys

create_block(KeyList1, KeyList2, LevelR) ->
    create_block(KeyList1, KeyList2, LevelR, []).


create_block([], [], _LevelR, BlockKeyList)
                                    when length(BlockKeyList)==?BLOCK_SIZE ->
    {all_complete, lists:reverse(BlockKeyList)};
create_block([], [], _LevelR, BlockKeyList) ->
    {partial, lists:reverse(BlockKeyList)};
create_block(KeyList1, KeyList2, _LevelR, BlockKeyList)
                                    when length(BlockKeyList)==?BLOCK_SIZE ->
    {{block_full, KeyList1, KeyList2}, lists:reverse(BlockKeyList)};
create_block(KeyList1, KeyList2, LevelR, BlockKeyList) ->
    case key_dominates(KeyList1, KeyList2,
                        {LevelR#level.is_basement, LevelR#level.timestamp}) of
        {{next_key, TopKey}, Rem1, Rem2} ->
            create_block(Rem1, Rem2, LevelR, [TopKey|BlockKeyList]);
        {skipped_key, Rem1, Rem2} ->
            create_block(Rem1, Rem2, LevelR, BlockKeyList)
    end.

%% create_slot should simply output a list of BlockKeyLists no bigger than
%% the BlockCount, the the status (with key remianders if not complete)

create_slot(KL1, KL2, LevelR) ->
    create_slot(KL1, KL2, LevelR, ?BLOCK_COUNT, []).

create_slot(KL1, KL2, LevelR, BlockCount, BlockKeyLists) ->
    {Status, KeyList} = create_block(KL1, KL2, LevelR),
    case {Status, BlockCount - 1} of
        {partial, _N} ->
            {partial, BlockKeyLists ++ [KeyList]};
        {all_complete, 0} ->
            {complete, BlockKeyLists ++ [KeyList]};
        {all_complete, _N} ->
            % From the perspective of the slot it is partially complete
            {partial, BlockKeyLists ++ [KeyList]};
        {{block_full, KL1Rem, KL2Rem}, 0} ->
            {{full, KL1Rem, KL2Rem}, BlockKeyLists ++ [KeyList]};
        {{block_full, KL1Rem, KL2Rem}, N} ->
            create_slot(KL1Rem, KL2Rem, LevelR, N, BlockKeyLists ++ [KeyList])
    end.



%% Fold over the List of BlockKeys updating the writer record
finalise_slot(BlockKeyLists, WriteState) ->
    BlockFolder =
        fun(KV, {AccMinSQN, AccMaxSQN, Bloom, SegmentIDList}) ->
                {SQN, Hash} = leveled_codec:strip_to_seqnhashonly(KV),  
                {min(AccMinSQN, SQN),
                    max(AccMaxSQN, SQN),
                    leveled_tinybloom:enter({hash, Hash}, Bloom),
                    [hash_for_segmentid(KV)|SegmentIDList]}
        end,
    SlotFolder =
        fun(BlockKeyList,
                {MinSQN, MaxSQN, Bloom, SegLists, KVBinary, Lengths}) ->
                    {BlockMinSQN, BlockMaxSQN, UpdBloom, Segs} = 
                        lists:foldr(BlockFolder,
                                    {infinity, 0, Bloom, []},
                                    BlockKeyList),
                    SerialisedBlock = serialise_block(BlockKeyList),
                    {min(MinSQN, BlockMinSQN),
                        max(MaxSQN, BlockMaxSQN),
                        UpdBloom,
                        SegLists ++ [Segs],
                        <<KVBinary/binary, SerialisedBlock/binary>>,
                        Lengths ++ [byte_size(SerialisedBlock)]}
        end,
    
    {SlotMinSQN,
        SlotMaxSQN,
        SlotUpdBloom,
        SlotSegLists,
        SlotBinary,
        BlockLengths} =
            lists:foldl(SlotFolder,
                            {WriteState#writer.min_sqn,
                                WriteState#writer.max_sqn,
                                WriteState#writer.bloom,
                                [],
                                WriteState#writer.slot_binary,
                                []},
                            BlockKeyLists),
    
    FirstSlotKey = leveled_codec:strip_to_keyonly(lists:nth(1,
                                                    lists:nth(1,
                                                    BlockKeyLists))),
    LastSlotKV = lists:last(lists:last(BlockKeyLists)), 
    SegFilter = generate_segment_filter(SlotSegLists),
    UpdSlotIndex = lists:append(WriteState#writer.slot_index,
                                [{FirstSlotKey, SegFilter, BlockLengths}]),
        
    #writer{slot_index = UpdSlotIndex,
            slot_binary = SlotBinary,
            bloom = SlotUpdBloom,
            min_sqn = SlotMinSQN,
            max_sqn = SlotMaxSQN,
            last_key = leveled_codec:strip_to_keyonly(LastSlotKV)}.


serialise_block(BlockKeyList) ->
    term_to_binary(BlockKeyList, [{compressed, ?COMPRESSION_LEVEL}]).


%% Compare the keys at the head of the list, and either skip that "best" key or
%% identify as the next key.
%%
%% The logic needs to change if the file is in the basement level, as keys with
%% expired timestamps need not be written at this level
%%
%% The best key is considered to be the lowest key in erlang term order.  If
%% there are matching keys then the highest sequence number must be chosen and
%% any lower sequence numbers should be compacted out of existence


key_dominates(KL1, KL2, Level) ->
    key_dominates_expanded(maybe_expand_pointer(KL1),
                            maybe_expand_pointer(KL2),
                            Level).

key_dominates_expanded([H1|T1], [], Level) ->
    case leveled_codec:maybe_reap_expiredkey(H1, Level) of
        true ->
            {skipped_key, maybe_expand_pointer(T1), []};
        false ->
            {{next_key, H1}, maybe_expand_pointer(T1), []}
    end;
key_dominates_expanded([], [H2|T2], Level) ->
    case leveled_codec:maybe_reap_expiredkey(H2, Level) of
        true ->
            {skipped_key, [], maybe_expand_pointer(T2)};
        false ->
            {{next_key, H2}, [], maybe_expand_pointer(T2)}
    end;
key_dominates_expanded([H1|T1], [H2|T2], Level) ->
    case leveled_codec:key_dominates(H1, H2) of
        left_hand_first ->
            case leveled_codec:maybe_reap_expiredkey(H1, Level) of
                true ->
                    {skipped_key, maybe_expand_pointer(T1), [H2|T2]};
                false ->
                    {{next_key, H1}, maybe_expand_pointer(T1), [H2|T2]}
            end;
        right_hand_first ->
            case leveled_codec:maybe_reap_expiredkey(H2, Level) of
                true ->
                    {skipped_key, [H1|T1], maybe_expand_pointer(T2)};
                false ->
                    {{next_key, H2}, [H1|T1], maybe_expand_pointer(T2)}
            end;
        left_hand_dominant ->
            {skipped_key, [H1|T1], maybe_expand_pointer(T2)};
        right_hand_dominant ->
            {skipped_key, maybe_expand_pointer(T1), [H2|T2]}
    end.


%% When a list is provided it may include a pointer to gain another batch of
%% entries from the same file, or a new batch of entries from another file
%%
%% This resultant list should include the Tail of any pointers added at the
%% end of the list

maybe_expand_pointer([]) ->
    [];
maybe_expand_pointer([H|Tail]) ->
    case H of
        {next, SFTPid, StartKey} ->
            %% io:format("Scanning further on PID ~w ~w~n", [SFTPid, StartKey]),
            SW = os:timestamp(),
            Acc = sft_getkvrange(SFTPid, StartKey, all, ?MERGE_SCANWIDTH),
            leveled_log:log_timer("SFT14", [SFTPid], SW),
            lists:append(Acc, Tail);
        _ ->
            [H|Tail]
    end.


pointer_append_queryresults(Results, QueryPid) ->
    case Results of
        {complete, Acc} ->
            Acc;
        {partial, Acc, StartKey} ->
            lists:append(Acc, [{next, QueryPid, StartKey}])
    end.


%% The Segment filter is a compressed filter representing the keys in a
%% given slot. The filter is delta-compressed list of integers using rice
%% encoding extended by the reference to each integer having an extra two bits
%% to indicate the block - there are four blocks in each slot.  
%%
%% So each delta is represented as
%% - variable length exponent ending in 0,
%% with 0 representing the exponent of 0,
%% 10 -> 2 ^ 13,
%% 110 -> 2^14,
%% 1110 -> 2^15 etc
%% - 13-bit fixed length remainder
%% - 2-bit block number
%% This gives about 2-bytes per key, with a 1:8000 (approx) false positive
%% ratio (when checking the key by hashing to the segment ID)
%%
%% Before the delta list are three 20-bit integers representing the highest
%% integer in each block.  Plus two bytes to indicate how many hashes
%% there are in the slot
%%
%% To check for the presence of a segment in a slot, roll over the deltas
%% keeping a running total overall and the current highest segment ID seen
%% per block.  Roll all the way through even if matches are found or passed
%% over to confirm that the totals match the expected value (hence creating
%% a natural checksum)
%%
%% The end-result is a 260-byte check for the presence of a key in a slot
%% returning the block in which the segment can be found, which may also be
%% used directly for checking for the presence of segments.
%%
%% This is more space efficient than the equivalent bloom filter and avoids
%% the calculation of many hash functions.

generate_segment_filter([SegL1]) ->
    generate_segment_filter({SegL1, [], [], []});
generate_segment_filter([SegL1, SegL2]) ->
    generate_segment_filter({SegL1, SegL2, [], []});
generate_segment_filter([SegL1, SegL2, SegL3]) ->
    generate_segment_filter({SegL1, SegL2, SegL3, []});
generate_segment_filter([SegL1, SegL2, SegL3, SegL4]) ->
    generate_segment_filter({SegL1, SegL2, SegL3, SegL4});
generate_segment_filter(SegLists) ->
    generate_segment_filter(merge_seglists(SegLists),
                                [],
                                [{0, 0}, {0, 1}, {0, 2}, {0, 3}]).

%% to generate the segment filter needs a sorted list of {Delta, Block} pairs
%% as DeltaList and a list of {TopHash, Block} pairs as TopHashes

generate_segment_filter([], DeltaList, TopHashes) ->
    {lists:reverse(DeltaList), TopHashes};
generate_segment_filter([NextSeg|SegTail], DeltaList, TopHashes) ->
    {TopHash, _} = lists:max(TopHashes),
    {NextSegHash, NextSegBlock} = NextSeg,
    DeltaList2 = [{NextSegHash - TopHash, NextSegBlock}|DeltaList],
    TopHashes2 = lists:keyreplace(NextSegBlock, 2, TopHashes,
                                    {NextSegHash, NextSegBlock}),
    generate_segment_filter(SegTail, DeltaList2, TopHashes2).


serialise_segment_filter({DeltaList, TopHashes}) ->
    TopHashesBin = lists:foldl(fun({X, _}, Acc) ->
                                <<Acc/bitstring, X:20>> end,
                                    <<>>, TopHashes),
    Length = length(DeltaList),
    HeaderBin = <<TopHashesBin/bitstring, Length:16/integer>>,
    {Divisor, Factor} = {?DIVISOR, ?DIVISOR_BITS},
    F = fun({Delta, Block}, Acc) ->
        Exponent = buildexponent(Delta div Divisor),
        Remainder = Delta rem Divisor,
        Block2Bit = Block,
        <<Acc/bitstring,
                Exponent/bitstring, Remainder:Factor/integer,
                Block2Bit:2/integer>> end,
    pad_binary(lists:foldl(F, HeaderBin, DeltaList)).
    

pad_binary(BitString) ->
    Pad  = 8 - bit_size(BitString) rem 8,
    case Pad of
        8 -> BitString;
        _ -> <<BitString/bitstring, 0:Pad/integer>>
    end.

buildexponent(Exponent) ->
	buildexponent(Exponent, <<0:1>>).

buildexponent(0, OutputBits) ->
	OutputBits;
buildexponent(Exponent, OutputBits) ->
	buildexponent(Exponent - 1, <<1:1, OutputBits/bitstring>>).

merge_seglists({SegList1, SegList2, SegList3, SegList4}) ->
    Stage1 = lists:foldl(fun(X, Acc) -> [{X, 0}|Acc] end, [], SegList1),
    Stage2 = lists:foldl(fun(X, Acc) -> [{X, 1}|Acc] end, Stage1, SegList2),
    Stage3 = lists:foldl(fun(X, Acc) -> [{X, 2}|Acc] end, Stage2, SegList3),
    Stage4 = lists:foldl(fun(X, Acc) -> [{X, 3}|Acc] end, Stage3, SegList4),
    lists:sort(Stage4).

hash_for_segmentid(KV) ->
    erlang:phash2(leveled_codec:strip_to_keyonly(KV), ?MAX_SEG_HASH).


%% Check for a given list of segments in the filter, returning in normal
%% operations a TupleList of {SegmentID, [ListOFBlocks]} where the ListOfBlocks
%% are the block IDs which contain keys in that given segment
%%
%% If there is a failure - perhaps due to a bit flip of some sort an error
%% willl be returned (error_so_maybe_present) and all blocks should be checked
%% as the filter cannot be relied upon

check_for_segments(SegFilter, SegmentList, CRCCheck) ->
    case CRCCheck of
        true ->
            <<T0:20/integer, T1:20/integer, T2:20/integer, T3:20/integer,
                Count:16/integer,
                    SegRem/bitstring>> = SegFilter,
            CheckSum = [T0, T1, T2, T3],
            case safecheck_for_segments(SegRem, SegmentList,
                                            [0, 0, 0, 0],
                                            0, Count, []) of
                {error_so_maybe_present, Reason} ->
                    leveled_log:log("SFT11", [Reason]),
                    error_so_maybe_present;
                {OutputCheck, BlockList} when OutputCheck == CheckSum,
                                                BlockList == [] ->
                    not_present;
                {OutputCheck, BlockList} when OutputCheck == CheckSum ->
                    {maybe_present, BlockList};
                {OutputCheck, _} ->
                    leveled_log:log("SFT12", [OutputCheck, CheckSum]),
                    error_so_maybe_present
            end;
        false ->
            <<_:80/bitstring, Count:16/integer, SegRem/bitstring>> = SegFilter,
            case quickcheck_for_segments(SegRem, SegmentList,
                                            lists:max(SegmentList),
                                            0, Count, []) of
                {error_so_maybe_present, Reason} ->
                    leveled_log:log("SFT13", [Reason]),
                    error_so_maybe_present;
                BlockList when BlockList == [] ->
                    not_present;
                BlockList ->
                    {maybe_present, BlockList}
            end
    end.


safecheck_for_segments(_, _, TopHashes, _, 0, BlockList) ->
    {TopHashes, BlockList};
safecheck_for_segments(Filter, SegmentList, TopHs, Acc, Count, BlockList) ->
    case findexponent(Filter) of
        {ok, Exp, FilterRem1} ->
            case findremainder(FilterRem1, ?DIVISOR_BITS) of
                {ok, Remainder, BlockID, FilterRem2} ->
                    {NextHash, BlockList2} = checkhash_forsegments(Acc,
                                                                Exp,
                                                                Remainder,
                                                                SegmentList,
                                                                BlockList,
                                                                BlockID),
                    TopHashes2 = setnth(BlockID, TopHs, NextHash),
                    safecheck_for_segments(FilterRem2, SegmentList,
                                            TopHashes2,
                                            NextHash, Count - 1,
                                            BlockList2);
                error ->
                    {error_so_maybe_present, "Remainder Check"}
            end;
        error ->
            {error_so_maybe_present, "Exponent Check"}
    end.

quickcheck_for_segments(_, _, _, _, 0, BlockList) ->
    BlockList;
quickcheck_for_segments(Filter, SegmentList, MaxSeg, Acc, Count, BlockList) ->
    case findexponent(Filter) of
        {ok, Exp, FilterRem1} ->
            case findremainder(FilterRem1, ?DIVISOR_BITS) of
                {ok, Remainder, BlockID, FilterRem2} ->
                    {NextHash, BlockList2} = checkhash_forsegments(Acc,
                                                                Exp,
                                                                Remainder,
                                                                SegmentList,
                                                                BlockList,
                                                                BlockID),
                    case NextHash > MaxSeg of
                        true ->
                            BlockList2;
                        false ->
                            quickcheck_for_segments(FilterRem2, SegmentList,
                                                        MaxSeg,
                                                        NextHash, Count - 1,
                                                        BlockList2)
                    end;
                error ->
                    {error_so_maybe_present, "Remainder Check"}
            end;
        error ->
            {error_so_maybe_present, "Exponent Check"}
    end.


checkhash_forsegments(Acc, Exp, Remainder, SegmentList, BlockList, BlockID) ->
    NextHash = Acc + ?DIVISOR * Exp + Remainder,
    case lists:member(NextHash, SegmentList) of
        true ->
            {NextHash, [BlockID|BlockList]};
        false ->
            {NextHash, BlockList}
    end.


setnth(0, [_|Rest], New) -> [New|Rest];
setnth(I, [E|Rest], New) -> [E|setnth(I-1, Rest, New)].
    

findexponent(BitStr) ->
	findexponent(BitStr, 0).

findexponent(<<>>, _) -> 
	error; 
findexponent(<<H:1/integer, T/bitstring>>, Acc) ->
	case H of
		1 -> findexponent(T, Acc + 1);
		0 -> {ok, Acc, T}
	end.


findremainder(BitStr, Factor) ->
	case BitStr of 
		<<Remainder:Factor/integer, BlockID:2/integer, Tail/bitstring>> ->
			{ok, Remainder, BlockID, Tail};
		_ ->
			error 
	end.



%%%============================================================================
%%% Test
%%%============================================================================


-ifdef(TEST).

generate_randomkeys({Count, StartSQN}) ->
    generate_randomkeys(Count, StartSQN, []);
generate_randomkeys(Count) ->
    generate_randomkeys(Count, 0, []).

generate_randomkeys(0, _SQN, Acc) ->
    lists:reverse(Acc);
generate_randomkeys(Count, SQN, Acc) ->
    K = {o,
            lists:concat(["Bucket", random:uniform(1024)]),
            lists:concat(["Key", random:uniform(1024)]),
            null},
    RandKey = {K,
                {SQN,
                {active, infinity},
                leveled_codec:magic_hash(K),
                null}},
    generate_randomkeys(Count - 1, SQN + 1, [RandKey|Acc]).
    
generate_sequentialkeys(Count, Start) ->
    generate_sequentialkeys(Count + Start, Start, []).

generate_sequentialkeys(Target, Incr, Acc) when Incr =:= Target ->
    Acc;
generate_sequentialkeys(Target, Incr, Acc) ->
    KeyStr = string:right(integer_to_list(Incr), 8, $0),
    K = {o, "BucketSeq", lists:concat(["Key", KeyStr]), null},
    NextKey = {K,
                {5,
                {active, infinity},
                leveled_codec:magic_hash(K),
                null}},
    generate_sequentialkeys(Target, Incr + 1, [NextKey|Acc]).

simple_create_block_test() ->
    KeyList1 = [{{o, "Bucket1", "Key1", null},
                        {1, {active, infinity}, no_lookup, null}},
                    {{o, "Bucket1", "Key3", null},
                        {2, {active, infinity}, no_lookup, null}}],
    KeyList2 = [{{o, "Bucket1", "Key2", null},
                        {3, {active, infinity}, no_lookup, null}}],
    {Status, BlockKeyList} = create_block(KeyList1,
                                            KeyList2,
                                            #level{level=1}),
    ?assertMatch(partial, Status),
    [H1|T1] = BlockKeyList,
    ?assertMatch({{o, "Bucket1", "Key1", null},
                    {1, {active, infinity}, no_lookup, null}}, H1),
    [H2|T2] = T1,
    ?assertMatch({{o, "Bucket1", "Key2", null},
                    {3, {active, infinity}, no_lookup, null}}, H2),
    ?assertMatch([{{o, "Bucket1", "Key3", null},
                    {2, {active, infinity}, no_lookup, null}}], T2).

dominate_create_block_test() ->
    KeyList1 = [{{o, "Bucket1", "Key1", null},
                        {1, {active, infinity}, no_lookup, null}},
                {{o, "Bucket1", "Key2", null},
                        {2, {active, infinity}, no_lookup, null}}],
    KeyList2 = [{{o, "Bucket1", "Key2", null},
                        {3, {tomb, infinity}, no_lookup, null}}],
    {Status, BlockKeyList} = create_block(KeyList1,
                                            KeyList2,
                                            #level{level=1}),
    ?assertMatch(partial, Status),
    [K1, K2] = BlockKeyList,
    ?assertMatch(K1, lists:nth(1, KeyList1)),
    ?assertMatch(K2, lists:nth(1, KeyList2)).

sample_keylist() ->
    KeyList1 =
        [{{o, "Bucket1", "Key1", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key3", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key5", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key7", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key9", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key1", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key3", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key5", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key7", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key9", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key1", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key3", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key5", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key7", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key9", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket4", "Key1", null}, {1, {active, infinity}, 0, null}}],
    KeyList2 =
        [{{o, "Bucket1", "Key2", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key4", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key6", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key8", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key9a", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key9b", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key9c", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket1", "Key9d", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key2", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key4", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key6", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket2", "Key8", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key2", null}, {1, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key4", null}, {3, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key6", null}, {2, {active, infinity}, 0, null}},
        {{o, "Bucket3", "Key8", null}, {1, {active, infinity}, 0, null}}],
    {KeyList1, KeyList2}.

alternating_create_block_test() ->
    {KeyList1, KeyList2} = sample_keylist(),
    {Status, BlockKeyList} = create_block(KeyList1,
                                            KeyList2,
                                            #level{level=1}),
    BlockSize = length(BlockKeyList),
    ?assertMatch(BlockSize, 32),
    ?assertMatch(all_complete, Status),
    K1 = lists:nth(1, BlockKeyList),
    ?assertMatch(K1, {{o, "Bucket1", "Key1", null}, {1, {active, infinity}, 0, null}}),
    K11 = lists:nth(11, BlockKeyList),
    ?assertMatch(K11, {{o, "Bucket1", "Key9b", null}, {1, {active, infinity}, 0, null}}),
    K32 = lists:nth(32, BlockKeyList),
    ?assertMatch(K32, {{o, "Bucket4", "Key1", null}, {1, {active, infinity}, 0, null}}),
    HKey = {{o, "Bucket1", "Key0", null}, {1, {active, infinity}, 0, null}},
    {Status2, _} = create_block([HKey|KeyList1], KeyList2, #level{level=1}),
    ?assertMatch(block_full, element(1, Status2)).


merge_seglists_test() ->
    SegList1 = [0, 100, 200],
    SegList2 = [50, 200],
    SegList3 = [75, 10000],
    SegList4 = [],
    MergedList = merge_seglists({SegList1, SegList2,
                                    SegList3, SegList4}),
    ?assertMatch(MergedList, [{0, 0}, {50, 1}, {75, 2}, {100, 0},
                                {200, 0}, {200,1}, {10000,2}]),
    SegTerm = generate_segment_filter({SegList1, SegList2,
                                        SegList3, SegList4}),
    ?assertMatch(SegTerm, {[{0, 0}, {50, 1}, {25, 2}, {25, 0},
                                {100, 0}, {0, 1}, {9800, 2}],
    [{200, 0}, {200, 1}, {10000, 2},{0, 3}]}),
    SegBin = serialise_segment_filter(SegTerm),
    ExpectedTopHashes = <<200:20, 200:20, 10000:20, 0:20>>,
    ExpectedDeltas = <<0:1, 0:13, 0:2,
                        0:1, 50:13, 1:2,
                        0:1, 25:13, 2:2,
                        0:1, 25:13, 0:2,
                        0:1, 100:13, 0:2,
                        0:1, 0:13, 1:2,
                        2:2, 1708:13, 2:2>>,
    ExpectedResult = <<ExpectedTopHashes/bitstring,
                            7:16/integer,
                            ExpectedDeltas/bitstring,
                            0:7/integer>>,
    ?assertMatch(SegBin, ExpectedResult),
    R1 = check_for_segments(SegBin, [100], true),
    ?assertMatch(R1,{maybe_present, [0]}),
    R2 = check_for_segments(SegBin, [900], true),
    ?assertMatch(R2, not_present),
    R3 = check_for_segments(SegBin, [200], true),
    ?assertMatch(R3, {maybe_present, [1,0]}),
    R4 = check_for_segments(SegBin, [0,900], true),
    ?assertMatch(R4, {maybe_present, [0]}),
    R5 = check_for_segments(SegBin, [100], false),
    ?assertMatch(R5, {maybe_present, [0]}),
    R6 = check_for_segments(SegBin, [900], false),
    ?assertMatch(R6, not_present),
    R7 = check_for_segments(SegBin, [200], false),
    ?assertMatch(R7, {maybe_present, [1,0]}),
    R8 = check_for_segments(SegBin, [0,900], false),
    ?assertMatch(R8, {maybe_present, [0]}),
    R9 = check_for_segments(SegBin, [1024*1024 - 1], false),
    ?assertMatch(R9, not_present),
    io:format("Try corrupted bloom filter with flipped bit in " ++
                "penultimate delta~n"),
    ExpectedDeltasFlippedBit = <<0:1, 0:13, 0:2,
                                    0:1, 50:13, 1:2,
                                    0:1, 25:13, 2:2,
                                    0:1, 25:13, 0:2,
                                    0:1, 100:13, 0:2,
                                    0:1, 0:13, 1:2,
                                    2:2, 1709:13, 2:2>>,
    SegBin1 = <<ExpectedTopHashes/bitstring,
                            7:16/integer,
                            ExpectedDeltasFlippedBit/bitstring,
                            0:7/integer>>,
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin1, [900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin1, [200], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin1, [0,900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin1, [1024*1024 - 1], true)),
    % This match is before the flipped bit, so still works without CRC check
    ?assertMatch({maybe_present, [0]},
                    check_for_segments(SegBin1, [0,900], false)),
    io:format("Try corrupted bloom filter with flipped bit in " ++
                "final block's top hash~n"),
    ExpectedTopHashesFlippedBit = <<200:20, 200:20, 10000:20, 1:20>>,
    SegBin2 = <<ExpectedTopHashesFlippedBit/bitstring,
                            7:16/integer,
                            ExpectedDeltas/bitstring,
                            0:7/integer>>,
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin2, [900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin2, [200], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin2, [0,900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin2, [1024*1024 - 1], true)),
    % This match is before the flipped bit, so still works without CRC check
    ?assertMatch({maybe_present, [0]},
                    check_for_segments(SegBin2, [0,900], false)),
    
    ExpectedDeltasAll1s = <<4294967295:32/integer>>,
    SegBin3 = <<ExpectedTopHashes/bitstring, ExpectedDeltasAll1s/bitstring>>,
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [200], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [0,900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [1024*1024 - 1], true)),
    % This is so badly mangled, the error gets detected event without CRC
    % checking being enforced
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [900], false)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [200], false)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [0,900], false)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin3, [1024*1024 - 1], false)),
    
    ExpectedDeltasNearlyAll1s = <<4294967287:32/integer>>,
    SegBin4 = <<ExpectedTopHashes/bitstring,
                    ExpectedDeltasNearlyAll1s/bitstring>>,
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [200], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [0,900], true)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [1024*1024 - 1], true)),
    % This is so badly mangled, the error gets detected event without CRC
    % checking being enforced
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [900], false)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [200], false)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [0,900], false)),
    ?assertMatch(error_so_maybe_present,
                    check_for_segments(SegBin4, [1024*1024 - 1], false)).
    
createslot_stage1_test() ->
    {KeyList1, KeyList2} = sample_keylist(),
    {Status, BlockKeyLists} = create_slot(KeyList1, KeyList2, #level{level=1}),
    WState = finalise_slot(BlockKeyLists, #writer{}),
    
    ?assertMatch({o, "Bucket4", "Key1", null}, WState#writer.last_key),
    ?assertMatch(partial, Status),
    
    %% Writer state has the SlotIndex which includes the segment filter
    SegFilter = element(2, lists:nth(1, WState#writer.slot_index)),
    
    R0 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly, {o, "Bucket1", "Key1", null}})],
            true),
    ?assertMatch({maybe_present, [0]}, R0),
    R1 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly, {o, "Bucket1", "Key99", null}})],
            true),
    ?assertMatch(not_present, R1),
    ?assertMatch(1, WState#writer.min_sqn),
    ?assertMatch(3, WState#writer.max_sqn).
    
    
createslot_stage2_test() ->
    {Status, BlockKeyLists} = create_slot(lists:sort(generate_randomkeys(100)),
                                            lists:sort(generate_randomkeys(100)),
                                            #level{level=1}),
    WState = finalise_slot(BlockKeyLists, #writer{}),
    LengthList = element(3, lists:nth(1, WState#writer.slot_index)),
    
    ?assertMatch(full, element(1, Status)),
    Sum1 = lists:sum(LengthList),
    Sum2 = byte_size(WState#writer.slot_binary),
    ?assertMatch(Sum1, Sum2).


createslot_stage3_test() ->
    {Status, BlockKeyLists} = create_slot(lists:sort(generate_sequentialkeys(100, 1)),
                                            lists:sort(generate_sequentialkeys(100, 101)),
                                            #level{level=1}),
    WState = finalise_slot(BlockKeyLists, #writer{}),
    {FirstKey, SegFilter, LengthList} = lists:nth(1, WState#writer.slot_index),
    
    ?assertMatch(full, element(1, Status)),
    Sum1 = lists:sum(LengthList),
    Sum2 = byte_size(WState#writer.slot_binary),
    ?assertMatch(Sum1, Sum2),
    ?assertMatch({o, "BucketSeq", "Key00000001", null}, FirstKey),
    ?assertMatch({o, "BucketSeq", "Key00000128", null}, WState#writer.last_key),
    ?assertMatch([], element(2, Status)),
    Rem = length(element(3, Status)),
    ?assertMatch(Rem, 72),
    R0 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "BucketSeq", "Key00000100", null}})],
            true),
    ?assertMatch({maybe_present, [3]}, R0),
    R1 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "Bucket1", "Key99", null}})],
            true),
    ?assertMatch(not_present, R1),
    R2 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "BucketSeq", "Key00000040", null}})],
            true),
    ?assertMatch({maybe_present, [1]}, R2),
    R3 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "BucketSeq", "Key00000004", null}})],
            true),
    ?assertMatch({maybe_present, [0]}, R3).


initial_create_header_test() ->
    Output = create_header(initial),
    ?assertMatch(?HEADER_LEN, byte_size(Output)).

initial_create_file_test() ->
    Filename = "../test/test1.sft",
    {KL1, KL2} = sample_keylist(),
    {Handle, FileMD} = create_file(Filename),
    {UpdHandle, UpdFileMD, {[], []}} = complete_file(Handle, FileMD,
                                                        KL1, KL2,
                                                        #level{level=1}),
    
    io:format("Slot Index of UpdFileMD ~w~n", [UpdFileMD#state.slot_index]),
    Result1 = fetch_keyvalue(UpdHandle, UpdFileMD, {o, "Bucket1", "Key8", null}),
    ?assertMatch({{o, "Bucket1", "Key8", null},
                            {1, {active, infinity}, 0, null}}, Result1),
    Result2 = fetch_keyvalue(UpdHandle, UpdFileMD, {o, "Bucket1", "Key88", null}),
    ?assertMatch(not_present, Result2),
    ok = file:close(UpdHandle),
    ok = file:delete(Filename).
    
big_create_file_test() ->
    Filename = "../test/bigtest1.sft",
    {KL1, KL2} = {lists:sort(generate_randomkeys(2000)),
                    lists:sort(generate_randomkeys(40000))},
    {InitHandle, InitFileMD} = create_file(Filename),
    {Handle, FileMD, {_KL1Rem, _KL2Rem}} = complete_file(InitHandle,
                                                            InitFileMD,
                                                            KL1, KL2,
                                                            #level{level=1}),
    [{K1, {Sq1, St1, MH1, V1}}|_] = KL1,
    [{K2, {Sq2, St2, MH2, V2}}|_] = KL2,
    Result1 = fetch_keyvalue(Handle, FileMD, K1),
    Result2 = fetch_keyvalue(Handle, FileMD, K2),
    ?assertMatch({K1, {Sq1, St1, MH1, V1}}, Result1),
    ?assertMatch({K2, {Sq2, St2, MH2, V2}}, Result2),
    SubList = lists:sublist(KL2, 1000),
    lists:foreach(fun(KV) ->
                        {Kn, _} = KV,
                        Rn = fetch_keyvalue(Handle, FileMD, Kn),
                        ?assertMatch({Kn, _}, Rn)
                    end,
                    SubList),
    Result3 = fetch_keyvalue(Handle,
                                FileMD,
                                {o, "Bucket1024", "Key1024Alt", null}),
    ?assertMatch(Result3, not_present),
    ok = file:close(Handle),
    ok = file:delete(Filename).

initial_iterator_test() ->
    Filename = "../test/test2.sft",
    {KL1, KL2} = sample_keylist(),
    {Handle, FileMD} = create_file(Filename),
    {UpdHandle, UpdFileMD, {[], []}} = complete_file(Handle, FileMD,
                                                        KL1, KL2,
                                                        #level{level=1}),
    Result1 = fetch_range_keysonly(UpdHandle, UpdFileMD,
                                    {o, "Bucket1", "Key8", null},
                                    {o, "Bucket1", "Key9d", null}),
    io:format("Result returned of ~w~n", [Result1]),
    ?assertMatch({complete,
                        [{{o, "Bucket1", "Key8", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9a", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9b", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9c", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9d", null}, 1, {active, infinity}}
                        ]},
                    Result1),
    Result2 = fetch_range_keysonly(UpdHandle, UpdFileMD,
                                    {o, "Bucket1", "Key8", null},
                                    {o, "Bucket1", "Key9b", null}),
    ?assertMatch({complete,
                        [{{o, "Bucket1", "Key8", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9a", null}, 1, {active, infinity}},
                        {{o, "Bucket1", "Key9b", null}, 1, {active, infinity}}
                        ]},
                    Result2),
    Result3 = fetch_range_keysonly(UpdHandle, UpdFileMD,
                                    {o, "Bucket3", "Key4", null},
                                    all),
    {partial, RL3, _} = Result3,
    ?assertMatch([{{o, "Bucket3", "Key4", null}, 3, {active, infinity}},
                        {{o, "Bucket3", "Key5", null}, 1, {active, infinity}},
                        {{o, "Bucket3", "Key6", null}, 2, {active, infinity}},
                        {{o, "Bucket3", "Key7", null}, 1, {active, infinity}},
                        {{o, "Bucket3", "Key8", null}, 1, {active, infinity}},
                        {{o, "Bucket3", "Key9", null}, 1, {active, infinity}},
                        {{o, "Bucket4", "Key1", null}, 1, {active, infinity}}],
                    RL3),
    ok = file:close(UpdHandle),
    ok = file:delete(Filename).

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


corrupted_sft_test() ->
    Filename = "../test/bigcorrupttest1.sft",
    {KL1, KL2} = {lists:ukeysort(1, generate_randomkeys(2000)), []},
    {InitHandle, InitFileMD} = create_file(Filename),
    {Handle, _FileMD, _Rems} = complete_file(InitHandle,
                                                InitFileMD,
                                                KL1, KL2,
                                                #level{level=1}),
    {ok, Lengths} = file:pread(Handle, 12, 12),
    <<BlocksLength:32/integer,
        IndexLength:32/integer,
        FilterLength:32/integer>> = Lengths,
    ok = file:close(Handle),
    
    {ok, Corrupter} = file:open(Filename , [binary, raw, read, write]),
    lists:foreach(fun(X) ->
                        case X * 5 of
                            Y when Y < FilterLength ->
                                Position = ?HEADER_LEN + X * 5
                                            + BlocksLength + IndexLength,
                                file:pwrite(Corrupter,
                                            Position,
                                            <<0:8/integer>>)
                        end
                        end,
                    lists:seq(1, 100)),
    ok = file:close(Corrupter),
    
    {ok, SFTr, _KeyExtremes} = sft_open(Filename),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, sft_get(SFTr, K))
                        end,
                    KL1),
    ok = sft_clear(SFTr).

big_iterator_test() ->
    Filename = "../test/bigtest1.sft",
    {KL1, KL2} = {lists:sort(generate_randomkeys(10000)), []},
    {InitHandle, InitFileMD} = create_file(Filename),
    {Handle, FileMD, {KL1Rem, KL2Rem}} = complete_file(InitHandle, InitFileMD,
                                                        KL1, KL2,
                                                        #level{level=1}),
    io:format("Remainder lengths are ~w and ~w ~n", [length(KL1Rem),
                                                        length(KL2Rem)]),
    {complete,
        Result1} = fetch_range_keysonly(Handle,
                                            FileMD,
                                            {o, "Bucket0000", "Key0000", null},
                                            {o, "Bucket9999", "Key9999", null},
                                            256),
    NumAddedKeys = 10000 - length(KL1Rem),
    ?assertMatch(NumAddedKeys, length(Result1)),
    {partial,
        Result2,
        _} = fetch_range_keysonly(Handle,
                                    FileMD,
                                    {o, "Bucket0000", "Key0000", null},
                                    {o, "Bucket9999", "Key9999", null},
                                    32),
    ?assertMatch(32 * 128, length(Result2)),
    {partial,
        Result3,
        _} = fetch_range_keysonly(Handle,
                                    FileMD,
                                    {o, "Bucket0000", "Key0000", null},
                                    {o, "Bucket9999", "Key9999", null},
                                    4),
    ?assertMatch(4 * 128, length(Result3)),
    ok = file:close(Handle),
    ok = file:delete(Filename).

hashclash_test() ->
    Filename = "../test/hashclash.sft",
    Key1 = {o, "Bucket", "Key838068", null},
    Key99 = {o, "Bucket", "Key898982", null},
    KeyNF = {o, "Bucket", "Key539122", null},
    ?assertMatch(4, hash_for_segmentid({keyonly, Key1})),
    ?assertMatch(4, hash_for_segmentid({keyonly, Key99})),
    ?assertMatch(4, hash_for_segmentid({keyonly, KeyNF})),
    KeyList = lists:foldl(fun(X, Acc) ->
                                Key = {o,
                                        "Bucket",
                                        "Key8400" ++ integer_to_list(X),
                                        null},
                                Value = {X,
                                            {active, infinity},
                                            leveled_codec:magic_hash(Key),
                                            null},
                                Acc ++ [{Key, Value}] end,
                            [],
                            lists:seq(10,98)),
    KeyListToUse = [{Key1,
                        {1,
                        {active, infinity},
                        leveled_codec:magic_hash(Key1),
                        null}}|KeyList]
                    ++ [{Key99,
                            {99,
                            {active, infinity},
                            leveled_codec:magic_hash(Key99),
                            null}}],
    {InitHandle, InitFileMD} = create_file(Filename),
    {Handle, _FileMD, _Rem} = complete_file(InitHandle, InitFileMD,
                                                KeyListToUse, [],
                                                #level{level=1}),
    ok = file:close(Handle),
    {ok, SFTr, _KeyExtremes} = sft_open(Filename),
    ?assertMatch({Key1,
                        {1, {active, infinity}, _, null}},
                    sft_get(SFTr, Key1)),
    ?assertMatch({Key99,
                        {99, {active, infinity}, _, null}},
                    sft_get(SFTr, Key99)),
    ?assertMatch(not_present,
                    sft_get(SFTr, KeyNF)),
    
    ok = sft_clear(SFTr).

filename_test() ->
    FN1 = "../tmp/filename",
    FN2 = "../tmp/filename.pnd",
    FN3 = "../tmp/subdir/file_name.pend",
    ?assertMatch({"../tmp/filename.pnd", "../tmp/filename.sft"},
                    generate_filenames(FN1)),
    ?assertMatch({"../tmp/filename.pnd", "../tmp/filename.sft"},
                    generate_filenames(FN2)),
    ?assertMatch({"../tmp/subdir/file_name.pnd",
                        "../tmp/subdir/file_name.sft"},
                    generate_filenames(FN3)).

empty_file_test() ->
    {ok, Pid, _Reply} = sft_new("../test/emptyfile.pnd", [], [], 1),
    ?assertMatch(not_present, sft_get(Pid, "Key1")),
    ?assertMatch([], sft_getkvrange(Pid, all, all, 16)),
    ok = sft_clear(Pid).
    

nonsense_coverage_test() ->
    {ok, Pid} = gen_fsm:start(?MODULE, [], []),
    undefined = gen_fsm:sync_send_all_state_event(Pid, nonsense),
    ok = gen_fsm:send_all_state_event(Pid, nonsense),
    ?assertMatch({next_state, reader, #state{}}, handle_info(nonsense,
                                                                reader,
                                                                #state{})),
    ?assertMatch({ok, reader, #state{}}, code_change(nonsense,
                                                        reader,
                                                        #state{},
                                                        nonsense)).

-endif.