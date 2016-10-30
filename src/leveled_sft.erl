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

-behaviour(gen_server).
-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        sft_new/4,
        sft_newfroml0cache/3,
        sft_open/1,
        sft_get/2,
        sft_getkvrange/4,
        sft_close/1,
        sft_clear/1,
        sft_checkready/1,
        sft_setfordelete/2,
        sft_deleteconfirmed/1,
        sft_getmaxsequencenumber/1,
        generate_randomkeys/1]).

-include_lib("eunit/include/eunit.hrl").


-define(WORD_SIZE, 4).
-define(DWORD_SIZE, 8).
-define(CURRENT_VERSION, {0,1}).
-define(SLOT_COUNT, 256).
-define(SLOT_GROUPWRITE_COUNT, 32).
-define(BLOCK_SIZE, 32).
-define(BLOCK_COUNT, 4).
-define(FOOTERPOS_HEADERPOS, 2).
-define(MAX_SEG_HASH, 1048576).
-define(DIVISOR_BITS, 13).
-define(DIVISOR, 8092).
-define(COMPRESSION_LEVEL, 1).
-define(HEADER_LEN, 56).
-define(ITERATOR_SCANWIDTH, 1).
-define(MERGE_SCANWIDTH, 8).
-define(DELETE_TIMEOUT, 10000).
-define(MAX_KEYS, ?SLOT_COUNT * ?BLOCK_COUNT * ?BLOCK_SIZE).
-define(DISCARD_EXT, ".discarded").

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
                background_failure :: tuple(),
                oversized_file = false :: boolean(),
                ready_for_delete = false ::boolean(),
                penciller :: pid()}).


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
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    Reply = gen_server:call(Pid,
                            {sft_new, Filename, KL1, KL2, LevelR},
                            infinity),
    {ok, Pid, Reply}.

sft_newfroml0cache(Filename, L0Cache, Options) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    case Options#sft_options.wait of
        true ->
            KL1 = leveled_pmem:to_list(L0Cache),
            Reply = gen_server:call(Pid,
                                    {sft_new,
                                        Filename,
                                        KL1,
                                        [],
                                        #level{level=0}},
                                    infinity),
            {ok, Pid, Reply};
        false ->
            gen_server:cast(Pid,
                            {sft_newfromcache,
                                Filename,
                                L0Cache,
                                #level{level=0}}),
            {ok, Pid, noreply}
    end.

sft_open(Filename) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    case gen_server:call(Pid, {sft_open, Filename}, infinity) of
        {ok, {SK, EK}} ->
            {ok, Pid, {SK, EK}}
    end.

sft_setfordelete(Pid, Penciller) ->
    gen_server:call(Pid, {set_for_delete, Penciller}, infinity).

sft_get(Pid, Key) ->
    gen_server:call(Pid, {get_kv, Key}, infinity).

sft_getkvrange(Pid, StartKey, EndKey, ScanWidth) ->
    gen_server:call(Pid, {get_kvrange, StartKey, EndKey, ScanWidth}, infinity).

sft_clear(Pid) ->
    gen_server:call(Pid, clear, infinity).

sft_close(Pid) ->
    gen_server:call(Pid, close, infinity).

sft_deleteconfirmed(Pid) ->
    gen_server:cast(Pid, close).

sft_checkready(Pid) ->
    gen_server:call(Pid, background_complete, 20).

sft_getmaxsequencenumber(Pid) ->
    gen_server:call(Pid, get_maxsqn, infinity).



%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({sft_new, Filename, KL1, [], _LevelR=#level{level=L}},
                _From,
                _State) when L == 0 ->
    {ok, State} = create_levelzero(KL1, Filename),
    {reply,
        {{[], []},
            State#state.smallest_key,
            State#state.highest_key},
        State};
handle_call({sft_new, Filename, KL1, KL2, LevelR}, _From, _State) ->
    case create_file(Filename) of
        {Handle, FileMD} ->
            {ReadHandle, UpdFileMD, KeyRemainders} = complete_file(Handle,
                                                                    FileMD,
                                                                    KL1, KL2,
                                                                    LevelR),
            {reply, {KeyRemainders,
                        UpdFileMD#state.smallest_key,
                        UpdFileMD#state.highest_key},
                    UpdFileMD#state{handle=ReadHandle, filename=Filename}}
    end;
handle_call({sft_open, Filename}, _From, _State) ->
    {_Handle, FileMD} = open_file(#state{filename=Filename}),
    io:format("Opened filename with name ~s~n", [Filename]),
    {reply,
        {ok,
            {FileMD#state.smallest_key, FileMD#state.highest_key}},
                FileMD};
handle_call({get_kv, Key}, _From, State) ->
    Reply = fetch_keyvalue(State#state.handle, State, Key),
    statecheck_onreply(Reply, State);
handle_call({get_kvrange, StartKey, EndKey, ScanWidth}, _From, State) ->
    Reply = pointer_append_queryresults(fetch_range_kv(State#state.handle,
                                                        State,
                                                        StartKey,
                                                        EndKey,
                                                        ScanWidth),
                                            self()),
    statecheck_onreply(Reply, State);
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(clear, _From, State) ->
    {stop, normal, ok, State#state{ready_for_delete=true}};
handle_call(background_complete, _From, State) ->
    case State#state.background_complete of
        true ->
            {reply,
                {ok,
                    State#state.filename,
                    State#state.smallest_key,
                    State#state.highest_key},
                State};
        false ->
            {reply, {error, State#state.background_failure}, State}
    end;
handle_call({set_for_delete, Penciller}, _From, State) ->
    io:format("File ~s has been set for delete~n", [State#state.filename]),
    {reply,
        ok,
        State#state{ready_for_delete=true,
                            penciller=Penciller},
        ?DELETE_TIMEOUT};
handle_call(get_maxsqn, _From, State) ->
    statecheck_onreply(State#state.highest_sqn, State).

handle_cast({sft_newfromcache, Filename, L0Cache, _LevelR=#level{level=L}},
                                                        _State) when L == 0->
    SW = os:timestamp(),
    Inp1 = leveled_pmem:to_list(L0Cache),
    {ok, State} = create_levelzero(Inp1, Filename),
    io:format("File creation of L0 file ~s took ~w microseconds~n",
                        [Filename, timer:now_diff(os:timestamp(), SW)]),
    {noreply, State};
handle_cast(close, State) ->
    {stop, normal, State}.

handle_info(timeout, State) ->
    case State#state.ready_for_delete of
        true ->
            io:format("File ~s prompting for delete status check~n",
                        [State#state.filename]),
            ok = leveled_penciller:pcl_confirmdelete(State#state.penciller,
                                                        State#state.filename),
            {noreply, State, ?DELETE_TIMEOUT};
        false ->
            {noreply, State}
    end.

terminate(Reason, State) ->
    io:format("Exit called for reason ~w on filename ~s~n",
                [Reason, State#state.filename]),
    case State#state.ready_for_delete of
        true ->
            io:format("Exit called and now clearing ~s~n",
                        [State#state.filename]),
            ok = file:close(State#state.handle),
            ok = case filelib:is_file(State#state.filename) of
                        true ->
                            file:delete(State#state.filename);
                        false ->
                            ok
                    end;
        _ ->
            case State#state.handle of
                undefined ->
                    ok;
                Handle ->
                    file:close(Handle)
            end
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


statecheck_onreply(Reply, State) ->
    case State#state.ready_for_delete of
        true ->
            {reply, Reply, State, ?DELETE_TIMEOUT};
        false ->
            {reply, Reply, State}
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================


create_levelzero(Inp1, Filename) ->
    ListForFile = case is_list(Inp1) of
                        true ->
                            Inp1;
                        false ->
                            leveled_penciller:roll_into_list(Inp1)
                    end,
    {TmpFilename, PrmFilename} = generate_filenames(Filename),
    case create_file(TmpFilename) of
        {error, Reason} ->
            {error,
                #state{background_complete=false, background_failure=Reason}};
        {Handle, FileMD} ->
            InputSize = length(ListForFile),
            io:format("Creating file with input of size ~w~n", [InputSize]),
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
                                oversized_file=InputSize>?MAX_KEYS}}
    end.


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
    io:format("Opening file with filename ~s~n", [FileName]),
    ok = filelib:ensure_dir(FileName),
    case file:open(FileName, [binary, raw, read, write]) of
        {ok, Handle} ->
            Header = create_header(initial),
            {ok, _} = file:position(Handle, bof),
            ok = file:write(Handle, Header),
            {ok, StartPos} = file:position(Handle, cur),
            FileMD = #state{next_position=StartPos, filename=FileName},
            {Handle, FileMD};
        {error, Reason} ->
            io:format("Error opening filename ~s with reason ~w",
                        [FileName, Reason]),
            {error, Reason}
    end.


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
    {ok, SummaryBin} = file:pread(Handle,
                                    ?HEADER_LEN + Blen + Ilen + Flen, Slen),
    {{LowSQN, HighSQN}, {LowKey, HighKey}} = binary_to_term(SummaryBin),
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
                           handle=Handle}}.
    
%% Take a file handle with a previously created header and complete it based on
%% the two key lists KL1 and KL2
complete_file(Handle, FileMD, KL1, KL2, LevelR) ->
    complete_file(Handle, FileMD, KL1, KL2, LevelR, false).

complete_file(Handle, FileMD, KL1, KL2, LevelR, Rename) ->
    {ok, KeyRemainders} = write_keys(Handle,
                                        maybe_expand_pointer(KL1),
                                        maybe_expand_pointer(KL2),
                                        [], <<>>,
                                        LevelR,
                                        fun sftwrite_function/2),
    {ReadHandle, UpdFileMD} = case Rename of
        false ->
            open_file(FileMD);
        {true, OldName, NewName} ->
            ok = rename_file(OldName, NewName),
            open_file(FileMD#state{filename=NewName})
    end,    
    {ReadHandle, UpdFileMD, KeyRemainders}.

rename_file(OldName, NewName) ->
    io:format("Renaming file from ~s to ~s~n", [OldName, NewName]),
    case filelib:is_file(NewName) of
        true ->
            io:format("Filename ~s already exists~n",
                            [NewName]),
            AltName = filename:join(filename:dirname(NewName),
                                    filename:basename(NewName))
                        ++ ?DISCARD_EXT,
            io:format("Rename rogue filename ~s to ~s~n",
                            [NewName, AltName]),
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
    {_NearestKey, {FilterLen, PointerF},
        {LengthList, PointerB}} = get_nearestkey(FileMD#state.slot_index, Key),
    {ok, SegFilter} = file:pread(Handle,
                                    PointerF + FileMD#state.filter_pointer,
                                    FilterLen),
    SegID = hash_for_segmentid({keyonly, Key}),
    case check_for_segments(SegFilter, [SegID], true) of
        {maybe_present, BlockList} ->
            fetch_keyvalue_fromblock(BlockList,
                                        Key,
                                        LengthList,
                                        Handle,
                                        PointerB + FileMD#state.slots_pointer);
        not_present ->
            not_present;
        error_so_maybe_present ->
            fetch_keyvalue_fromblock(lists:seq(0,length(LengthList)),
                                        Key,
                                        LengthList,
                                        Handle,
                                        PointerB + FileMD#state.slots_pointer)
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
acc_list_keysonly(R, RList) ->
    lists:append(RList, [leveled_codec:strip_to_keyseqstatusonly(R)]).

acc_list_kv(null, empty) ->
    [];
acc_list_kv(null, RList) ->
    RList;
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
    Results = scan_block(Block, StartKey, EndKey, AccFun, Acc),
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
get_nearestkey(KVList, all) ->
    case KVList of
        [] ->
            not_found;
        [H|_Tail] ->
            H
    end;
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
%% be written in groups of 32

write_keys(Handle,
            KL1, KL2,
            SlotIndex, SerialisedSlots,
            LevelR, WriteFun) ->
    write_keys(Handle,
                    KL1, KL2,
                    {0, 0},
                    SlotIndex, SerialisedSlots,
                    {infinity, 0}, null, {last, null},
                    LevelR, WriteFun).


write_keys(Handle,
            KL1, KL2,
            {SlotCount, SlotTotal},
            SlotIndex, SerialisedSlots,
            {LSN, HSN}, LowKey, LastKey,
            LevelR, WriteFun)
                    when SlotCount =:= ?SLOT_GROUPWRITE_COUNT ->
    UpdHandle = WriteFun(slots , {Handle, SerialisedSlots}),
    case maxslots_bylevel(SlotTotal, LevelR#level.level) of
        reached ->
            {complete_keywrite(UpdHandle,
                                SlotIndex,
                                {LSN, HSN}, {LowKey, LastKey},
                                WriteFun),
                {KL1, KL2}};
        continue ->
            write_keys(UpdHandle,
                        KL1, KL2,
                        {0, SlotTotal},
                        SlotIndex, <<>>,
                        {LSN, HSN}, LowKey, LastKey,
                        LevelR, WriteFun)
    end;
write_keys(Handle,
            KL1, KL2,
            {SlotCount, SlotTotal},
            SlotIndex, SerialisedSlots,
            {LSN, HSN}, LowKey, LastKey,
            LevelR, WriteFun) ->
    SlotOutput = create_slot(KL1, KL2, LevelR),
    {{LowKey_Slot, SegFilter, SerialisedSlot, LengthList},
        {{LSN_Slot, HSN_Slot}, LastKey_Slot, Status},
        KL1rem, KL2rem} = SlotOutput,
    UpdSlotIndex = lists:append(SlotIndex,
                                [{LowKey_Slot, SegFilter, LengthList}]),
    UpdSlots = <<SerialisedSlots/binary, SerialisedSlot/binary>>,
    SNExtremes = {min(LSN_Slot, LSN), max(HSN_Slot, HSN)},
    FinalKey = case LastKey_Slot of null -> LastKey; _ -> LastKey_Slot end,
    FirstKey = case LowKey of null -> LowKey_Slot; _ -> LowKey end,
    case Status of
        partial ->
            UpdHandle = WriteFun(slots , {Handle, UpdSlots}),
            {complete_keywrite(UpdHandle,
                                UpdSlotIndex,
                                SNExtremes, {FirstKey, FinalKey},
                                WriteFun),
                {KL1rem, KL2rem}};
        full ->
            write_keys(Handle,
                        KL1rem, KL2rem,
                        {SlotCount + 1, SlotTotal + 1},
                        UpdSlotIndex, UpdSlots,
                        SNExtremes, FirstKey, FinalKey,
                        LevelR, WriteFun);
        complete ->
            UpdHandle = WriteFun(slots , {Handle, UpdSlots}),
            {complete_keywrite(UpdHandle,
                                UpdSlotIndex,
                                SNExtremes, {FirstKey, FinalKey},
                                WriteFun),
                {KL1rem, KL2rem}}
    end.
        

complete_keywrite(Handle, SlotIndex,
                    SNExtremes, {FirstKey, FinalKey},
                    WriteFun) ->
    ConvSlotIndex = convert_slotindex(SlotIndex),
    WriteFun(finalise, {Handle,
                        ConvSlotIndex,
                        SNExtremes,
                        {FirstKey, FinalKey}}).


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
    {SlotFilters, PointerIndex, _FLength, _BLength} = lists:foldl(SlotFun,
                                                            {<<>>, [], 0, 0},
                                                            SlotIndex),
    {SlotFilters, PointerIndex}.

sftwrite_function(slots, {Handle, SerialisedSlots}) ->
    ok = file:write(Handle, SerialisedSlots),
    Handle;
sftwrite_function(finalise,
                    {Handle,
                    {SlotFilters, PointerIndex},
                    SNExtremes,
                    KeyExtremes}) ->
    {ok, Position} = file:position(Handle, cur),
    
    BlocksLength = Position - ?HEADER_LEN,
    Index = term_to_binary(PointerIndex),
    IndexLength = byte_size(Index),
    FilterLength = byte_size(SlotFilters),
    Summary = term_to_binary({SNExtremes, KeyExtremes}),
    SummaryLength = byte_size(Summary),
    %% Write Index, Filter and Summary
    ok = file:write(Handle, <<Index/binary,
                                SlotFilters/binary,
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



%% Take two potentially overlapping lists of keys and output a Block,
%% together with:
%% - block status (full, partial)
%% - the lowest and highest sequence numbers in the block
%% - the list of segment IDs in the block
%% - the remainders of the lists
%% The Key lists must be sorted in key order.  The last key in a list may be
%% a pointer to request more keys for the file (otherwise it is assumed there
%% are no more keys)
%%
%% Level also to be passed in
%% This is either an integer (to be ignored) of {floor, os:timestamp()}
%% if this is the basement level of the LevelDB database and expired keys
%% and tombstone should be reaped


%% Do we need to check here that KeyList1 and KeyList2 are not just a [pointer]
%% Otherwise the pointer will never be expanded
%%
%% Also this should return a partial block if the KeyLists have been exhausted
%% but the block is full

create_block(KeyList1, KeyList2, LevelR) ->
    create_block(KeyList1, KeyList2, [], {infinity, 0}, [], LevelR).

create_block(KeyList1, KeyList2,
                BlockKeyList, {LSN, HSN}, SegmentList, _LevelR)
                                    when length(BlockKeyList)==?BLOCK_SIZE ->
    case {KeyList1, KeyList2} of
        {[], []} ->
            {BlockKeyList, complete, {LSN, HSN}, SegmentList, [], []};
        _ ->
            {BlockKeyList, full, {LSN, HSN}, SegmentList, KeyList1, KeyList2}
    end;
create_block([], [],
                BlockKeyList, {LSN, HSN}, SegmentList, _LevelR) ->
    {BlockKeyList, partial, {LSN, HSN}, SegmentList, [], []};
create_block(KeyList1, KeyList2,
                BlockKeyList, {LSN, HSN}, SegmentList, LevelR) ->
    case key_dominates(KeyList1,
                        KeyList2,
                        {LevelR#level.is_basement, LevelR#level.timestamp}) of
        {{next_key, TopKey}, Rem1, Rem2} ->
            {UpdLSN, UpdHSN} = update_sequencenumbers(TopKey, LSN, HSN),
            NewBlockKeyList = lists:append(BlockKeyList,
                                            [TopKey]),
            NewSegmentList = lists:append(SegmentList,
                                            [hash_for_segmentid(TopKey)]), 
            create_block(Rem1, Rem2,
                            NewBlockKeyList, {UpdLSN, UpdHSN},
                            NewSegmentList, LevelR);
        {skipped_key, Rem1, Rem2} ->
            create_block(Rem1, Rem2,
                            BlockKeyList, {LSN, HSN},
                            SegmentList, LevelR)
    end.



%% Should return an index entry in the Slot Index.  Each entry consists of:
%% - Start Key
%% - SegmentIDFilter for the  (will eventually be replaced with a pointer)
%% - Serialised Slot (will eventually be replaced with a pointer)
%% - Length for each Block within the Serialised Slot
%% Additional information will also be provided
%% - {Low Seq Number, High Seq Number} within the slot
%% - End Key
%% - Whether the slot is full or partially filled
%% - Remainder of any KeyLists used to make the slot


create_slot(KeyList1, KeyList2, Level)  ->
    create_slot(KeyList1, KeyList2, Level, ?BLOCK_COUNT, [], <<>>, [],
                                    {null, infinity, 0, null, full}).

%% Keep adding blocks to the slot until either the block count is reached or
%% there is a partial block

create_slot(KL1, KL2, _, 0, SegLists, SerialisedSlot, LengthList,
                                    {LowKey, LSN, HSN, LastKey, Status}) ->
    {{LowKey, generate_segment_filter(SegLists), SerialisedSlot, LengthList},
        {{LSN, HSN}, LastKey, Status},
        KL1, KL2};
create_slot(KL1, KL2, _, _, SegLists, SerialisedSlot, LengthList,
                                    {LowKey, LSN, HSN, LastKey, partial}) ->
    {{LowKey, generate_segment_filter(SegLists), SerialisedSlot, LengthList},
        {{LSN, HSN}, LastKey, partial},
        KL1, KL2};
create_slot(KL1, KL2, _, _, SegLists, SerialisedSlot, LengthList,
                                    {LowKey, LSN, HSN, LastKey, complete}) ->
    {{LowKey, generate_segment_filter(SegLists), SerialisedSlot, LengthList},
        {{LSN, HSN}, LastKey, partial},
        KL1, KL2};
create_slot(KL1, KL2, LevelR, BlockCount, SegLists, SerialisedSlot, LengthList,
                                    {LowKey, LSN, HSN, LastKey, _Status}) ->
    {BlockKeyList, Status,
        {LSNb, HSNb},
        SegmentList, KL1b, KL2b} = create_block(KL1, KL2, LevelR),
    TrackingMetadata = case {LowKey, BlockKeyList} of
        {null, []} ->
            {null, LSN, HSN, LastKey, Status};
        {null, _} ->
            [NewLowKeyV|_] = BlockKeyList,
            {leveled_codec:strip_to_keyonly(NewLowKeyV),
                min(LSN, LSNb), max(HSN, HSNb),
                leveled_codec:strip_to_keyonly(last(BlockKeyList,
                                                    {last, LastKey})),
                Status};
        {_, _} ->
            {LowKey,
                min(LSN, LSNb), max(HSN, HSNb),
                leveled_codec:strip_to_keyonly(last(BlockKeyList,
                                                    {last, LastKey})),
                Status}
    end,
    SerialisedBlock = serialise_block(BlockKeyList),
    BlockLength = byte_size(SerialisedBlock),
    SerialisedSlot2 = <<SerialisedSlot/binary, SerialisedBlock/binary>>,
    create_slot(KL1b, KL2b, LevelR, BlockCount - 1, SegLists ++ [SegmentList],
                SerialisedSlot2, LengthList ++ [BlockLength],
                TrackingMetadata).


last([], {last, LastKey}) -> {keyonly, LastKey};
last([E|Es], PrevLast) -> last(E, Es, PrevLast).

last(_, [E|Es], PrevLast) -> last(E, Es, PrevLast);
last(E, [], _) -> E.

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
            Acc = sft_getkvrange(SFTPid, StartKey, all, ?MERGE_SCANWIDTH),
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

    
%% Update the sequence numbers
update_sequencenumbers(Item, LSN, HSN) when is_tuple(Item) ->
    update_sequencenumbers(leveled_codec:strip_to_seqonly(Item), LSN, HSN);    
update_sequencenumbers(SN, 0, 0) ->
    {SN, SN};
update_sequencenumbers(SN, LSN, HSN) when SN < LSN ->
    {SN, HSN};
update_sequencenumbers(SN, LSN, HSN) when SN > HSN ->
    {LSN, SN};
update_sequencenumbers(_SN, LSN, HSN) ->
    {LSN, HSN}.


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
                    io:format("Segment filter failed due to ~s~n", [Reason]),
                    error_so_maybe_present;
                {OutputCheck, BlockList} when OutputCheck == CheckSum,
                                                BlockList == [] ->
                    not_present;
                {OutputCheck, BlockList} when OutputCheck == CheckSum ->
                    {maybe_present, BlockList};
                {OutputCheck, _} ->
                    io:format("Segment filter failed due to CRC check~n
                                    ~w did not match ~w~n",
                            [OutputCheck, CheckSum]),
                    error_so_maybe_present
            end;
        false ->
            <<_:80/bitstring, Count:16/integer, SegRem/bitstring>> = SegFilter,
            case quickcheck_for_segments(SegRem, SegmentList,
                                            lists:max(SegmentList),
                                            0, Count, []) of
                {error_so_maybe_present, Reason} ->
                    io:format("Segment filter failed due to ~s~n", [Reason]),
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
    RandKey = {{o,
                    lists:concat(["Bucket", random:uniform(1024)]),
                    lists:concat(["Key", random:uniform(1024)]),
                    null},
                {SQN,
                {active, infinity}, null}},
    generate_randomkeys(Count - 1, SQN + 1, [RandKey|Acc]).
    
generate_sequentialkeys(Count, Start) ->
    generate_sequentialkeys(Count + Start, Start, []).

generate_sequentialkeys(Target, Incr, Acc) when Incr =:= Target ->
    Acc;
generate_sequentialkeys(Target, Incr, Acc) ->
    KeyStr = string:right(integer_to_list(Incr), 8, $0),
    NextKey = {{o,
                    "BucketSeq",
                    lists:concat(["Key", KeyStr]),
                    null},
                {5,
                {active, infinity}, null}},
    generate_sequentialkeys(Target, Incr + 1, [NextKey|Acc]).

simple_create_block_test() ->
    KeyList1 = [{{o, "Bucket1", "Key1", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key3", null}, {2, {active, infinity}, null}}],
    KeyList2 = [{{o, "Bucket1", "Key2", null}, {3, {active, infinity}, null}}],
    {MergedKeyList, ListStatus, SN, _, _, _} = create_block(KeyList1,
                                                            KeyList2,
                                                            #level{level=1}),
    ?assertMatch(partial, ListStatus),
    [H1|T1] = MergedKeyList,
    ?assertMatch(H1, {{o, "Bucket1", "Key1", null}, {1, {active, infinity}, null}}),
    [H2|T2] = T1,
    ?assertMatch(H2, {{o, "Bucket1", "Key2", null}, {3, {active, infinity}, null}}),
    ?assertMatch(T2, [{{o, "Bucket1", "Key3", null}, {2, {active, infinity}, null}}]),
    ?assertMatch(SN, {1,3}).

dominate_create_block_test() ->
    KeyList1 = [{{o, "Bucket1", "Key1", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key2", null}, {2, {active, infinity}, null}}],
    KeyList2 = [{{o, "Bucket1", "Key2", null}, {3, {tomb, infinity}, null}}],
    {MergedKeyList, ListStatus, SN, _, _, _} = create_block(KeyList1,
                                                            KeyList2,
                                                            #level{level=1}),
    ?assertMatch(partial, ListStatus),
    [K1, K2] = MergedKeyList,
    ?assertMatch(K1, {{o, "Bucket1", "Key1", null}, {1, {active, infinity}, null}}),
    ?assertMatch(K2, {{o, "Bucket1", "Key2", null}, {3, {tomb, infinity}, null}}),
    ?assertMatch(SN, {1,3}).

sample_keylist() ->
    KeyList1 = [{{o, "Bucket1", "Key1", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key3", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key5", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key7", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key9", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key1", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key3", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key5", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key7", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key9", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key1", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key3", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key5", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key7", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key9", null}, {1, {active, infinity}, null}},
    {{o, "Bucket4", "Key1", null}, {1, {active, infinity}, null}}],
    KeyList2 = [{{o, "Bucket1", "Key2", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key4", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key6", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key8", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key9a", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key9b", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key9c", null}, {1, {active, infinity}, null}},
    {{o, "Bucket1", "Key9d", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key2", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key4", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key6", null}, {1, {active, infinity}, null}},
    {{o, "Bucket2", "Key8", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key2", null}, {1, {active, infinity}, null}},
    {{o, "Bucket3", "Key4", null}, {3, {active, infinity}, null}},
    {{o, "Bucket3", "Key6", null}, {2, {active, infinity}, null}},
    {{o, "Bucket3", "Key8", null}, {1, {active, infinity}, null}}],
    {KeyList1, KeyList2}.

alternating_create_block_test() ->
    {KeyList1, KeyList2} = sample_keylist(),
    {MergedKeyList, ListStatus, _, _, _, _} = create_block(KeyList1,
                                                            KeyList2,
                                                            #level{level=1}),
    BlockSize = length(MergedKeyList),
    ?assertMatch(BlockSize, 32),
    ?assertMatch(ListStatus, complete),
    K1 = lists:nth(1, MergedKeyList),
    ?assertMatch(K1, {{o, "Bucket1", "Key1", null}, {1, {active, infinity}, null}}),
    K11 = lists:nth(11, MergedKeyList),
    ?assertMatch(K11, {{o, "Bucket1", "Key9b", null}, {1, {active, infinity}, null}}),
    K32 = lists:nth(32, MergedKeyList),
    ?assertMatch(K32, {{o, "Bucket4", "Key1", null}, {1, {active, infinity}, null}}),
    HKey = {{o, "Bucket1", "Key0", null}, {1, {active, infinity}, null}},
    {_, ListStatus2, _, _, _, _} = create_block([HKey|KeyList1],
                                                    KeyList2,
                                                    #level{level=1}),
    ?assertMatch(ListStatus2, full).


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
    Out = create_slot(KeyList1, KeyList2, #level{level=1}),
    {{LowKey, SegFilter, _SerialisedSlot, _LengthList},
        {{LSN, HSN}, LastKey, Status},
        KL1, KL2} = Out,
    ?assertMatch(LowKey, {o, "Bucket1", "Key1", null}),
    ?assertMatch(LastKey, {o, "Bucket4", "Key1", null}),
    ?assertMatch(Status, partial),
    ?assertMatch(KL1, []),
    ?assertMatch(KL2, []),
    R0 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly, {o, "Bucket1", "Key1", null}})],
            true),
    ?assertMatch(R0, {maybe_present, [0]}),
    R1 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly, {o, "Bucket1", "Key99", null}})],
            true),
    ?assertMatch(R1, not_present),
    ?assertMatch(LSN, 1),
    ?assertMatch(HSN, 3).
    
createslot_stage2_test() ->
    Out = create_slot(lists:sort(generate_randomkeys(100)),
                        lists:sort(generate_randomkeys(100)),
                        #level{level=1}),
    {{_LowKey, _SegFilter, SerialisedSlot, LengthList},
        {{_LSN, _HSN}, _LastKey, Status},
        _KL1, _KL2} = Out,
    ?assertMatch(Status, full),
    Sum1 = lists:foldl(fun(X, Sum) -> Sum + X end, 0, LengthList),
    Sum2 = byte_size(SerialisedSlot),
    ?assertMatch(Sum1, Sum2).


createslot_stage3_test() ->
    Out = create_slot(lists:sort(generate_sequentialkeys(100, 1)),
                        lists:sort(generate_sequentialkeys(100, 101)),
                        #level{level=1}),
    {{LowKey, SegFilter, SerialisedSlot, LengthList},
        {{_LSN, _HSN}, LastKey, Status},
        KL1, KL2} = Out,
    ?assertMatch(Status, full),
    Sum1 = lists:foldl(fun(X, Sum) -> Sum + X end, 0, LengthList),
    Sum2 = byte_size(SerialisedSlot),
    ?assertMatch(Sum1, Sum2),
    ?assertMatch(LowKey, {o, "BucketSeq", "Key00000001", null}),
    ?assertMatch(LastKey, {o, "BucketSeq", "Key00000128", null}),
    ?assertMatch(KL1, []),
    Rem = length(KL2),
    ?assertMatch(Rem, 72),
    R0 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "BucketSeq", "Key00000100", null}})],
            true),
    ?assertMatch(R0, {maybe_present, [3]}),
    R1 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "Bucket1", "Key99", null}})],
            true),
    ?assertMatch(R1, not_present),
    R2 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "BucketSeq", "Key00000040", null}})],
            true),
    ?assertMatch(R2, {maybe_present, [1]}),
    R3 = check_for_segments(serialise_segment_filter(SegFilter),
            [hash_for_segmentid({keyonly,
                                    {o, "BucketSeq", "Key00000004", null}})],
            true),
    ?assertMatch(R3, {maybe_present, [0]}).



testwrite_function(slots, {Handle, SerialisedSlots}) ->
    lists:append(Handle, [SerialisedSlots]);
testwrite_function(finalise, {Handle, C_SlotIndex, SNExtremes, KeyExtremes}) ->
    {Handle, C_SlotIndex, SNExtremes, KeyExtremes}.

writekeys_stage1_test() ->
    {KL1, KL2} = sample_keylist(),
    {FunOut, {_KL1Rem, _KL2Rem}} = write_keys([],
                                                KL1, KL2,
                                                [], <<>>,
                                                #level{level=1},
                                                fun testwrite_function/2),
    {Handle, {_, PointerIndex}, SNExtremes, KeyExtremes} = FunOut,
    ?assertMatch(SNExtremes, {1,3}),
    ?assertMatch(KeyExtremes, {{o, "Bucket1", "Key1", null},
                                {o, "Bucket4", "Key1", null}}),
    [TopIndex|[]] = PointerIndex,
    {TopKey, _SegFilter,  {LengthList, _Total}} = TopIndex,
    ?assertMatch(TopKey, {o, "Bucket1", "Key1", null}),
    TotalLength = lists:foldl(fun(X, Acc) -> Acc + X end,
                                0, LengthList),
    ActualLength = lists:foldl(fun(X, Acc) -> Acc + byte_size(X) end,
                                0, Handle),
    ?assertMatch(TotalLength, ActualLength).

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
    Result1 = fetch_keyvalue(UpdHandle, UpdFileMD, {o, "Bucket1", "Key8", null}),
    io:format("Result is ~w~n", [Result1]),
    ?assertMatch(Result1, {{o, "Bucket1", "Key8", null},
                            {1, {active, infinity}, null}}),
    Result2 = fetch_keyvalue(UpdHandle, UpdFileMD, {o, "Bucket1", "Key88", null}),
    io:format("Result is ~w~n", [Result2]),
    ?assertMatch(Result2, not_present),
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
    [{K1, {Sq1, St1, V1}}|_] = KL1,
    [{K2, {Sq2, St2, V2}}|_] = KL2,
    Result1 = fetch_keyvalue(Handle, FileMD, K1),
    Result2 = fetch_keyvalue(Handle, FileMD, K2),
    ?assertMatch(Result1, {K1, {Sq1, St1, V1}}),
    ?assertMatch(Result2, {K2, {Sq2, St2, V2}}),
    SubList = lists:sublist(KL2, 1000),
    FailedFinds = lists:foldl(fun(K, Acc) ->
                                    {Kn, {_, _, _}} = K,
                                    Rn = fetch_keyvalue(Handle, FileMD, Kn),
                                    case Rn of
                                        {Kn, {_, _, _}} ->
                                            Acc;
                                        _ ->
                                            Acc + 1
                                    end
                                end,
                                0,
                                SubList),
    io:format("FailedFinds of ~w~n", [FailedFinds]),
    ?assertMatch(FailedFinds, 0),
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
    KV1 = {{o, "Bucket", "Key1", null}, {5, {active, infinity}, []}},
    KV2 = {{o, "Bucket", "Key3", null}, {6, {active, infinity}, []}},
    KV3 = {{o, "Bucket", "Key2", null}, {3, {active, infinity}, []}},
    KV4 = {{o, "Bucket", "Key4", null}, {7, {active, infinity}, []}},
    KV5 = {{o, "Bucket", "Key1", null}, {4, {active, infinity}, []}},
    KV6 = {{o, "Bucket", "Key1", null}, {99, {tomb, 999}, []}},
    KV7 = {{o, "Bucket", "Key1", null}, {99, tomb, []}},
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


big_iterator_test() ->
    Filename = "../test/bigtest1.sft",
    {KL1, KL2} = {lists:sort(generate_randomkeys(10000)), []},
    {InitHandle, InitFileMD} = create_file(Filename),
    {Handle, FileMD, {KL1Rem, KL2Rem}} = complete_file(InitHandle, InitFileMD,
                                                        KL1, KL2,
                                                        #level{level=1}),
    io:format("Remainder lengths are ~w and ~w ~n", [length(KL1Rem), length(KL2Rem)]),
    {complete, Result1} = fetch_range_keysonly(Handle,
                                                    FileMD,
                                                    {o, "Bucket0000", "Key0000", null},
                                                    {o, "Bucket9999", "Key9999", null},
                                                    256),
    NumFoundKeys1 = length(Result1),
    NumAddedKeys = 10000 - length(KL1Rem),
    ?assertMatch(NumFoundKeys1, NumAddedKeys),
    {partial, Result2, _} = fetch_range_keysonly(Handle,
                                                    FileMD,
                                                    {o, "Bucket0000", "Key0000", null},
                                                    {o, "Bucket9999", "Key9999", null},
                                                    32),
    ?assertMatch(32 * 128, length(Result2)),
    {partial, Result3, _} = fetch_range_keysonly(Handle,
                                                    FileMD,
                                                    {o, "Bucket0000", "Key0000", null},
                                                    {o, "Bucket9999", "Key9999", null},
                                                    4),
    ?assertMatch(4 * 128, length(Result3)),
    ok = file:close(Handle),
    ok = file:delete(Filename).

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

-endif.