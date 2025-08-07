%% -------- CDB File Clerk ---------
%%
%% This is a modified version of the cdb module provided by Tom Whitcomb.
%%
%% - https://github.com/thomaswhitcomb/erlang-cdb
%%
%% The CDB module is an implementation of the constant database format
%% described by DJ Bernstein
%%
%% - https://cr.yp.to/cdb.html
%%
%% The primary differences are:
%% - Support for incrementally writing a CDB file while keeping the hash table
%% in memory
%% - The ability to scan a database in blocks of sequence numbers
%% - The applictaion of a CRC check by default to all values
%%
%% Because of the final delta - this is incompatible with standard CDB files
%% (in that you won't be able to fetch values if the file was written by
%% another CDB writer as the CRC check is missing)
%%
%% This module provides functions to create and query a CDB (constant database).
%% A CDB implements a two-level hashtable which provides fast {key,value}
%% lookups that remain fairly constant in speed regardless of the CDBs size.
%%
%% The first level in the CDB occupies the first 255 doublewords in the file.
%% Each doubleword slot contains two values.  The first is a file pointer to
%% the primary hashtable (at the end of the file) and the second value is the
%% number of entries in the hashtable.  The first level table of 255 entries
%% is indexed with the lower eight bits of the hash of the input key.
%%
%% Following the 255 doublewords are the {key,value} tuples.  The tuples are
%% packed in the file without regard to word boundaries.  Each {key,value}
%% tuple is represented with a four byte key length, a four byte value length,
%% the actual key value followed by the actual value.
%%
%% Following the {key,value} tuples are the primary hash tables.  There are
%% at most 255 hash tables.  Each hash table is referenced by one of the 255
%% doubleword entries at the top of the file. For efficiency reasons, each
%% hash table is allocated twice the number of entries that it will need.
%% Each entry in the hash table is a doubleword.
%% The first word is the corresponding hash value and the second word is a
%% file pointer to the actual {key,value} tuple higher in the file.
%%

-module(leveled_cdb).

-behaviour(gen_statem).
-include("leveled.hrl").

-export([init/1,
            callback_mode/0,
            terminate/3,
            code_change/4]).

%% states
-export([starting/3,
            writer/3,
            rolling/3,
            reader/3,
            delete_pending/3]).

-export([cdb_open_writer/1,
            cdb_open_writer/2,
            cdb_open_reader/1,
            cdb_open_reader/2,
            cdb_reopen_reader/3,
            cdb_get/2,
            cdb_put/3,
            cdb_put/4,
            cdb_mput/2,
            cdb_getpositions/2,
            cdb_directfetch/3,
            cdb_lastkey/1,
            cdb_firstkey/1,
            cdb_filename/1,
            cdb_keycheck/2,
            cdb_scan/4,
            cdb_close/1,
            cdb_complete/1,
            cdb_roll/1,
            cdb_returnhashtable/3,
            cdb_checkhashtable/1,
            cdb_destroy/1,
            cdb_deletepending/1,
            cdb_deletepending/3,
            cdb_isrolling/1,
            cdb_clerkcomplete/1,
            cdb_getcachedscore/2,
            cdb_putcachedscore/2,
            cdb_deleteconfirmed/1]).

-export([finished_rolling/1,
            hashtable_calc/2]).

-define(DWORD_SIZE, 8).
-define(MAX_FILE_SIZE, 3221225472).
-define(BINARY_MODE, false).
-define(BASE_POSITION, 2048).
-define(WRITE_OPS, [binary, raw, read, write]).
-define(DELETE_TIMEOUT, 10000).
-define(GETPOS_FACTOR, 8).
-define(MAX_OBJECT_SIZE, 1000000000).
    % 1GB but really should be much smaller than this
-define(MEGA, 1000000).
-define(CACHE_LIFE, 86400).

-record(state, {hashtree,
                last_position :: integer() | undefined,
                    % defined when writing, not required once rolled
                last_key = empty,
                current_count = 0 :: non_neg_integer(),
                hash_index = {} :: tuple(),
                filename :: string() | undefined,
                    % defined when starting
                handle :: file:io_device() | undefined,
                    % defined when starting
                max_size :: pos_integer(),
                max_count :: pos_integer(),
                binary_mode = false :: boolean(),
                delete_point = 0 :: integer(),
                inker :: pid() | undefined,
                    % undefined until delete_pending
                deferred_delete = false :: boolean(),
                waste_path :: string()|undefined,
                    % undefined has functional meaning
                    % - no sending to waste on delete
                sync_strategy = none,
                log_options = leveled_log:get_opts()
                    :: leveled_log:log_options(),
                cached_score :: {float(), erlang:timestamp()}|undefined,
                monitor = {no_monitor, 0} :: leveled_monitor:monitor()}).

-type cdb_options() :: #cdb_options{}.
-type hashtable_index() :: tuple().
-type file_location() :: integer()|eof.
-type filter_fun() ::
        fun((any(),
                binary(),
                integer(),
                term()|{term(), term()},
                fun((binary()) -> any())) -> {stop|loop, any()}).

-export_type([filter_fun/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec cdb_open_writer(string()) -> {ok, pid()}.
%% @doc
%% Open a file for writing using default options
cdb_open_writer(Filename) ->
    %% No options passed
    cdb_open_writer(Filename, #cdb_options{binary_mode=true}).

-spec cdb_open_writer(string(), cdb_options()) -> {ok, pid()}.
%% @doc
%% The filename should be a full file system reference to an existing CDB
%% file, and it will be opened and a FSM started to manage the file - with the
%% hashtree cached in memory (the file will need to be scanned to build the
%% hashtree)
cdb_open_writer(Filename, Opts) ->
    {ok, Pid} = gen_statem:start_link(?MODULE, [Opts], []),
    ok = gen_statem:call(Pid, {open_writer, Filename}, infinity),
    {ok, Pid}.

-spec cdb_reopen_reader(string(), binary(), cdb_options()) -> {ok, pid()}.
%% @doc
%% Open an existing file that has already been moved into read-only mode. The
%% LastKey should be known, as it has been stored in the manifest.  Knowing the
%% LastKey stops the file from needing to be scanned on start-up to discover
%% the LastKey.
%%
%% The LastKey is the Key of the last object added to the file - and is used to
%% determine when scans over a file have completed.
cdb_reopen_reader(Filename, LastKey, CDBopts) ->
    {ok, Pid} =
        gen_statem:start_link(?MODULE,
                              [CDBopts#cdb_options{binary_mode=true}],
                              []),
    ok = gen_statem:call(Pid,
                         {open_reader, Filename, LastKey},
                         infinity),
    {ok, Pid}.

-spec cdb_open_reader(string()) -> {ok, pid()}.
%% @doc
%% Open an existing file that has already been moved into read-only mode.
%% Don't use this if the LastKey is known, as this requires an expensive scan
%% to discover the LastKey.
cdb_open_reader(Filename) ->
    cdb_open_reader(Filename, #cdb_options{binary_mode=true}).

-spec cdb_open_reader(string(), #cdb_options{}) -> {ok, pid()}.
%% @doc
%% Open an existing file that has already been moved into read-only mode.
%% Don't use this if the LastKey is known, as this requires an expensive scan
%% to discover the LastKey.
%% Allows non-default cdb_options to be passed
cdb_open_reader(Filename, Opts) ->
    {ok, Pid} = gen_statem:start_link(?MODULE, [Opts], []),
    ok = gen_statem:call(Pid, {open_reader, Filename}, infinity),
    {ok, Pid}.

-spec cdb_get(pid(), any()) -> {any(), any()}|missing.
%% @doc
%% Extract a Key and Value from a CDB file by passing in a Key.
cdb_get(Pid, Key) ->
    gen_statem:call(Pid, {get_kv, Key}, infinity).

-spec cdb_put(pid(), any(), any()) -> ok|roll.
%% @doc
%% Put a key and value into a cdb file that is open as a writer, will fail
%% if the FSM is in any other state.
%%
%% Response can be roll - if there is no space to put this value in the file.
%% It is assumed that the response to a "roll" will be to roll the file, which
%% will close this file for writing after persisting the hashtree.
cdb_put(Pid, Key, Value) ->
    cdb_put(Pid, Key, Value, false).

-spec cdb_put(pid(), any(), any(), boolean()) -> ok|roll.
%% @doc
%% See cdb_put/3.  Addition of force-sync option, to be used when sync mode is
%% none to force a sync to disk on this particlar put.
cdb_put(Pid, Key, Value, Sync) ->
    gen_statem:call(Pid, {put_kv, Key, Value, Sync}, infinity).

-spec cdb_mput(pid(), list()) -> ok|roll.
%% @doc
%% Add multiple keys and values in one call.  The file will request a roll if
%% all of the keys and values cnanot be written (and in this case none of them
%% will).  Mput is an all_or_nothing operation.
%%
%% It may be preferable to respond to roll by trying individual PUTs until
%% roll is returned again
cdb_mput(Pid, KVList) ->
    gen_statem:call(Pid, {mput_kv, KVList}, infinity).

-spec cdb_getpositions(pid(), integer()|all) -> list().
%% @doc
%% Get the positions in the file of a random sample of Keys.  cdb_directfetch
%% can then be used to fetch those keys.  SampleSize can be an integer or the
%% atom all.  To be used for sampling queries, for example to assess the
%% potential for compaction.
cdb_getpositions(Pid, SampleSize) ->
    % Getting many positions from the index, especially getting all positions
    % can take time (about 1s for all positions).  Rather than queue all
    % requests waiting for this to complete, loop over each of the 256 indexes
    % outside of the FSM processing loop - to allow for other messages to be
    % interleaved
    case SampleSize of
        all ->
            FoldFun =
                fun(Index, Acc) ->
                    PosList = cdb_getpositions_fromidx(Pid, all, Index, []),
                    lists:merge(Acc, lists:sort(PosList))
                end,
            IdxList = lists:seq(0, 255),
            lists:foldl(FoldFun, [], IdxList);
        S0 ->
            FC = ?GETPOS_FACTOR * S0,
            FoldFun =
                fun({_R, Index}, Acc) ->
                    case length(Acc) of
                        FC ->
                            Acc;
                        L when L < FC ->
                            cdb_getpositions_fromidx(Pid, FC, Index, Acc)
                    end
                end,
            RandFun = fun(X) -> {rand:uniform(), X} end,
            SeededL = lists:map(RandFun, lists:seq(0, 255)),
            SortedL = lists:keysort(1, SeededL),
            PosList0 = lists:foldl(FoldFun, [], SortedL),
            P1 = rand:uniform(max(1, length(PosList0) - S0)),
            lists:sublist(lists:sort(PosList0), P1, S0)
    end.

cdb_getpositions_fromidx(Pid, SampleSize, Index, Acc) ->
    gen_statem:call(Pid,
                    {get_positions, SampleSize, Index, Acc}, infinity).

-spec cdb_directfetch(pid(), list(), key_only|key_size|key_value_check) ->
                                                                        list().
%% @doc
%% Info can be key_only, key_size (size being the size of the value) or
%% key_value_check (with the check part indicating if the CRC is correct for
%% the value)
cdb_directfetch(Pid, PositionList, Info) ->
    gen_statem:call(Pid, {direct_fetch, PositionList, Info}, infinity).

-spec cdb_close(pid()) -> ok.
%% @doc
%% RONSEAL
cdb_close(Pid) ->
    gen_statem:call(Pid, cdb_close, infinity).

-spec cdb_deleteconfirmed(pid()) -> ok.
%% @doc
%% Delete has been confirmed, so close (state should be delete_pending)
cdb_deleteconfirmed(Pid) ->
    gen_statem:cast(Pid, delete_confirmed).

-spec cdb_complete(pid()) -> {ok, string()}.
%% @doc
%% Persists the hashtable to the end of the file, to close it for further
%% writing then exit.  Returns the filename that was saved.
cdb_complete(Pid) ->
    gen_statem:call(Pid, cdb_complete, infinity).

-spec cdb_roll(pid()) -> ok.
%% @doc
%% Persists the hashtable to the end of the file, to close it for further
%% writing but do not exit, this will continue to service requests in the
%% rolling state whilst the hashtable is being written, and will become a
%% reader (read-only) CDB file process on completion
cdb_roll(Pid) ->
    gen_statem:cast(Pid, cdb_roll).

-spec cdb_returnhashtable(pid(), list(), binary()) -> ok.
%% @doc
%% Used for handling the return of a calulcated hashtable from a spawnded
%% process - the building of the hashtable should not block the servicing of
%% requests.  Returned is the binary for writing and the IndexList
%% [{Index, CurrPos, IndexLength}] which can be used to locate the slices of
%% the hashtree within that binary
cdb_returnhashtable(Pid, IndexList, HashTreeBin) ->
    gen_statem:call(Pid, {return_hashtable, IndexList, HashTreeBin}, infinity).

-spec cdb_checkhashtable(pid()) -> boolean().
%% @doc
%% Hash the hashtable been written for this file?
cdb_checkhashtable(Pid) ->
    % only used in tests - so OK to be call
    gen_statem:call(Pid, check_hashtable).

-spec cdb_destroy(pid()) -> ok.
%% @doc
%% If the file is in a delete_pending state close (and will destroy)
cdb_destroy(Pid) ->
    gen_statem:cast(Pid, destroy).

cdb_deletepending(Pid) ->
    % Only used in unit tests
    cdb_deletepending(Pid, 0, no_poll).

-spec cdb_deletepending(pid(), integer(), pid()|no_poll) -> ok.
%% @doc
%% Puts the file in a delete_pending state.  From that state the Inker will be
%% polled to discover if the Manifest SQN at which the file is deleted now
%% means that the file can safely be destroyed (as there are no snapshots with
%% any outstanding dependencies).
%% Passing no_poll means there's no inker to poll, and the process will close
%% on timeout rather than poll.
cdb_deletepending(Pid, ManSQN, Inker) ->
    gen_statem:cast(Pid, {delete_pending, ManSQN, Inker}).

-spec cdb_scan(
        pid(), filter_fun(), any(), integer()|undefined)
            -> {integer()|eof, any()}.
%% @doc
%% cdb_scan returns {LastPosition, Acc}.  Use LastPosition as StartPosiiton to
%% continue from that point (calling function has to protect against) double
%% counting.
%%
%% LastPosition could be the atom complete when the last key processed was at
%% the end of the file.  last_key must be defined in LoopState.
cdb_scan(Pid, FilterFun, InitAcc, StartPosition) ->
    gen_statem:call(Pid,
                    {cdb_scan, FilterFun, InitAcc, StartPosition},
                    infinity).

-spec cdb_lastkey(pid()) -> leveled_codec:journal_key()|empty.
%% @doc
%% Get the last key to be added to the file (which will have the highest
%% sequence number)
cdb_lastkey(Pid) ->
    gen_statem:call(Pid, cdb_lastkey, infinity).

-spec cdb_firstkey(pid()) -> any().
cdb_firstkey(Pid) ->
    gen_statem:call(Pid, cdb_firstkey, infinity).

-spec cdb_filename(pid()) -> string().
%% @doc
%% Get the filename of the database
cdb_filename(Pid) ->
    gen_statem:call(Pid, cdb_filename, infinity).

-spec cdb_keycheck(pid(), any()) -> probably|missing.
%% @doc
%% Check to see if the key is probably present, will return either
%% probably or missing.  Does not do a definitive check
cdb_keycheck(Pid, Key) ->
    gen_statem:call(Pid, {key_check, Key}, infinity).

-spec cdb_isrolling(pid()) -> boolean().
%% @doc
%% Check to see if a cdb file is still rolling
cdb_isrolling(Pid) ->
    gen_statem:call(Pid, cdb_isrolling, infinity).

-spec cdb_clerkcomplete(pid()) -> ok.
%% @doc
%% When an Inker's clerk has finished with a CDB process, then it will call
%% complete.  Currently this will prompt hibernation, as the CDB process may
%% not be needed for a period.
cdb_clerkcomplete(Pid) ->
    gen_statem:cast(Pid, clerk_complete).

-spec cdb_getcachedscore(pid(), erlang:timestamp()) -> undefined|float().
%% @doc
%% Return the cached score for a CDB file
cdb_getcachedscore(Pid, Now) ->
    gen_statem:call(Pid, {get_cachedscore, Now}, infinity).


-spec cdb_putcachedscore(pid(), float()) -> ok.
%% @doc
%% Return the cached score for a CDB file
cdb_putcachedscore(Pid, Score) ->
    gen_statem:call(Pid, {put_cachedscore, Score}, infinity).



%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    MaxSize =
        case Opts#cdb_options.max_size of
            undefined ->
                ?MAX_FILE_SIZE;
            MS ->
                MS
        end,
    MaxCount =
        case Opts#cdb_options.max_count of
            undefined ->
                ?MAX_FILE_SIZE div 1000;
            MC ->
                MC
        end,
    {ok,
        starting,
        #state{max_size=MaxSize,
                max_count=MaxCount,
                binary_mode=Opts#cdb_options.binary_mode,
                waste_path=Opts#cdb_options.waste_path,
                sync_strategy=Opts#cdb_options.sync_strategy,
                log_options=Opts#cdb_options.log_options,
                monitor=Opts#cdb_options.monitor}}.

callback_mode() ->
    state_functions.

starting({call, From}, {open_writer, Filename}, State) ->
    leveled_log:save(State#state.log_options),
    leveled_log:log(cdb01, [Filename]),
    {LastPosition, HashTree, LastKey} = open_active_file(Filename),
    {WriteOps, UpdStrategy} = set_writeops(State#state.sync_strategy),
    leveled_log:log(cdb13, [WriteOps]),
    {ok, Handle} = file:open(Filename, WriteOps),
    State0 = State#state{handle=Handle,
                            current_count = size_hashtree(HashTree),
                            sync_strategy = UpdStrategy,
                            last_position=LastPosition,
                            last_key=LastKey,
                            filename=Filename,
                            hashtree=HashTree},
    {next_state, writer, State0, [{reply, From, ok}, hibernate]};
starting({call, From}, {open_reader, Filename}, State) ->
    leveled_log:save(State#state.log_options),
    leveled_log:log(cdb02, [Filename]),
    {Handle, Index, LastKey} = open_for_readonly(Filename, false),
    State0 = State#state{handle=Handle,
                            last_key=LastKey,
                            filename=Filename,
                            hash_index=Index},
    {next_state, reader, State0, [{reply, From, ok}, hibernate]};
starting({call, From}, {open_reader, Filename, LastKey}, State) ->
    leveled_log:save(State#state.log_options),
    leveled_log:log(cdb02, [Filename]),
    {Handle, Index, LastKey} = open_for_readonly(Filename, LastKey),
    State0 = State#state{handle=Handle,
                            last_key=LastKey,
                            filename=Filename,
                            hash_index=Index},
    {next_state, reader, State0, [{reply, From, ok}, hibernate]}.


writer(
    {call, From}, {get_kv, Key}, State = #state{handle =IO})
        when ?IS_DEF(IO) ->
    {keep_state_and_data,
        [{reply,
            From,
            get_mem(
                Key,
                IO,
                State#state.hashtree,
                State#state.binary_mode)}]};
writer(
    {call, From}, {key_check, Key}, State = #state{handle =IO})
        when ?IS_DEF(IO) ->
    {keep_state_and_data,
        [{reply,
            From,
            get_mem(
                Key,
                IO,
                State#state.hashtree,
                State#state.binary_mode,
                loose_presence)}]};
writer(
    {call, From},
    {put_kv, Key, Value, Sync},
    State = #state{last_position = LP, handle = IO})
        when ?IS_DEF(last_position), ?IS_DEF(IO) ->
    NewCount = State#state.current_count + 1,
    case NewCount >= State#state.max_count of
        true ->
            {keep_state_and_data, [{reply, From, roll}]};
        false ->
            Result =
                put(
                    IO,
                    Key,
                    Value,
                    {LP, State#state.hashtree},
                    State#state.binary_mode,
                    State#state.max_size,
                    State#state.last_key == empty
                ),
            case Result of
                roll ->
                    %% Key and value could not be written
                    {keep_state_and_data, [{reply, From, roll}]};
                {UpdHandle, NewPosition, HashTree} ->
                    ok =
                        case {State#state.sync_strategy, Sync} of
                            {riak_sync, _} ->
                                file:datasync(UpdHandle);
                            {none, true} ->
                                file:datasync(UpdHandle);
                            _ ->
                                ok
                        end,
                    {keep_state,
                        State#state{
                            handle=UpdHandle,
                            current_count=NewCount,
                            last_position=NewPosition,
                            last_key=Key,
                            hashtree=HashTree},
                        [{reply, From, ok}]}
            end
    end;
writer({call, From}, {mput_kv, []}, _State) ->
    {keep_state_and_data, [{reply, From, ok}]};
writer(
    {call, From},
    {mput_kv, KVList},
    State = #state{last_position = LP, handle = IO})
        when ?IS_DEF(last_position), ?IS_DEF(IO) ->
    NewCount = State#state.current_count + length(KVList),
    TooMany = NewCount >= State#state.max_count,
    NotEmpty = State#state.current_count > 0,
    case (TooMany and NotEmpty) of
        true ->
           {keep_state_and_data, [{reply, From, roll}]};
        false ->
            Result =
                mput(
                    IO,
                    KVList, 
                    {LP, State#state.hashtree},
                    State#state.binary_mode,
                    State#state.max_size
                ),
            case Result of
                roll ->
                    %% Keys and values could not be written
                    {keep_state_and_data, [{reply, From, roll}]};
                {UpdHandle, NewPosition, HashTree, LastKey} ->
                    {keep_state,
                        State#state{
                            handle=UpdHandle,
                            current_count=NewCount,
                            last_position=NewPosition,
                            last_key=LastKey,
                            hashtree=HashTree},
                        [{reply, From, ok}]}
            end
    end;
writer(
    {call, From}, cdb_complete, State = #state{filename = FN})
        when ?IS_DEF(FN) ->
    NewName = determine_new_filename(FN),
    ok = close_file(State#state.handle,
                        State#state.hashtree,
                        State#state.last_position),
    ok = rename_for_read(FN, NewName),
    {stop_and_reply, normal, [{reply, From, {ok, NewName}}]};
writer({call, From}, Event, State) ->
    handle_sync_event(Event, From, State);
writer(
    cast, cdb_roll, State = #state{last_position = LP})
        when ?IS_DEF(LP) ->
    ok = 
        leveled_iclerk:clerk_hashtablecalc(
            State#state.hashtree, LP, self()),
    {next_state, rolling, State}.


rolling(
    {call, From}, {get_kv, Key}, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    {keep_state_and_data,
        [{reply,
            From,
            get_mem(
                Key,
                IO,
                State#state.hashtree,
                State#state.binary_mode)}]};
rolling(
    {call, From}, {key_check, Key}, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    {keep_state_and_data,
        [{reply,
            From,
            get_mem(
                Key,
                IO,
                State#state.hashtree,
                State#state.binary_mode,
                loose_presence)}]};
rolling({call, From},
        {get_positions, _SampleSize, _Index, SampleAcc},
        _State) ->
    {keep_state_and_data, [{reply, From, SampleAcc}]};
rolling(
    {call, From},
    {return_hashtable, IndexList, HashTreeBin},
    State = #state{filename = FN})
        when ?IS_DEF(FN) ->
    SW = os:timestamp(),
    Handle = State#state.handle,
    {ok, BasePos} = file:position(Handle, State#state.last_position),
    NewName = determine_new_filename(FN),
    ok = perform_write_hash_tables(Handle, HashTreeBin, BasePos),
    ok = write_top_index_table(Handle, BasePos, IndexList),
    file:close(Handle),
    ok = rename_for_read(FN, NewName),
    leveled_log:log(cdb03, [NewName]),
    ets:delete(State#state.hashtree),
    {NewHandle, Index, LastKey} =
        open_for_readonly(NewName, State#state.last_key),
    State0 = State#state{handle=NewHandle,
                            last_key=LastKey,
                            filename=NewName,
                            hash_index=Index},
    case State#state.deferred_delete of
        true ->
            {next_state, delete_pending, State0, [{reply, From, ok}]};
        false ->
            leveled_log:log_timer(cdb18, [], SW),
            {next_state, reader, State0, [{reply, From, ok}, hibernate]}
    end;
rolling({call, From}, check_hashtable, _State) ->
    {keep_state_and_data, [{reply, From, false}]};
rolling({call, From}, cdb_isrolling, _State) ->
    {keep_state_and_data, [{reply, From, true}]};
rolling({call, From}, Event, State) ->
    handle_sync_event(Event, From, State);
rolling(cast, {delete_pending, ManSQN, Inker}, State) ->
    {keep_state,
        State#state{delete_point=ManSQN, inker=Inker, deferred_delete=true}}.

reader(
    {call, From}, {get_kv, Key}, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    Result =
        get_withcache(
            IO,
            Key,
            State#state.hash_index,
            State#state.binary_mode,
            State#state.monitor
        ),
    {keep_state_and_data, [{reply, From, Result}]};
reader({call, From}, {key_check, Key}, State) ->
    Result =
        get_withcache(State#state.handle,
                        Key,
                        State#state.hash_index,
                        loose_presence,
                        State#state.binary_mode,
                        {no_monitor, 0}),
    {keep_state_and_data, [{reply, From, Result}]};
reader({call, From}, {get_positions, SampleSize, Index, Acc}, State) ->
    {Pos, Count} = element(Index + 1, State#state.hash_index),
    UpdAcc = scan_index_returnpositions(State#state.handle, Pos, Count, Acc),
    case SampleSize of
        all ->
            {keep_state_and_data, [{reply, From, UpdAcc}]};
        _ ->
            {keep_state_and_data,
                [{reply, From, lists:sublist(UpdAcc, SampleSize)}]}
    end;
reader(
    {call, From},
    {direct_fetch, PositionList, Info},
    State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    FilterFalseKey =
        fun(Tpl) ->
            case element(1, Tpl) of
                false ->
                    false;
                _Key ->
                    {true, Tpl}
            end
        end,

    case Info of
        key_only ->
            FM =
                lists:filtermap(
                    fun(P) ->
                            FilterFalseKey(extract_key(IO, P))
                        end,
                        PositionList
                    ),
            MapFun = fun(T) -> element(1, T) end,
            {keep_state_and_data,
                [{reply, From, lists:map(MapFun, FM)}]};
        key_size ->
            FilterFun = fun(P) -> FilterFalseKey(extract_key_size(IO, P)) end,
            {keep_state_and_data,
                [{reply, From, lists:filtermap(FilterFun, PositionList)}]};
        key_value_check ->
            BM = State#state.binary_mode,
            MapFun = fun(P) -> extract_key_value_check(IO, P, BM) end,
            % direct_fetch will occur in batches, so it doesn't make sense to
            % hibernate the process that is likely to be used again.  However,
            % a significant amount of unused binary references may have
            % accumulated, so push a GC at this point
            gen_statem:reply(From, lists:map(MapFun, PositionList)),
            garbage_collect(),
            {keep_state_and_data, []}
    end;
reader(
    {call, From}, cdb_complete, State = #state{filename = FN, handle = IO})
        when ?IS_DEF(FN), ?IS_DEF(IO) ->
    leveled_log:log(cdb05, [FN, reader, cdb_ccomplete]),
    ok = file:close(IO),
    {stop_and_reply, normal,
        [{reply, From, {ok, FN}}],
        State#state{handle=undefined}};
reader({call, From}, check_hashtable, _State) ->
    {keep_state_and_data, [{reply, From, true}]};
reader({call, From}, Event, State) ->
    handle_sync_event(Event, From, State);
reader(cast, {delete_pending, 0, no_poll}, State) ->
    {next_state, delete_pending, State#state{delete_point=0}};
reader(cast, {delete_pending, ManSQN, Inker}, State) ->
    {next_state,
        delete_pending,
        State#state{delete_point=ManSQN, inker=Inker},
        ?DELETE_TIMEOUT};
reader(cast, clerk_complete, _State) ->
    {keep_state_and_data, [hibernate]}.


delete_pending(
    {call, From}, {get_kv, Key}, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    Result =
        get_withcache(
            IO,
            Key,
            State#state.hash_index,
            State#state.binary_mode,
            State#state.monitor
        ),
    {keep_state_and_data, [{reply, From, Result}, ?DELETE_TIMEOUT]};
delete_pending(
    {call, From}, {key_check, Key}, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    Result =
        get_withcache(
            IO,
            Key,
            State#state.hash_index,
            loose_presence,
            State#state.binary_mode,
            {no_monitor, 0}
        ),
    {keep_state_and_data, [{reply, From, Result}, ?DELETE_TIMEOUT]};
delete_pending(
    {call, From}, cdb_close, State = #state{handle = IO, filename = FN})
        when ?IS_DEF(FN), ?IS_DEF(IO) ->
    leveled_log:log(cdb05, [FN, delete_pending, cdb_close]),
    close_pendingdelete(IO, FN, State#state.waste_path),
    {stop_and_reply, normal, [{reply, From, ok}]};
delete_pending({call, From}, Event, State) ->
    handle_sync_event(Event, From, State);
delete_pending(
    cast, delete_confirmed, State = #state{handle = IO, filename = FN})
        when ?IS_DEF(FN), ?IS_DEF(IO) ->
    leveled_log:log(cdb04, [FN, State#state.delete_point]),
    close_pendingdelete(IO, FN, State#state.waste_path),
    {stop, normal};
delete_pending(
    cast, destroy, State = #state{handle = IO, filename = FN})
        when ?IS_DEF(FN), ?IS_DEF(IO) ->
    leveled_log:log(cdb05, [FN, delete_pending, destroy]),
    close_pendingdelete(IO, FN, State#state.waste_path),
    {stop, normal};
delete_pending(
    timeout, _, State=#state{delete_point=ManSQN, handle = IO, filename = FN})
        when ManSQN > 0, ?IS_DEF(FN), ?IS_DEF(IO) ->
    case is_process_alive(State#state.inker) of
        true ->
            ok =
                leveled_inker:ink_confirmdelete(
                    State#state.inker, ManSQN, self()),
            {keep_state_and_data, [?DELETE_TIMEOUT]};
        false ->
            leveled_log:log(cdb04, [FN, ManSQN]),
            close_pendingdelete(IO, FN, State#state.waste_path),
            {stop, normal}
    end.


handle_sync_event(
    {cdb_scan, FilterFun, Acc, StartPos}, From, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    {ok, EndPos0} = file:position(IO, eof),
    {ok, StartPos0} =
        case StartPos of
            undefined ->
                file:position(IO, ?BASE_POSITION);
            StartPos ->
                {ok, StartPos}
        end,
    file:position(IO, StartPos0),
    MaybeEnd =
        (check_last_key(State#state.last_key) == empty) or
        (StartPos0 >= (EndPos0 - ?DWORD_SIZE)),
    {LastPosition, Acc2} =
        case MaybeEnd of
            true ->
                {eof, Acc};
            false ->
                scan_over_file(
                    IO,
                    StartPos0,
                    FilterFun,
                    Acc,
                    State#state.last_key
                )
        end,
    % The scan may have created a lot of binary references, clear up the
    % reference counters for this process here manually.  The cdb process
    % may be inactive for a period after the scan, and so GC may not kick in
    % otherwise
    %
    % garbage_collect/0 is used in preference to hibernate, as we're generally
    % scanning in batches at startup - so the process will be needed straight
    % away.
    gen_statem:reply(From, {LastPosition, Acc2}),
    garbage_collect(),
    {keep_state_and_data, []};
handle_sync_event(cdb_lastkey, From, State) ->
    {keep_state_and_data, [{reply, From, State#state.last_key}]};
handle_sync_event(
    cdb_firstkey, From, State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    {ok, EOFPos} = file:position(IO, eof),
    FirstKey =
        case EOFPos of
            ?BASE_POSITION ->
                empty;
            _ ->
                FindFirstKeyFun =
                    fun(Key, _V, _P, _O, _Fun) -> {stop, Key} end,
                file:position(IO, ?BASE_POSITION),
                {_Pos, FirstScanKey} =
                    scan_over_file(
                        IO,
                        ?BASE_POSITION,
                        FindFirstKeyFun,
                        empty,
                        State#state.last_key
                    ),
                FirstScanKey
        end,
    {keep_state_and_data, [{reply, From, FirstKey}]};
handle_sync_event(cdb_filename, From, State) ->
    {keep_state_and_data, [{reply, From, State#state.filename}]};
handle_sync_event(cdb_isrolling, From, _State) ->
    {keep_state_and_data, [{reply, From, false}]};
handle_sync_event({get_cachedscore, {NowMega, NowSecs, _}}, From, State) ->
    ScoreToReturn =
        case State#state.cached_score of
            undefined ->
                undefined;
            {Score, {CacheMega, CacheSecs, _}} ->
                case (NowMega * ?MEGA + NowSecs) >
                        (CacheMega * ?MEGA + CacheSecs + ?CACHE_LIFE) of
                    true ->
                        undefined;
                    false ->
                        Score
                end
        end,
    {keep_state_and_data, [{reply, From, ScoreToReturn}]};
handle_sync_event({put_cachedscore, Score}, From, State) ->
    {keep_state,
        State#state{cached_score = {Score,os:timestamp()}},
        [{reply, From, ok}]};
handle_sync_event(
    cdb_close, From, _State = #state{handle = IO})
        when ?IS_DEF(IO) ->
    file:close(IO),
    {stop_and_reply, normal, [{reply, From, ok}]}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


%%%============================================================================
%%% External functions
%%%============================================================================

finished_rolling(CDB) ->
    RollerFun =
        fun(Sleep, FinishedRolling) ->
            case FinishedRolling of
                true ->
                    true;
                false ->
                    timer:sleep(Sleep),
                    not leveled_cdb:cdb_isrolling(CDB)
            end
        end,
    lists:foldl(RollerFun, false, [0, 1000, 10000, 100000]).

%%%============================================================================
%%% Internal functions
%%%============================================================================


-spec close_pendingdelete(file:io_device(), list(), list()|undefined) -> ok.
%% @doc
%% If delete is pending - then the close behaviour needs to actuallly delete
%% the file
close_pendingdelete(Handle, Filename, WasteFP) ->
    ok = file:close(Handle),
    case filelib:is_file(Filename) of
        true ->
            case WasteFP of
                undefined ->
                    ok = file:delete(Filename);
                WasteFP ->
                    FN = filename:basename(Filename),
                    NewName = filename:join(WasteFP, FN),
                    ok = file:rename(Filename, NewName)
            end;
        false ->
            % This may happen when there has been a destroy while files are
            % still pending deletion
            leveled_log:log(cdb21, [Filename])
    end.

-spec set_writeops(sync|riak_sync|none) -> {list(), sync|riak_sync|none}.
%% @doc
%% Sync should be used - it is a transaction log - in single node
%% implementations. `riak_sync` is a legacy of earlier OTP versions when
%% passing the sync option was not supported
set_writeops(SyncStrategy) ->
    case SyncStrategy of
        sync ->
            {[sync | ?WRITE_OPS], sync};
        riak_sync ->
            {?WRITE_OPS, riak_sync};
        none ->
            {?WRITE_OPS, none}
    end.

-spec open_active_file(list()) -> {integer(), ets:tid(), any()}.
%% @doc
%% Open an active file - one for which it is assumed the hash tables have not
%% yet been written
%%
%% Needs to scan over file to incrementally produce the hash list, starting at
%% the end of the top index table.
%%
%% Should return a dictionary keyed by index containing a list of {Hash, Pos}
%% tuples as the write_key_value_pairs function, and the current position, and
%% the file handle
open_active_file(FileName) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName, ?WRITE_OPS),
    {ok, Position} = file:position(Handle, {bof, 256 * ?DWORD_SIZE}),
    {LastPosition, {HashTree, LastKey}} =
        startup_scan_over_file(Handle, Position),
    case file:position(Handle, eof) of
        {ok, LastPosition} ->
            ok = file:close(Handle);
        {ok, EndPosition} ->
            case {LastPosition, EndPosition} of
                {?BASE_POSITION, 0} ->
                    ok;
                _ ->
                    leveled_log:log(cdb06, [LastPosition, EndPosition])
            end,
            {ok, _LastPosition} = file:position(Handle, LastPosition),
            ok = file:truncate(Handle),
            ok = file:close(Handle)
    end,
    {LastPosition, HashTree, LastKey}.

-spec put(file:io_device(), 
            any(), any(), 
            {integer(), ets:tid()}, boolean(), integer(), boolean())
                            -> roll|{file:io_device(), integer(), ets:tid()}.
%% @doc
%% put(Handle, Key, Value, {LastPosition, HashDict}) -> {NewPosition, KeyDict}
%% Append to an active file a new key/value pair returning an updated
%% dictionary of Keys and positions.  Returns an updated Position
%%
put(Handle, Key, Value, {LastPosition, HashTree}, 
        BinaryMode, MaxSize, IsEmpty) ->
    Bin = key_value_to_record({Key, Value}, BinaryMode),
    ObjectSize = byte_size(Bin),
    SizeWithinReason = ObjectSize < ?MAX_OBJECT_SIZE,
    PotentialNewSize = LastPosition + ObjectSize,
    case {IsEmpty, PotentialNewSize > MaxSize} of
        {false, true} ->
            roll;
        _ ->
            if
                SizeWithinReason ->
                    ok = file:pwrite(Handle, LastPosition, Bin),
                    {Handle,
                        PotentialNewSize,
                        put_hashtree(Key, LastPosition, HashTree)}
            end
    end.


-spec mput(file:io_device(),
            list(tuple()),
            {integer(), ets:tid()}, boolean(), integer())
                    -> roll|{file:io_device(), integer(), ets:tid(), any()}.
%% @doc
%% Multiple puts - either all will succeed or it will return roll with non
%% succeeding.
mput(Handle, KVList, {LastPosition, HashTree0}, BinaryMode, MaxSize) ->
    {KPList, Bin, LastKey} = multi_key_value_to_record(KVList,
                                                        BinaryMode,
                                                        LastPosition),
    PotentialNewSize = LastPosition + byte_size(Bin),
    if
        PotentialNewSize > MaxSize ->
            roll;
        true ->
            ok = file:pwrite(Handle, LastPosition, Bin),
            HashTree1 = lists:foldl(fun({K, P}, Acc) ->
                                            put_hashtree(K, P, Acc)
                                            end,
                                        HashTree0,
                                        KPList),
            {Handle, PotentialNewSize, HashTree1, LastKey}
    end.


-spec get_withcache(
        file:io_device(), any(), tuple(),  boolean(),
        leveled_monitor:monitor())  -> missing|probably|tuple().
%% @doc
%%
%% Using a cache of the Index array - get a K/V pair from the file using the
%% Key.  should return an updated timings object (if timings are being taken)
%% along with the result (which may be missing if the no matching entry is
%% found, or probably in QuickCheck scenarios)
get_withcache(Handle, Key, Cache, BinaryMode, Monitor) ->
    get(Handle, Key, Cache, true, BinaryMode, Monitor).

get_withcache(Handle, Key, Cache, QuickCheck, BinaryMode, Monitor) ->
    get(Handle, Key, Cache, QuickCheck, BinaryMode, Monitor).


-spec get(
    file:io_device(), 
    any(), 
    tuple(), 
    loose_presence|any(), 
    boolean(),
    leveled_monitor:monitor()) -> tuple()|probably|missing.
%% @doc
%%
%% Get a K/V pair from the file using the Key.  QuickCheck can be set to
%% loose_presence if all is required is a loose check of presence (that the
%% Key is probably present as there is a hash in the hash table which matches
%% that Key)
%%
%% Timings also passed in and can be updated based on results
get(Handle, Key, Cache, QuickCheck, BinaryMode, Monitor) 
                                                    when is_tuple(Handle) ->
    get(Handle, Key, Cache, fun get_index/3, QuickCheck, BinaryMode, Monitor).

get(Handle, Key, Cache, CacheFun, QuickCheck, BinaryMode, Monitor) ->
    SW0 = leveled_monitor:maybe_time(Monitor),
    Hash = hash(Key),
    Index = hash_to_index(Hash),
    {HashTable, Count} = CacheFun(Handle, Index, Cache),
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    % If the count is 0 for that index - key must be missing
    case Count of
        0 ->
            missing;
        _ ->
            % Get starting slot in hashtable
            {ok, FirstHashPosition} =
                file:position(Handle, {bof, HashTable}),
            Slot = hash_to_slot(Hash, Count),
            {CycleCount, Result} =
                search_hash_table(
                    Handle,
                    {FirstHashPosition, Slot, 1, Count},
                    Hash,
                    Key,
                    QuickCheck,
                    BinaryMode),
            {TS1, _SW2} = leveled_monitor:step_time(SW1),
            maybelog_get_timing(Monitor, TS0, TS1, CycleCount),
            Result
    end.

get_index(_Handle, Index, Cache) ->
    element(Index + 1, Cache).

-spec get_mem(any(), list()|file:io_device(), ets:tid(), boolean()) ->
                                                    tuple()|probably|missing.
%% @doc
%% Get a Key/Value pair from an active CDB file (with no hash table written)
get_mem(Key, FNOrHandle, HashTree, BinaryMode) ->
    get_mem(Key, FNOrHandle, HashTree, BinaryMode, true).

get_mem(Key, Handle, HashTree, BinaryMode, QuickCheck) ->
    ListToCheck = get_hashtree(Key, HashTree),
    case {QuickCheck, ListToCheck} of
        {loose_presence, []} ->
            missing;
        {loose_presence, _L} ->
            probably;
        _ ->
            extract_kvpair(Handle, ListToCheck, Key, BinaryMode)
    end.

-spec hashtable_calc(ets:tid(), integer()) -> {list(), binary()}.
%% @doc
%% Create a binary representation of the hash table to be written to the end
%% of the file
hashtable_calc(HashTree, StartPos) ->
    Seq = lists:seq(0, 255),
    SWC = os:timestamp(),
    {IndexList, HashTreeBin} = write_hash_tables(Seq, HashTree, StartPos),
    leveled_log:log_timer(cdb07, [], SWC),
    {IndexList, HashTreeBin}.

%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%


determine_new_filename(Filename) ->
    filename:rootname(Filename, ".pnd") ++ ".cdb".

rename_for_read(Filename, NewName) ->
    %% Rename file
    leveled_log:log(cdb08, [Filename, NewName, filelib:is_file(NewName)]),
    file:rename(Filename, NewName).


-spec open_for_readonly(string(), term())
                            -> {file:io_device(), hashtable_index(), term()}.
%% @doc
%% Open a CDB file to accept read requests (e.g. key/value lookups) but no
%% additions or changes
open_for_readonly(Filename, LastKeyKnown) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    Index = load_index(Handle),
    LastKey =
        case LastKeyKnown of
            false ->
                find_lastkey(Handle, Index);
            LastKeyKnown ->
                LastKeyKnown
        end,
    {Handle, Index, LastKey}.


-spec load_index(file:io_device()) -> hashtable_index().
%% @doc
%% The CDB file has at the beginning an index of how many keys are present in
%% each of 256 slices of the hashtable.  This loads that index
load_index(Handle) ->
    Index = lists:seq(0, 255),
    LoadIndexFun =
        fun(X) ->
            file:position(Handle, {bof, ?DWORD_SIZE * X}),
            read_next_2_integers(Handle)
        end,
    list_to_tuple(lists:map(LoadIndexFun, Index)).


-spec find_lastkey(file:io_device(), hashtable_index()) -> empty|term().
%% @doc
%% Function to find the LastKey in the file
find_lastkey(Handle, IndexCache) ->
    ScanIndexFun =
        fun(Index, {LastPos, KeyCount}) ->
            {Pos, Count} = element(Index + 1, IndexCache),
            scan_index_findlast(Handle, Pos, Count, {LastPos, KeyCount})
        end,
    {LastPosition, TotalKeys} = lists:foldl(ScanIndexFun,
                                            {0, 0},
                                            lists:seq(0, 255)),
    case TotalKeys of
        0 ->
            empty;
        _ ->
            {ok, _} = file:position(Handle, LastPosition),
            {KeyLength, _ValueLength} = read_next_2_integers(Handle),
            safe_read_next(Handle, KeyLength, key)
    end.


scan_index_findlast(Handle, Position, Count, {LastPosition, TotalKeys}) ->
    {ok, _} = file:position(Handle, Position),
    MaxPosFun = fun({_Hash, HPos}, MaxPos) -> max(HPos, MaxPos) end,
    MaxPos = lists:foldl(MaxPosFun,
                            LastPosition,
                            read_next_n_integerpairs(Handle, Count)),
    {MaxPos, TotalKeys + Count}.

scan_index_returnpositions(Handle, Position, Count, PosList0) ->
    {ok, _} = file:position(Handle, Position),
    AddPosFun =
        fun({Hash, HPosition}, PosList) ->
            case {Hash, HPosition} of
                {0, 0} ->
                    PosList;
                _ ->
                    [HPosition|PosList]
            end
        end,
    lists:foldl(AddPosFun,
                PosList0,
                read_next_n_integerpairs(Handle, Count)).


%% Take an active file and write the hash details necessary to close that
%% file and roll a new active file if requested.
%%
%% Base Pos should be at the end of the KV pairs written (the position for)
%% the hash tables
close_file(Handle, HashTree, BasePos) ->
    {ok, BasePos} = file:position(Handle, BasePos),
    IndexList = write_hash_tables(Handle, HashTree),
    ok = write_top_index_table(Handle, BasePos, IndexList),
    file:close(Handle).


%% Fetch a list of positions by passing a key to the HashTree
get_hashtree(Key, HashTree) ->
    Hash = hash(Key),
    Index = hash_to_index(Hash),
    lookup_positions(HashTree, Index, Hash).

%% Add to hash tree - this is an array of 256 skiplists that contains the Hash
%% and position of objects which have been added to an open CDB file
put_hashtree(Key, Position, HashTree) ->
  Hash = hash(Key),
  Index = hash_to_index(Hash),
  add_position_tohashtree(HashTree, Index, Hash, Position).

%% Function to extract a Key-Value pair given a file handle and a position
%% Will confirm that the key matches and do a CRC check
extract_kvpair(_H, [], _K, _BinaryMode) ->
    missing;
extract_kvpair(Handle, [Position|Rest], Key, BinaryMode) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    case safe_read_next(Handle, KeyLength, keybin) of
        {Key, KeyBin} ->  % If same key as passed in, then found!
            case checkread_next_value(Handle, ValueLength, KeyBin) of
                {false, _} ->
                    crc_wonky;
                {_, Value} ->
                    case BinaryMode of
                        true ->
                            {Key, Value};
                        false ->
                            {Key, binary_to_term(Value)}
                    end
            end;
        _ ->
            extract_kvpair(Handle, Rest, Key, BinaryMode)
    end.

extract_key(Handle, Position) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, _ValueLength} = read_next_2_integers(Handle),
    {safe_read_next(Handle, KeyLength, key)}.

extract_key_size(Handle, Position) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    K = safe_read_next(Handle, KeyLength, key),
    {K, ValueLength}.

extract_key_value_check(Handle, Position, BinaryMode) ->
    {ok, _} = file:position(Handle, Position),
    case {BinaryMode, saferead_keyvalue(Handle)} of
        {_, false} ->
            {null, crc_wonky, false};
        {true, {Key, Value, _KeyL, _ValueL}} ->
            {Key, Value, true};
        {false, {Key, Value, _KeyL, _ValueL}} ->
            {Key, binary_to_term(Value), true}
    end.


-spec startup_scan_over_file(
    file:io_device(), integer()) -> {integer(), {ets:tid(), term()}}.
%% @doc
%% Scan through the file until there is a failure to crc check an input, and
%% at that point return the position and the key dictionary scanned so far
startup_scan_over_file(Handle, Position) ->
    Hashtree = new_hashtree(),
    FilterFun = startup_filter(Hashtree),
    {eof, LastKey} = scan_over_file(Handle, Position, FilterFun, empty, empty),
    {ok, FinalPos} = file:position(Handle, cur),
    {FinalPos, {Hashtree, LastKey}}.

-spec startup_filter(ets:tid()) -> filter_fun().
%% @doc
%% Specific filter to be used at startup to build a hashtree for an incomplete
%% cdb file, and returns at the end the hashtree and the final Key seen in the
%% journal
startup_filter(Hashtree) ->
    FilterFun =
        fun(Key, _ValueAsBin, Position, _LastKey, _ExtractFun) ->
            put_hashtree(Key, Position, Hashtree),
            {loop, Key}
        end,
    FilterFun.


-spec scan_over_file
    (file:io_device(), integer(), filter_fun(), term(), any()) ->
        {file_location(), term()}.
%% Scan for key changes - scan over file returning applying FilterFun
%% The FilterFun should accept as input:
%% - Key, ValueBin, Position, Accumulator, Fun (to extract values from Binary)
%% -> outputting a new Accumulator and a loop|stop instruction as a tuple
%% i.e. {loop, Acc} or {stop, Acc}
scan_over_file(Handle, Position, FilterFun, Output, LastKey) ->
    case saferead_keyvalue(Handle) of
        false ->
            case {LastKey, Position} of
                {empty, ?BASE_POSITION} ->
                    % Not interesting that we've nothing to read at base
                    ok;
                _ ->
                    leveled_log:log(cdb09, [Position])
            end,
            % Bring file back to that position
            {ok, Position} = file:position(Handle, {bof, Position}),
            {eof, Output};
        {Key, ValueAsBin, KeyLength, ValueLength} ->
            NewPosition =
                case Key of
                    LastKey ->
                        eof;
                    _ ->
                        Position + KeyLength + ValueLength
                        + ?DWORD_SIZE
                end,
            case FilterFun(Key,
                            ValueAsBin,
                            Position,
                            Output,
                            fun extract_valueandsize/1) of
                {stop, UpdOutput} ->
                    {Position, UpdOutput};
                {loop, UpdOutput} ->
                    case NewPosition of
                        eof ->
                            {eof, UpdOutput};
                        _ ->
                            scan_over_file(Handle,
                                            NewPosition,
                                            FilterFun,
                                            UpdOutput,
                                            LastKey)
                    end
            end
    end.


%% @doc
%% Confirm that the last key has been defined and set to a non-default value
check_last_key(empty) ->
    empty;
check_last_key(_LK) ->
    ok.

-spec saferead_keyvalue(
    file:io_device()) -> false|{any(), binary(), integer(), integer()}.
%% @doc
%% Read the Key/Value at this point, returning {ok, Key, Value}
%% catch expected exceptions associated with file corruption (or end) and
%% return eof
saferead_keyvalue(Handle) ->
    case read_next_2_integers(Handle) of
        eof ->
            false;
        {KeyL, ValueL} when is_integer(KeyL), is_integer(ValueL) ->
            case safe_read_next(Handle, KeyL, keybin) of
                false ->
                    false;
                {Key, KeyBin} ->
                    case safe_read_next(Handle, ValueL, {value, KeyBin}) of
                        false ->
                            false;
                        TrueValue ->
                            % i.e. value with no CRC
                            {Key, TrueValue, KeyL, ValueL}
                    end
            end;
        _ ->
            false
    end.

-spec safe_read_next
    (file:io_device(), integer(), key) -> false|term();
    (file:io_device(), integer(), keybin) -> false|{term(), binary()};
    (file:io_device(), integer(), {value, binary()}) -> false|binary().
%% @doc
%% Read the next item of length Length
%% Previously catching error:badarg was sufficient to capture errors of
%% corruption, but on some OS versions may need to catch error:einval as well
safe_read_next(Handle, Length, ReadType) ->
    ReadFun =
        case ReadType of
            key ->
                fun(Bin) -> binary_to_term(Bin) end;
            keybin ->
                fun(KBin) -> {binary_to_term(KBin), KBin} end;
            {value, KeyBin} ->
                fun(VBin) -> crccheck(VBin, KeyBin) end
        end,
    try
        case file:read(Handle, Length) of
            eof ->
                false;
            {ok, Result} ->
                ReadFun(Result)
        end
    catch
        error:ReadError ->
            leveled_log:log(cdb20, [ReadError, Length]),
            false
    end.

-spec crccheck(binary()|bitstring(), binary()) -> any().
%% @doc
%% CRC chaeck the value which should be a binary, where the first four bytes
%% are a CRC check.  If the binary is truncated, it could be a bitstring or
%% less than 4 bytes - in which case return false to recognise the corruption.
crccheck(<<CRC:32/integer, Value/binary>>, KeyBin) when is_binary(KeyBin) ->
    case calc_crc(KeyBin, Value) of
        CRC ->
            Value;
        _ ->
            leveled_log:log(cdb10, ["mismatch"]),
            false
        end;
crccheck(_V, _KB) ->
    leveled_log:log(cdb10, ["size"]),
    false.


-spec calc_crc(binary(), binary()) -> integer().
%% @doc
%% Do a vaanilla CRC calculation on the binary
calc_crc(KeyBin, Value) -> erlang:crc32(<<KeyBin/binary, Value/binary>>).


-spec checkread_next_value
    (file:io_device(), integer(), binary()) ->
        {true, binary()}|{false, crc_wonky}.
%% @doc
%% Read next string where the string has a CRC prepended - stripping the crc
%% and checking if requested
checkread_next_value(Handle, Length, KeyBin) ->
    {ok, <<CRC:32/integer, Value/binary>>} = file:read(Handle, Length),
    case calc_crc(KeyBin, Value) of
        CRC ->
            {true, Value};
        _ ->
            {false, crc_wonky}
    end.

%% Extract value and size from binary containing CRC
extract_valueandsize(ValueAsBin) ->
    {ValueAsBin, byte_size(ValueAsBin)}.


%% Used for reading lengths with CDB
read_next_2_integers(Handle) ->
    case file:read(Handle, ?DWORD_SIZE) of
        {ok, <<Int1:32/little-integer, Int2:32/little-integer>>} ->
            {Int1, Int2};
        ReadError ->
            ReadError
    end.

read_next_n_integerpairs(Handle, NumberOfPairs) ->
    {ok, Block} = file:read(Handle, ?DWORD_SIZE * NumberOfPairs),
    read_integerpairs(Block, []).

read_integerpairs(<<>>, Pairs) ->
    Pairs;
read_integerpairs(<<Int1:32/little-integer, Int2:32/little-integer,
                        Rest/binary>>, Pairs) ->
    read_integerpairs(<<Rest/binary>>, Pairs ++ [{Int1, Int2}]).



-spec search_hash_table(
    file:io_device(), tuple(), integer(), any(),
    loose_presence|boolean(), boolean())
        -> {pos_integer(), missing|probably|tuple()}.
%% @doc
%%
%% Seach the hash table for the matching hash and key.  Be prepared for
%% multiple keys to have the same hash value.
%%
%% There are three possible values of CRCCheck:
%% true - check the CRC before returning key & value
%% false - don't check the CRC before returning key & value
%% loose_presence - confirm that the hash of the key is present
search_hash_table(_Handle,
                    {_, _, TotalSlots, TotalSlots},
                    _Hash, _Key,
                    _QuickCheck, _BinaryMode) ->
    % We have done the full loop - value must not be present
    {TotalSlots, missing};
search_hash_table(Handle,
                    {FirstHashPosition, Slot, CycleCount, TotalSlots},
                    Hash, Key,
                    QuickCheck, BinaryMode) ->
    % Read the next 2 integers at current position, see if it matches the hash
    % we're after
    Offset =
        ((Slot + CycleCount - 1) rem TotalSlots) * ?DWORD_SIZE
        + FirstHashPosition,
    {ok, _} = file:position(Handle, Offset),

    case read_next_2_integers(Handle) of
        {0, 0} ->
            {CycleCount, missing};
        {Hash, DataLoc} ->
            KV =
                case QuickCheck of
                    loose_presence ->
                        probably;
                    _ ->
                        extract_kvpair(Handle, [DataLoc], Key, BinaryMode)
                end,
            case KV of
                missing ->
                    leveled_log:log(cdb15, [Hash]),
                    search_hash_table(
                        Handle,
                        {FirstHashPosition, Slot, CycleCount + 1, TotalSlots},
                        Hash, Key,
                        QuickCheck, BinaryMode);
                _ ->
                    {CycleCount, KV}
            end;
        _ ->
            search_hash_table(
                Handle,
                {FirstHashPosition, Slot, CycleCount + 1, TotalSlots},
                Hash, Key,
                QuickCheck, BinaryMode)
    end.


-spec maybelog_get_timing(
        leveled_monitor:monitor(),
        leveled_monitor:timing(),
        leveled_monitor:timing(),
        pos_integer()) -> ok.
maybelog_get_timing(
        {Pid, _StatsFreq}, IndexTime, ReadTime, CycleCount)
        when is_pid(Pid), is_integer(IndexTime), is_integer(ReadTime) ->
    leveled_monitor:add_stat(
        Pid, {cdb_get_update, CycleCount, IndexTime, ReadTime});
maybelog_get_timing(_Monitor, _IndexTime, _ReadTime, _CC) ->
    ok.
    

%% Write the actual hashtables at the bottom of the file.  Each hash table
%% entry is a doubleword in length.  The first word is the hash value
%% corresponding to a key and the second word is a file pointer to the
%% corresponding {key,value} tuple.
write_hash_tables(Handle, HashTree) ->
    {ok, StartPos} = file:position(Handle, cur),
    {IndexList, HashTreeBin} = hashtable_calc(HashTree, StartPos),
    ok = perform_write_hash_tables(Handle, HashTreeBin, StartPos),
    IndexList.

perform_write_hash_tables(Handle, HashTreeBin, StartPos) ->
    SWW = os:timestamp(),
    ok = file:write(Handle, HashTreeBin),
    {ok, EndPos} = file:position(Handle, cur),
    ok = file:advise(Handle, StartPos, EndPos - StartPos, will_need),
    leveled_log:log_timer(cdb12, [], SWW),
    ok.


%% Write the top most 255 doubleword entries.  First word is the
%% file pointer to a hashtable and the second word is the number of entries
%% in the hash table
%% The List passed in should be made up of {Index, Position, Count} tuples
write_top_index_table(Handle, BasePos, IndexList) ->
    FnWriteIndex =
        fun({_Index, Pos, Count}, {AccBin, CurrPos}) ->
            {Position, NextPos} =
                case Count == 0 of
                    true ->
                        {CurrPos, CurrPos};
                    false ->
                        {Pos, Pos + (Count * ?DWORD_SIZE)}
                end,
                {<<AccBin/binary,
                        Position:32/little-integer,
                        Count:32/little-integer>>,
                    NextPos}
            end,

    {IndexBin, _Pos} = lists:foldl(FnWriteIndex,
                                    {<<>>, BasePos},
                                    IndexList),
    {ok, _} = file:position(Handle, 0),
    ok = file:write(Handle, IndexBin),
    ok = file:advise(Handle, 0, ?DWORD_SIZE * 256, will_need),
    ok.


hash(Key) ->
    leveled_util:magic_hash(Key).

% Get the least significant 8 bits from the hash.
hash_to_index(Hash) ->
    Hash band 255.

hash_to_slot(Hash, L) ->
    (Hash bsr 8) rem L.

%% Create a binary of the LengthKeyLengthValue, adding a CRC check
%% at the front of the value
key_value_to_record({Key, Value}, BinaryMode) ->
    BK = term_to_binary(Key),
    BV = case BinaryMode of
                true ->
                    Value;
                false ->
                    term_to_binary(Value)
            end,
    KS = byte_size(BK),
    VS = byte_size(BV),
    CRC = calc_crc(BK, BV),
    <<KS:32/little-integer, (VS + 4):32/little-integer,
        BK:KS/binary, CRC:32/integer, BV:VS/binary>>.


multi_key_value_to_record(KVList, BinaryMode, LastPosition) ->
    lists:foldl(fun({K, V}, {KPosL, Bin, _LK}) ->
                        Bin0 = key_value_to_record({K, V}, BinaryMode),
                        {[{K, byte_size(Bin) + LastPosition}|KPosL],
                            <<Bin/binary, Bin0/binary>>,
                            K} end,
                    {[], <<>>, empty},
                    KVList).

%%%============================================================================
%%% HashTree Implementation
%%%============================================================================

lookup_positions(HashTree, Index, Hash) ->
    lookup_positions(HashTree, Index, Hash, -1, []).

lookup_positions(HashTree, Index, Hash, Pos, PosList) ->
    case ets:next(HashTree, {Index, Hash, Pos}) of
        {Index, Hash, NewPos} ->
            lookup_positions(HashTree, Index, Hash, NewPos, [NewPos|PosList]);
        _ ->
            PosList
    end.

add_position_tohashtree(HashTree, Index, Hash, Position) ->
    ets:insert(HashTree, {{Index, Hash, Position}}),
    HashTree.

new_hashtree() ->
    ets:new(hashtree, [ordered_set]).

size_hashtree(HashTree) ->
    ets:info(HashTree, size).

to_list(HashTree, Index) ->
    to_list(HashTree, Index, {0, -1}, []).

to_list(HashTree, Index, {LastHash, LastPos}, Acc) ->
    case ets:next(HashTree, {Index, LastHash, LastPos}) of
        {Index, Hash, Pos} ->
            to_list(HashTree, Index, {Hash, Pos}, [{Hash, Pos}|Acc]);
        _ ->
            Acc
    end.

to_slotmap(HashTree, Index) ->
    HPList = to_list(HashTree, Index),
    IndexLength = length(HPList) * 2,
    ConvertObjFun =
        fun({Hash, Position}) ->
            NewBin = <<Hash:32/little-integer, Position:32/little-integer>>,
            {hash_to_slot(Hash, IndexLength), NewBin}
        end,
    lists:map(ConvertObjFun, HPList).

build_hashtree_binary(SlotMap, IndexLength) ->
    build_hashtree_binary(SlotMap, IndexLength, 0, []).

build_hashtree_binary([], IdxLen, SlotPos, Bin) ->
    case SlotPos of
        N when N == IdxLen ->
            Bin;
        N when N < IdxLen ->
            ZeroLen = (IdxLen - N) * 64,
            [<<0:ZeroLen>>|Bin]
    end;
build_hashtree_binary([{TopSlot, TopBin}|SlotMapTail], IdxLen, SlotPos, Bin) ->
    case TopSlot of
        N when N > SlotPos ->
            D = N - SlotPos,
            Bridge = lists:duplicate(D, <<0:64>>) ++ Bin,
            UpdBin = [<<TopBin/binary>>|Bridge],
            build_hashtree_binary(SlotMapTail,
                                    IdxLen,
                                    SlotPos + D + 1,
                                    UpdBin);
        N when N =< SlotPos, SlotPos < IdxLen ->
            UpdBin = [<<TopBin/binary>>|Bin],
            build_hashtree_binary(SlotMapTail,
                                    IdxLen,
                                    SlotPos + 1,
                                    UpdBin);
        N when N < SlotPos, SlotPos == IdxLen ->
            % Need to wrap round and put in the first empty slot from the
            % beginning
            Pos = find_firstzero(Bin, length(Bin)),
            {LHS, [<<0:64>>|RHS]} = lists:split(Pos - 1, Bin),
            UpdBin = lists:append(LHS, [TopBin|RHS]),
            build_hashtree_binary(SlotMapTail,
                                    IdxLen,
                                    SlotPos,
                                    UpdBin)
    end.


% Search from the tail of the list to find the first zero
find_firstzero(Bin, Pos) ->
    case lists:nth(Pos, Bin) of
        <<0:64>> ->
            Pos;
        _ ->
            find_firstzero(Bin, Pos - 1)
    end.


write_hash_tables(Indexes, HashTree, CurrPos) ->
    write_hash_tables(Indexes, HashTree, CurrPos, CurrPos, [], [], {0, 0, 0}).

write_hash_tables([], _HashTree, _CurrPos, _BasePos,
                                        IndexList, HT_BinList, {T1, T2, T3}) ->
    leveled_log:log(cdb14, [T1, T2, T3]),
    IL = lists:reverse(IndexList),
    {IL, list_to_binary(lists:reverse(HT_BinList))};
write_hash_tables([Index|Rest], HashTree, CurrPos, BasePos,
                                        IndexList, HT_BinList, Timers) ->
    SW1 = os:timestamp(),
    SlotMap = to_slotmap(HashTree, Index),
    T1 = timer:now_diff(os:timestamp(), SW1) + element(1, Timers),
    case SlotMap of
        [] ->
            write_hash_tables(
                Rest,
                HashTree,
                CurrPos,
                BasePos,
                [{Index, BasePos, 0}|IndexList],
                HT_BinList,
                Timers
            );
        _ ->
            SW2 = os:timestamp(),
            IndexLength = length(SlotMap) * 2,
            SortedMap = lists:keysort(1, SlotMap),
            T2 = timer:now_diff(os:timestamp(), SW2) + element(2, Timers),
            SW3 = os:timestamp(),
            NewSlotBin = build_hashtree_binary(SortedMap, IndexLength),
            T3 = timer:now_diff(os:timestamp(), SW3) + element(3, Timers),
            write_hash_tables(
                Rest,
                HashTree,
                CurrPos + IndexLength * ?DWORD_SIZE,
                BasePos,
                [{Index, CurrPos, IndexLength}|IndexList],
                lists:append(NewSlotBin, HT_BinList),
                {T1, T2, T3}
            )
    end.



%%%%%%%%%%%%%%%%
% T E S T
%%%%%%%%%%%%%%%
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

% Write Key and Value tuples into the CDB.  Each tuple consists of a
% 4 byte key length, a 4 byte value length, the actual key followed
% by the value.
%
% Returns a dictionary that is keyed by
% the least significant 8 bits of each hash with the
% values being a list of the hash and the position of the 
% key/value binary in the file.
write_key_value_pairs(Handle, KeyValueList) ->
    {ok, Position} = file:position(Handle, cur),
    HashTree = new_hashtree(),
    write_key_value_pairs(Handle, KeyValueList, {Position, HashTree}).

write_key_value_pairs(_, [], Acc) ->
    Acc;
write_key_value_pairs(Handle, [HeadPair|TailList], Acc) -> 
    {Key, Value} = HeadPair,
    {Handle, NewPosition, HashTree} = put(Handle, Key, Value, Acc),
    write_key_value_pairs(Handle, TailList, {NewPosition, HashTree}).

get(FileName, Key, BinaryMode) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName,[binary, raw, read]),
    get(Handle, Key, BinaryMode);
get(Handle, Key, BinaryMode) ->
    get(
        Handle, Key, no_cache, fun get_uncached_index/3,
        true, BinaryMode, {no_monitor, 0}).    

get_uncached_index(Handle, Index, no_cache) ->
    {ok,_} = file:position(Handle, {bof, ?DWORD_SIZE * Index}),
    % Get location of hashtable and number of entries in the hash
    read_next_2_integers(Handle).
    
file_put(FileName,
    Key,
    Value,
    {LastPosition, HashTree},
    BinaryMode,
    MaxSize,
    IsEmpty) when is_list(FileName) ->
{ok, Handle} = file:open(FileName, ?WRITE_OPS),
put(Handle, Key, Value, {LastPosition, HashTree}, 
    BinaryMode, MaxSize, IsEmpty).

file_get_mem(Key, Filename, HashTree, BinaryMode) ->
    file_get_mem(Key, Filename, HashTree, BinaryMode, true).

file_get_mem(Key, Filename, HashTree, BinaryMode, QuickCheck)
        when is_list(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    get_mem(Key, Handle, HashTree, BinaryMode, QuickCheck).

%% To make this compatible with original Bernstein format this endian flip
%% and also the use of the standard hash function required.
endian_flip(Int) ->
    <<X:32/unsigned-little-integer>> = <<Int:32>>,
    X.

%% from_dict(FileName,ListOfKeyValueTuples)
%% Given a filename and a dictionary, create a cdb
%% using the key value pairs from the dict.
from_dict(FileName,Dict) ->
    KeyValueList = dict:to_list(Dict),
    create(FileName, KeyValueList).


%%
%% create(FileName,ListOfKeyValueTuples) -> ok
%% Given a filename and a list of {key,value} tuples,
%% this function creates a CDB
%%
create(FileName,KeyValueList) ->
    {ok, Handle} = file:open(FileName, ?WRITE_OPS),
    {ok, _} = file:position(Handle, {bof, ?BASE_POSITION}),
    {BasePos, HashTree} = write_key_value_pairs(Handle, KeyValueList),
    close_file(Handle, HashTree, BasePos).


%% Should not be used for non-test PUTs by the inker - as the Max File Size
%% should be taken from the startup options not the default
put(FileName, Key, Value, {LastPosition, HashTree}) when is_list(FileName) ->
    file_put(FileName, Key, Value, {LastPosition, HashTree},
            ?BINARY_MODE, ?MAX_FILE_SIZE, false);
put(Handle, Key, Value, {LastPosition, HashTree}) ->
    put(Handle, Key, Value, {LastPosition, HashTree},
        ?BINARY_MODE, ?MAX_FILE_SIZE, false).

dump(FileName) ->
    {ok, Handle} = file:open(FileName, [binary, raw, read]),
    Fn = fun(Index, Acc) ->
        {ok, _} = file:position(Handle, ?DWORD_SIZE * Index),
        {_, Count} = read_next_2_integers(Handle),
        Acc + Count
    end,
    NumberOfPairs = lists:foldl(Fn, 0, lists:seq(0,255)) bsr 1,
    io:format("Count of keys in db is ~w~n", [NumberOfPairs]),
    {ok, _} = file:position(Handle, {bof, ?BASE_POSITION}),
    Fn1 = fun(_I, Acc) ->
        {KL, VL} = read_next_2_integers(Handle),
        {Key, KB} = safe_read_next(Handle, KL, keybin),
        Value =
            case checkread_next_value(Handle, VL, KB) of
                {true, V0} ->
                    binary_to_term(V0)
            end,
        {Key, Value} = get(Handle, Key, false),
        [{Key,Value} | Acc]
    end,
    lists:foldr(Fn1, [], lists:seq(0, NumberOfPairs-1)).

%%
%% to_dict(FileName)
%% Given a filename returns a dict containing
%% the key value pairs from the dict.
%%
%% @spec to_dict(filename()) -> dictionary()
%% where
%%  filename() = string(),
%%  dictionary() = dict()
%%
to_dict(FileName) ->
    KeyValueList = dump(FileName),
    dict:from_list(KeyValueList).


build_hashtree_bunchedatend_binary_test() ->
    SlotMap = [{1, <<10:32, 0:32>>},
                {4, <<11:32, 100:32>>},
                {8, <<12:32, 200:32>>},
                {8, <<13:32, 300:32>>},
                {14, <<14:32, 400:32>>},
                {14, <<15:32, 500:32>>},
                {15, <<16:32, 600:32>>},
                {15, <<17:32, 700:32>>}],
    Bin =
        list_to_binary(
            lists:reverse(
                build_hashtree_binary(SlotMap, 16)
            )
        ),
    ExpBinP1 = <<16:32, 600:32, 10:32, 0:32, 17:32, 700:32, 0:64>>,
    ExpBinP2 = <<11:32, 100:32, 0:192, 12:32, 200:32, 13:32, 300:32, 0:256>>,
    ExpBinP3 = <<14:32, 400:32, 15:32, 500:32>>,
    ExpBin = <<ExpBinP1/binary, ExpBinP2/binary, ExpBinP3/binary>>,
    ?assertMatch(ExpBin, Bin).

build_hashtree_bunchedatstart_binary_test() ->
    SlotMap = [{1, <<10:32, 0:32>>},
                {2, <<11:32, 100:32>>},
                {3, <<12:32, 200:32>>},
                {4, <<13:32, 300:32>>},
                {5, <<14:32, 400:32>>},
                {6, <<15:32, 500:32>>},
                {7, <<16:32, 600:32>>},
                {8, <<17:32, 700:32>>}],
    Bin =
        list_to_binary(
            lists:reverse(
                build_hashtree_binary(SlotMap, 16)
            )
        ),
    ExpBinP1 = <<0:64, 10:32, 0:32, 11:32, 100:32, 12:32, 200:32>>,
    ExpBinP2 = <<13:32, 300:32, 14:32, 400:32, 15:32, 500:32, 16:32, 600:32>>,
    ExpBinP3 = <<17:32, 700:32, 0:448>>,
    ExpBin = <<ExpBinP1/binary, ExpBinP2/binary, ExpBinP3/binary>>,
    ExpSize = byte_size(ExpBin),
    ?assertMatch(ExpSize, byte_size(Bin)),
    ?assertMatch(ExpBin, Bin).


build_hashtree_test() ->
    SlotMap = [{3, <<2424914688:32, 100:32>>},
                {3, <<2424917760:32, 200:32>>},
                {7, <<2424915712:32, 300:32>>},
                {9, <<2424903936:32, 400:32>>},
                {9, <<2424907008:32, 500:32>>},
                {10, <<2424913408:32, 600:32>>}],
    BinList = build_hashtree_binary(SlotMap, 12),
    ExpOut = [<<0:64>>, <<0:64>>, <<0:64>>, <<2424914688:32, 100:32>>] ++
                [<<2424917760:32, 200:32>>, <<0:64>>, <<0:64>>] ++
                [<<2424915712:32, 300:32>>, <<0:64>>] ++
                [<<2424903936:32, 400:32>>, <<2424907008:32, 500:32>>] ++
                [<<2424913408:32, 600:32>>],
    ?assertMatch(ExpOut, lists:reverse(BinList)).


find_firstzero_test() ->
    Bin = [<<1:64/integer>>, <<0:64/integer>>,
                <<89:64/integer>>, <<89:64/integer>>,
                <<0:64/integer>>,
                <<71:64/integer>>, <<72:64/integer>>],
    ?assertMatch(5, find_firstzero(Bin, length(Bin))),
    {LHS, [<<0:64>>|RHS]} = lists:split(4, Bin),
    ?assertMatch([<<1:64/integer>>, <<0:64/integer>>,
                <<89:64/integer>>, <<89:64/integer>>], LHS),
    ?assertMatch([<<71:64/integer>>, <<72:64/integer>>], RHS).


magickey_test() ->
    {C, L1, L2} = {247, 10, 100},
        % Magic constants - will lead to first hash slot being empty
        % prompts potential issue when first hash slot is empty but
        % hash is 0

    MagicKey =
        {315781,
            stnd,
            {o_rkv,
                <<100,111,109,97,105,110,68,111,99,117,109,101,110,116>>,
                <<48,48,48,49,52,54,56,54,51,48,48,48,51,50,49,54,51,51>>,
                null}},
    ?assertEqual(0, hash(MagicKey)),
    NotMagicKVGen =
        fun(I) ->
            {{I + C, stnd, {o_rkv, <<"B">>, integer_to_binary(I + C), null}},
                <<"V">>}
        end,
    Set1 = lists:map(NotMagicKVGen, lists:seq(1, L1)),
    Set2 = lists:map(NotMagicKVGen, lists:seq(L1 + 1, L2)),
    {ok, P1} =
        cdb_open_writer("test/test_area/magic_hash.pnd",
                        #cdb_options{binary_mode=true}),

    ok = cdb_put(P1, MagicKey, <<"MagicV0">>),
    lists:foreach(fun({K, V}) -> cdb_put(P1, K, V) end, Set1),
    ok = cdb_put(P1, MagicKey, <<"MagicV1">>),
    lists:foreach(fun({K, V}) -> cdb_put(P1, K, V) end, Set2),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=true}),
    {GetK, GetV} = cdb_get(P2, MagicKey),
    ?assertEqual(<<"MagicV1">>, GetV),

    AllKeys = cdb_directfetch(P2, cdb_getpositions(P2, all), key_only),
    ?assertMatch(true, lists:member(MagicKey, AllKeys)),
    ok = cdb_close(P2),

    ok = file:delete("test/test_area/magic_hash.cdb"),

    {ok, P3} =
        cdb_open_writer("test/test_area/magic_hash.pnd",
                        #cdb_options{binary_mode=true}),
    KVL = Set1 ++ [{MagicKey, <<"MagicV1">>}] ++ Set2,
    ok = cdb_mput(P3, KVL),
    {ok, F2} = cdb_complete(P3),
    {ok, P4} = cdb_open_reader(F2, #cdb_options{binary_mode=true}),

    {GetK, GetV} = cdb_get(P4, MagicKey),
    ?assertEqual(<<"MagicV1">>, GetV),
    ok = cdb_close(P4),
    ok = file:delete("test/test_area/magic_hash.cdb"),

    {ok, P5} =
        cdb_open_writer("test/test_area/magic_hash.pnd",
                        #cdb_options{binary_mode=true}),
    KVL5 = Set1 ++ Set2,
    ok = cdb_mput(P5, KVL5),
    {ok, F2} = cdb_complete(P5),
    {ok, P6} = cdb_open_reader(F2, #cdb_options{binary_mode=true}),
    missing = cdb_get(P6, MagicKey),
    ok = cdb_close(P6),
    ok = file:delete("test/test_area/magic_hash.cdb").


cyclecount_test() ->
    io:format("~n~nStarting cycle count test~n"),
    KVL1 = generate_sequentialkeys(5000, []),
    KVL2 = lists:foldl(fun({K, V}, Acc) ->
                            H = hash(K),
                            I = hash_to_index(H),
                            case I of
                                0 ->
                                    [{K, V}|Acc];
                                _ ->
                                    Acc
                            end end,
                        [],
                        KVL1),
    {ok, P1} = cdb_open_writer("test/test_area/cycle_count.pnd",
                                #cdb_options{binary_mode=false}),
    ok = cdb_mput(P1, KVL2),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, cdb_get(P2, K)) end,
                    KVL2),
    % Test many missing keys
    lists:foreach(fun(X) ->
                        K = "NotKey" ++ integer_to_list(X),
                        ?assertMatch(missing, cdb_get(P2, K))
                    end,
                    lists:seq(1, 5000)),

    ok = cdb_close(P2),
    ok = file:delete("test/test_area/cycle_count.cdb").


full_1_test() ->
    List1 = lists:sort([{"key1","value1"},{"key2","value2"}]),
    create("test/test_area/simple.cdb",
            lists:sort([{"key1","value1"},{"key2","value2"}])),
    List2 = lists:sort(dump("test/test_area/simple.cdb")),
    ?assertMatch(List1,List2),
    ok = file:delete("test/test_area/simple.cdb").

full_2_test() ->
    List1 = lists:sort([{lists:flatten(io_lib:format("~s~p",[Prefix,Plug])),
                lists:flatten(io_lib:format("value~p",[Plug]))}
                ||  Plug <- lists:seq(1,200),
                Prefix <- ["dsd","so39ds","oe9%#*(","020dkslsldclsldowlslf%$#",
                  "tiep4||","qweq"]]),
    create("test/test_area/full.cdb",List1),
    List2 = lists:sort(dump("test/test_area/full.cdb")),
    ?assertMatch(List1,List2),
    ok = file:delete("test/test_area/full.cdb").

from_dict_test() ->
    D = dict:new(),
    D1 = dict:store("a","b",D),
    D2 = dict:store("c","d",D1),
    ok = from_dict("test/test_area/from_dict_test.cdb",D2),
    io:format("Store created ~n", []),
    KVP = lists:sort(dump("test/test_area/from_dict_test.cdb")),
    D3 = lists:sort(dict:to_list(D2)),
    io:format("KVP is ~w~n", [KVP]),
    io:format("D3 is ~w~n", [D3]),
    ?assertMatch(KVP, D3),
    ok = file:delete("test/test_area/from_dict_test.cdb").

to_dict_test() ->
    D = dict:new(),
    D1 = dict:store("a","b",D),
    D2 = dict:store("c","d",D1),
    ok = from_dict("test/test_area/from_dict_test1.cdb",D2),
    Dict = to_dict("test/test_area/from_dict_test1.cdb"),
    D3 = lists:sort(dict:to_list(D2)),
    D4 = lists:sort(dict:to_list(Dict)),
    ?assertMatch(D4,D3),
    ok = file:delete("test/test_area/from_dict_test1.cdb").

crccheck_emptyvalue_test() ->
    ?assertMatch(false, crccheck(<<>>, <<"Key">>)).

crccheck_shortvalue_test() ->
    Value = <<128,128,32>>,
    ?assertMatch(false, crccheck(Value, <<"Key">>)).

crccheck_justshortvalue_test() ->
    Value = <<128,128,32,64>>,
    ?assertMatch(false, crccheck(Value, <<"Key">>)).

crccheck_wronghash_test() ->
    Value = term_to_binary("some text as value"),
    Key = <<"K">>,
    BadHash = erlang:crc32(<<Key/binary, Value/binary, 1:8/integer>>),
    GoodHash = erlang:crc32(<<Key/binary, Value/binary>>),
    GValueOnDisk = <<GoodHash:32/integer, Value/binary>>,
    BValueOnDisk = <<BadHash:32/integer, Value/binary>>,
    ?assertMatch(false, crccheck(BValueOnDisk, Key)),
    ?assertMatch(Value, crccheck(GValueOnDisk, Key)).

crccheck_truncatedvalue_test() ->
    Value = term_to_binary("some text as value"),
    Key = <<"K">>,
    Hash = erlang:crc32(<<Key/binary, Value/binary>>),
    ValueOnDisk = <<Hash:32/integer, Value/binary>>,
    Size = bit_size(ValueOnDisk) - 1,
    <<TruncatedValue:Size/bitstring, _/bitstring>> = ValueOnDisk,
    ?assertMatch(false, crccheck(TruncatedValue, Key)),
    ?assertMatch(Value, crccheck(ValueOnDisk, Key)).

activewrite_singlewrite_test() ->
    Key = "0002",
    Value = "some text as new value",
    InitialD = dict:new(),
    InitialD1 = dict:store("0001", "Initial value", InitialD),
    ok = from_dict("test/test_area/test_mem.cdb", InitialD1),
    io:format("New db file created ~n", []),
    {LastPosition, KeyDict, _} =
        open_active_file("test/test_area/test_mem.cdb"),
    io:format("File opened as new active file "
                    "with LastPosition=~w ~n", [LastPosition]),
    {_, _, UpdKeyDict} =
        put(
            "test/test_area/test_mem.cdb",
            Key, Value, {LastPosition, KeyDict}),
    io:format("New key and value added to active file ~n", []),
    ?assertMatch(
        {Key, Value},
        file_get_mem(
            Key, "test/test_area/test_mem.cdb", UpdKeyDict, false)),
    ?assertMatch(
        probably,
        file_get_mem(
            Key, "test/test_area/test_mem.cdb",
            UpdKeyDict, false, loose_presence)),
    ?assertMatch(
        missing,
        file_get_mem(
            "not_present", "test/test_area/test_mem.cdb",
            UpdKeyDict, false, loose_presence)),
    ok = file:delete("test/test_area/test_mem.cdb").

search_hash_table_findinslot_test() ->
    Key1 = "key1", % this is in slot 3 if count is 8
    D = dict:from_list([{Key1, "value1"}, {"K2", "V2"}, {"K3", "V3"},
      {"K4", "V4"}, {"K5", "V5"}, {"K6", "V6"}, {"K7", "V7"},
      {"K8", "V8"}]),
    ok = from_dict("test/test_area/hashtable1_test.cdb",D),
    {ok, Handle} = file:open("test/test_area/hashtable1_test.cdb",
                                [binary, raw, read, write]),
    Hash = hash(Key1),
    Index = hash_to_index(Hash),
    {ok, _} = file:position(Handle, {bof, ?DWORD_SIZE*Index}),
    {HashTable, Count} = read_next_2_integers(Handle),
    io:format("Count of ~w~n", [Count]),
    {ok, FirstHashPosition} = file:position(Handle, {bof, HashTable}),
    Slot = hash_to_slot(Hash, Count),
    io:format("Slot of ~w~n", [Slot]),
    {ok, _} = file:position(Handle, {cur, Slot * ?DWORD_SIZE}),
    {ReadH3, ReadP3} = read_next_2_integers(Handle),
    {ReadH4, ReadP4} = read_next_2_integers(Handle),
    io:format("Slot 1 has Hash ~w Position ~w~n", [ReadH3, ReadP3]),
    io:format("Slot 2 has Hash ~w Position ~w~n", [ReadH4, ReadP4]),
    ?assertMatch(0, ReadH4),
    ?assertMatch({"key1", "value1"}, get(Handle, Key1, false)),
    NoMonitor = {no_monitor, 0},
    ?assertMatch(
        probably, 
        get(Handle, Key1, no_cache, fun get_uncached_index/3,
            loose_presence, false, NoMonitor)),
    ?assertMatch(
        missing, 
        get(Handle, "Key99", no_cache, fun get_uncached_index/3,
            loose_presence, false, NoMonitor)),
    {ok, _} = file:position(Handle, FirstHashPosition),
    FlipH3 = endian_flip(ReadH3),
    FlipP3 = endian_flip(ReadP3),
    RBin = <<FlipH3:32/integer,
                FlipP3:32/integer,
                0:32/integer,
                0:32/integer>>,
    io:format("Replacement binary of ~w~n", [RBin]),
    {ok, OldBin} = file:pread(Handle,
      FirstHashPosition + (Slot -1)  * ?DWORD_SIZE, 16),
    io:format("Bin to be replaced is ~w ~n", [OldBin]),
    ok = file:pwrite(Handle,
                        FirstHashPosition + (Slot -1) * ?DWORD_SIZE,
                        RBin),
    ok = file:close(Handle),
    io:format("Find key following change to hash table~n"),
    ?assertMatch(missing, get("test/test_area/hashtable1_test.cdb", Key1, false)),
    ok = file:delete("test/test_area/hashtable1_test.cdb").

newactivefile_test() ->
    {LastPosition, _, _} = open_active_file("test/test_area/activefile_test.cdb"),
    ?assertMatch(256 * ?DWORD_SIZE, LastPosition),
    ok = file:delete("test/test_area/activefile_test.cdb").

emptyvalue_fromdict_test() ->
    D = dict:new(),
    D1 = dict:store("K1", "V1", D),
    D2 = dict:store("K2", "", D1),
    D3 = dict:store("K3", "V3", D2),
    D4 = dict:store("K4", "", D3),
    ok = from_dict("test/test_area/from_dict_test_ev.cdb",D4),
    io:format("Store created ~n", []),
    KVP = lists:sort(dump("test/test_area/from_dict_test_ev.cdb")),
    D_Result = lists:sort(dict:to_list(D4)),
    io:format("KVP is ~w~n", [KVP]),
    io:format("D_Result is ~w~n", [D_Result]),
    ?assertMatch(KVP, D_Result),
    ok = file:delete("test/test_area/from_dict_test_ev.cdb").


empty_roll_test() ->
    file:delete("test/test_area/empty_roll.cdb"),
    file:delete("test/test_area/empty_roll.pnd"),
    {ok, P1} = cdb_open_writer("test/test_area/empty_roll.pnd",
                                #cdb_options{binary_mode=true}),
    ok = cdb_roll(P1),
    true = finished_rolling(P1),
    {ok, P2} = cdb_open_reader("test/test_area/empty_roll.cdb",
                                #cdb_options{binary_mode=true}),
    ok = cdb_close(P2),
    ok = file:delete("test/test_area/empty_roll.cdb").

find_lastkey_test() ->
    file:delete("test/test_area/lastkey.pnd"),
    {ok, P1} = cdb_open_writer("test/test_area/lastkey.pnd",
                                #cdb_options{binary_mode=false}),
    ok = cdb_put(P1, "Key1", "Value1"),
    ok = cdb_put(P1, "Key3", "Value3"),
    ok = cdb_put(P1, "Key2", "Value2"),
    ?assertMatch("Key2", cdb_lastkey(P1)),
    ?assertMatch("Key1", cdb_firstkey(P1)),
    probably = cdb_keycheck(P1, "Key2"),
    ok = cdb_close(P1),
    {ok, P2} = cdb_open_writer("test/test_area/lastkey.pnd",
                                #cdb_options{binary_mode=false}),
    ?assertMatch("Key2", cdb_lastkey(P2)),
    probably = cdb_keycheck(P2, "Key2"),
    {ok, F2} = cdb_complete(P2),
    {ok, P3} = cdb_open_reader(F2),
    ?assertMatch("Key2", cdb_lastkey(P3)),
    {ok, _FN} = cdb_complete(P3),
    {ok, P4} = cdb_open_reader(F2),
    ?assertMatch("Key2", cdb_lastkey(P4)),
    ok = cdb_close(P4),
    ok = file:delete("test/test_area/lastkey.cdb").

get_keys_byposition_simple_test() ->
    {ok, P1} = cdb_open_writer("test/test_area/poskey.pnd",
                                #cdb_options{binary_mode=false}),
    ok = cdb_put(P1, "Key1", "Value1"),
    ok = cdb_put(P1, "Key3", "Value3"),
    ok = cdb_put(P1, "Key2", "Value2"),
    KeyList = ["Key1", "Key2", "Key3"],
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    PositionList = cdb_getpositions(P2, all),
    io:format("Position list of ~w~n", [PositionList]),
    ?assertMatch(3, length(PositionList)),
    R1 = cdb_directfetch(P2, PositionList, key_only),
    io:format("R1 ~w~n", [R1]),
    ?assertMatch(3, length(R1)),
    lists:foreach(fun(Key) ->
                        ?assertMatch(true, lists:member(Key, KeyList)) end,
                    R1),
    R2 = cdb_directfetch(P2, PositionList, key_size),
    ?assertMatch(3, length(R2)),
    lists:foreach(fun({Key, _Size}) ->
                        ?assertMatch(true, lists:member(Key, KeyList)) end,
                    R2),
    R3 = cdb_directfetch(P2, PositionList, key_value_check),
    ?assertMatch(3, length(R3)),
    lists:foreach(fun({Key, Value, Check}) ->
                        ?assertMatch(true, Check),
                        {K, V} = cdb_get(P2, Key),
                        ?assertMatch(K, Key),
                        ?assertMatch(V, Value) end,
                    R3),
    ok = cdb_close(P2),
    ok = file:delete(F2).

generate_sequentialkeys(0, KVList) ->
    KVList;
generate_sequentialkeys(Count, KVList) ->
    KV = {"Key" ++ integer_to_list(Count), "Value" ++ integer_to_list(Count)},
    generate_sequentialkeys(Count - 1, [KV|KVList]).

get_keys_byposition_manykeys_test_() ->
    {timeout, 600, fun get_keys_byposition_manykeys_test_to/0}.

get_keys_byposition_manykeys_test_to() ->
    KeyCount = 16384,
    {ok, P1} = cdb_open_writer("test/test_area/poskeymany.pnd",
                                #cdb_options{binary_mode=false,
                                                sync_strategy=none}),
    KVList = generate_sequentialkeys(KeyCount, []),
    lists:foreach(fun({K, V}) -> cdb_put(P1, K, V) end, KVList),
    ok = cdb_roll(P1),
    % Should not return positions when rolling
    % There is an implicit race here - if cdb_roll is too fast, then the test
    % will fail.  It appears to be safe that if KeyCount is set to a high value
    % (e.g. > 10K) it is implausible that cdb_roll will ever finish before the
    % call to cdb_getpositions is executed.  So the race is tolerated
    ?assertMatch([], cdb_getpositions(P1, 10)),
    lists:foldl(fun(X, Complete) ->
                        case Complete of
                            true ->
                                true;
                            false ->
                                case cdb_checkhashtable(P1) of
                                    true ->
                                        true;
                                    false ->
                                        timer:sleep(X),
                                        false
                                end
                        end end,
                        false,
                        lists:seq(1, 30)),
    ?assertMatch(10, length(cdb_getpositions(P1, 10))),
    {ok, F2} = cdb_complete(P1),

    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    PositionList = cdb_getpositions(P2, all),
    L1 = length(PositionList),
    io:format("Length of all positions ~w~n", [L1]),
    ?assertMatch(KeyCount, L1),

    SampleList1 = cdb_getpositions(P2, 10),
    ?assertMatch(10, length(SampleList1)),
    SampleList2 = cdb_getpositions(P2, KeyCount),
    ?assertMatch(KeyCount, length(SampleList2)),
    SampleList3 = cdb_getpositions(P2, KeyCount + 1),
    ?assertMatch(KeyCount, length(SampleList3)),

    ?assertMatch(undefined, cdb_getcachedscore(P2, os:timestamp())),
    ok = cdb_putcachedscore(P2, 80.0),
    ?assertMatch(80.0, cdb_getcachedscore(P2, os:timestamp())),
    timer:sleep(1000),
    {NowMega, NowSecs, _} = Now = os:timestamp(),
    ?assertMatch(80.0, cdb_getcachedscore(P2, Now)),
    FutureEpoch = NowMega * ?MEGA + NowSecs + ?CACHE_LIFE,
    Future = {FutureEpoch div ?MEGA, FutureEpoch rem ?MEGA, 0},
    ?assertMatch(undefined, cdb_getcachedscore(P2, Future)),

    ok = cdb_close(P2),
    ok = file:delete(F2).


nokeys_test() ->
    {ok, P1} = cdb_open_writer("test/test_area/nohash_emptyfile.pnd",
                                #cdb_options{binary_mode=false}),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    io:format("FirstKey is ~s~n", [cdb_firstkey(P2)]),
    io:format("LastKey is ~s~n", [cdb_lastkey(P2)]),
    ?assertMatch(empty, cdb_firstkey(P2)),
    ?assertMatch(empty, cdb_lastkey(P2)),
    ok = cdb_close(P2),
    ok = file:delete(F2).

mput_test() ->
    KeyCount = 1024,
    {ok, P1} = cdb_open_writer("test/test_area/nohash_keysinfile.pnd",
                                #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(KeyCount, []),
    ok = cdb_mput(P1, KVList),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ?assertMatch({"Key1024", "Value1024"}, cdb_get(P1, "Key1024")),
    ?assertMatch(missing, cdb_get(P1, "Key1025")),
    ?assertMatch(missing, cdb_get(P1, "Key1026")),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    ?assertMatch("Key1", cdb_firstkey(P2)),
    ?assertMatch("Key1024", cdb_lastkey(P2)),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P2, "Key1")),
    ?assertMatch({"Key1024", "Value1024"}, cdb_get(P2, "Key1024")),
    ?assertMatch(missing, cdb_get(P2, "Key1025")),
    ?assertMatch(missing, cdb_get(P2, "Key1026")),
    ok = cdb_close(P2),
    ok = file:delete(F2).

state_test() ->
    {ok, P1} = cdb_open_writer("test/test_area/state_test.pnd",
                                #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(1000, []),
    ok = cdb_mput(P1, KVList),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ok = cdb_roll(P1),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ok = cdb_deletepending(P1),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    timer:sleep(500),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ok = cdb_close(P1).


hashclash_test() ->
    {ok, P1} = cdb_open_writer("test/test_area/hashclash_test.pnd",
                                #cdb_options{binary_mode=false}),
    Key1 = "Key4184465780",
    Key99 = "Key4254669179",
    KeyNF = "Key9070567319",
    ?assertMatch(22, hash(Key1)),
    ?assertMatch(22, hash(Key99)),
    ?assertMatch(22, hash(KeyNF)),

    ok = cdb_mput(P1, [{Key1, 1}, {Key99, 99}]),

    ?assertMatch(probably, cdb_keycheck(P1, Key1)),
    ?assertMatch(probably, cdb_keycheck(P1, Key99)),
    ?assertMatch(probably, cdb_keycheck(P1, KeyNF)),

    ?assertMatch({Key1, 1}, cdb_get(P1, Key1)),
    ?assertMatch({Key99, 99}, cdb_get(P1, Key99)),
    ?assertMatch(missing, cdb_get(P1, KeyNF)),

    {ok, FN} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(FN, #cdb_options{binary_mode=false}),

    ?assertMatch(probably, cdb_keycheck(P2, Key1)),
    ?assertMatch(probably, cdb_keycheck(P2, Key99)),
    ?assertMatch(probably, cdb_keycheck(P2, KeyNF)),

    ?assertMatch({Key1, 1}, cdb_get(P2, Key1)),
    ?assertMatch({Key99, 99}, cdb_get(P2, Key99)),
    ?assertMatch(missing, cdb_get(P2, KeyNF)),

    ok = cdb_deletepending(P2),

    ?assertMatch(probably, cdb_keycheck(P2, Key1)),
    ?assertMatch(probably, cdb_keycheck(P2, Key99)),
    ?assertMatch(probably, cdb_keycheck(P2, KeyNF)),

    ?assertMatch({Key1, 1}, cdb_get(P2, Key1)),
    ?assertMatch({Key99, 99}, cdb_get(P2, Key99)),
    ?assertMatch(missing, cdb_get(P2, KeyNF)),

    ok = cdb_close(P2).

corruptfile_test() ->
    file:delete("test/test_area/corrupt_test.pnd"),
    {ok, P1} = cdb_open_writer("test/test_area/corrupt_test.pnd",
                                #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(100, []),
    ok = cdb_mput(P1, []), % Not relevant to this test, but needs testing
    lists:foreach(fun({K, V}) -> cdb_put(P1, K, V) end, KVList),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P1, "Key100")),
    ok = cdb_close(P1),
    lists:foreach(fun(Offset) -> corrupt_testfile_at_offset(Offset) end,
                    lists:seq(1, 40)),
    ok = file:delete("test/test_area/corrupt_test.pnd").

corrupt_testfile_at_offset(Offset) ->
    {ok, F1} = file:open("test/test_area/corrupt_test.pnd", ?WRITE_OPS),
    {ok, EofPos} = file:position(F1, eof),
    file:position(F1, EofPos - Offset),
    ok = file:truncate(F1),
    ok = file:close(F1),
    {ok, P2} = cdb_open_writer("test/test_area/corrupt_test.pnd",
                                #cdb_options{binary_mode=false}),
    ?assertMatch(probably, cdb_keycheck(P2, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P2, "Key1")),
    ?assertMatch(missing, cdb_get(P2, "Key100")),
    ok = cdb_put(P2, "Key100", "Value100"),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P2, "Key100")),
    ok = cdb_close(P2).

crc_corrupt_writer_test() ->
    file:delete("test/test_area/corruptwrt_test.pnd"),
    {ok, P1} = cdb_open_writer("test/test_area/corruptwrt_test.pnd",
                                #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(100, []),
    ok = cdb_mput(P1, KVList),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P1, "Key100")),
    ok = cdb_close(P1),
    {ok, Handle} = file:open("test/test_area/corruptwrt_test.pnd", ?WRITE_OPS),
    {ok, EofPos} = file:position(Handle, eof),
    % zero the last byte of the last value
    ok = file:pwrite(Handle, EofPos - 5, <<0:8/integer>>),
    ok = file:close(Handle),
    {ok, P2} = cdb_open_writer("test/test_area/corruptwrt_test.pnd",
                                #cdb_options{binary_mode=false}),
    ?assertMatch(probably, cdb_keycheck(P2, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P2, "Key1")),
    ?assertMatch(missing, cdb_get(P2, "Key100")),
    ok = cdb_put(P2, "Key100", "Value100"),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P2, "Key100")),
    ok = cdb_close(P2).

safe_read_test() ->
    % should return the right thing or false, or the wrong thing if and
    % only if we understand why
    Key = term_to_binary(<<"Key">>),
    Value = <<"Value">>,
    CRC = calc_crc(Key, Value),
    ValToWrite = <<CRC:32/integer, Value/binary>>,
    KeyL = byte_size(Key),
    FlippedKeyL = endian_flip(KeyL),
    ValueL= byte_size(ValToWrite),
    FlippedValL = endian_flip(ValueL),

    TestFN = "test/test_area/saferead.pnd",
    BinToWrite =
        <<FlippedKeyL:32/integer,
            FlippedValL:32/integer,
            Key/binary,
            ValToWrite/binary>>,

    TestCorruptedWriteFun =
        fun(BitNumber) ->
            <<PreBin:BitNumber/bitstring,
                Bit:1/integer,
                PostBin/bitstring>> = BinToWrite,
            BadBit = Bit bxor 1,
            AltBin = <<PreBin:BitNumber/bitstring,
                        BadBit:1/integer,
                        PostBin/bitstring>>,
            file:delete(TestFN),
            {ok, Handle} = file:open(TestFN, ?WRITE_OPS),
            ok = file:pwrite(Handle, 0, AltBin),
            {ok, _} = file:position(Handle, bof),
            case saferead_keyvalue(Handle) of
                false ->
                    % Result OK to be false - should get that on error
                    ok;
                {<<"Key">>, Value, KeyL, BadValueL} ->
                    % Sometimes corruption may yield a correct answer
                    % for example if Value Length is too big
                    %
                    % This can only happen with a corrupted value length at
                    % the end of the file - which is just a peculiarity of
                    % the test
                    ?assertMatch(true, BadValueL > ValueL)
            end,
            ok = file:close(Handle)
        end,

    lists:foreach(TestCorruptedWriteFun,
                    lists:seq(1, -1 + 8 * (KeyL + ValueL + 8))),

    {ok, HandleK} = file:open(TestFN, ?WRITE_OPS),
    ok = file:pwrite(HandleK, 0, BinToWrite),
    {ok, _} = file:position(HandleK, 8 + KeyL + ValueL),
    ?assertMatch(false, safe_read_next(HandleK, KeyL, key)),
    ok = file:close(HandleK),

    WrongKeyL = endian_flip(KeyL + ValueL),
    {ok, HandleV0} = file:open(TestFN, ?WRITE_OPS),
    ok = file:pwrite(HandleV0, 0, BinToWrite),
    ok = file:pwrite(HandleV0, 0, <<WrongKeyL:32/integer>>),
    {ok, _} = file:position(HandleV0, bof),
    ?assertMatch(false, saferead_keyvalue(HandleV0)),
    ok = file:close(HandleV0),

    WrongValL = 0,
    {ok, HandleV1} = file:open(TestFN, ?WRITE_OPS),
    ok = file:pwrite(HandleV1, 0, BinToWrite),
    ok = file:pwrite(HandleV1, 4, <<WrongValL:32/integer>>),
    {ok, _} = file:position(HandleV1, bof),
    ?assertMatch(false, saferead_keyvalue(HandleV1)),
    ok = file:close(HandleV1),

    io:format("Happy check ~n"),
    {ok, HandleHappy} = file:open(TestFN, ?WRITE_OPS),
    ok = file:pwrite(HandleHappy, 0, BinToWrite),
    {ok, _} = file:position(HandleHappy, bof),
    ?assertMatch({<<"Key">>, Value, KeyL, ValueL},
                    saferead_keyvalue(HandleHappy)),

    file:delete(TestFN).


get_positions_corruption_test() ->
    F1 = "test/test_area/corruptpos_test.pnd",
    file:delete(F1),
    {ok, P1} = cdb_open_writer(F1, #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(1000, []),
    ok = cdb_mput(P1, KVList),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P1, "Key100")),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    PositionList = cdb_getpositions(P2, all),
    ?assertMatch(1000, length(PositionList)),
    ok = cdb_close(P2),

    {ok, Handle} = file:open(F2, ?WRITE_OPS),
    Positions = lists:sublist(PositionList, 200, 10),
    CorruptFun =
        fun(Offset) ->
            ok = file:pwrite(Handle, Offset, <<0:8/integer>>)
        end,
    ok = lists:foreach(CorruptFun, Positions),
    ok = file:close(Handle),

    {ok, P3} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),

    PositionList = cdb_getpositions(P3, all),
    ?assertMatch(1000, length(PositionList)),

    KVCL = cdb_directfetch(P3, PositionList, key_size),
    ?assertMatch(true, length(KVCL) < 1000),
    ok = cdb_close(P3),
    file:delete(F2).

badly_written_test() ->
    F1 = "test/test_area/badfirstwrite_test.pnd",
    file:delete(F1),
    {ok, Handle} = file:open(F1, ?WRITE_OPS),
    ok = file:pwrite(Handle, 256 * ?DWORD_SIZE, <<1:8/integer>>),
    ok = file:close(Handle),
    {ok, P1} = cdb_open_writer(F1, #cdb_options{binary_mode=false}),
    ok = cdb_put(P1, "Key100", "Value100"),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P1, "Key100")),
    ok = cdb_close(P1),
    {ok, P2} = cdb_open_writer(F1, #cdb_options{binary_mode=false}),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P2, "Key100")),
    ok = cdb_close(P2),
    file:delete(F1).

pendingdelete_test() ->
    F1 = "test/test_area/deletfile_test.pnd",
    file:delete(F1),
    {ok, P1} = cdb_open_writer(F1, #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(1000, []),
    ok = cdb_mput(P1, KVList),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P1, "Key100")),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P2, "Key1")),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P2, "Key100")),
    ok = file:delete(F2),
    ok = cdb_deletepending(P2),
        % No issues destroying even though the file has already been removed
    ok = cdb_destroy(P2).

getpositions_sample_test() ->
    % what if we try and get positions with a file with o(1000) entries
    F1 = "test/test_area/getpos_sample_test.pnd",
    {ok, P1} = cdb_open_writer(F1, #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(1000, []),
    ok = cdb_mput(P1, KVList),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),

    PositionList100 = cdb_getpositions(P2, 100),
    PositionList101 = cdb_getpositions(P2, 101),
    PositionList102 = cdb_getpositions(P2, 102),
    PositionList103 = cdb_getpositions(P2, 103),
    ?assertMatch(100, length(PositionList100)),
    ?assertMatch(101, length(PositionList101)),
    ?assertMatch(102, length(PositionList102)),
    ?assertMatch(103, length(PositionList103)),

    ok = cdb_close(P2),
    file:delete(F2).

nonsense_coverage_test() ->
    ?assertMatch(
        {ok, reader, #state{}},
        code_change(
            nonsense,
            reader,
            #state{max_count=1, max_size=100},
            nonsense
        )
    ).

-endif.
