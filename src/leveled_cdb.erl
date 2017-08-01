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
%%


-module(leveled_cdb).

-behaviour(gen_fsm).
-include("include/leveled.hrl").

-export([init/1,
            handle_sync_event/4,
            handle_event/3,
            handle_info/3,
            terminate/3,
            code_change/4,
            starting/3,
            writer/3,
            writer/2,
            rolling/2,
            rolling/3,
            reader/3,
            reader/2,
            delete_pending/3,
            delete_pending/2]).

-export([cdb_open_writer/1,
            cdb_open_writer/2,
            cdb_open_reader/1,
            cdb_open_reader/2,
            cdb_reopen_reader/2,
            cdb_get/2,
            cdb_put/3,
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
            hashtable_calc/2]).

-include_lib("eunit/include/eunit.hrl").

-define(DWORD_SIZE, 8).
-define(WORD_SIZE, 4).
-define(MAX_FILE_SIZE, 3221225472).
-define(BINARY_MODE, false).
-define(BASE_POSITION, 2048).
-define(WRITE_OPS, [binary, raw, read, write]).
-define(PENDING_ROLL_WAIT, 30).
-define(DELETE_TIMEOUT, 10000).

-record(state, {hashtree,
                last_position :: integer() | undefined,
                last_key = empty,
                hash_index = {} :: tuple(),
                filename :: string() | undefined,
                handle :: file:fd() | undefined,
                max_size :: integer() | undefined,
                binary_mode = false :: boolean(),
                delete_point = 0 :: integer(),
                inker :: pid() | undefined,
                deferred_delete = false :: boolean(),
                waste_path :: string() | undefined,
                sync_strategy = none}).

-type cdb_options() :: #cdb_options{}.



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
    {ok, Pid} = gen_fsm:start(?MODULE, [Opts], []),
    ok = gen_fsm:sync_send_event(Pid, {open_writer, Filename}, infinity),
    {ok, Pid}.

-spec cdb_reopen_reader(string(), binary()) -> {ok, pid()}.
%% @doc
%% Open an existing file that has already been moved into read-only mode. The
%% LastKey should be known, as it has been stored in the manifest.  Knowing the
%% LastKey stops the file from needing to be scanned on start-up to discover
%% the LastKey.
%%
%% The LastKey is the Key of the last object added to the file - and is used to
%% determine when scans over a file have completed.
cdb_reopen_reader(Filename, LastKey) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [#cdb_options{binary_mode=true}], []),
    ok = gen_fsm:sync_send_event(Pid,
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
    {ok, Pid} = gen_fsm:start(?MODULE, [Opts], []),
    ok = gen_fsm:sync_send_event(Pid, {open_reader, Filename}, infinity),
    {ok, Pid}.

-spec cdb_get(pid(), any()) -> {any(), any()}|missing.
%% @doc
%% Extract a Key and Value from a CDB file by passing in a Key.  
cdb_get(Pid, Key) ->
    gen_fsm:sync_send_event(Pid, {get_kv, Key}, infinity).

-spec cdb_put(pid(), any(), any()) -> ok|roll.
%% @doc
%% Put a key and value into a cdb file that is open as a writer, will fail
%% if the FSM is in any other state.
%%
%% Response can be roll - if there is no space to put this value in the file.
%% It is assumed that the response to a "roll" will be to roll the file, which
%% will close this file for writing after persisting the hashtree.  
cdb_put(Pid, Key, Value) ->
    gen_fsm:sync_send_event(Pid, {put_kv, Key, Value}, infinity).

-spec cdb_mput(pid(), list()) -> ok|roll.
%% @doc
%% Add multiple keys and values in one call.  The file will request a roll if
%% all of the keys and values cnanot be written (and in this case none of them
%% will).  Mput is an all_or_nothing operation.
%%
%% It may be preferable to respond to roll by trying individual PUTs until
%% roll is returned again
cdb_mput(Pid, KVList) ->
    gen_fsm:sync_send_event(Pid, {mput_kv, KVList}, infinity).

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
                    cdb_getpositions_fromidx(Pid, all, Index, Acc)
                end,
            IdxList = lists:seq(0, 255),
            lists:foldl(FoldFun, [], IdxList);
        S0 ->
            FoldFun = 
                fun({_R, Index}, Acc) ->
                    case length(Acc) of
                        S0 ->
                            Acc;
                        L when L < S0 ->
                            cdb_getpositions_fromidx(Pid, S0, Index, Acc)
                    end
                end,
            RandFun = fun(X) -> {leveled_rand:uniform(), X} end,
            SeededL = lists:map(RandFun, lists:seq(0, 255)),
            SortedL = lists:keysort(1, SeededL),
            lists:foldl(FoldFun, [], SortedL)
    end.

cdb_getpositions_fromidx(Pid, SampleSize, Index, Acc) ->
    gen_fsm:sync_send_event(Pid, {get_positions, SampleSize, Index, Acc}).

-spec cdb_directfetch(pid(), list(), key_only|key_size|key_value_check) ->
                                                                        list().
%% @doc
%% Info can be key_only, key_size (size being the size of the value) or
%% key_value_check (with the check part indicating if the CRC is correct for
%% the value)
cdb_directfetch(Pid, PositionList, Info) ->
    gen_fsm:sync_send_event(Pid, {direct_fetch, PositionList, Info}, infinity).

-spec cdb_close(pid()) -> ok.
%% @doc
%% RONSEAL
cdb_close(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, cdb_close, infinity).

-spec cdb_complete(pid()) -> {ok, string()}.
%% @doc
%% Persists the hashtable to the end of the file, to close it for further
%% writing then exit.  Returns the filename that was saved.
cdb_complete(Pid) ->
    gen_fsm:sync_send_event(Pid, cdb_complete, infinity).

-spec cdb_roll(pid()) -> ok.
%% @doc
%% Persists the hashtable to the end of the file, to close it for further
%% writing but do not exit, this will continue to service requests in the
%% rolling state whilst the hashtable is being written, and will become a
%% reader (read-only) CDB file process on completion
cdb_roll(Pid) ->
    gen_fsm:send_event(Pid, cdb_roll).

-spec cdb_returnhashtable(pid(), list(), binary()) -> ok.
%% @doc
%% Used for handling the return of a calulcated hashtable from a spawnded
%% process - the building of the hashtable should not block the servicing of
%% requests.  Returned is the binary for writing and the IndexList
%% [{Index, CurrPos, IndexLength}] which can be used to locate the slices of
%% the hashtree within that binary
cdb_returnhashtable(Pid, IndexList, HashTreeBin) ->
    gen_fsm:sync_send_event(Pid, {return_hashtable, IndexList, HashTreeBin}, infinity).

-spec cdb_checkhashtable(pid()) -> boolean().
%% @doc
%% Hash the hashtable been written for this file?
cdb_checkhashtable(Pid) ->
    gen_fsm:sync_send_event(Pid, check_hashtable).

-spec cdb_destroy(pid()) -> ok.
%% @doc
%% If the file is in a delete_pending state close (and will destroy)
cdb_destroy(Pid) ->
    gen_fsm:send_event(Pid, destroy).

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
    gen_fsm:send_event(Pid, {delete_pending, ManSQN, Inker}).

-spec cdb_scan(pid(), fun(), any(), integer()|undefined) ->
                                                    {integer()|eof, any()}.
%% @doc
%% cdb_scan returns {LastPosition, Acc}.  Use LastPosition as StartPosiiton to
%% continue from that point (calling function has to protect against) double
%% counting.
%%
%% LastPosition could be the atom complete when the last key processed was at
%% the end of the file.  last_key must be defined in LoopState.
cdb_scan(Pid, FilterFun, InitAcc, StartPosition) ->
    gen_fsm:sync_send_all_state_event(Pid,
                                        {cdb_scan,
                                            FilterFun,
                                            InitAcc,
                                            StartPosition},
                                        infinity).

-spec cdb_lastkey(pid()) -> any().
%% @doc
%% Get the last key to be added to the file (which will have the highest
%% sequence number)
cdb_lastkey(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, cdb_lastkey, infinity).

-spec cdb_firstkey(pid()) -> any().
cdb_firstkey(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, cdb_firstkey, infinity).

-spec cdb_filename(pid()) -> string().
%% @doc
%% Get the filename of the database
cdb_filename(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, cdb_filename, infinity).

-spec cdb_keycheck(pid(), any()) -> probably|missing.
%% @doc
%% Check to see if the key is probably present, will return either
%% probably or missing.  Does not do a definitive check
cdb_keycheck(Pid, Key) ->
    gen_fsm:sync_send_event(Pid, {key_check, Key}, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    MaxSize = case Opts#cdb_options.max_size of
                    undefined ->
                        ?MAX_FILE_SIZE;
                    M ->
                        M
                end,
    {ok,
        starting,
        #state{max_size=MaxSize,
                binary_mode=Opts#cdb_options.binary_mode,
                waste_path=Opts#cdb_options.waste_path,
                sync_strategy=Opts#cdb_options.sync_strategy}}.

starting({open_writer, Filename}, _From, State) ->
    leveled_log:log("CDB01", [Filename]),
    {LastPosition, HashTree, LastKey} = open_active_file(Filename),
    WriteOps = set_writeops(State#state.sync_strategy),
    leveled_log:log("CDB13", [WriteOps]),
    {ok, Handle} = file:open(Filename, WriteOps),
    {reply, ok, writer, State#state{handle=Handle,
                                        last_position=LastPosition,
                                        last_key=LastKey,
                                        filename=Filename,
                                        hashtree=HashTree}};
starting({open_reader, Filename}, _From, State) ->
    leveled_log:log("CDB02", [Filename]),
    {Handle, Index, LastKey} = open_for_readonly(Filename, false),
    {reply, ok, reader, State#state{handle=Handle,
                                        last_key=LastKey,
                                        filename=Filename,
                                        hash_index=Index}};
starting({open_reader, Filename, LastKey}, _From, State) ->
    leveled_log:log("CDB02", [Filename]),
    {Handle, Index, LastKey} = open_for_readonly(Filename, LastKey),
    {reply, ok, reader, State#state{handle=Handle,
                                        last_key=LastKey,
                                        filename=Filename,
                                        hash_index=Index}}.

writer({get_kv, Key}, _From, State) ->
    {reply,
        get_mem(Key,
                    State#state.handle,
                    State#state.hashtree,
                    State#state.binary_mode),
        writer,
        State};
writer({key_check, Key}, _From, State) ->
    {reply,
        get_mem(Key,
                    State#state.handle,
                    State#state.hashtree,
                    State#state.binary_mode,
                    loose_presence),
        writer,
        State};
writer({put_kv, Key, Value}, _From, State) ->
    Result = put(State#state.handle,
                    Key,
                    Value,
                    {State#state.last_position, State#state.hashtree},
                    State#state.binary_mode,
                    State#state.max_size),
    case Result of
        roll ->
            %% Key and value could not be written
            {reply, roll, writer, State};
        {UpdHandle, NewPosition, HashTree} ->
            ok =
                case State#state.sync_strategy of
                    riak_sync ->
                        file:datasync(UpdHandle);
                    _ ->
                        ok
                end,
            {reply, ok, writer, State#state{handle=UpdHandle,
                                                last_position=NewPosition,
                                                last_key=Key,
                                                hashtree=HashTree}}
    end;
writer({mput_kv, []}, _From, State) ->
    {reply, ok, writer, State};
writer({mput_kv, KVList}, _From, State) ->
    Result = mput(State#state.handle,
                    KVList,
                    {State#state.last_position, State#state.hashtree},
                    State#state.binary_mode,
                    State#state.max_size),
    case Result of
        roll ->
            %% Keys and values could not be written
            {reply, roll, writer, State};
        {UpdHandle, NewPosition, HashTree, LastKey} ->
            {reply, ok, writer, State#state{handle=UpdHandle,
                                                last_position=NewPosition,
                                                last_key=LastKey,
                                                hashtree=HashTree}}
    end;
writer(cdb_complete, _From, State) ->
    NewName = determine_new_filename(State#state.filename),
    ok = close_file(State#state.handle,
                        State#state.hashtree,
                        State#state.last_position),
    ok = rename_for_read(State#state.filename, NewName),
    {stop, normal, {ok, NewName}, State}.

writer(cdb_roll, State) ->
    ok = leveled_iclerk:clerk_hashtablecalc(State#state.hashtree,
                                            State#state.last_position,
                                            self()),
    {next_state, rolling, State}.


rolling({get_kv, Key}, _From, State) ->
    {reply,
        get_mem(Key,
                    State#state.handle,
                    State#state.hashtree,
                    State#state.binary_mode),
        rolling,
        State};
rolling({key_check, Key}, _From, State) ->
    {reply,
        get_mem(Key,
                    State#state.handle,
                    State#state.hashtree,
                    State#state.binary_mode,
                    loose_presence),
        rolling,
        State};
rolling({get_positions, _SampleSize, _Index, SampleAcc}, _From, State) ->
    {reply, SampleAcc, rolling, State};
rolling({return_hashtable, IndexList, HashTreeBin}, _From, State) ->
    SW = os:timestamp(),
    Handle = State#state.handle,
    {ok, BasePos} = file:position(Handle, State#state.last_position), 
    NewName = determine_new_filename(State#state.filename),
    ok = perform_write_hash_tables(Handle, HashTreeBin, BasePos),
    ok = write_top_index_table(Handle, BasePos, IndexList),
    file:close(Handle),
    ok = rename_for_read(State#state.filename, NewName),
    leveled_log:log("CDB03", [NewName]),
    ets:delete(State#state.hashtree),
    {NewHandle, Index, LastKey} = open_for_readonly(NewName,
                                                    State#state.last_key),
    case State#state.deferred_delete of
        true ->
            {reply, ok, delete_pending, State#state{handle=NewHandle,
                                                    last_key=LastKey,
                                                    filename=NewName,
                                                    hash_index=Index}};
        false ->
            leveled_log:log_timer("CDB18", [], SW),
            {reply, ok, reader, State#state{handle=NewHandle,
                                            last_key=LastKey,
                                            filename=NewName,
                                            hash_index=Index}}
    end;
rolling(check_hashtable, _From, State) ->
    {reply, false, rolling, State}.

rolling({delete_pending, ManSQN, Inker}, State) ->
    {next_state,
        rolling,
        State#state{delete_point=ManSQN, inker=Inker, deferred_delete=true}}.

reader({get_kv, Key}, _From, State) ->
    {reply,
        get_withcache(State#state.handle,
                        Key,
                        State#state.hash_index,
                        State#state.binary_mode),
        reader,
        State};
reader({key_check, Key}, _From, State) ->
    {reply,
        get_withcache(State#state.handle,
                        Key,
                        State#state.hash_index,
                        loose_presence,
                        State#state.binary_mode),
        reader,
        State};
reader({get_positions, SampleSize, Index, Acc}, _From, State) ->
    {Pos, Count} = element(Index + 1, State#state.hash_index),
    UpdAcc = scan_index_returnpositions(State#state.handle, Pos, Count, Acc),
    case SampleSize of
        all ->
            {reply, UpdAcc, reader, State};
        _ ->
            {reply, lists:sublist(UpdAcc, SampleSize), reader, State}
    end;
reader({direct_fetch, PositionList, Info}, _From, State) ->
    H = State#state.handle,
    FilterFalseKey = fun(Tpl) -> case element(1, Tpl) of
                                        false ->
                                            false;
                                        _Key ->
                                            {true, Tpl}
                                    end end,
    Reply =
        case Info of
            key_only ->
                FM = lists:filtermap(
                        fun(P) ->
                                FilterFalseKey(extract_key(H, P)) end,
                            PositionList),
                lists:map(fun(T) -> element(1, T) end, FM);
            key_size ->
                lists:filtermap(
                    fun(P) ->
                            FilterFalseKey(extract_key_size(H, P)) end,
                        PositionList);
            key_value_check ->
                BM = State#state.binary_mode,
                lists:filtermap(
                    fun(P) ->
                            FilterFalseKey(extract_key_value_check(H, P, BM))
                    end,
                        PositionList)
        end,
    {reply, Reply, reader, State};
reader(cdb_complete, _From, State) ->
    ok = file:close(State#state.handle),
    {stop, normal, {ok, State#state.filename}, State#state{handle=undefined}};
reader(check_hashtable, _From, State) ->
    {reply, true, reader, State}.


reader({delete_pending, 0, no_poll}, State) ->
    {next_state,
        delete_pending,
        State#state{delete_point=0}};
reader({delete_pending, ManSQN, Inker}, State) ->
    {next_state,
        delete_pending,
        State#state{delete_point=ManSQN, inker=Inker},
        ?DELETE_TIMEOUT}.


delete_pending({get_kv, Key}, _From, State) ->
    {reply,
        get_withcache(State#state.handle,
                        Key,
                        State#state.hash_index,
                        State#state.binary_mode),
        delete_pending,
        State,
        ?DELETE_TIMEOUT};
delete_pending({key_check, Key}, _From, State) ->
    {reply,
        get_withcache(State#state.handle,
                        Key,
                        State#state.hash_index,
                        loose_presence,
                        State#state.binary_mode),
        delete_pending,
        State,
        ?DELETE_TIMEOUT}.

delete_pending(timeout, State=#state{delete_point=ManSQN}) when ManSQN > 0 ->
    case is_process_alive(State#state.inker) of
        true ->
            case leveled_inker:ink_confirmdelete(State#state.inker, ManSQN) of
                true ->
                    leveled_log:log("CDB04", [State#state.filename, ManSQN]),
                    {stop, normal, State};
                false ->
                    {next_state,
                        delete_pending,
                        State,
                        ?DELETE_TIMEOUT}
            end;
        false ->
            {stop, normal, State}
    end;
delete_pending(destroy, State) ->
    {stop, normal, State}.


handle_sync_event({cdb_scan, FilterFun, Acc, StartPos},
                    _From,
                    StateName,
                    State) ->
    {ok, EndPos0} = file:position(State#state.handle, eof),
    {ok, StartPos0} = case StartPos of
                            undefined ->
                                file:position(State#state.handle,
                                                ?BASE_POSITION);
                            StartPos ->
                                {ok, StartPos}
                        end,
    file:position(State#state.handle, StartPos0),
    MaybeEnd = (check_last_key(State#state.last_key) == empty) or
                    (StartPos0 >= (EndPos0 - ?DWORD_SIZE)),
    case MaybeEnd of
        true ->
            {reply, {eof, Acc}, StateName, State};
        false ->
            {LastPosition, Acc2} = scan_over_file(State#state.handle,
                                                    StartPos0,
                                                    FilterFun,
                                                    Acc,
                                                    State#state.last_key),
            {reply, {LastPosition, Acc2}, StateName, State}
    end;
handle_sync_event(cdb_lastkey, _From, StateName, State) ->
    {reply, State#state.last_key, StateName, State};
handle_sync_event(cdb_firstkey, _From, StateName, State) ->
    {ok, EOFPos} = file:position(State#state.handle, eof),
    FilterFun = fun(Key, _V, _P, _O, _Fun) -> {stop, Key} end,
    FirstKey =
        case EOFPos of
            ?BASE_POSITION ->
                empty;
            _ ->
                file:position(State#state.handle, ?BASE_POSITION),
                {_Pos, FirstScanKey} = scan_over_file(State#state.handle,
                                                        ?BASE_POSITION,
                                                        FilterFun,
                                                        empty,
                                                        State#state.last_key),
                FirstScanKey
        end,
    {reply, FirstKey, StateName, State};
handle_sync_event(cdb_filename, _From, StateName, State) ->
    {reply, State#state.filename, StateName, State};
handle_sync_event(cdb_close, _From, _StateName, State) ->
    {stop, normal, ok, State}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, StateName, State) ->
    leveled_log:log("CDB05", [State#state.filename, Reason]),
    case {State#state.handle, StateName, State#state.waste_path} of
        {undefined, _, _} ->
            ok;
        {Handle, delete_pending, undefined} ->
            ok = file:close(Handle),
            ok = file:delete(State#state.filename);
        {Handle, delete_pending, WasteFP} ->
            file:close(Handle),
            Components = filename:split(State#state.filename),
            NewName = WasteFP ++ lists:last(Components),
            file:rename(State#state.filename, NewName);
        {Handle, _, _} ->
            file:close(Handle)
    end.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

%% Assumption is that sync should be used - it is a transaction log.
%%
%% However this flag is not supported in OTP 16.  Bitcask appears to pass an
%% o_sync flag, but this isn't supported either (maybe it works with the
%% bitcask nif fileops).
%%
%% To get round this will try and datasync on each PUT with riak_sync
set_writeops(SyncStrategy) ->
    case SyncStrategy of
        sync ->
            [sync | ?WRITE_OPS];
        riak_sync ->
            ?WRITE_OPS;
        none ->
            ?WRITE_OPS
    end.


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
    {ok, Position} = file:position(Handle, {bof, 256*?DWORD_SIZE}),
    {LastPosition, {HashTree, LastKey}} = startup_scan_over_file(Handle,
                                                                    Position),
    case file:position(Handle, eof) of 
        {ok, LastPosition} ->
            ok = file:close(Handle);
        {ok, EndPosition} ->
            leveled_log:log("CDB06", [LastPosition, EndPosition]),
            {ok, _LastPosition} = file:position(Handle, LastPosition),
            ok = file:truncate(Handle),
            ok = file:close(Handle)
    end,
    {LastPosition, HashTree, LastKey}.

%% put(Handle, Key, Value, {LastPosition, HashDict}) -> {NewPosition, KeyDict}
%% Append to an active file a new key/value pair returning an updated 
%% dictionary of Keys and positions.  Returns an updated Position
%%
put(FileName,
        Key,
        Value,
        {LastPosition, HashTree},
        BinaryMode,
        MaxSize) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName, ?WRITE_OPS),
    put(Handle, Key, Value, {LastPosition, HashTree}, BinaryMode, MaxSize);
put(Handle, Key, Value, {LastPosition, HashTree}, BinaryMode, MaxSize) ->
    Bin = key_value_to_record({Key, Value}, BinaryMode),
    PotentialNewSize = LastPosition + byte_size(Bin),
    if
        PotentialNewSize > MaxSize ->
            roll;
        true ->
            ok = file:pwrite(Handle, LastPosition, Bin),
            {Handle,
                PotentialNewSize,
                put_hashtree(Key, LastPosition, HashTree)}
    end.

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

%% Should not be used for non-test PUTs by the inker - as the Max File Size
%% should be taken from the startup options not the default
put(FileName, Key, Value, {LastPosition, HashTree}) ->
    put(FileName, Key, Value, {LastPosition, HashTree},
            ?BINARY_MODE, ?MAX_FILE_SIZE).

%%
%% get(FileName,Key) -> {key,value}
%% Given a filename and a key, returns a key and value tuple.
%%


get_withcache(Handle, Key, Cache, BinaryMode) ->
    get(Handle, Key, Cache, true, BinaryMode).

get_withcache(Handle, Key, Cache, QuickCheck, BinaryMode) ->
    get(Handle, Key, Cache, QuickCheck, BinaryMode).

get(FileNameOrHandle, Key, BinaryMode) ->
    get(FileNameOrHandle, Key, no_cache, true, BinaryMode).

get(FileName, Key, Cache, QuickCheck, BinaryMode) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName,[binary, raw, read]),
    get(Handle, Key, Cache, QuickCheck, BinaryMode);
get(Handle, Key, Cache, QuickCheck, BinaryMode) when is_tuple(Handle) ->
    Hash = hash(Key),
    Index = hash_to_index(Hash),
    {HashTable, Count} = get_index(Handle, Index, Cache),
    % If the count is 0 for that index - key must be missing
    case Count of
        0 ->
            missing;
        _ ->
            % Get starting slot in hashtable
            {ok, FirstHashPosition} = file:position(Handle, {bof, HashTable}),
            Slot = hash_to_slot(Hash, Count),
            {ok, _} = file:position(Handle, {cur, Slot * ?DWORD_SIZE}),
            LastHashPosition = HashTable + ((Count-1) * ?DWORD_SIZE),
            LocList = lists:seq(FirstHashPosition,
                                    LastHashPosition,
                                    ?DWORD_SIZE), 
            % Split list around starting slot.
            {L1, L2} = lists:split(Slot, LocList),
            search_hash_table(Handle,
                                lists:append(L2, L1),
                                Hash,
                                Key,
                                QuickCheck,
                                BinaryMode)
    end.

get_index(Handle, Index, no_cache) ->
    {ok,_} = file:position(Handle, {bof, ?DWORD_SIZE * Index}),
    % Get location of hashtable and number of entries in the hash
    read_next_2_integers(Handle);
get_index(_Handle, Index, Cache) ->
    element(Index +  1, Cache).

%% Get a Key/Value pair from an active CDB file (with no hash table written)
%% This requires a key dictionary to be passed in (mapping keys to positions)
%% Will return {Key, Value} or missing
get_mem(Key, FNOrHandle, HashTree, BinaryMode) ->
    get_mem(Key, FNOrHandle, HashTree, BinaryMode, true).

get_mem(Key, Filename, HashTree, BinaryMode, QuickCheck) when is_list(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    get_mem(Key, Handle, HashTree, BinaryMode, QuickCheck);
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

%% Get the next key at a position in the file (or the first key if no position 
%% is passed).  Will return both a key and the next position
get_nextkey(Filename) when is_list(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    get_nextkey(Handle);
get_nextkey(Handle) ->
    {ok, _} = file:position(Handle, bof),
    {FirstHashPosition, _} = read_next_2_integers(Handle),
    get_nextkey(Handle, {256 * ?DWORD_SIZE, FirstHashPosition}).

get_nextkey(Handle, {Position, FirstHashPosition}) ->
    {ok, Position} = file:position(Handle, Position),
    case read_next_2_integers(Handle) of 
        {KeyLength, ValueLength} ->
            NextKey = read_next_key(Handle, KeyLength),
            NextPosition = Position + KeyLength + ValueLength + ?DWORD_SIZE,
            case NextPosition of 
                FirstHashPosition ->
                    {NextKey, nomorekeys};
                _ ->
                    {NextKey, Handle, {NextPosition, FirstHashPosition}}
            end;
        eof ->
            nomorekeys
end.

hashtable_calc(HashTree, StartPos) ->
    Seq = lists:seq(0, 255),
    SWC = os:timestamp(),
    {IndexList, HashTreeBin} = write_hash_tables(Seq, HashTree, StartPos),
    leveled_log:log_timer("CDB07", [], SWC),
    {IndexList, HashTreeBin}.

%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%

determine_new_filename(Filename) ->
    filename:rootname(Filename, ".pnd") ++ ".cdb".
    
rename_for_read(Filename, NewName) ->
    %% Rename file
    leveled_log:log("CDB08", [Filename, NewName, filelib:is_file(NewName)]),
    file:rename(Filename, NewName).

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

load_index(Handle) ->
    Index = lists:seq(0, 255),
    LoadIndexFun =
        fun(X) ->
            file:position(Handle, {bof, ?DWORD_SIZE * X}),
            {HashTablePos, Count} = read_next_2_integers(Handle),
            {HashTablePos, Count}
        end,
    list_to_tuple(lists:map(LoadIndexFun, Index)).

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
            read_next_key(Handle, KeyLength)
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
            case Hash of
                0 ->
                    PosList;
                _ ->
                    [HPosition|PosList]
            end
        end,
    PosList = lists:foldl(AddPosFun,
                            PosList0,
                            read_next_n_integerpairs(Handle, Count)),
    lists:reverse(PosList).


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
    case safe_read_next_key(Handle, KeyLength) of
        Key ->  % If same key as passed in, then found!
            case read_next_value(Handle, ValueLength, crc) of
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
    {safe_read_next_key(Handle, KeyLength)}.

extract_key_size(Handle, Position) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    {safe_read_next_key(Handle, KeyLength), ValueLength}.

extract_key_value_check(Handle, Position, BinaryMode) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    K = safe_read_next_key(Handle, KeyLength),
    {Check, V} = read_next_value(Handle, ValueLength, crc),
    case BinaryMode of
        true ->
            {K, V, Check};
        false ->
            {K, binary_to_term(V), Check}
    end.

%% Scan through the file until there is a failure to crc check an input, and 
%% at that point return the position and the key dictionary scanned so far
startup_scan_over_file(Handle, Position) ->
    HashTree = new_hashtree(),
    {eof, Output} = scan_over_file(Handle,
                                    Position,
                                    fun startup_filter/5,
                                    {HashTree, empty},
                                    empty),
    {ok, FinalPos} = file:position(Handle, cur),
    {FinalPos, Output}.

%% Specific filter to be used at startup to build a hashtree for an incomplete
%% cdb file, and returns at the end the hashtree and the final Key seen in the
%% journal

startup_filter(Key, ValueAsBin, Position, {Hashtree, _LastKey}, _ExtractFun) ->
    case crccheck_value(ValueAsBin) of
        true ->
            % This function is preceeded by a "safe read" of the key and value
            % and so the crccheck should always be true, as a failed check
            % should not reach this stage
            {loop, {put_hashtree(Key, Position, Hashtree), Key}}
    end.


%% Scan for key changes - scan over file returning applying FilterFun
%% The FilterFun should accept as input:
%% - Key, ValueBin, Position, Accumulator, Fun (to extract values from Binary)
%% -> outputting a new Accumulator and a loop|stop instruction as a tuple
%% i.e. {loop, Acc} or {stop, Acc}

scan_over_file(Handle, Position, FilterFun, Output, LastKey) ->
    case saferead_keyvalue(Handle) of
        false ->
            leveled_log:log("CDB09", [Position]),
            {eof, Output};
        {Key, ValueAsBin, KeyLength, ValueLength} ->
            NewPosition = case Key of
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

%% Confirm that the last key has been defined and set to a non-default value

check_last_key(LastKey) ->
    case LastKey of
        empty -> empty;
        _ -> ok
    end.

%% Read the Key/Value at this point, returning {ok, Key, Value}
%% catch expected exceptions associated with file corruption (or end) and 
%% return eof
saferead_keyvalue(Handle) ->
    case read_next_2_integers(Handle) of 
        eof ->
            false;
        {KeyL, ValueL} ->
            case safe_read_next_key(Handle, KeyL) of 
                {error, _} ->
                    false;
                eof ->
                    false;
                false ->
                    false;
                Key ->
                    case file:read(Handle, ValueL) of 
                        eof ->
                            false;
                        {ok, Value} ->
                            case crccheck_value(Value) of
                                true ->
                                    {Key, Value, KeyL, ValueL};
                                false ->
                                    false
                            end
                    end 
            end
    end.


safe_read_next_key(Handle, Length) ->
    try read_next_key(Handle, Length) of
        Term ->
            Term
    catch
        error:badarg ->
            false
    end.

%% The first four bytes of the value are the crc check
crccheck_value(Value) when byte_size(Value) >4 ->
    << Hash:32/integer, Tail/bitstring>> = Value,
    case calc_crc(Tail) of 
        Hash -> 
            true;
        _ -> 
            leveled_log:log("CDB10", []),
            false
        end;
crccheck_value(_) ->
    leveled_log:log("CDB11", []),
    false.

%% Run a crc check filling out any values which don't fit on byte boundary
calc_crc(Value) ->
    case bit_size(Value) rem 8 of 
        0 -> 
            erlang:crc32(Value);
        N ->
            M = 8 - N,
            erlang:crc32(<<Value/bitstring,0:M>>)
    end.

read_next_key(Handle, Length) ->
    case file:read(Handle, Length) of
        {ok, Bin} ->
            binary_to_term(Bin);
        ReadError ->
            ReadError
    end.


%% Read next string where the string has a CRC prepended - stripping the crc 
%% and checking if requested
read_next_value(Handle, Length, crc) ->
    {ok, <<CRC:32/integer, Bin/binary>>} = file:read(Handle, Length),
    case calc_crc(Bin) of 
        CRC ->
            {true, Bin};
        _ ->
            {false, crc_wonky}
    end.

%% Extract value and size from binary containing CRC
extract_valueandsize(ValueAsBin) ->
    <<_CRC:32/integer, Bin/binary>> = ValueAsBin,
    {Bin, byte_size(Bin)}.


%% Used for reading lengths
%% Note that the endian_flip is required to make the file format compatible 
%% with CDB 
read_next_2_integers(Handle) ->
    case file:read(Handle,?DWORD_SIZE) of 
        {ok, <<Int1:32,Int2:32>>} -> 
            {endian_flip(Int1), endian_flip(Int2)};
        ReadError ->
            ReadError
    end.

read_next_n_integerpairs(Handle, NumberOfPairs) ->
    {ok, Block} = file:read(Handle, ?DWORD_SIZE * NumberOfPairs),
    read_integerpairs(Block, []).

read_integerpairs(<<>>, Pairs) ->
    Pairs;
read_integerpairs(<<Int1:32, Int2:32, Rest/binary>>, Pairs) ->
    read_integerpairs(<<Rest/binary>>,
                        Pairs ++ [{endian_flip(Int1),
                                    endian_flip(Int2)}]).

%% Seach the hash table for the matching hash and key.  Be prepared for 
%% multiple keys to have the same hash value.
%%
%% There are three possible values of CRCCheck:
%% true - check the CRC before returning key & value
%% false - don't check the CRC before returning key & value
%% loose_presence - confirm that the hash of the key is present

search_hash_table(Handle, Entries, Hash, Key, QuickCheck, BinaryMode) ->
    search_hash_table(Handle, Entries, Hash, Key, QuickCheck, BinaryMode, 0).

search_hash_table(_Handle, [], Hash, _Key,
                                    _QuickCheck, _BinaryMode, CycleCount) -> 
    log_cyclecount(CycleCount, Hash, missing),
    missing;
search_hash_table(Handle, [Entry|RestOfEntries], Hash, Key,
                                        QuickCheck, BinaryMode, CycleCount) ->
    {ok, _} = file:position(Handle, Entry),
    {StoredHash, DataLoc} = read_next_2_integers(Handle),
    case StoredHash of
        Hash ->
            KV = case QuickCheck of
                loose_presence ->
                    probably;
                _ ->
                    extract_kvpair(Handle, [DataLoc], Key, BinaryMode)
            end,
            case KV of
                missing ->
                    search_hash_table(Handle,
                                        RestOfEntries,
                                        Hash,
                                        Key,
                                        QuickCheck,
                                        BinaryMode,
                                        CycleCount + 1);
                _ ->
                    log_cyclecount(CycleCount, Hash, found),
                    KV 
            end;
        %0 ->
        %    % Hash is 0 so key must be missing as 0 found before Hash matched
        %    missing;
        _ ->
            search_hash_table(Handle, RestOfEntries, Hash, Key,
                                        QuickCheck, BinaryMode, CycleCount + 1)
    end.

log_cyclecount(CycleCount, Hash, Result) ->
    if
        CycleCount > 8 ->
            leveled_log:log("CDB15", [CycleCount, Hash, Result]);
        true ->
            ok
    end.

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
    leveled_log:log_timer("CDB12", [], SWW),
    ok.


%% Write the top most 255 doubleword entries.  First word is the 
%% file pointer to a hashtable and the second word is the number of entries 
%% in the hash table
%% The List passed in should be made up of {Index, Position, Count} tuples
write_top_index_table(Handle, BasePos, IndexList) ->
    FnWriteIndex = fun({_Index, Pos, Count}, {AccBin, CurrPos}) ->
        case Count == 0 of
            true ->
                PosLE = endian_flip(CurrPos),
                NextPos = CurrPos;
            false ->
                PosLE = endian_flip(Pos),
                NextPos = Pos + (Count * ?DWORD_SIZE)
        end, 
        CountLE = endian_flip(Count),
        {<<AccBin/binary, PosLE:32, CountLE:32>>, NextPos}
    end,
    
    {IndexBin, _Pos} = lists:foldl(FnWriteIndex,
                                    {<<>>, BasePos},
                                    IndexList),
    {ok, _} = file:position(Handle, 0),
    ok = file:write(Handle, IndexBin),
    ok = file:advise(Handle, 0, ?DWORD_SIZE * 256, will_need),
    ok.

%% To make this compatible with original Bernstein format this endian flip
%% and also the use of the standard hash function required.
  
endian_flip(Int) ->
    <<X:32/unsigned-little-integer>> = <<Int:32>>,
    X.

hash(Key) ->
    leveled_codec:magic_hash(Key).

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
    LK = byte_size(BK),
    LV = byte_size(BV),
    LK_FL = endian_flip(LK),
    LV_FL = endian_flip(LV + 4),
    CRC = calc_crc(BV),
    <<LK_FL:32, LV_FL:32, BK:LK/binary, CRC:32/integer, BV:LV/binary>>.


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
            HashLE = endian_flip(Hash),
            PosLE = endian_flip(Position),
            NewBin = <<HashLE:32, PosLE:32>>,
            {hash_to_slot(Hash, IndexLength), NewBin}
        end,
    lists:map(ConvertObjFun, HPList).

build_hashtree_binary(SlotMap, IndexLength) ->
    build_hashtree_binary(SlotMap, IndexLength, 0, []).

build_hashtree_binary([], IdxLen, SlotPos, Bin) ->
    case SlotPos of
        IdxLen ->
            lists:reverse(Bin);
        N when N < IdxLen ->
            ZeroLen = (IdxLen - N) * 64,
            lists:reverse([<<0:ZeroLen>>|Bin])
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
    leveled_log:log("CDB14", [T1, T2, T3]),
    IL = lists:reverse(IndexList),
    {IL, list_to_binary(HT_BinList)};
write_hash_tables([Index|Rest], HashTree, CurrPos, BasePos,
                                        IndexList, HT_BinList, Timers) ->
    SW1 = os:timestamp(),
    SlotMap = to_slotmap(HashTree, Index),
    T1 = timer:now_diff(os:timestamp(), SW1) + element(1, Timers),
    case SlotMap of 
        [] ->
            write_hash_tables(Rest,
                                HashTree,
                                CurrPos,
                                BasePos,
                                [{Index, BasePos, 0}|IndexList],
                                HT_BinList,
                                Timers);
        _ ->
            SW2 = os:timestamp(),
            IndexLength = length(SlotMap) * 2,
            SortedMap = lists:keysort(1, SlotMap),
            T2 = timer:now_diff(os:timestamp(), SW2) + element(2, Timers),
            SW3 = os:timestamp(),
            NewSlotBin = build_hashtree_binary(SortedMap, IndexLength),
            T3 = timer:now_diff(os:timestamp(), SW3) + element(3, Timers),
            write_hash_tables(Rest,
                                HashTree,
                                CurrPos + IndexLength * ?DWORD_SIZE,
                                BasePos,
                                [{Index, CurrPos, IndexLength}|IndexList],
                                HT_BinList ++ NewSlotBin,
                                {T1, T2, T3})
    end.



%%%%%%%%%%%%%%%%
% T E S T 
%%%%%%%%%%%%%%%  
-ifdef(TEST).

%%
%% dump(FileName) -> List
%% Given a file name, this function returns a list
%% of {key,value} tuples from the CDB.
%%

dump(FileName) ->
    {ok, Handle} = file:open(FileName, [binary, raw, read]),
    Fn = fun(Index, Acc) ->
        {ok, _} = file:position(Handle, ?DWORD_SIZE * Index),
        {_, Count} = read_next_2_integers(Handle),
        Acc + Count    
    end,
    NumberOfPairs = lists:foldl(Fn, 0, lists:seq(0,255)) bsr 1,
    io:format("Count of keys in db is ~w~n", [NumberOfPairs]),  
    {ok, _} = file:position(Handle, {bof, 2048}),
    Fn1 = fun(_I, Acc) ->
        {KL, VL} = read_next_2_integers(Handle),
        Key = read_next_key(Handle, KL),
        Value =
            case read_next_value(Handle, VL, crc) of
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
    Bin = list_to_binary(build_hashtree_binary(SlotMap, 16)),
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
    Bin = list_to_binary(build_hashtree_binary(SlotMap, 16)),
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
    ?assertMatch(ExpOut, BinList).


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
    {ok, P1} = cdb_open_writer("../test/cycle_count.pnd",
                                #cdb_options{binary_mode=false}),
    ok = cdb_mput(P1, KVL2),
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2, #cdb_options{binary_mode=false}),
    lists:foreach(fun({K, V}) ->
                        ?assertMatch({K, V}, cdb_get(P2, K)) end,
                    KVL2),
    ok = cdb_close(P2),
    ok = file:delete("../test/cycle_count.cdb").
    

full_1_test() ->
    List1 = lists:sort([{"key1","value1"},{"key2","value2"}]),
    create("../test/simple.cdb",
            lists:sort([{"key1","value1"},{"key2","value2"}])),
    List2 = lists:sort(dump("../test/simple.cdb")),
    ?assertMatch(List1,List2),
    ok = file:delete("../test/simple.cdb").

full_2_test() ->
    List1 = lists:sort([{lists:flatten(io_lib:format("~s~p",[Prefix,Plug])),
                lists:flatten(io_lib:format("value~p",[Plug]))} 
                ||  Plug <- lists:seq(1,200),
                Prefix <- ["dsd","so39ds","oe9%#*(","020dkslsldclsldowlslf%$#",
                  "tiep4||","qweq"]]),
    create("../test/full.cdb",List1),
    List2 = lists:sort(dump("../test/full.cdb")),
    ?assertMatch(List1,List2),
    ok = file:delete("../test/full.cdb").

from_dict_test() ->
    D = dict:new(),
    D1 = dict:store("a","b",D),
    D2 = dict:store("c","d",D1),
    ok = from_dict("../test/from_dict_test.cdb",D2),
    io:format("Store created ~n", []),
    KVP = lists:sort(dump("../test/from_dict_test.cdb")),
    D3 = lists:sort(dict:to_list(D2)),
    io:format("KVP is ~w~n", [KVP]),
    io:format("D3 is ~w~n", [D3]),
    ?assertMatch(KVP, D3),
    ok = file:delete("../test/from_dict_test.cdb").

to_dict_test() ->
    D = dict:new(),
    D1 = dict:store("a","b",D),
    D2 = dict:store("c","d",D1),
    ok = from_dict("../test/from_dict_test1.cdb",D2),
    Dict = to_dict("../test/from_dict_test1.cdb"),
    D3 = lists:sort(dict:to_list(D2)),
    D4 = lists:sort(dict:to_list(Dict)),
    ?assertMatch(D4,D3),
    ok = file:delete("../test/from_dict_test1.cdb").

crccheck_emptyvalue_test() ->
    ?assertMatch(false, crccheck_value(<<>>)).    

crccheck_shortvalue_test() ->
    Value = <<128,128,32>>,
    ?assertMatch(false, crccheck_value(Value)).

crccheck_justshortvalue_test() ->
    Value = <<128,128,32,64>>,
    ?assertMatch(false, crccheck_value(Value)).

crccheck_correctvalue_test() ->
    Value = term_to_binary("some text as value"),
    Hash = erlang:crc32(Value),
    ValueOnDisk = <<Hash:32/integer, Value/binary>>,
    ?assertMatch(true, crccheck_value(ValueOnDisk)).

crccheck_wronghash_test() ->
    Value = term_to_binary("some text as value"),
    Hash = erlang:crc32(Value) + 1,
    ValueOnDisk = <<Hash:32/integer, Value/binary>>,
    ?assertMatch(false, crccheck_value(ValueOnDisk)).

crccheck_truncatedvalue_test() ->
    Value = term_to_binary("some text as value"),
    Hash = erlang:crc32(Value),
    ValueOnDisk = <<Hash:32/integer, Value/binary>>,
    Size = bit_size(ValueOnDisk) - 1,
    <<TruncatedValue:Size/bitstring, _/bitstring>> = ValueOnDisk,
    ?assertMatch(false, crccheck_value(TruncatedValue)).

activewrite_singlewrite_test() ->
    Key = "0002",
    Value = "some text as new value",
    InitialD = dict:new(),
    InitialD1 = dict:store("0001", "Initial value", InitialD),
    ok = from_dict("../test/test_mem.cdb", InitialD1),
    io:format("New db file created ~n", []),
    {LastPosition, KeyDict, _} = open_active_file("../test/test_mem.cdb"),
    io:format("File opened as new active file " 
                    "with LastPosition=~w ~n", [LastPosition]),
    {_, _, UpdKeyDict} = put("../test/test_mem.cdb",
                                Key, Value,
                                {LastPosition, KeyDict}),
    io:format("New key and value added to active file ~n", []),
    ?assertMatch({Key, Value},
                    get_mem(Key, "../test/test_mem.cdb", UpdKeyDict, false)),
    ?assertMatch(probably,
                    get_mem(Key, "../test/test_mem.cdb", UpdKeyDict,
                                false, loose_presence)),
    ?assertMatch(missing,
                    get_mem("not_present", "../test/test_mem.cdb", UpdKeyDict,
                                false, loose_presence)),
    ok = file:delete("../test/test_mem.cdb").

search_hash_table_findinslot_test() ->
    Key1 = "key1", % this is in slot 3 if count is 8
    D = dict:from_list([{Key1, "value1"}, {"K2", "V2"}, {"K3", "V3"}, 
      {"K4", "V4"}, {"K5", "V5"}, {"K6", "V6"}, {"K7", "V7"}, 
      {"K8", "V8"}]),
    ok = from_dict("../test/hashtable1_test.cdb",D),
    {ok, Handle} = file:open("../test/hashtable1_test.cdb",
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
    ?assertMatch(probably, get(Handle, Key1,
                                no_cache, loose_presence, false)),
    ?assertMatch(missing, get(Handle, "Key99",
                                no_cache, loose_presence, false)),
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
    ?assertMatch(missing, get("../test/hashtable1_test.cdb", Key1, false)),
    ok = file:delete("../test/hashtable1_test.cdb").

getnextkey_inclemptyvalue_test() ->
    L = [{"K9", "V9"}, {"K2", "V2"}, {"K3", ""}, 
      {"K4", "V4"}, {"K5", "V5"}, {"K6", "V6"}, {"K7", "V7"}, 
      {"K8", "V8"}, {"K1", "V1"}],
    ok = create("../test/hashtable2_test.cdb", L),
    {FirstKey, Handle, P1} = get_nextkey("../test/hashtable2_test.cdb"),
    io:format("Next position details of ~w~n", [P1]),
    ?assertMatch("K9", FirstKey),
    {SecondKey, Handle, P2} = get_nextkey(Handle, P1),
    ?assertMatch("K2", SecondKey),
    {ThirdKeyNoValue, Handle, P3} = get_nextkey(Handle, P2),
    ?assertMatch("K3", ThirdKeyNoValue),
    {_, Handle, P4} = get_nextkey(Handle, P3),
    {_, Handle, P5} = get_nextkey(Handle, P4),
    {_, Handle, P6} = get_nextkey(Handle, P5),
    {_, Handle, P7} = get_nextkey(Handle, P6),
    {_, Handle, P8} = get_nextkey(Handle, P7),
    {LastKey, nomorekeys} = get_nextkey(Handle, P8),
    ?assertMatch("K1", LastKey),
    ok = file:delete("../test/hashtable2_test.cdb").

newactivefile_test() ->
    {LastPosition, _, _} = open_active_file("../test/activefile_test.cdb"),
    ?assertMatch(256 * ?DWORD_SIZE, LastPosition),
    Response = get_nextkey("../test/activefile_test.cdb"),
    ?assertMatch(nomorekeys, Response),
    ok = file:delete("../test/activefile_test.cdb").

emptyvalue_fromdict_test() ->
    D = dict:new(),
    D1 = dict:store("K1", "V1", D),
    D2 = dict:store("K2", "", D1),
    D3 = dict:store("K3", "V3", D2),
    D4 = dict:store("K4", "", D3),
    ok = from_dict("../test/from_dict_test_ev.cdb",D4),
    io:format("Store created ~n", []),
    KVP = lists:sort(dump("../test/from_dict_test_ev.cdb")),
    D_Result = lists:sort(dict:to_list(D4)),
    io:format("KVP is ~w~n", [KVP]),
    io:format("D_Result is ~w~n", [D_Result]),
    ?assertMatch(KVP, D_Result),
    ok = file:delete("../test/from_dict_test_ev.cdb").

find_lastkey_test() ->
    file:delete("../test/lastkey.pnd"),
    {ok, P1} = cdb_open_writer("../test/lastkey.pnd",
                                #cdb_options{binary_mode=false}),
    ok = cdb_put(P1, "Key1", "Value1"),
    ok = cdb_put(P1, "Key3", "Value3"),
    ok = cdb_put(P1, "Key2", "Value2"),
    ?assertMatch("Key2", cdb_lastkey(P1)),
    ?assertMatch("Key1", cdb_firstkey(P1)),
    probably = cdb_keycheck(P1, "Key2"),
    ok = cdb_close(P1),
    {ok, P2} = cdb_open_writer("../test/lastkey.pnd",
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
    ok = file:delete("../test/lastkey.cdb").

get_keys_byposition_simple_test() ->
    {ok, P1} = cdb_open_writer("../test/poskey.pnd",
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
    lists:reverse(KVList);
generate_sequentialkeys(Count, KVList) ->
    KV = {"Key" ++ integer_to_list(Count), "Value" ++ integer_to_list(Count)},
    generate_sequentialkeys(Count - 1, KVList ++ [KV]).

get_keys_byposition_manykeys_test() ->
    KeyCount = 1024,
    {ok, P1} = cdb_open_writer("../test/poskeymany.pnd",
                                #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(KeyCount, []),
    lists:foreach(fun({K, V}) -> cdb_put(P1, K, V) end, KVList),
    ok = cdb_roll(P1),
    % Should not return posiitons when rolling
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
                        lists:seq(1, 20)),
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
    
    ok = cdb_close(P2),
    ok = file:delete(F2).


nokeys_test() ->
    {ok, P1} = cdb_open_writer("../test/nohash_emptyfile.pnd",
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
    {ok, P1} = cdb_open_writer("../test/nohash_keysinfile.pnd",
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
    {ok, P1} = cdb_open_writer("../test/state_test.pnd",
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
    {ok, P1} = cdb_open_writer("../test/hashclash_test.pnd",
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
    file:delete("../test/corrupt_test.pnd"),
    {ok, P1} = cdb_open_writer("../test/corrupt_test.pnd",
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
    ok = file:delete("../test/corrupt_test.pnd").

corrupt_testfile_at_offset(Offset) ->
    {ok, F1} = file:open("../test/corrupt_test.pnd", ?WRITE_OPS),
    {ok, EofPos} = file:position(F1, eof),
    file:position(F1, EofPos - Offset),
    ok = file:truncate(F1),
    ok = file:close(F1),
    {ok, P2} = cdb_open_writer("../test/corrupt_test.pnd",
                                #cdb_options{binary_mode=false}),
    ?assertMatch(probably, cdb_keycheck(P2, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P2, "Key1")),
    ?assertMatch(missing, cdb_get(P2, "Key100")),
    ok = cdb_put(P2, "Key100", "Value100"),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P2, "Key100")),
    ok = cdb_close(P2).

crc_corrupt_writer_test() ->
    file:delete("../test/corruptwrt_test.pnd"),
    {ok, P1} = cdb_open_writer("../test/corruptwrt_test.pnd",
                                #cdb_options{binary_mode=false}),
    KVList = generate_sequentialkeys(100, []),
    ok = cdb_mput(P1, KVList),
    ?assertMatch(probably, cdb_keycheck(P1, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P1, "Key1")),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P1, "Key100")),
    ok = cdb_close(P1),
    {ok, Handle} = file:open("../test/corruptwrt_test.pnd", ?WRITE_OPS),
    {ok, EofPos} = file:position(Handle, eof),
    % zero the last byte of the last value
    ok = file:pwrite(Handle, EofPos - 5, <<0:8/integer>>),
    ok = file:close(Handle),
    {ok, P2} = cdb_open_writer("../test/corruptwrt_test.pnd",
                                #cdb_options{binary_mode=false}),
                                ?assertMatch(probably, cdb_keycheck(P2, "Key1")),
    ?assertMatch({"Key1", "Value1"}, cdb_get(P2, "Key1")),
    ?assertMatch(missing, cdb_get(P2, "Key100")),
    ok = cdb_put(P2, "Key100", "Value100"),
    ?assertMatch({"Key100", "Value100"}, cdb_get(P2, "Key100")),
    ok = cdb_close(P2).

nonsense_coverage_test() ->
    {ok, Pid} = gen_fsm:start(?MODULE, [#cdb_options{}], []),
    ok = gen_fsm:send_all_state_event(Pid, nonsense),
    ?assertMatch({next_state, reader, #state{}}, handle_info(nonsense,
                                                                reader,
                                                                #state{})),
    ?assertMatch({ok, reader, #state{}}, code_change(nonsense,
                                                        reader,
                                                        #state{},
                                                        nonsense)).

-endif.
