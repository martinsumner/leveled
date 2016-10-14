%%
%% This is a modified version of the cdb module provided by Tom Whitcomb.  
%%
%% - https://github.com/thomaswhitcomb/erlang-cdb
%%
%% The primary differences are: 
%% - Support for incrementally writing a CDB file while keeping the hash table 
%% in memory
%% - The ability to scan a database and accumulate all the Key, Values to 
%% rebuild in-memory tables on startup 
%% - The ability to scan a database in blocks of sequence numbers
%%
%% This is to be used in eleveledb, and in this context: 
%% - Keys will be a combinatio of the PrimaryKey and the Sequence Number
%% - Values will be a serialised version on the whole object, and the
%% IndexChanges associated with the transaction
%% Where the IndexChanges are all the Key changes required to be added to the
%% ledger to complete the changes (the addition of postings and tombstones).
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

-behaviour(gen_server).
-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        cdb_open_writer/1,
        cdb_open_writer/2,
        cdb_open_reader/1,
        cdb_get/2,
        cdb_put/3,
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
        cdb_destroy/1,
        cdb_deletepending/1,
        hashtable_calc/2]).

-include_lib("eunit/include/eunit.hrl").

-define(DWORD_SIZE, 8).
-define(WORD_SIZE, 4).
-define(CRC_CHECK, true).
-define(MAX_FILE_SIZE, 3221225472).
-define(BINARY_MODE, false).
-define(BASE_POSITION, 2048).
-define(WRITE_OPS, [binary, raw, read, write]).
-define(PENDING_ROLL_WAIT, 30).

-record(state, {hashtree,
                last_position :: integer(),
                last_key = empty,
                hash_index = [] :: list(),
                filename :: string(),
                handle :: file:fd(),
                writer :: boolean(),
                max_size :: integer(),
                pending_roll = false :: boolean(),
                pending_delete = false :: boolean(),
                binary_mode = false :: boolean()}).


%%%============================================================================
%%% API
%%%============================================================================

cdb_open_writer(Filename) ->
    %% No options passed
    cdb_open_writer(Filename, #cdb_options{}).

cdb_open_writer(Filename, Opts) ->
    {ok, Pid} = gen_server:start(?MODULE, [Opts], []),
    case gen_server:call(Pid, {open_writer, Filename}, infinity) of
        ok ->
            {ok, Pid};
        Error ->
            Error
    end.

cdb_open_reader(Filename) ->
    {ok, Pid} = gen_server:start(?MODULE, [#cdb_options{}], []),
    case gen_server:call(Pid, {open_reader, Filename}, infinity) of
        ok ->
            {ok, Pid};
        Error ->
            Error
    end.

cdb_get(Pid, Key) ->
    gen_server:call(Pid, {get_kv, Key}, infinity).

cdb_put(Pid, Key, Value) ->
    gen_server:call(Pid, {put_kv, Key, Value}, infinity).

%% SampleSize can be an integer or the atom all
cdb_getpositions(Pid, SampleSize) ->
    gen_server:call(Pid, {get_positions, SampleSize}, infinity).

%% Info can be key_only, key_size (size being the size of the value) or
%% key_value_check (with the check part indicating if the CRC is correct for
%% the value)
cdb_directfetch(Pid, PositionList, Info) ->
    gen_server:call(Pid, {direct_fetch, PositionList, Info}, infinity).

cdb_close(Pid) ->
    cdb_close(Pid, ?PENDING_ROLL_WAIT).

cdb_close(Pid, WaitsLeft) ->
    if
        WaitsLeft > 0 ->
            case gen_server:call(Pid, cdb_close, infinity) of
                pending_roll ->
                    timer:sleep(1),
                    cdb_close(Pid, WaitsLeft - 1);
                R ->
                    R
            end;
        true ->
            gen_server:call(Pid, cdb_kill, infinity)
    end.

cdb_complete(Pid) ->
    gen_server:call(Pid, cdb_complete, infinity).

cdb_roll(Pid) ->
    gen_server:cast(Pid, cdb_roll).

cdb_returnhashtable(Pid, IndexList, HashTreeBin) ->
    gen_server:call(Pid, {return_hashtable, IndexList, HashTreeBin}, infinity).

cdb_destroy(Pid) ->
    gen_server:cast(Pid, destroy).

cdb_deletepending(Pid) ->
    gen_server:cast(Pid, delete_pending).

%% cdb_scan returns {LastPosition, Acc}.  Use LastPosition as StartPosiiton to
%% continue from that point (calling function has to protect against) double
%% counting.
%%
%% LastPosition could be the atom complete when the last key processed was at
%% the end of the file.  last_key must be defined in LoopState.

cdb_scan(Pid, FilterFun, InitAcc, StartPosition) ->
    gen_server:call(Pid,
                    {cdb_scan, FilterFun, InitAcc, StartPosition},
                    infinity).

%% Get the last key to be added to the file (which will have the highest
%% sequence number)
cdb_lastkey(Pid) ->
    gen_server:call(Pid, cdb_lastkey, infinity).

cdb_firstkey(Pid) ->
    gen_server:call(Pid, cdb_firstkey, infinity).

%% Get the filename of the database
cdb_filename(Pid) ->
    gen_server:call(Pid, cdb_filename, infinity).

%% Check to see if the key is probably present, will return either
%% probably or missing.  Does not do a definitive check
cdb_keycheck(Pid, Key) ->
    gen_server:call(Pid, {key_check, Key}, infinity).

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
    {ok, #state{max_size=MaxSize, binary_mode=Opts#cdb_options.binary_mode}}.

handle_call({open_writer, Filename}, _From, State) ->
    io:format("Opening file for writing with filename ~s~n", [Filename]),
    {LastPosition, HashTree, LastKey} = open_active_file(Filename),
    {ok, Handle} = file:open(Filename, [sync | ?WRITE_OPS]),
    {reply, ok, State#state{handle=Handle,
                                last_position=LastPosition,
                                last_key=LastKey,
                                filename=Filename,
                                hashtree=HashTree,
                                writer=true}};
handle_call({open_reader, Filename}, _From, State) ->
    io:format("Opening file for reading with filename ~s~n", [Filename]),
    {Handle, Index, LastKey} = open_for_readonly(Filename),
    {reply, ok, State#state{handle=Handle,
                                last_key=LastKey,
                                filename=Filename,
                                writer=false,
                                hash_index=Index}};
handle_call({get_kv, Key}, _From, State) ->
    case State#state.writer of
        true ->
            {reply,
                get_mem(Key, State#state.handle, State#state.hashtree),
                State};
        false ->
            {reply,
                get_withcache(State#state.handle, Key, State#state.hash_index),
                State}
    end;
handle_call({key_check, Key}, _From, State) ->
    case State#state.writer of
        true ->
            {reply,
                get_mem(Key,
                        State#state.handle,
                        State#state.hashtree,
                        loose_presence),
                State};
        false ->
            {reply,
                get(State#state.handle,
                    Key,
                    loose_presence,
                    State#state.hash_index),
                State}
    end;
handle_call({put_kv, Key, Value}, _From, State) ->
    case {State#state.writer, State#state.pending_roll} of
        {true, false} ->
            Result = put(State#state.handle,
                            Key, Value,
                            {State#state.last_position, State#state.hashtree},
                            State#state.binary_mode,
                            State#state.max_size),
            case Result of
                roll ->
                    %% Key and value could not be written
                    {reply, roll, State};
                {UpdHandle, NewPosition, HashTree} ->
                    {reply, ok, State#state{handle=UpdHandle,
                                                last_position=NewPosition,
                                                last_key=Key,
                                                hashtree=HashTree}}
                end;
        _ ->
            {reply,
                {error, read_only},
                State}
    end;
handle_call(cdb_lastkey, _From, State) ->
    {reply, State#state.last_key, State};
handle_call(cdb_firstkey, _From, State) ->
    {reply, extract_key(State#state.handle, ?BASE_POSITION), State};
handle_call(cdb_filename, _From, State) ->
    {reply, State#state.filename, State};
handle_call({get_positions, SampleSize}, _From, State) ->
    case SampleSize of
        all ->
            {reply, scan_index(State#state.handle,
                                State#state.hash_index,
                                {fun scan_index_returnpositions/4, []}),
                        State};
        _ ->
            SeededL = lists:map(fun(X) -> {random:uniform(), X} end,
                                State#state.hash_index),
            SortedL = lists:keysort(1, SeededL),
            RandomisedHashIndex = lists:map(fun({_R, X}) -> X end, SortedL),
            {reply,
                scan_index_forsample(State#state.handle,
                                        RandomisedHashIndex,
                                        fun scan_index_returnpositions/4,
                                        [],
                                        SampleSize),
                State}
    end;
handle_call({direct_fetch, PositionList, Info}, _From, State) ->
    H = State#state.handle,
    case Info of
        key_only ->
            KeyList = lists:map(fun(P) ->
                                        extract_key(H, P) end,
                                    PositionList),
            {reply, KeyList, State};
        key_size ->
            KeySizeList = lists:map(fun(P) ->
                                            extract_key_size(H, P) end,
                                        PositionList),
            {reply, KeySizeList, State};
        key_value_check ->
            KVCList = lists:map(fun(P) ->
                                        extract_key_value_check(H, P) end,
                                    PositionList),
            {reply, KVCList, State}
    end;
handle_call({cdb_scan, FilterFun, Acc, StartPos}, _From, State) ->
    {ok, StartPos0} = case StartPos of
                            undefined ->
                                file:position(State#state.handle,
                                                ?BASE_POSITION);
                            StartPos ->
                                {ok, StartPos}
                        end,
    case check_last_key(State#state.last_key) of
        ok ->
            {LastPosition, Acc2} = scan_over_file(State#state.handle,
                                                    StartPos0,
                                                    FilterFun,
                                                    Acc,
                                                    State#state.last_key),
            {reply, {LastPosition, Acc2}, State};
        empty ->
            {reply, {eof, Acc}, State}
    end;
handle_call(cdb_close, _From, State=#state{pending_roll=RollPending})
                                                when RollPending == true ->
    {reply, pending_roll, State};
handle_call(cdb_close, _From, State) ->
    ok = file:close(State#state.handle),
    {stop, normal, ok, State#state{handle=undefined}};
handle_call(cdb_kill, _From, State) ->
    {stop, killed, ok, State};
handle_call(cdb_complete, _From, State=#state{writer=Writer})
                                                when Writer == true ->
    NewName = determine_new_filename(State#state.filename),
    ok = close_file(State#state.handle,
                        State#state.hashtree,
                        State#state.last_position),
    ok = rename_for_read(State#state.filename, NewName),
    {stop, normal, {ok, NewName}, State};
handle_call(cdb_complete, _From, State) ->
    ok = file:close(State#state.handle),
    {stop, normal, {ok, State#state.filename}, State};
handle_call({return_hashtable, IndexList, HashTreeBin},
            _From,
            State=#state{pending_roll=RollPending}) when RollPending == true ->
    Handle = State#state.handle,
    {ok, BasePos} = file:position(Handle, State#state.last_position), 
    NewName = determine_new_filename(State#state.filename),
    ok = perform_write_hash_tables(Handle, HashTreeBin, BasePos),
    ok = write_top_index_table(Handle, BasePos, IndexList),
    file:close(Handle),
    ok = rename_for_read(State#state.filename, NewName),
    io:format("Opening file for reading with filename ~s~n", [NewName]),
    {NewHandle, Index, LastKey} = open_for_readonly(NewName),
    {reply, ok, State#state{handle=NewHandle,
                            last_key=LastKey,
                            filename=NewName,
                            writer=false,
                            pending_roll=false,
                            hash_index=Index}}.


handle_cast(destroy, State) ->
    ok = file:close(State#state.handle),
    ok = file:delete(State#state.filename),
    {noreply, State};
handle_cast(delete_pending, State) ->
    {noreply, State#state{pending_delete=true}};
handle_cast(cdb_roll, State=#state{writer=Writer}) when Writer == true ->
    ok = leveled_iclerk:clerk_hashtablecalc(State#state.hashtree,
                                            State#state.last_position,
                                            self()),
    {noreply, State#state{pending_roll=true}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case {State#state.handle, State#state.pending_delete} of
        {undefined, _} ->
            ok;
        {Handle, false} ->
            file:close(Handle);
        {Handle, true} ->
            file:close(Handle),
            file:delete(State#state.filename)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================


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

%%
%% dump(FileName) -> List
%% Given a file name, this function returns a list
%% of {key,value} tuples from the CDB.
%%
dump(FileName) ->
    dump(FileName, ?CRC_CHECK).

dump(FileName, CRCCheck) ->
    {ok, Handle} = file:open(FileName, [binary, raw, read]),
    Fn = fun(Index, Acc) ->
        {ok, _} = file:position(Handle, ?DWORD_SIZE * Index),
        {_, Count} = read_next_2_integers(Handle),
        Acc + Count    
    end,
    NumberOfPairs = lists:foldl(Fn, 0, lists:seq(0,255)) bsr 1,
    io:format("Count of keys in db is ~w~n", [NumberOfPairs]),  
    {ok, _} = file:position(Handle, {bof, 2048}),
    Fn1 = fun(_I,Acc) ->
        {KL,VL} = read_next_2_integers(Handle),
        Key = read_next_term(Handle, KL),
        case read_next_term(Handle, VL, crc, CRCCheck) of
            {false, _} ->
            {ok, CurrLoc} = file:position(Handle, cur),
            Return = {crc_wonky, get(Handle, Key)};
        {_, Value} ->
            {ok, CurrLoc} = file:position(Handle, cur),
            Return =
                case get(Handle, Key) of
                    {Key,Value} -> {Key ,Value};
                    X ->  {wonky, X}
                end
        end,
        {ok, _} = file:position(Handle, CurrLoc),
        [Return | Acc]
    end,
    lists:foldr(Fn1, [], lists:seq(0, NumberOfPairs-1)).

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
            LogDetails = [LastPosition, EndPosition],
            io:format("File to be truncated at last position of ~w " 
                        "with end of file at ~w~n", LogDetails),
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

%% Should not be used for non-test PUTs by the inker - as the Max File Size
%% should be taken from the startup options not the default
put(FileName, Key, Value, {LastPosition, HashTree}) ->
    put(FileName, Key, Value, {LastPosition, HashTree},
            ?BINARY_MODE, ?MAX_FILE_SIZE).


%%
%% get(FileName,Key) -> {key,value}
%% Given a filename and a key, returns a key and value tuple.
%%
get_withcache(Handle, Key, Cache) ->
    get(Handle, Key, ?CRC_CHECK, Cache).

get(FileNameOrHandle, Key) ->
    get(FileNameOrHandle, Key, ?CRC_CHECK).

get(FileNameOrHandle, Key, CRCCheck) ->
    get(FileNameOrHandle, Key, CRCCheck, no_cache).

get(FileName, Key, CRCCheck, Cache) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName,[binary, raw, read]),
    get(Handle, Key, CRCCheck, Cache);
get(Handle, Key, CRCCheck, Cache) when is_tuple(Handle) ->
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
                                Hash, Key, CRCCheck)
    end.

get_index(Handle, Index, no_cache) ->
    {ok,_} = file:position(Handle, {bof, ?DWORD_SIZE * Index}),
    % Get location of hashtable and number of entries in the hash
    read_next_2_integers(Handle);
get_index(_Handle, Index, Cache) ->
    {Index, {Pointer, Count}} = lists:keyfind(Index, 1, Cache),
    {Pointer, Count}.

%% Get a Key/Value pair from an active CDB file (with no hash table written)
%% This requires a key dictionary to be passed in (mapping keys to positions)
%% Will return {Key, Value} or missing
get_mem(Key, FNOrHandle, HashTree) ->
    get_mem(Key, FNOrHandle, HashTree, ?CRC_CHECK).

get_mem(Key, Filename, HashTree, CRCCheck) when is_list(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    get_mem(Key, Handle, HashTree, CRCCheck);
get_mem(Key, Handle, HashTree, CRCCheck) ->
    ListToCheck = get_hashtree(Key, HashTree),
    case {CRCCheck, ListToCheck} of
        {loose_presence, []} ->
            missing;
        {loose_presence, _L} ->
            probably;
        _ ->
        extract_kvpair(Handle, ListToCheck, Key, CRCCheck)
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
            NextKey = read_next_term(Handle, KeyLength),
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


%% Fold over all of the objects in the file, applying FoldFun to each object
%% where FoldFun(K, V, Acc0) -> Acc , or FoldFun(K, Acc0) -> Acc if KeyOnly is
%% set to true

fold(FileName, FoldFun, Acc0) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName, [binary, raw, read]),
    fold(Handle, FoldFun, Acc0);
fold(Handle, FoldFun, Acc0) ->
    {ok, _} = file:position(Handle, bof),
    {FirstHashPosition, _} = read_next_2_integers(Handle),
    fold(Handle, FoldFun, Acc0, {256 * ?DWORD_SIZE, FirstHashPosition}, false).

fold(Handle, FoldFun, Acc0, {Position, FirstHashPosition}, KeyOnly) ->
    {ok, Position} = file:position(Handle, Position),
    case Position of 
        FirstHashPosition ->
            Acc0;
        _ ->
            case read_next_2_integers(Handle) of 
                {KeyLength, ValueLength} ->
                    NextKey = read_next_term(Handle, KeyLength),
                    NextPosition = Position
                                    + KeyLength + ValueLength +
                                    ?DWORD_SIZE,
                    case KeyOnly of 
                        true ->
                            fold(Handle,
                                    FoldFun,
                                    FoldFun(NextKey, Acc0), 
                                    {NextPosition, FirstHashPosition},
                                    KeyOnly);
                        false ->
                            case read_next_term(Handle,
                                                    ValueLength,
                                                    crc,
                                                    ?CRC_CHECK) of
                                {false, _} -> 
                                    io:format("Skipping value for Key ~w as CRC
                                                check failed~n", [NextKey]),
                                    fold(Handle,
                                            FoldFun,
                                            Acc0, 
                                            {NextPosition, FirstHashPosition},
                                            KeyOnly);
                                {_, Value} ->
                                    fold(Handle,
                                            FoldFun,
                                            FoldFun(NextKey, Value, Acc0), 
                                            {NextPosition, FirstHashPosition},
                                            KeyOnly)
                            end
                    end;
                eof ->
                    Acc0
            end
    end.


fold_keys(FileName, FoldFun, Acc0) when is_list(FileName) ->
    {ok, Handle} = file:open(FileName, [binary, raw, read]),
    fold_keys(Handle, FoldFun, Acc0);
fold_keys(Handle, FoldFun, Acc0) ->
    {ok, _} = file:position(Handle, bof),
    {FirstHashPosition, _} = read_next_2_integers(Handle),
    fold(Handle, FoldFun, Acc0, {256 * ?DWORD_SIZE, FirstHashPosition}, true).

hashtable_calc(HashTree, StartPos) ->
    Seq = lists:seq(0, 255),
    SWC = os:timestamp(),
    {IndexList, HashTreeBin} = write_hash_tables(Seq,
                                                    HashTree,
                                                    StartPos,
                                                    [],
                                                    <<>>),
    io:format("HashTree computed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWC)]),
    {IndexList, HashTreeBin}.

%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%

determine_new_filename(Filename) ->
    filename:rootname(Filename, ".pnd") ++ ".cdb".
    
rename_for_read(Filename, NewName) ->
    %% Rename file
    io:format("Renaming file from ~s to ~s " ++ 
                "for which existence is ~w~n",
                [Filename, NewName,
                    filelib:is_file(NewName)]),
    file:rename(Filename, NewName).

open_for_readonly(Filename) ->
    {ok, Handle} = file:open(Filename, [binary, raw, read]),
    Index = load_index(Handle),
    LastKey = find_lastkey(Handle, Index),
    {Handle, Index, LastKey}.

load_index(Handle) ->
    Index = lists:seq(0, 255),
    lists:map(fun(X) ->
                    file:position(Handle, {bof, ?DWORD_SIZE * X}),
                    {HashTablePos, Count} = read_next_2_integers(Handle),
                    {X, {HashTablePos, Count}} end,
                Index).

%% Function to find the LastKey in the file
find_lastkey(Handle, IndexCache) ->
    LastPosition = scan_index(Handle,
                                IndexCache,
                                {fun scan_index_findlast/4, 0}),
    {ok, _} = file:position(Handle, LastPosition),
    {KeyLength, _ValueLength} = read_next_2_integers(Handle),
    read_next_term(Handle, KeyLength).

scan_index(Handle, IndexCache, {ScanFun, InitAcc}) ->
    lists:foldl(fun({_X, {Pos, Count}}, Acc) ->
                        ScanFun(Handle, Pos, Count, Acc)
                        end,
                        InitAcc,
                        IndexCache).

scan_index_forsample(_Handle, [], _ScanFun, Acc, SampleSize) ->
    lists:sublist(Acc, SampleSize);
scan_index_forsample(Handle, [CacheEntry|Tail], ScanFun, Acc, SampleSize) ->
    case length(Acc) of
        L when L >= SampleSize ->
            lists:sublist(Acc, SampleSize);
        _ ->
            {_X, {Pos, Count}} = CacheEntry,
            scan_index_forsample(Handle,
                                    Tail,
                                    ScanFun,
                                    ScanFun(Handle, Pos, Count, Acc),
                                    SampleSize)
    end.


scan_index_findlast(Handle, Position, Count, LastPosition) ->
    {ok, _} = file:position(Handle, Position),
    lists:foldl(fun({_Hash, HPos}, MaxPos) -> max(HPos, MaxPos) end,
                        LastPosition,
                        read_next_n_integerpairs(Handle, Count)).

scan_index_returnpositions(Handle, Position, Count, PosList0) ->
    {ok, _} = file:position(Handle, Position),
    lists:foldl(fun({Hash, HPosition}, PosList) ->
                                case Hash of
                                    0 -> PosList;
                                    _ -> PosList ++ [HPosition]
                                end end,
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
    Tree = array:get(Index, HashTree),
    case gb_trees:lookup(Hash, Tree) of 
        {value, List} ->
            List;
        _ ->
            []
    end.

%% Add to hash tree - this is an array of 256 gb_trees that contains the Hash 
%% and position of objects which have been added to an open CDB file
put_hashtree(Key, Position, HashTree) ->
  Hash = hash(Key),
  Index = hash_to_index(Hash),
  Tree = array:get(Index, HashTree),
  case gb_trees:lookup(Hash, Tree) of 
      none ->
          array:set(Index, gb_trees:insert(Hash, [Position], Tree), HashTree);
      {value, L} ->
          array:set(Index, gb_trees:update(Hash, [Position|L], Tree), HashTree)
  end. 

%% Function to extract a Key-Value pair given a file handle and a position
%% Will confirm that the key matches and do a CRC check when requested
extract_kvpair(_, [], _, _) ->
    missing;
extract_kvpair(Handle, [Position|Rest], Key, Check) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    case read_next_term(Handle, KeyLength) of
        Key ->  % If same key as passed in, then found!
            case read_next_term(Handle, ValueLength, crc, Check) of
                {false, _} -> 
                    crc_wonky;
                {_, Value} ->
                    {Key,Value}
            end;
        _ ->
            extract_kvpair(Handle, Rest, Key, Check)
    end.

extract_key(Handle, Position) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, _ValueLength} = read_next_2_integers(Handle),
    read_next_term(Handle, KeyLength).

extract_key_size(Handle, Position) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    {read_next_term(Handle, KeyLength), ValueLength}.

extract_key_value_check(Handle, Position) ->
    {ok, _} = file:position(Handle, Position),
    {KeyLength, ValueLength} = read_next_2_integers(Handle),
    K = read_next_term(Handle, KeyLength),
    {Check, V} = read_next_term(Handle, ValueLength, crc, true),
    {K, V, Check}.

%% Scan through the file until there is a failure to crc check an input, and 
%% at that point return the position and the key dictionary scanned so far
startup_scan_over_file(Handle, Position) ->
    HashTree = array:new(256, {default, gb_trees:empty()}),
    scan_over_file(Handle,
                    Position,
                    fun startup_filter/5,
                    {HashTree, empty},
                    empty).

%% Specific filter to be used at startup to build a hashtree for an incomplete
%% cdb file, and returns at the end the hashtree and the final Key seen in the
%% journal

startup_filter(Key, ValueAsBin, Position, {Hashtree, LastKey}, _ExtractFun) ->
    case crccheck_value(ValueAsBin) of
        true ->
            {loop, {put_hashtree(Key, Position, Hashtree), Key}};
        false ->
            {stop, {Hashtree, LastKey}}
    end.


%% Scan for key changes - scan over file returning applying FilterFun
%% The FilterFun should accept as input:
%% - Key, ValueBin, Position, Accumulator, Fun (to extract values from Binary)
%% -> outputting a new Accumulator and a loop|stop instruction as a tuple
%% i.e. {loop, Acc} or {stop, Acc}

scan_over_file(Handle, Position, FilterFun, Output, LastKey) ->
    case saferead_keyvalue(Handle) of
        false ->
            io:format("Failure to read Key/Value at Position ~w"
                            ++ " in scan~n", [Position]),
            {Position, Output};
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
                    {NewPosition, UpdOutput};
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
%% catch expected exceptiosn associated with file corruption (or end) and 
%% return eof
saferead_keyvalue(Handle) ->
    case read_next_2_integers(Handle) of 
        {error, einval} ->
            false;
        eof ->
            false;
        {KeyL, ValueL} ->
            case safe_read_next_term(Handle, KeyL) of 
                {error, einval} ->
                    false;
                eof ->
                    false;
                false ->
                    false;
                Key ->
                    case file:read(Handle, ValueL) of 
                        {error, einval} ->
                            false;
                        eof ->
                            false;
                        {ok, Value} ->
                            {Key, Value, KeyL, ValueL}
                    end 
            end
    end.


safe_read_next_term(Handle, Length) ->
    try read_next_term(Handle, Length) of
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
            io:format("CRC check failed due to mismatch ~n"),
            false
        end;
crccheck_value(_) ->
    io:format("CRC check failed due to size ~n"),
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

read_next_term(Handle, Length) ->
    case file:read(Handle, Length) of  
        {ok, Bin} ->
            binary_to_term(Bin);
        ReadError ->
            ReadError
    end.

%% Read next string where the string has a CRC prepended - stripping the crc 
%% and checking if requested
read_next_term(Handle, Length, crc, Check) ->
    case Check of 
        true ->
            {ok, <<CRC:32/integer, Bin/binary>>} = file:read(Handle, Length),
            case calc_crc(Bin) of 
                CRC ->
                    {true, binary_to_term(Bin)};
                _ ->
                    {false, binary_to_term(Bin)}
            end;
        false ->
            {ok, _} = file:position(Handle, {cur, 4}),
            {ok, Bin} = file:read(Handle, Length - 4),
            {unchecked, binary_to_term(Bin)}
    end.

%% Extract value and size from binary containing CRC
extract_valueandsize(ValueAsBin) ->
    <<_CRC:32/integer, Bin/binary>> = ValueAsBin,
    {binary_to_term(Bin), byte_size(Bin)}.


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

search_hash_table(_Handle, [], _Hash, _Key, _CRCCheck) -> 
    missing;
search_hash_table(Handle, [Entry|RestOfEntries], Hash, Key, CRCCheck) ->
    {ok, _} = file:position(Handle, Entry),
    {StoredHash, DataLoc} = read_next_2_integers(Handle),
    case StoredHash of
        Hash ->
            KV = case CRCCheck of
                loose_presence ->
                    probably;
                _ ->
                    extract_kvpair(Handle, [DataLoc], Key, CRCCheck)
            end,
            case KV of
                missing ->
                    search_hash_table(Handle,
                                        RestOfEntries,
                                        Hash,
                                        Key,
                                        CRCCheck);
                _ ->
                    KV 
            end;
        0 ->
            % Hash is 0 so key must be missing as 0 found before Hash matched
            missing;
        _ ->
            search_hash_table(Handle, RestOfEntries, Hash, Key, CRCCheck)
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
    HashTree = array:new(256, {default, gb_trees:empty()}),
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
    io:format("HashTree written in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWW)]),
    ok.


write_hash_tables([], _HashTree, _CurrPos, IndexList, HashTreeBin) ->
    {IndexList, HashTreeBin};
write_hash_tables([Index|Rest], HashTree, CurrPos, IndexList, HashTreeBin) ->
    Tree = array:get(Index, HashTree),
    case gb_trees:keys(Tree) of 
        [] ->
            write_hash_tables(Rest, HashTree, CurrPos, IndexList, HashTreeBin);
        _ ->
            HashList = gb_trees:to_list(Tree),
            BinList = build_binaryhashlist(HashList, []),
            IndexLength = length(BinList) * 2,
            SlotList = lists:duplicate(IndexLength, <<0:32, 0:32>>),
    
            Fn = fun({Hash, Binary}, AccSlotList) ->
                Slot1 = find_open_slot(AccSlotList, Hash),
                {L1, [<<0:32, 0:32>>|L2]} = lists:split(Slot1, AccSlotList),
                lists:append(L1, [Binary|L2])
            end,
            
            NewSlotList = lists:foldl(Fn, SlotList, BinList),
            NewSlotBin = lists:foldl(fun(X, Acc) ->
                                            <<Acc/binary, X/binary>> end,
                                        HashTreeBin,
                                        NewSlotList),
            write_hash_tables(Rest,
                                HashTree,
                                CurrPos + length(NewSlotList) * ?DWORD_SIZE,
                                [{Index, CurrPos, IndexLength}|IndexList],
                                NewSlotBin)
    end.

%% The list created from the original HashTree may have duplicate positions 
%% e.g. {Key, [Value1, Value2]}.  Before any writing is done it is necessary
%% to know the actual number of hashes - or the Slot may not be sized correctly
%%
%% This function creates {Hash, Binary} pairs on a list where there is a unique
%% entry for eveyr Key/Value
build_binaryhashlist([], BinList) ->
    BinList;
build_binaryhashlist([{Hash, [Position|TailP]}|TailKV], BinList) ->
    HashLE = endian_flip(Hash),
    PosLE = endian_flip(Position),
    NewBin = <<HashLE:32, PosLE:32>>,
    case TailP of 
        [] ->
            build_binaryhashlist(TailKV,
                                    [{Hash, NewBin}|BinList]);
        _ ->
            build_binaryhashlist([{Hash, TailP}|TailKV],
                                    [{Hash, NewBin}|BinList])
    end.

%% Slot is zero based because it comes from a REM
find_open_slot(List, Hash) ->
    Len = length(List),
    Slot = hash_to_slot(Hash, Len),
    Seq = lists:seq(1, Len),
    {CL1, CL2} = lists:split(Slot, Seq),
    {L1, L2} = lists:split(Slot, List),
    find_open_slot1(lists:append(CL2, CL1), lists:append(L2, L1)).
  
find_open_slot1([Slot|_RestOfSlots], [<<0:32,0:32>>|_RestOfEntries]) -> 
    Slot - 1;
find_open_slot1([_|RestOfSlots], [_|RestOfEntries]) -> 
    find_open_slot1(RestOfSlots, RestOfEntries).


%% Write the top most 255 doubleword entries.  First word is the 
%% file pointer to a hashtable and the second word is the number of entries 
%% in the hash table
%% The List passed in should be made up of {Index, Position, Count} tuples
write_top_index_table(Handle, BasePos, List) ->
  % fold function to find any missing index tuples, and add one a replacement 
  % in this case with a count of 0.  Also orders the list by index
    FnMakeIndex = fun(I) ->
        case lists:keysearch(I, 1, List) of
            {value, Tuple} ->
                Tuple;
            false ->
                {I, BasePos, 0}
        end
    end,
    % Fold function to write the index entries
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
    
    Seq = lists:seq(0, 255),
    CompleteList = lists:keysort(1, lists:map(FnMakeIndex, Seq)),
    {IndexBin, _Pos} = lists:foldl(FnWriteIndex,
                                    {<<>>, BasePos},
                                    CompleteList),
    {ok, _} = file:position(Handle, 0),
    ok = file:write(Handle, IndexBin),
    ok = file:advise(Handle, 0, ?DWORD_SIZE * 256, will_need),
    ok.

%% To make this compatible with original Bernstein format this endian flip
%% and also the use of the standard hash function required.
%%
%% Hash function contains mysterious constants, some explanation here as to
%% what they are -
%% http://stackoverflow.com/ ++
%% questions/10696223/reason-for-5381-number-in-djb-hash-function
  
endian_flip(Int) ->
    <<X:32/unsigned-little-integer>> = <<Int:32>>,
    X.

hash(Key) ->
    BK = term_to_binary(Key),
    H = 5381,
    hash1(H, BK) band 16#FFFFFFFF.

hash1(H, <<>>) -> 
    H;
hash1(H, <<B:8/integer, Rest/bytes>>) ->
    H1 = H * 33,
    H2 = H1 bxor B,
    hash1(H2, Rest).

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


%%%%%%%%%%%%%%%%
% T E S T 
%%%%%%%%%%%%%%%  
-ifdef(TEST).

write_key_value_pairs_1_test() ->
    {ok,Handle} = file:open("../test/test.cdb",[write]),
    {_, HashTree} = write_key_value_pairs(Handle,
                                            [{"key1","value1"},
                                                {"key2","value2"}]),
    Hash1 = hash("key1"),
    Index1 = hash_to_index(Hash1),
    Hash2 = hash("key2"),
    Index2 = hash_to_index(Hash2),
    R0 = array:new(256, {default, gb_trees:empty()}),
    R1 = array:set(Index1,
                    gb_trees:insert(Hash1,
                                        [0],
                                        array:get(Index1, R0)),
                    R0),
    R2 = array:set(Index2,
                    gb_trees:insert(Hash2,
                                        [30],
                                        array:get(Index2, R1)),
                    R1),
    io:format("HashTree is ~w~n", [HashTree]),
    io:format("Expected HashTree is ~w~n", [R2]),
    ?assertMatch(R2, HashTree),
    ok = file:delete("../test/test.cdb").


write_hash_tables_1_test() ->
    {ok, Handle} = file:open("../test/testx.cdb", [write]),
    R0 = array:new(256, {default, gb_trees:empty()}),
    R1 = array:set(64,
                    gb_trees:insert(6383014720,
                                    [18],
                                    array:get(64, R0)),
                    R0),
    R2 = array:set(67,
                    gb_trees:insert(6383014723,
                                    [0],
                                    array:get(67, R1)),
                    R1),
    Result = write_hash_tables(Handle, R2),
    io:format("write hash tables result of ~w ~n", [Result]),
    ?assertMatch(Result,[{67,16,2},{64,0,2}]),
    ok = file:delete("../test/testx.cdb").

find_open_slot_1_test() ->
    List = [<<1:32,1:32>>,<<0:32,0:32>>,<<1:32,1:32>>,<<1:32,1:32>>],
    Slot = find_open_slot(List,0),
    ?assertMatch(Slot,1).

find_open_slot_2_test() ->
    List = [<<0:32,0:32>>,<<0:32,0:32>>,<<1:32,1:32>>,<<1:32,1:32>>],
    Slot = find_open_slot(List,0),
    ?assertMatch(Slot,0).

find_open_slot_3_test() ->
    List = [<<1:32,1:32>>,<<1:32,1:32>>,<<1:32,1:32>>,<<0:32,0:32>>],
    Slot = find_open_slot(List,2),
    ?assertMatch(Slot,3).

find_open_slot_4_test() ->
    List = [<<0:32,0:32>>,<<1:32,1:32>>,<<1:32,1:32>>,<<1:32,1:32>>],
    Slot = find_open_slot(List,1),
    ?assertMatch(Slot,0).

find_open_slot_5_test() ->
    List = [<<1:32,1:32>>,<<1:32,1:32>>,<<0:32,0:32>>,<<1:32,1:32>>],
    Slot = find_open_slot(List,3),
    ?assertMatch(Slot,2).

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
                    get_mem(Key, "../test/test_mem.cdb",
                    UpdKeyDict)),
    ?assertMatch(probably,
                    get_mem(Key, "../test/test_mem.cdb",
                    UpdKeyDict,
                    loose_presence)),
    ?assertMatch(missing,
                    get_mem("not_present", "../test/test_mem.cdb",
                    UpdKeyDict,
                    loose_presence)),
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
    ?assertMatch({"key1", "value1"}, get(Handle, Key1)),
    ?assertMatch(probably, get(Handle, Key1, loose_presence)),
    ?assertMatch(missing, get(Handle, "Key99", loose_presence)),
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
    ?assertMatch(missing, get("../test/hashtable1_test.cdb", Key1)),
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

fold_test() ->
    K1 = {"Key1", 1},
    V1 = 2,
    K2 = {"Key1", 2},
    V2 = 4,
    K3 = {"Key1", 3},
    V3 = 8,
    K4 = {"Key1", 4},
    V4 = 16,
    K5 = {"Key1", 5},
    V5 = 32,
    D = dict:from_list([{K1, V1}, {K2, V2}, {K3, V3}, {K4, V4}, {K5, V5}]),
    ok = from_dict("../test/fold_test.cdb", D),
    FromSN = 2,
    FoldFun = fun(K, V, Acc) ->
                {_Key, Seq} = K,
                if Seq > FromSN ->
                    Acc + V;
                true ->
                    Acc
                end
                end,
    ?assertMatch(56, fold("../test/fold_test.cdb", FoldFun, 0)),
    ok = file:delete("../test/fold_test.cdb").

fold_keys_test() ->
    K1 = {"Key1", 1},
    V1 = 2,
    K2 = {"Key2", 2},
    V2 = 4,
    K3 = {"Key3", 3},
    V3 = 8,
    K4 = {"Key4", 4},
    V4 = 16,
    K5 = {"Key5", 5},
    V5 = 32,
    D = dict:from_list([{K1, V1}, {K2, V2}, {K3, V3}, {K4, V4}, {K5, V5}]),
    ok = from_dict("../test/fold_keys_test.cdb", D),
    FromSN = 2,
    FoldFun = fun(K, Acc) ->
    {Key, Seq} = K,
    if Seq > FromSN ->
            lists:append(Acc, [Key]);
        true ->
            Acc
    end
    end,
    Result = fold_keys("../test/fold_keys_test.cdb", FoldFun, []),
    ?assertMatch(["Key3", "Key4", "Key5"], lists:sort(Result)),
    ok = file:delete("../test/fold_keys_test.cdb").

fold2_test() ->
    K1 = {"Key1", 1},
    V1 = 2,
    K2 = {"Key1", 2},
    V2 = 4,
    K3 = {"Key1", 3},
    V3 = 8,
    K4 = {"Key1", 4},
    V4 = 16,
    K5 = {"Key1", 5},
    V5 = 32,
    K6 = {"Key2", 1},
    V6 = 64,
    D = dict:from_list([{K1, V1}, {K2, V2}, {K3, V3}, 
    {K4, V4}, {K5, V5}, {K6, V6}]),
    ok = from_dict("../test/fold2_test.cdb", D),
    FoldFun = fun(K, V, Acc) ->
                    {Key, Seq} = K,
                    case dict:find(Key, Acc) of 
                        error ->
                            dict:store(Key, {Seq, V}, Acc);
                        {ok, {LSN, _V}} when Seq > LSN ->
                            dict:store(Key, {Seq, V}, Acc);
                        _ ->
                            Acc 
                    end 
                end,
    RD = dict:new(),
    RD1 = dict:store("Key1", {5, 32}, RD),
    RD2 = dict:store("Key2", {1, 64}, RD1),
    Result = fold("../test/fold2_test.cdb", FoldFun, dict:new()),
    ?assertMatch(RD2, Result),
    ok = file:delete("../test/fold2_test.cdb").

find_lastkey_test() ->
    {ok, P1} = cdb_open_writer("../test/lastkey.pnd"),
    ok = cdb_put(P1, "Key1", "Value1"),
    ok = cdb_put(P1, "Key3", "Value3"),
    ok = cdb_put(P1, "Key2", "Value2"),
    ?assertMatch("Key2", cdb_lastkey(P1)),
    ?assertMatch("Key1", cdb_firstkey(P1)),
    probably = cdb_keycheck(P1, "Key2"),
    ok = cdb_close(P1),
    {ok, P2} = cdb_open_writer("../test/lastkey.pnd"),
    ?assertMatch("Key2", cdb_lastkey(P2)),
    probably = cdb_keycheck(P2, "Key2"),
    {ok, F2} = cdb_complete(P2),
    {ok, P3} = cdb_open_reader(F2),
    ?assertMatch("Key2", cdb_lastkey(P3)),
    ok = cdb_close(P3),
    ok = file:delete("../test/lastkey.cdb").

get_keys_byposition_simple_test() ->
    {ok, P1} = cdb_open_writer("../test/poskey.pnd"),
    ok = cdb_put(P1, "Key1", "Value1"),
    ok = cdb_put(P1, "Key3", "Value3"),
    ok = cdb_put(P1, "Key2", "Value2"),
    KeyList = ["Key1", "Key2", "Key3"],
    {ok, F2} = cdb_complete(P1),
    {ok, P2} = cdb_open_reader(F2),
    PositionList = cdb_getpositions(P2, all),
    io:format("Position list of ~w~n", [PositionList]),
    ?assertMatch(3, length(PositionList)),
    R1 = cdb_directfetch(P2, PositionList, key_only),
    ?assertMatch(3, length(R1)),
    lists:foreach(fun(Key) ->
                        Check = lists:member(Key, KeyList),
                        ?assertMatch(Check, true) end,
                    R1),
    R2 = cdb_directfetch(P2, PositionList, key_size),
    ?assertMatch(3, length(R2)),
    lists:foreach(fun({Key, _Size}) ->
                        Check = lists:member(Key, KeyList),
                        ?assertMatch(Check, true) end,
                    R2),
    R3 = cdb_directfetch(P2, PositionList, key_value_check),
    ?assertMatch(3, length(R3)),
    lists:foreach(fun({Key, Value, Check}) ->
                        ?assertMatch(Check, true),
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
    generate_sequentialkeys(Count - 1, KVList ++ [KV]).

get_keys_byposition_manykeys_test() ->
    KeyCount = 1024,
    {ok, P1} = cdb_open_writer("../test/poskeymany.pnd"),
    KVList = generate_sequentialkeys(KeyCount, []),
    lists:foreach(fun({K, V}) -> cdb_put(P1, K, V) end, KVList),
    SW1 = os:timestamp(),
    {ok, F2} = cdb_complete(P1),
    SW2 = os:timestamp(),
    io:format("CDB completed in ~w microseconds~n",
                [timer:now_diff(SW2, SW1)]),
    {ok, P2} = cdb_open_reader(F2),
    SW3 = os:timestamp(),
    io:format("CDB opened for read in ~w microseconds~n",
                [timer:now_diff(SW3, SW2)]),
    PositionList = cdb_getpositions(P2, all),
    io:format("Positions fetched in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW3)]),
    L1 = length(PositionList),
    ?assertMatch(L1, KeyCount),
    
    SampleList1 = cdb_getpositions(P2, 10),
    ?assertMatch(10, length(SampleList1)),
    SampleList2 = cdb_getpositions(P2, KeyCount),
    ?assertMatch(KeyCount, length(SampleList2)),
    SampleList3 = cdb_getpositions(P2, KeyCount + 1),
    ?assertMatch(KeyCount, length(SampleList3)),
    
    ok = cdb_close(P2),
    ok = file:delete(F2).


-endif.
