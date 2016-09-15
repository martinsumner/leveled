%% -------- Overview ---------
%%
%% The eleveleddb is based on the LSM-tree similar to leveldb, except that:
%% - Keys, Metadata and Values are not persisted together - the Keys and
%% Metadata are kept in a tree-based ledger, whereas the values are stored
%% only in a sequential Journal.
%% - Different file formats are used for Journal (based on constant
%% database), and the ledger (sft, based on sst)
%% - It is not intended to be general purpose, but be specifically suited for
%% use as a Riak backend in specific circumstances (relatively large values,
%% and frequent use of iterators)
%% - The Journal is an extended nursery log in leveldb terms.  It is keyed
%% on the sequence number of the write
%% - The ledger is a LSM tree, where the key is the actaul object key, and
%% the value is the metadata of the object including the sequence number
%%
%%
%% -------- The actors ---------
%% 
%% The store is fronted by a Bookie, who takes support from different actors:
%% - An Inker who persists new data into the jornal, and returns items from
%% the journal based on sequence number
%% - A Penciller who periodically redraws the ledger
%% - One or more Clerks, who may be used by either the inker or the penciller
%% to fulfill background tasks
%%
%% Both the Inker and the Penciller maintain a manifest of the files which
%% represent the current state of the Journal and the Ledger repsectively.
%% For the Inker the manifest maps ranges of sequence numbers to cdb files.
%% For the Penciller the manifest maps key ranges to files at each level of
%% the Ledger.
%%
%% -------- PUT --------
%%
%% A PUT request consists of
%% - A Primary Key and a Value
%% - IndexSpecs - a set of secondary key changes associated with the
%% transaction
%%
%% The Bookie takes the place request and passes it first to the Inker to add
%% the request to the ledger.
%%
%% The inker will pass the PK/Value/IndexSpecs to the current (append only)
%% CDB journal file to persist the change.  The call should return either 'ok'
%% or 'roll'. -'roll' indicates that the CDB file has insufficient capacity for
%% this write.
%%
%% (Note that storing the IndexSpecs will create some duplication with the
%% Metadata wrapped up within the Object value.  This Value and the IndexSpecs
%% are compressed before storage, so this should provide some mitigation for
%% the duplication).
%%
%% In resonse to a 'roll', the inker should:
%% - start a new active journal file with an open_write_request, and then;
%% - call to PUT the object in this file;
%% - reply to the bookie, but then in the background
%% - close the previously active journal file (writing the hashtree), and move
%% it to the historic journal
%%
%% The inker will also return the SQN which the change has been made at, as
%% well as the object size on disk within the Journal.
%%
%% Once the object has been persisted to the Journal, the Ledger can be updated.
%% The Ledger is updated by the Bookie applying a function (passed in at
%% startup) to the Value to return the Object Metadata, a function to generate
%% a hash of the Value and also taking the Primary Key, the IndexSpecs, the
%% Sequence Number in the Journal and the Object Size (returned from the
%% Inker).
%%
%% The Bookie should generate a series of ledger key changes from this
%% information, using a function passed in at startup.  For Riak this will be
%% of the form:
%% {{o, Bucket, Key},
%%      SQN,
%%      {Hash, Size, {Riak_Metadata}},
%%      {active, TS}|{tomb, TS}} or
%% {{i, Bucket, IndexTerm, IndexField, Key},
%%      SQN,
%%      null,
%%      {active, TS}|{tomb, TS}}
%%
%% Recent Ledger changes are retained initially in the Bookies' memory (in an
%% in-memory ets table).  Periodically, the current table is pushed to the
%% Penciller for eventual persistence, and a new table is started.
%%
%% This completes the non-deferrable work associated with a PUT
%%
%% -------- Snapshots (Key & Metadata Only) --------
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% must first produce a tree representing the results of the request which are
%% present in its in-memory view of the ledger.  The Bookie then requests
%% a copy of the current Ledger manifest from the Penciller, and the Penciller
%5 should interest of the iterator at the manifest sequence number at the time
%% of the request.
%%
%% Iterators should de-register themselves from the Penciller on completion.
%% Iterators should be automatically release after a timeout period.  A file
%% can only be deleted from the Ledger if it is no longer in the manifest, and
%% there are no registered iterators from before the point the file was
%% removed from the manifest.
%%
%% Snapshots may be non-recent, if recency is unimportant.  Non-recent
%% snapshots do no require the Bookie to return the results of the in-memory
%% table, the Penciller alone cna be asked.
%%
%% -------- Special Ops --------
%%
%% e.g. Get all for SegmentID/Partition
%%
%%
%%
%% -------- On Startup --------
%%
%% On startup the Bookie must restart both the Inker to load the Journal, and
%% the Penciller to load the Ledger.  Once the Penciller has started, the
%% Bookie should request the highest sequence number in the Ledger, and then
%% and try and rebuild any missing information from the Journal
%%
%% To rebuild the Ledger it requests the Inker to scan over the files from
%% the sequence number and re-generate the Ledger changes.



-module(leveled_bookie).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        book_start/1,
        book_put/4,
        book_get/2,
        book_head/2,
        strip_to_keyonly/1,
        strip_to_keyseqonly/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        strip_to_details/1]).

-include_lib("eunit/include/eunit.hrl").

-define(CACHE_SIZE, 1000).

-record(state, {inker :: pid(),
                penciller :: pid(),
                metadata_extractor :: function(),
                indexspec_converter :: function(),
                cache_size :: integer(),
                back_pressure :: boolean(),
                ledger_cache :: gb_trees:tree()}).



%%%============================================================================
%%% API
%%%============================================================================

book_start(Opts) ->
    gen_server:start(?MODULE, [Opts], []).

book_put(Pid, PrimaryKey, Object, IndexSpecs) ->
    gen_server:call(Pid, {put, PrimaryKey, Object, IndexSpecs}, infinity).

book_get(Pid, PrimaryKey) ->
    gen_server:call(Pid, {get, PrimaryKey}, infinity).

book_head(Pid, PrimaryKey) ->
    gen_server:call(Pid, {head, PrimaryKey}, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    {InkerOpts, PencillerOpts} = set_options(Opts),
    {Inker, Penciller} = startup(InkerOpts, PencillerOpts),
    Extractor = if
                    Opts#bookie_options.metadata_extractor == undefined ->
                        fun extract_metadata/2;
                    true ->
                        Opts#bookie_options.metadata_extractor
                end,
    Converter = if
                    Opts#bookie_options.indexspec_converter == undefined ->
                        fun convert_indexspecs/3;
                    true ->
                        Opts#bookie_options.indexspec_converter
                end,
    CacheSize = if
                    Opts#bookie_options.cache_size == undefined ->
                        ?CACHE_SIZE;
                    true ->
                        Opts#bookie_options.cache_size
                end,
    {ok, #state{inker=Inker,
                penciller=Penciller,
                metadata_extractor=Extractor,
                indexspec_converter=Converter,
                cache_size=CacheSize,
                ledger_cache=gb_trees:empty()}}.


handle_call({put, PrimaryKey, Object, IndexSpecs}, From, State) ->
    {ok, SQN, ObjSize} = leveled_inker:ink_put(PrimaryKey, Object, IndexSpecs),
    Changes = preparefor_ledgercache(PrimaryKey,
                                        SQN,
                                        Object,
                                        ObjSize,
                                        IndexSpecs),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    gen_server:reply(From, ok),
    case maybepush_ledgercache(State#state.cache_size,
                                            Cache0,
                                            State#state.penciller) of
        {ok, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache, back_pressure=false}};
        {pause, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache, back_pressure=true}}
    end;
handle_call({get, Key}, _From, State) ->
    case fetch_head(Key, State#state.penciller, State#state.ledger_cache) of
        not_present ->
            {reply, not_found, State};
        Head ->
            {Key, Seqn, Status} = strip_to_details(Head),
            case Status of
                {tomb, _} ->
                    {reply, not_found, State};
                {active, _} ->
                    case fetch_value(Key, Seqn, State#state.inker) of
                        not_present ->
                            {reply, not_found, State};
                        Object ->
                            {reply, {ok, Object}, State}
                    end
            end
    end;
handle_call({head, Key}, _From, State) ->
    case fetch_head(Key, State#state.penciller, State#state.ledger_cache) of
        not_present ->
            {reply, not_found, State};
        Head ->
            {Key, _Seqn, Status} = strip_to_details(Head),
            case Status of
                {tomb, _} ->
                    {reply, not_found, State};
                {active, _} ->
                    MD = strip_to_mdonly(Head),
                    {reply, {ok, MD}, State}
            end
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

set_options(Opts) ->
    {#inker_options{root_path=Opts#bookie_options.root_path},
        #penciller_options{root_path=Opts#bookie_options.root_path}}.

startup(InkerOpts, PencillerOpts) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    ok = leveled_inker:ink_loadpcl(LedgerSQN, fun load_fun/4, Penciller),
    {Inker, Penciller}.


fetch_head(Key, Penciller, Cache) ->
    case gb_trees:lookup(Key, Cache) of
        {value, Head} ->
            Head;
        none ->
            case leveled_penciller:pcl_fetch(Penciller, Key) of
                {Key, Head} ->
                    Head;
                not_present ->
                    not_present
            end
    end.

fetch_value(Key, SQN, Inker) ->
    case leveled_inker:ink_fetch(Inker, Key, SQN) of
        {ok, Value} ->
            Value;
        not_present ->
            not_present
    end.

%% Format of a Key within the ledger is
%% {PrimaryKey, SQN, Metadata, Status} 

strip_to_keyonly({keyonly, K}) -> K;
strip_to_keyonly({K, _V}) -> K.

strip_to_keyseqonly({K, {SeqN, _, _}}) -> {K, SeqN}.

strip_to_statusonly({_, {_, St, _}}) -> St.

strip_to_seqonly({_, {SeqN, _, _}}) -> SeqN.

strip_to_details({K, {SeqN, St, _}}) -> {K, SeqN, St}.

strip_to_mdonly({_, {_, _, MD}}) -> MD.

get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents].

set_vclock(Object=#r_object{}, VClock) -> Object#r_object{vclock=VClock}.

vclock(#r_object{vclock=VClock}) -> VClock.

to_binary(v0, Obj) ->
    term_to_binary(Obj).

hash(Obj=#r_object{}) ->
    Vclock = vclock(Obj),
    UpdObj = set_vclock(Obj, lists:sort(Vclock)),
    erlang:phash2(to_binary(v0, UpdObj)).

extract_metadata(Obj, Size) ->
    {get_metadatas(Obj), vclock(Obj), hash(Obj), Size}.

convert_indexspecs(IndexSpecs, SQN, PrimaryKey) ->
    lists:map(fun({IndexOp, IndexField, IndexValue}) ->
                        Status = case IndexOp of
                                    add ->
                                        %% TODO: timestamp support
                                        {active, infinity};
                                    remove ->
                                        %% TODO: timestamps for delayed reaping 
                                        {tomb, infinity}
                                end,
                        {o, B, K} = PrimaryKey,
                        {{i, B, IndexField, IndexValue, K},
                            {SQN, Status, null}}
                    end,
                IndexSpecs).


preparefor_ledgercache(PK, SQN, Obj, Size, IndexSpecs) ->
    PrimaryChange = {PK,
                        {SQN,
                            {active, infinity},
                            extract_metadata(Obj, Size)}},
    SecChanges = convert_indexspecs(IndexSpecs, SQN, PK),
    [PrimaryChange] ++ SecChanges.

addto_ledgercache(Changes, Cache) ->
    lists:foldl(fun({{K, V}, Acc}) -> gb_trees:enter(K, V, Acc) end,
                    Cache,
                    Changes).

maybepush_ledgercache(MaxCacheSize, Cache, Penciller) ->
    CacheSize = gb_trees:size(Cache),
    if
        CacheSize > MaxCacheSize ->
            case leveled_penciller:pcl_pushmem(Penciller,
                                                gb_trees:to_list(Cache)) of
                ok ->
                    {ok, gb_trees:empty()};
                pause ->
                    {pause, gb_trees:empty()};
                refused ->
                    {ok, Cache}
            end;
        true ->
            {ok, Cache}
    end.

load_fun(KeyInLedger, ValueInLedger, _Position, Acc0) ->
    {MinSQN, MaxSQN, Output} = Acc0,
    {SQN, PK} = KeyInLedger,
    {Obj, IndexSpecs} = ValueInLedger,
    case SQN of
        SQN when SQN < MinSQN ->
            {loop, Acc0};    
        SQN when SQN =< MaxSQN ->
            %% TODO - get correct size in a more efficient manner
            %% Need to have compressed size
            Size = byte_size(term_to_binary(ValueInLedger, [compressed])),
            Changes = preparefor_ledgercache(PK, SQN, Obj, Size, IndexSpecs),
            {loop, {MinSQN, MaxSQN, Output ++ Changes}};
        SQN when SQN > MaxSQN ->
            {stop, Acc0}
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-endif.