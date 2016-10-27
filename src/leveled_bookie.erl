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
%% - The ledger is a merge tree, where the key is the actaul object key, and
%% the value is the metadata of the object including the sequence number
%%
%%
%% -------- The actors ---------
%% 
%% The store is fronted by a Bookie, who takes support from different actors:
%% - An Inker who persists new data into the journal, and returns items from
%% the journal based on sequence number
%% - A Penciller who periodically redraws the ledger, that associates keys with
%% sequence numbers and other metadata, as well as secondary keys (for index
%% queries)
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
%% The Ledger is updated by the Bookie applying a function (extract_metadata/4)
%% to the Value to return the Object Metadata, a function to generate a hash
%% of the Value and also taking the Primary Key, the IndexSpecs, the Sequence
%% Number in the Journal and the Object Size (returned from the Inker).
%%
%% The Bookie should generate a series of ledger key changes from this
%% information, using a function passed in at startup.  For Riak this will be
%% of the form:
%% {{o_rkv, Bucket, Key, SubKey|null},
%%      SQN,
%%      {Hash, Size, {Riak_Metadata}},
%%      {active, TS}|{tomb, TS}} or
%% {{i, Bucket, {IndexTerm, IndexField}, Key},
%%      SQN,
%%      null,
%%      {active, TS}|{tomb, TS}}
%%
%% Recent Ledger changes are retained initially in the Bookies' memory (in a
%% small generally balanced tree).  Periodically, the current table is pushed to
%% the Penciller for eventual persistence, and a new table is started.
%%
%% This completes the non-deferrable work associated with a PUT
%%
%% -------- Snapshots (Key & Metadata Only) --------
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% may request a clone of the Penciller, or the Penciller and the Inker.
%%
%% The clone is seeded with the manifest.  Teh clone should be registered with
%% the real Inker/Penciller, so that the real Inker/Penciller may prevent the
%% deletion of files still in use by a snapshot clone.
%%
%% Iterators should de-register themselves from the Penciller on completion.
%% Iterators should be automatically release after a timeout period.  A file
%% can only be deleted from the Ledger if it is no longer in the manifest, and
%% there are no registered iterators from before the point the file was
%% removed from the manifest.
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
%% and try and rebuild any missing information from the Journal.
%%
%% To rebuild the Ledger it requests the Inker to scan over the files from
%% the sequence number and re-generate the Ledger changes - pushing the changes
%% directly back into the Ledger.



-module(leveled_bookie).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        book_start/1,
        book_start/3,
        book_riakput/3,
        book_riakdelete/4,
        book_riakget/3,
        book_riakhead/3,
        book_put/5,
        book_delete/4,
        book_get/3,
        book_head/3,
        book_returnfolder/2,
        book_snapshotstore/3,
        book_snapshotledger/3,
        book_compactjournal/2,
        book_islastcompactionpending/1,
        book_close/1]).

-include_lib("eunit/include/eunit.hrl").

-define(CACHE_SIZE, 1600).
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").
-define(SHUTDOWN_WAITS, 60).
-define(SHUTDOWN_PAUSE, 10000).
-define(SNAPSHOT_TIMEOUT, 300000).
-define(JITTER_PROBABILITY, 0.1).

-record(state, {inker :: pid(),
                penciller :: pid(),
                cache_size :: integer(),
                back_pressure :: boolean(),
                ledger_cache :: gb_trees:tree(),
                is_snapshot :: boolean()}).



%%%============================================================================
%%% API
%%%============================================================================

book_start(RootPath, LedgerCacheSize, JournalSize) ->
    book_start(#bookie_options{root_path=RootPath,
                                cache_size=LedgerCacheSize,
                                max_journalsize=JournalSize}).

book_start(Opts) ->
    gen_server:start(?MODULE, [Opts], []).

book_riakput(Pid, RiakObject, IndexSpecs) ->
    {Bucket, Key} = leveled_codec:riakto_keydetails(RiakObject),
    book_put(Pid, Bucket, Key, RiakObject, IndexSpecs, ?RIAK_TAG).

book_put(Pid, Bucket, Key, Object, IndexSpecs) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, ?STD_TAG).

%% TODO:
%% It is not enough simply to change the value to delete, as the journal
%% needs to know the key is a tombstone at compaction time, and currently at
%% compaction time the clerk only knows the Key and not the Value.
%%
%% The tombstone cannot be removed from the Journal on compaction, as the
%% journal entry the tombstone deletes may not have been reaped - and so if the
%% ledger got erased, the value would be resurrected.

book_riakdelete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?RIAK_TAG).

book_delete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?STD_TAG).

book_riakget(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?RIAK_TAG).

book_get(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?STD_TAG).

book_riakhead(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?RIAK_TAG).

book_head(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?STD_TAG).

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag) ->
    gen_server:call(Pid, {put, Bucket, Key, Object, IndexSpecs, Tag}, infinity).

book_get(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {get, Bucket, Key, Tag}, infinity).

book_head(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag}, infinity).

book_returnfolder(Pid, FolderType) ->
    gen_server:call(Pid, {return_folder, FolderType}, infinity).

book_snapshotstore(Pid, Requestor, Timeout) ->
    gen_server:call(Pid, {snapshot, Requestor, store, Timeout}, infinity).

book_snapshotledger(Pid, Requestor, Timeout) ->
    gen_server:call(Pid, {snapshot, Requestor, ledger, Timeout}, infinity).

book_compactjournal(Pid, Timeout) ->
    gen_server:call(Pid, {compact_journal, Timeout}, infinity).

book_islastcompactionpending(Pid) ->
    gen_server:call(Pid, confirm_compact, infinity).

book_close(Pid) ->
    gen_server:call(Pid, close, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    case Opts#bookie_options.snapshot_bookie of
        undefined ->
            % Start from file not snapshot
            {InkerOpts, PencillerOpts} = set_options(Opts),
            {Inker, Penciller} = startup(InkerOpts, PencillerOpts),
            CacheSize = if
                            Opts#bookie_options.cache_size == undefined ->
                                ?CACHE_SIZE;
                            true ->
                                Opts#bookie_options.cache_size
                        end,
            io:format("Bookie starting with Pcl ~w Ink ~w~n",
                                                [Penciller, Inker]),
            {ok, #state{inker=Inker,
                        penciller=Penciller,
                        cache_size=CacheSize,
                        ledger_cache=gb_trees:empty(),
                        is_snapshot=false}};
        Bookie ->
            {ok,
                {Penciller, LedgerCache},
                Inker} = book_snapshotstore(Bookie, self(), ?SNAPSHOT_TIMEOUT),
            ok = leveled_penciller:pcl_loadsnapshot(Penciller,
                                                    gb_trees:empty()),
            io:format("Snapshot starting with Pcl ~w Ink ~w~n",
                                                [Penciller, Inker]),
            {ok, #state{penciller=Penciller,
                        inker=Inker,
                        ledger_cache=LedgerCache,
                        is_snapshot=true}}
    end.


handle_call({put, Bucket, Key, Object, IndexSpecs, Tag}, From, State) ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    {ok, SQN, ObjSize} = leveled_inker:ink_put(State#state.inker,
                                                LedgerKey,
                                                Object,
                                                IndexSpecs),
    Changes = preparefor_ledgercache(no_type_assigned,
                                        LedgerKey,
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
handle_call({get, Bucket, Key, Tag}, _From, State) ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    case fetch_head(LedgerKey,
                    State#state.penciller,
                    State#state.ledger_cache) of
        not_present ->
            {reply, not_found, State};
        Head ->
            {Seqn, Status, _MD} = leveled_codec:striphead_to_details(Head),
            case Status of
                tomb ->
                    {reply, not_found, State};
                {active, _} ->
                    case fetch_value(LedgerKey, Seqn, State#state.inker) of
                        not_present ->
                            {reply, not_found, State};
                        Object ->
                            {reply, {ok, Object}, State}
                    end
            end
    end;
handle_call({head, Bucket, Key, Tag}, _From, State) ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    case fetch_head(LedgerKey,
                    State#state.penciller,
                    State#state.ledger_cache) of
        not_present ->
            {reply, not_found, State};
        Head ->
            {_Seqn, Status, MD} = leveled_codec:striphead_to_details(Head),
            case Status of
                tomb ->
                    {reply, not_found, State};
                {active, _} ->
                    OMD = leveled_codec:build_metadata_object(LedgerKey, MD),
                    {reply, {ok, OMD}, State}
            end
    end;
handle_call({snapshot, _Requestor, SnapType, _Timeout}, _From, State) ->
    PCLopts = #penciller_options{start_snapshot=true,
                                    source_penciller=State#state.penciller},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    case SnapType of
        store ->
            InkerOpts = #inker_options{start_snapshot=true,
                                        source_inker=State#state.inker},
            {ok, JournalSnapshot} = leveled_inker:ink_start(InkerOpts),
            {reply,
                {ok,
                    {LedgerSnapshot,
                        State#state.ledger_cache},
                    JournalSnapshot},
                State};
        ledger ->
            {reply,
                {ok,
                    {LedgerSnapshot,
                        State#state.ledger_cache},
                    null},
                State}
    end;
handle_call({return_folder, FolderType}, _From, State) ->
    case FolderType of
        {riakbucket_stats, Bucket} ->
            {reply,
                bucket_stats(State#state.penciller,
                                    State#state.ledger_cache,
                                    Bucket,
                                    ?RIAK_TAG),
                State};
        {index_query,
                Bucket,
                {IdxField, StartValue, EndValue},
                {ReturnTerms, TermRegex}} ->
            {reply,
                index_query(State#state.penciller,
                                State#state.ledger_cache,
                                Bucket,
                                {IdxField, StartValue, EndValue},
                                {ReturnTerms, TermRegex}),
                State};
        {keylist, Tag} ->
                {reply,
                    allkey_query(State#state.penciller,
                                    State#state.ledger_cache,
                                    Tag),
                    State}
    end;
handle_call({compact_journal, Timeout}, _From, State) ->
    ok = leveled_inker:ink_compactjournal(State#state.inker,
                                            self(),
                                            Timeout),
    {reply, ok, State};
handle_call(confirm_compact, _From, State) ->
    {reply, leveled_inker:ink_compactionpending(State#state.inker), State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    io:format("Bookie closing for reason ~w~n", [Reason]),
    WaitList = lists:duplicate(?SHUTDOWN_WAITS, ?SHUTDOWN_PAUSE),
    ok = case shutdown_wait(WaitList, State#state.inker) of
            false ->
                io:format("Forcing close of inker following wait of "
                                ++ "~w milliseconds~n",
                            [lists:sum(WaitList)]),
                leveled_inker:ink_forceclose(State#state.inker);
            true ->
                ok
        end,
    ok = leveled_penciller:pcl_close(State#state.penciller).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

bucket_stats(Penciller, LedgerCache, Bucket, Tag) ->
    PCLopts = #penciller_options{start_snapshot=true,
                                    source_penciller=Penciller},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    Folder = fun() ->
                io:format("Length of increment in snapshot is ~w~n",
                            [gb_trees:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
                EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        fun accumulate_size/3,
                                                        {0, 0}),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                Acc
                end,
    {async, Folder}.

index_query(Penciller, LedgerCache,
                Bucket,
                {IdxField, StartValue, EndValue},
                {ReturnTerms, TermRegex}) ->
    PCLopts = #penciller_options{start_snapshot=true,
                                    source_penciller=Penciller},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    Folder = fun() ->
                io:format("Length of increment in snapshot is ~w~n",
                            [gb_trees:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                StartKey = leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG,
                                                        IdxField, StartValue),
                EndKey = leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG,
                                                        IdxField, EndValue),
                AddFun = case ReturnTerms of
                                true ->
                                    fun add_terms/3;
                                _ ->
                                    fun add_keys/3
                            end,
                AccFun = accumulate_index(TermRegex, AddFun),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        AccFun,
                                                        []),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                Acc
                end,
    {async, Folder}.

allkey_query(Penciller, LedgerCache, Tag) ->
    PCLopts = #penciller_options{start_snapshot=true,
                                    source_penciller=Penciller},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    Folder = fun() ->
                io:format("Length of increment in snapshot is ~w~n",
                            [gb_trees:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                SK = leveled_codec:to_ledgerkey(null, null, Tag),
                EK = leveled_codec:to_ledgerkey(null, null, Tag),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        SK,
                                                        EK,
                                                        fun accumulate_keys/3,
                                                        []),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                lists:reverse(Acc)
                end,
    {async, Folder}.


shutdown_wait([], _Inker) ->
    false;
shutdown_wait([TopPause|Rest], Inker) ->
    case leveled_inker:ink_close(Inker) of
        ok ->
            true;
        pause ->
            io:format("Inker shutdown stil waiting for process to complete" ++
                        " with further wait of ~w~n", [lists:sum(Rest)]),
            ok = timer:sleep(TopPause),
            shutdown_wait(Rest, Inker)
    end.
    

set_options(Opts) ->
    MaxJournalSize = case Opts#bookie_options.max_journalsize of
                            undefined ->
                                30000;
                            MS ->
                                MS
                        end,
    
    AltStrategy = Opts#bookie_options.reload_strategy,
    ReloadStrategy = leveled_codec:inker_reload_strategy(AltStrategy),
    
    JournalFP = Opts#bookie_options.root_path ++ "/" ++ ?JOURNAL_FP,
    LedgerFP = Opts#bookie_options.root_path ++ "/" ++ ?LEDGER_FP,
    {#inker_options{root_path = JournalFP,
                        reload_strategy = ReloadStrategy,
                        cdb_options = #cdb_options{max_size=MaxJournalSize,
                                                    binary_mode=true}},
        #penciller_options{root_path = LedgerFP}}.

startup(InkerOpts, PencillerOpts) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    io:format("LedgerSQN=~w at startup~n", [LedgerSQN]),
    ok = leveled_inker:ink_loadpcl(Inker,
                                    LedgerSQN + 1,
                                    fun load_fun/5,
                                    Penciller),
    {Inker, Penciller}.


fetch_head(Key, Penciller, LedgerCache) ->
    case gb_trees:lookup(Key, LedgerCache) of
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

accumulate_size(Key, Value, {Size, Count}) ->
    case leveled_codec:is_active(Key, Value) of
        true ->
            {Size + leveled_codec:get_size(Key, Value), Count + 1};
        false ->
            {Size, Count}
    end.

accumulate_keys(Key, Value, KeyList) ->
    case leveled_codec:is_active(Key, Value) of
        true ->
            [leveled_codec:from_ledgerkey(Key)|KeyList];
        false ->
            KeyList
    end.


add_keys(ObjKey, _IdxValue, Acc) ->
    Acc ++ [ObjKey].

add_terms(ObjKey, IdxValue, Acc) ->
    Acc ++ [{IdxValue, ObjKey}].

accumulate_index(TermRe, AddFun) ->
    case TermRe of
        undefined ->
            fun(Key, Value, Acc) ->
                case leveled_codec:is_active(Key, Value) of
                    true ->
                        {_Bucket,
                            ObjKey,
                            IdxValue} = leveled_codec:from_ledgerkey(Key),
                        AddFun(ObjKey, IdxValue, Acc);
                    false ->
                        Acc
                end end;
        TermRe ->
            fun(Key, Value, Acc) ->
                case leveled_codec:is_active(Key, Value) of
                    true ->
                        {_Bucket,
                            ObjKey,
                            IdxValue} = leveled_codec:from_ledgerkey(Key),
                        case re:run(IdxValue, TermRe) of
                            nomatch ->
                                Acc;
                            _ ->
                                AddFun(ObjKey, IdxValue, Acc)
                        end;
                    false ->
                        Acc
                end end
    end.


preparefor_ledgercache(?INKT_KEYD, LedgerKey, SQN, _Obj, _Size, IndexSpecs) ->
    {Bucket, Key} = leveled_codec:from_ledgerkey(LedgerKey),
    leveled_codec:convert_indexspecs(IndexSpecs, Bucket, Key, SQN);
preparefor_ledgercache(_Type, LedgerKey, SQN, Obj, Size, IndexSpecs) ->
    {Bucket, Key, PrimaryChange} = leveled_codec:generate_ledgerkv(LedgerKey,
                                                                    SQN,
                                                                    Obj,
                                                                    Size),
    ConvSpecs = leveled_codec:convert_indexspecs(IndexSpecs, Bucket, Key, SQN),
    [PrimaryChange] ++ ConvSpecs.


addto_ledgercache(Changes, Cache) ->
    lists:foldl(fun({K, V}, Acc) -> gb_trees:enter(K, V, Acc) end,
                    Cache,
                    Changes).

maybepush_ledgercache(MaxCacheSize, Cache, Penciller) ->
    CacheSize = gb_trees:size(Cache),
    TimeToPush = maybe_withjitter(CacheSize, MaxCacheSize),
    if
        TimeToPush ->
            case leveled_penciller:pcl_pushmem(Penciller, Cache) of
                ok ->
                    {ok, gb_trees:empty()};
                {returned, _Reason} ->
                    {ok, Cache}
            end;
        true ->
             {ok, Cache}
    end.


maybe_withjitter(CacheSize, MaxCacheSize) ->
    if
        CacheSize > 2 * MaxCacheSize ->
            true;
        CacheSize > MaxCacheSize ->
            R = random:uniform(),
            if
                R < ?JITTER_PROBABILITY ->
                    true;
                true ->
                    false
            end;
        true ->
            false
    end.

load_fun(KeyInLedger, ValueInLedger, _Position, Acc0, ExtractFun) ->
    {MinSQN, MaxSQN, OutputTree} = Acc0,
    {SQN, Type, PK} = KeyInLedger,
    % VBin may already be a term
    {VBin, VSize} = ExtractFun(ValueInLedger), 
    {Obj, IndexSpecs} = leveled_codec:split_inkvalue(VBin),
    case SQN of
        SQN when SQN < MinSQN ->
            {loop, Acc0};    
        SQN when SQN < MaxSQN ->
            Changes = preparefor_ledgercache(Type, PK, SQN,
                                                Obj, VSize, IndexSpecs),
            {loop, {MinSQN, MaxSQN, addto_ledgercache(Changes, OutputTree)}};
        MaxSQN ->
            io:format("Reached end of load batch with SQN ~w~n", [SQN]),
            Changes = preparefor_ledgercache(Type, PK, SQN,
                                                Obj, VSize, IndexSpecs),
            {stop, {MinSQN, MaxSQN, addto_ledgercache(Changes, OutputTree)}};
        SQN when SQN > MaxSQN ->
            io:format("Skipping as exceeded MaxSQN ~w with SQN ~w~n",
                        [MaxSQN, SQN]),
            {stop, Acc0}
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

reset_filestructure() ->
    RootPath  = "../test",
    leveled_inker:clean_testdir(RootPath ++ "/" ++ ?JOURNAL_FP),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    RootPath.




generate_multiple_objects(Count, KeyNumber) ->
    generate_multiple_objects(Count, KeyNumber, []).
    
generate_multiple_objects(0, _KeyNumber, ObjL) ->
    ObjL;
generate_multiple_objects(Count, KeyNumber, ObjL) ->
    Obj = {"Bucket",
            "Key" ++ integer_to_list(KeyNumber),
            crypto:rand_bytes(1024),
            [],
            [{"MDK", "MDV" ++ integer_to_list(KeyNumber)},
                {"MDK2", "MDV" ++ integer_to_list(KeyNumber)}]},
    {B1, K1, V1, Spec1, MD} = Obj,
    Content = #r_content{metadata=MD, value=V1},
    Obj1 = #r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
    generate_multiple_objects(Count - 1, KeyNumber + 1, ObjL ++ [{Obj1, Spec1}]).


single_key_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start(#bookie_options{root_path=RootPath}),
    {B1, K1, V1, Spec1, MD} = {"Bucket1",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    Content = #r_content{metadata=MD, value=V1},
    Object = #r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
    ok = book_riakput(Bookie1, Object, Spec1),
    {ok, F1} = book_riakget(Bookie1, B1, K1),
    ?assertMatch(F1, Object),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start(#bookie_options{root_path=RootPath}),
    {ok, F2} = book_riakget(Bookie2, B1, K1),
    ?assertMatch(F2, Object),
    ok = book_close(Bookie2),
    reset_filestructure().

multi_key_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start(#bookie_options{root_path=RootPath}),
    {B1, K1, V1, Spec1, MD1} = {"Bucket",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    C1 = #r_content{metadata=MD1, value=V1},
    Obj1 = #r_object{bucket=B1, key=K1, contents=[C1], vclock=[{'a',1}]},
    {B2, K2, V2, Spec2, MD2} = {"Bucket",
                                "Key2",
                                "Value2",
                                [],
                                {"MDK2", "MDV2"}},
    C2 = #r_content{metadata=MD2, value=V2},
    Obj2 = #r_object{bucket=B2, key=K2, contents=[C2], vclock=[{'a',1}]},
    ok = book_riakput(Bookie1, Obj1, Spec1),
    ObjL1 = generate_multiple_objects(100, 3),
    SW1 = os:timestamp(),
    lists:foreach(fun({O, S}) -> ok = book_riakput(Bookie1, O, S) end, ObjL1),
    io:format("PUT of 100 objects completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),SW1)]),
    ok = book_riakput(Bookie1, Obj2, Spec2),
    {ok, F1A} = book_riakget(Bookie1, B1, K1),
    ?assertMatch(F1A, Obj1),
    {ok, F2A} = book_riakget(Bookie1, B2, K2),
    ?assertMatch(F2A, Obj2),
    ObjL2 = generate_multiple_objects(100, 103),
    SW2 = os:timestamp(),
    lists:foreach(fun({O, S}) -> ok = book_riakput(Bookie1, O, S) end, ObjL2),
    io:format("PUT of 100 objects completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),SW2)]),
    {ok, F1B} = book_riakget(Bookie1, B1, K1),
    ?assertMatch(F1B, Obj1),
    {ok, F2B} = book_riakget(Bookie1, B2, K2),
    ?assertMatch(F2B, Obj2),
    ok = book_close(Bookie1),
    % Now reopen the file, and confirm that a fetch is still possible
    {ok, Bookie2} = book_start(#bookie_options{root_path=RootPath}),
    {ok, F1C} = book_riakget(Bookie2, B1, K1),
    ?assertMatch(F1C, Obj1),
    {ok, F2C} = book_riakget(Bookie2, B2, K2),
    ?assertMatch(F2C, Obj2),
    ObjL3 = generate_multiple_objects(100, 203),
    SW3 = os:timestamp(),
    lists:foreach(fun({O, S}) -> ok = book_riakput(Bookie2, O, S) end, ObjL3),
    io:format("PUT of 100 objects completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),SW3)]),
    {ok, F1D} = book_riakget(Bookie2, B1, K1),
    ?assertMatch(F1D, Obj1),
    {ok, F2D} = book_riakget(Bookie2, B2, K2),
    ?assertMatch(F2D, Obj2),
    ok = book_close(Bookie2),
    reset_filestructure().
    
-endif.