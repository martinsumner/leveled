%% -------- Overview ---------
%%
%% The eleveleddb is based on the LSM-tree similar to leveldb, except that:
%% - Keys, Metadata and Values are not persisted together - the Keys and
%% Metadata are kept in a tree-based ledger, whereas the values are stored
%% only in a sequential Journal.
%% - Different file formats are used for Journal (based on constant
%% database), and the ledger (sft, based on sst)
%% - It is not intended to be general purpose, but be primarily suited for
%% use as a Riak backend in specific circumstances (relatively large values,
%% and frequent use of iterators)
%% - The Journal is an extended nursery log in leveldb terms.  It is keyed
%% on the sequence number of the write
%% - The ledger is a merge tree, where the key is the actaul object key, and
%% the value is the metadata of the object including the sequence number
%%
%%
%% -------- Actors ---------
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
%% The Bookie takes the request and passes it first to the Inker to add the
%% request to the journal.
%%
%% The inker will pass the PK/Value/IndexSpecs to the current (append only)
%% CDB journal file to persist the change.  The call should return either 'ok'
%% or 'roll'. -'roll' indicates that the CDB file has insufficient capacity for
%% this write, and a new journal file should be created (with appropriate
%% manifest changes to be made).
%%
%% The inker will return the SQN which the change has been made at, as well as
%% the object size on disk within the Journal.
%%
%% Once the object has been persisted to the Journal, the Ledger can be updated.
%% The Ledger is updated by the Bookie applying a function (extract_metadata/4)
%% to the Value to return the Object Metadata, a function to generate a hash
%% of the Value and also taking the Primary Key, the IndexSpecs, the Sequence
%% Number in the Journal and the Object Size (returned from the Inker).
%%
%% A set of Ledger Key changes are then generated and placed in the Bookie's
%% Ledger Key cache (a gb_tree).
%%
%% The PUT can now be acknowledged.  In the background the Bookie may then
%% choose to push the cache to the Penciller for eventual persistence within
%% the ledger.  This push will either be acccepted or returned (if the
%% Penciller has a backlog of key changes).  The back-pressure should lead to
%% the Bookie entering into a slow-offer status whereby the next PUT will be
%% acknowledged by a PAUSE signal - with the expectation that the this will
%% lead to a back-off behaviour.
%%
%% -------- GET, HEAD --------
%%
%% The Bookie supports both GET and HEAD requests, with the HEAD request
%% returning only the metadata and not the actual object value.  The HEAD
%% requets cna be serviced by reference to the Ledger Cache and the Penciller.
%%
%% GET requests first follow the path of a HEAD request, and if an object is
%% found, then fetch the value from the Journal via the Inker.
%%
%% -------- Snapshots/Clones --------
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% may request a clone of the Penciller, or the Penciller and the Inker.
%%
%% The clone is seeded with the manifest.  The clone should be registered with
%% the real Inker/Penciller, so that the real Inker/Penciller may prevent the
%% deletion of files still in use by a snapshot clone.
%%
%% Iterators should de-register themselves from the Penciller on completion.
%% Iterators should be automatically release after a timeout period.  A file
%% can only be deleted from the Ledger if it is no longer in the manifest, and
%% there are no registered iterators from before the point the file was
%% removed from the manifest.
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
        book_start/4,
        book_put/5,
        book_put/6,
        book_tempput/7,
        book_delete/4,
        book_get/3,
        book_get/4,
        book_head/3,
        book_head/4,
        book_returnfolder/2,
        book_snapshotstore/3,
        book_snapshotledger/3,
        book_compactjournal/2,
        book_islastcompactionpending/1,
        book_close/1,
        book_destroy/1]).

-export([get_opt/2,
            get_opt/3]).  

-include_lib("eunit/include/eunit.hrl").

-define(CACHE_SIZE, 2000).
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").
-define(SNAPSHOT_TIMEOUT, 300000).
-define(CHECKJOURNAL_PROB, 0.2).
-define(CACHE_SIZE_JITTER, 20).
-define(JOURNAL_SIZE_JITTER, 10)

-record(state, {inker :: pid(),
                penciller :: pid(),
                cache_size :: integer(),
                ledger_cache :: list(), % a skiplist
                is_snapshot :: boolean(),
                slow_offer = false :: boolean()}).



%%%============================================================================
%%% API
%%%============================================================================

book_start(RootPath, LedgerCacheSize, JournalSize, SyncStrategy) ->
    book_start([{root_path, RootPath},
                    {cache_size, LedgerCacheSize},
                    {max_journalsize, JournalSize},
                    {sync_strategy, SyncStrategy}]).

book_start(Opts) ->
    gen_server:start(?MODULE, [Opts], []).


book_tempput(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL) when is_integer(TTL) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL).

book_put(Pid, Bucket, Key, Object, IndexSpecs) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, ?STD_TAG).

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, infinity).

book_delete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?STD_TAG).

book_get(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?STD_TAG).

book_head(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?STD_TAG).

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL) ->
    gen_server:call(Pid,
                    {put, Bucket, Key, Object, IndexSpecs, Tag, TTL},
                    infinity).

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

book_destroy(Pid) ->
    gen_server:call(Pid, destroy, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    case get_opt(snapshot_bookie, Opts) of
        undefined ->
            % Start from file not snapshot
            {InkerOpts, PencillerOpts} = set_options(Opts),
            {Inker, Penciller} = startup(InkerOpts, PencillerOpts),
            CacheJitter = ?CACHE_SIZE div ?CACHE_SIZE_JITTER,
            CacheSize = get_opt(cache_size, Opts, ?CACHE_SIZE)
                        + erlang:phash2(self()) band CacheJitter,
            leveled_log:log("B0001", [Inker, Penciller]),
            {ok, #state{inker=Inker,
                        penciller=Penciller,
                        cache_size=CacheSize,
                        ledger_cache=leveled_skiplist:empty(),
                        is_snapshot=false}};
        Bookie ->
            {ok,
                {Penciller, LedgerCache},
                Inker} = book_snapshotstore(Bookie, self(), ?SNAPSHOT_TIMEOUT),
            ok = leveled_penciller:pcl_loadsnapshot(Penciller,
                                                    leveled_skiplist:empty()),
            leveled_log:log("B0002", [Inker, Penciller]),
            {ok, #state{penciller=Penciller,
                        inker=Inker,
                        ledger_cache=LedgerCache,
                        is_snapshot=true}}
    end.


handle_call({put, Bucket, Key, Object, IndexSpecs, Tag, TTL}, From, State) ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    {ok, SQN, ObjSize} = leveled_inker:ink_put(State#state.inker,
                                                LedgerKey,
                                                Object,
                                                {IndexSpecs, TTL}),
    Changes = preparefor_ledgercache(no_type_assigned,
                                        LedgerKey,
                                        SQN,
                                        Object,
                                        ObjSize,
                                        {IndexSpecs, TTL}),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    % If the previous push to memory was returned then punish this PUT with a
    % delay.  If the back-pressure in the Penciller continues, these delays
    % will beocme more frequent
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    case  maybepush_ledgercache(State#state.cache_size,
                                            Cache0,
                                            State#state.penciller) of
        {ok, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache, slow_offer=false}};
        {returned, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache, slow_offer=true}}
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
                {active, TS} ->
                    Active = TS >= leveled_codec:integer_now(),
                    case {Active,
                            fetch_value(LedgerKey, Seqn, State#state.inker)} of
                        {_, not_present} ->
                            {reply, not_found, State};
                        {true, Object} ->
                            {reply, {ok, Object}, State};
                        _ ->
                            {reply, not_found, State}
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
                {active, TS} ->
                    case TS >= leveled_codec:integer_now() of
                        true ->
                            OMD = leveled_codec:build_metadata_object(LedgerKey, MD),
                            {reply, {ok, OMD}, State};
                        false ->
                            {reply, not_found, State}
                    end
            end
    end;
handle_call({snapshot, _Requestor, SnapType, _Timeout}, _From, State) ->
    Reply = snapshot_store(State, SnapType),
    {reply, Reply, State};
handle_call({return_folder, FolderType}, _From, State) ->
    case FolderType of
        {bucket_stats, Bucket} ->
            {reply,
                bucket_stats(State, Bucket, ?STD_TAG),
                State};
        {riakbucket_stats, Bucket} ->
            {reply,
                bucket_stats(State, Bucket, ?RIAK_TAG),
                State};
        {binary_bucketlist, Tag, {FoldKeysFun, Acc}} ->
            {reply,
                binary_bucketlist(State, Tag, {FoldKeysFun, Acc}),
                State};
        {index_query,
                Constraint,
                {FoldKeysFun, Acc},
                {IdxField, StartValue, EndValue},
                {ReturnTerms, TermRegex}} ->
            {reply,
                index_query(State,
                                Constraint,
                                {FoldKeysFun, Acc},
                                {IdxField, StartValue, EndValue},
                                {ReturnTerms, TermRegex}),
                State};
        {keylist, Tag, {FoldKeysFun, Acc}} ->
            {reply,
                allkey_query(State, Tag, {FoldKeysFun, Acc}),
                State};
        {keylist, Tag, Bucket, {FoldKeysFun, Acc}} ->
            {reply,
                bucketkey_query(State, Tag, Bucket, {FoldKeysFun, Acc}),
                State};
        {hashtree_query, Tag, JournalCheck} ->
            {reply,
                hashtree_query(State, Tag, JournalCheck),
                State};
        {foldobjects_allkeys, Tag, FoldObjectsFun} ->
            {reply,
                foldobjects_allkeys(State, Tag, FoldObjectsFun),
                State};
        {foldobjects_bybucket, Tag, Bucket, FoldObjectsFun} ->
            {reply,
                foldobjects_bybucket(State, Tag, Bucket, FoldObjectsFun),
                State};
        {foldobjects_byindex,
                Tag,
                Bucket,
                {Field, FromTerm, ToTerm},
                FoldObjectsFun} ->
            {reply,
                foldobjects_byindex(State,
                                    Tag, Bucket,
                                    Field, FromTerm, ToTerm,
                                    FoldObjectsFun),
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
    {stop, normal, ok, State};
handle_call(destroy, _From, State=#state{is_snapshot=Snp}) when Snp == false ->
    {stop, destroy, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(destroy, State) ->
    leveled_log:log("B0011", []),
    {ok, InkPathList} = leveled_inker:ink_doom(State#state.inker),
    {ok, PCLPathList} = leveled_penciller:pcl_doom(State#state.penciller),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, InkPathList),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, PCLPathList),
    ok;
terminate(Reason, State) ->
    leveled_log:log("B0003", [Reason]),
    ok = leveled_inker:ink_close(State#state.inker),
    ok = leveled_penciller:pcl_close(State#state.penciller).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

bucket_stats(State, Bucket, Tag) ->
    {ok,
        {LedgerSnapshot, LedgerCache},
        _JournalSnapshot} = snapshot_store(State, ledger),
    Folder = fun() ->
                leveled_log:log("B0004", [leveled_skiplist:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
                EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
                AccFun = accumulate_size(),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        AccFun,
                                                        {0, 0}),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                Acc
                end,
    {async, Folder}.


binary_bucketlist(State, Tag, {FoldBucketsFun, InitAcc}) ->
    % List buckets for tag, assuming bucket names are all binary type
    {ok,
        {LedgerSnapshot, LedgerCache},
        _JournalSnapshot} = snapshot_store(State, ledger),
    Folder = fun() ->
                leveled_log:log("B0004", [leveled_skiplist:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                BucketAcc = get_nextbucket(null,
                                            Tag,
                                            LedgerSnapshot,
                                            []),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                lists:foldl(fun({B, _K}, Acc) -> FoldBucketsFun(B, Acc) end,
                                InitAcc,
                                BucketAcc)
                end,
    {async, Folder}.

get_nextbucket(NextBucket, Tag, LedgerSnapshot, BKList) ->
    StartKey = leveled_codec:to_ledgerkey(NextBucket, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    ExtractFun = fun(LK, _V, _Acc) -> leveled_codec:from_ledgerkey(LK) end,
    BK = leveled_penciller:pcl_fetchnextkey(LedgerSnapshot,
                                                StartKey,
                                                EndKey,
                                                ExtractFun,
                                                null),
    case BK of
        null ->
            leveled_log:log("B0008",[]),
            BKList;
        {B, K} when is_binary(B) ->
            leveled_log:log("B0009",[B]),
            get_nextbucket(<<B/binary, 0>>,
                            Tag,
                            LedgerSnapshot,
                            [{B, K}|BKList]);
        NB ->
            leveled_log:log("B0010",[NB]),
            []
    end.
            

index_query(State,
                Constraint,
                {FoldKeysFun, InitAcc},
                {IdxField, StartValue, EndValue},
                {ReturnTerms, TermRegex}) ->
    {ok,
        {LedgerSnapshot, LedgerCache},
        _JournalSnapshot} = snapshot_store(State, ledger),
    {Bucket, StartObjKey} =
        case Constraint of
            {B, SK} ->
                {B, SK};
            B ->
                {B, null}
        end,
    Folder = fun() ->
                leveled_log:log("B0004", [leveled_skiplist:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                StartKey = leveled_codec:to_ledgerkey(Bucket,
                                                        StartObjKey,
                                                        ?IDX_TAG,
                                                        IdxField,
                                                        StartValue),
                EndKey = leveled_codec:to_ledgerkey(Bucket,
                                                        null,
                                                        ?IDX_TAG,
                                                        IdxField,
                                                        EndValue),
                AddFun = case ReturnTerms of
                                true ->
                                    fun add_terms/2;
                                _ ->
                                    fun add_keys/2
                            end,
                AccFun = accumulate_index(TermRegex, AddFun, FoldKeysFun),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        AccFun,
                                                        InitAcc),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                Acc
                end,
    {async, Folder}.


hashtree_query(State, Tag, JournalCheck) ->
    SnapType = case JournalCheck of
                            false ->
                                ledger;
                            check_presence ->
                                store
                        end,
    {ok,
        {LedgerSnapshot, LedgerCache},
        JournalSnapshot} = snapshot_store(State, SnapType),
    Folder = fun() ->
                leveled_log:log("B0004", [leveled_skiplist:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
                EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
                AccFun = accumulate_hashes(JournalCheck, JournalSnapshot),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        AccFun,
                                                        []),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                case JournalCheck of
                    false ->
                        ok;
                    check_presence ->
                        leveled_inker:ink_close(JournalSnapshot)
                end,
                Acc
                end,
    {async, Folder}.


foldobjects_allkeys(State, Tag, FoldObjectsFun) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun).

foldobjects_bybucket(State, Tag, Bucket, FoldObjectsFun) ->
    StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun).

foldobjects_byindex(State, Tag, Bucket, Field, FromTerm, ToTerm, FoldObjectsFun) ->
    StartKey = leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, Field,
                                            FromTerm),
    EndKey =  leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, Field,
                                            ToTerm),
    foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun).

foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun) ->
    {ok,
        {LedgerSnapshot, LedgerCache},
        JournalSnapshot} = snapshot_store(State, store),
    {FoldFun, InitAcc} = case is_tuple(FoldObjectsFun) of
                                true ->
                                    FoldObjectsFun;
                                false ->
                                    {FoldObjectsFun, []}
                            end,
    Folder = fun() ->
                leveled_log:log("B0004", [leveled_skiplist:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                AccFun = accumulate_objects(FoldFun, JournalSnapshot, Tag),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        AccFun,
                                                        InitAcc),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                ok = leveled_inker:ink_close(JournalSnapshot),
                Acc
                end,
    {async, Folder}.


bucketkey_query(State, Tag, Bucket, {FoldKeysFun, InitAcc}) ->
    {ok,
        {LedgerSnapshot, LedgerCache},
        _JournalSnapshot} = snapshot_store(State, ledger),
    Folder = fun() ->
                leveled_log:log("B0004", [leveled_skiplist:size(LedgerCache)]),
                ok = leveled_penciller:pcl_loadsnapshot(LedgerSnapshot,
                                                            LedgerCache),
                SK = leveled_codec:to_ledgerkey(Bucket, null, Tag),
                EK = leveled_codec:to_ledgerkey(Bucket, null, Tag),
                AccFun = accumulate_keys(FoldKeysFun),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        SK,
                                                        EK,
                                                        AccFun,
                                                        InitAcc),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                Acc
                end,
    {async, Folder}.

allkey_query(State, Tag, {FoldKeysFun, InitAcc}) ->
    bucketkey_query(State, Tag, null, {FoldKeysFun, InitAcc}).


snapshot_store(State, SnapType) ->
    PCLopts = #penciller_options{start_snapshot=true,
                                    source_penciller=State#state.penciller},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    case SnapType of
        store ->
            InkerOpts = #inker_options{start_snapshot=true,
                                        source_inker=State#state.inker},
            {ok, JournalSnapshot} = leveled_inker:ink_start(InkerOpts),
            {ok, {LedgerSnapshot, State#state.ledger_cache},
                    JournalSnapshot};
        ledger ->
            {ok, {LedgerSnapshot, State#state.ledger_cache},
                    null}
    end.    

set_options(Opts) ->
    MaxJournalSize0 = get_opt(max_journalsize, Opts, 10000000000),
    JournalSizeJitter = MaxJournalSize0 div ?JOURNAL_SIZE_JITTER,
    MaxJournalSize = MaxJournalSize0 -
                        erlang:phash2(self()) band JournalSizeJitter,
    
    SyncStrat = get_opt(sync_strategy, Opts, sync),
    WRP = get_opt(waste_retention_period, Opts),
    
    AltStrategy = get_opt(reload_strategy, Opts, []),
    ReloadStrategy = leveled_codec:inker_reload_strategy(AltStrategy),
    
    PCLL0CacheSize = get_opt(max_pencillercachesize, Opts),
    RootPath = get_opt(root_path, Opts),
    
    JournalFP = RootPath ++ "/" ++ ?JOURNAL_FP,
    LedgerFP = RootPath ++ "/" ++ ?LEDGER_FP,
    ok =filelib:ensure_dir(JournalFP),
    ok =filelib:ensure_dir(LedgerFP),
    
    {#inker_options{root_path = JournalFP,
                        reload_strategy = ReloadStrategy,
                        max_run_length = get_opt(max_run_length, Opts),
                        waste_retention_period = WRP,
                        cdb_options = #cdb_options{max_size=MaxJournalSize,
                                                    binary_mode=true,
                                                    sync_strategy=SyncStrat}},
        #penciller_options{root_path = LedgerFP,
                            max_inmemory_tablesize = PCLL0CacheSize}}.

startup(InkerOpts, PencillerOpts) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    leveled_log:log("B0005", [LedgerSQN]),
    ok = leveled_inker:ink_loadpcl(Inker,
                                    LedgerSQN + 1,
                                    fun load_fun/5,
                                    Penciller),
    {Inker, Penciller}.


fetch_head(Key, Penciller, LedgerCache) ->
    case leveled_skiplist:lookup(Key, LedgerCache) of
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


accumulate_size() ->
    Now = leveled_codec:integer_now(),
    AccFun = fun(Key, Value, {Size, Count}) ->
                    case leveled_codec:is_active(Key, Value, Now) of
                            true ->
                                {Size + leveled_codec:get_size(Key, Value),
                                    Count + 1};
                            false ->
                                {Size, Count}
                        end
                end,
    AccFun.

accumulate_hashes(JournalCheck, InkerClone) ->
    Now = leveled_codec:integer_now(),
    AccFun = fun(LK, V, KHList) ->
                    case leveled_codec:is_active(LK, V, Now) of
                        true ->
                            {B, K, H} = leveled_codec:get_keyandhash(LK, V),
                            Check = random:uniform() < ?CHECKJOURNAL_PROB,
                            case {JournalCheck, Check} of
                                {check_presence, true} ->
                                    case check_presence(LK, V, InkerClone) of
                                        true ->
                                            [{B, K, H}|KHList];
                                        false ->
                                            KHList
                                    end;
                                _ ->    
                                    [{B, K, H}|KHList]
                            end;
                        false ->
                            KHList
                    end
                end,
    AccFun.

accumulate_objects(FoldObjectsFun, InkerClone, Tag) ->
    Now = leveled_codec:integer_now(),
    AccFun  = fun(LK, V, Acc) ->
                    case leveled_codec:is_active(LK, V, Now) of
                        true ->
                            SQN = leveled_codec:strip_to_seqonly({LK, V}),
                            {B, K} = case leveled_codec:from_ledgerkey(LK) of
                                            {B0, K0} -> {B0, K0};
                                            {B0, K0, _T0} -> {B0, K0}
                                        end,
                            QK = leveled_codec:to_ledgerkey(B, K, Tag),
                            R = leveled_inker:ink_fetch(InkerClone, QK, SQN),
                            case R of
                                {ok, Value} ->
                                    FoldObjectsFun(B, K, Value, Acc);
                                not_present ->
                                    Acc
                            end;
                        false ->
                            Acc
                    end
                end,
    AccFun.




check_presence(Key, Value, InkerClone) ->
    {LedgerKey, SQN} = leveled_codec:strip_to_keyseqonly({Key, Value}),
    case leveled_inker:ink_keycheck(InkerClone, LedgerKey, SQN) of
        probably ->
            true;
        missing ->
            false
    end.

accumulate_keys(FoldKeysFun) ->
    Now = leveled_codec:integer_now(),
    AccFun = fun(Key, Value, Acc) ->
                    case leveled_codec:is_active(Key, Value, Now) of
                        true ->
                            {B, K} = leveled_codec:from_ledgerkey(Key),
                            FoldKeysFun(B, K, Acc);
                        false ->
                            Acc
                    end
                end,
    AccFun.

add_keys(ObjKey, _IdxValue) ->
    ObjKey.

add_terms(ObjKey, IdxValue) ->
    {IdxValue, ObjKey}.

accumulate_index(TermRe, AddFun, FoldKeysFun) ->
    Now = leveled_codec:integer_now(),
    case TermRe of
        undefined ->
            fun(Key, Value, Acc) ->
                case leveled_codec:is_active(Key, Value, Now) of
                    true ->
                        {Bucket,
                            ObjKey,
                            IdxValue} = leveled_codec:from_ledgerkey(Key),
                        FoldKeysFun(Bucket, AddFun(ObjKey, IdxValue), Acc);
                    false ->
                        Acc
                end end;
        TermRe ->
            fun(Key, Value, Acc) ->
                case leveled_codec:is_active(Key, Value, Now) of
                    true ->
                        {Bucket,
                            ObjKey,
                            IdxValue} = leveled_codec:from_ledgerkey(Key),
                        case re:run(IdxValue, TermRe) of
                            nomatch ->
                                Acc;
                            _ ->
                                FoldKeysFun(Bucket,
                                            AddFun(ObjKey, IdxValue),
                                            Acc)
                        end;
                    false ->
                        Acc
                end end
    end.


preparefor_ledgercache(?INKT_KEYD,
                        LedgerKey, SQN, _Obj, _Size, {IndexSpecs, TTL}) ->
    {Bucket, Key} = leveled_codec:from_ledgerkey(LedgerKey),
    leveled_codec:convert_indexspecs(IndexSpecs, Bucket, Key, SQN, TTL);
preparefor_ledgercache(_Type, LedgerKey, SQN, Obj, Size, {IndexSpecs, TTL}) ->
    {Bucket, Key, PrimaryChange} = leveled_codec:generate_ledgerkv(LedgerKey,
                                                                    SQN,
                                                                    Obj,
                                                                    Size,
                                                                    TTL),
    [PrimaryChange] ++ leveled_codec:convert_indexspecs(IndexSpecs,
                                                        Bucket,
                                                        Key,
                                                        SQN,
                                                        TTL).


addto_ledgercache(Changes, Cache) ->
    lists:foldl(fun({K, V}, Acc) -> leveled_skiplist:enter(K, V, Acc) end,
                    Cache,
                    Changes).

maybepush_ledgercache(MaxCacheSize, Cache, Penciller) ->
    CacheSize = leveled_skiplist:size(Cache),
    TimeToPush = maybe_withjitter(CacheSize, MaxCacheSize),
    if
        TimeToPush ->
            case leveled_penciller:pcl_pushmem(Penciller, Cache) of
                ok ->
                    {ok, leveled_skiplist:empty()};
                returned ->
                    {returned, Cache}
            end;
        true ->
             {ok, Cache}
    end.


maybe_withjitter(CacheSize, MaxCacheSize) ->    
    if
        CacheSize > MaxCacheSize ->
            R = random:uniform(7 * MaxCacheSize),
            if
                (CacheSize - MaxCacheSize) > R ->
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
            leveled_log:log("B0006", [SQN]),
            Changes = preparefor_ledgercache(Type, PK, SQN,
                                                Obj, VSize, IndexSpecs),
            {stop, {MinSQN, MaxSQN, addto_ledgercache(Changes, OutputTree)}};
        SQN when SQN > MaxSQN ->
            leveled_log:log("B0007", [MaxSQN, SQN]),
            {stop, Acc0}
    end.


get_opt(Key, Opts) ->
    get_opt(Key, Opts, undefined).

get_opt(Key, Opts, Default) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
            Default;
        Value ->
            Value
    end.

delete_path(DirPath) ->
    ok = filelib:ensure_dir(DirPath),
    {ok, Files} = file:list_dir(DirPath),
    [file:delete(filename:join([DirPath, File])) || File <- Files],
    file:del_dir(DirPath).

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
    Key = "Key" ++ integer_to_list(KeyNumber),
    Value = crypto:rand_bytes(256),
    IndexSpec = [{add, "idx1_bin", "f" ++ integer_to_list(KeyNumber rem 10)}],
    generate_multiple_objects(Count - 1,
                                KeyNumber + 1,
                                ObjL ++ [{Key, Value, IndexSpec}]).

    
ttl_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}]),
    ObjL1 = generate_multiple_objects(100, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_codec:integer_now() + 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL1),
    lists:foreach(fun({K, V, _S}) ->
                        {ok, V} = book_get(Bookie1, "Bucket", K, ?STD_TAG)
                        end,
                    ObjL1),
    lists:foreach(fun({K, _V, _S}) ->
                        {ok, _} = book_head(Bookie1, "Bucket", K, ?STD_TAG)
                        end,
                    ObjL1),
    
    ObjL2 = generate_multiple_objects(100, 101),
    Past = leveled_codec:integer_now() - 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Past) end,
                    ObjL2),
    lists:foreach(fun({K, _V, _S}) ->
                        not_found = book_get(Bookie1, "Bucket", K, ?STD_TAG)
                        end,
                    ObjL2),
    lists:foreach(fun({K, _V, _S}) ->
                        not_found = book_head(Bookie1, "Bucket", K, ?STD_TAG)
                        end,
                    ObjL2),
    
    {async, BucketFolder} = book_returnfolder(Bookie1,
                                                {bucket_stats, "Bucket"}),
    {_Size, Count} = BucketFolder(),
    ?assertMatch(100, Count),
    FoldKeysFun = fun(_B, Item, FKFAcc) -> FKFAcc ++ [Item] end,
    {async,
        IndexFolder} = book_returnfolder(Bookie1,
                                            {index_query,
                                            "Bucket",
                                            {FoldKeysFun, []},
                                            {"idx1_bin", "f8", "f9"},
                                            {false, undefined}}),
    KeyList = IndexFolder(),
    ?assertMatch(20, length(KeyList)),
    
    {ok, Regex} = re:compile("f8"),
    {async,
        IndexFolderTR} = book_returnfolder(Bookie1,
                                            {index_query,
                                            "Bucket",
                                            {FoldKeysFun, []},
                                            {"idx1_bin", "f8", "f9"},
                                            {true, Regex}}),
    TermKeyList = IndexFolderTR(),
    ?assertMatch(10, length(TermKeyList)),
    
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    
    {async,
        IndexFolderTR2} = book_returnfolder(Bookie2,
                                            {index_query,
                                            "Bucket",
                                            {FoldKeysFun, []},
                                            {"idx1_bin", "f7", "f9"},
                                            {false, Regex}}),
    KeyList2 = IndexFolderTR2(),
    ?assertMatch(10, length(KeyList2)),
    
    lists:foreach(fun({K, _V, _S}) ->
                        not_found = book_get(Bookie2, "Bucket", K, ?STD_TAG)
                        end,
                    ObjL2),
    lists:foreach(fun({K, _V, _S}) ->
                        not_found = book_head(Bookie2, "Bucket", K, ?STD_TAG)
                        end,
                    ObjL2),
    
    ok = book_close(Bookie2),
    reset_filestructure().

hashtree_query_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                {max_journalsize, 1000000},
                                {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(1200, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_codec:integer_now() + 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL1),
    ObjL2 = generate_multiple_objects(20, 1201),
    % Put in a few objects with a TTL in the past
    Past = leveled_codec:integer_now() - 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Past) end,
                    ObjL2),
    % Scan the store for the Bucket, Keys and Hashes
    {async, HTFolder} = book_returnfolder(Bookie1,
                                                {hashtree_query,
                                                    ?STD_TAG,
                                                    false}),
    KeyHashList = HTFolder(),
        lists:foreach(fun({B, _K, H}) ->
                            ?assertMatch("Bucket", B),
                            ?assertMatch(true, is_integer(H))
                            end,
                        KeyHashList),
    ?assertMatch(1200, length(KeyHashList)),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath},
                                    {max_journalsize, 200000},
                                    {cache_size, 500}]),
    {async, HTFolder2} = book_returnfolder(Bookie2,
                                                {hashtree_query,
                                                    ?STD_TAG,
                                                    false}),
    ?assertMatch(KeyHashList, HTFolder2()),
    ok = book_close(Bookie2),
    reset_filestructure().

hashtree_query_withjournalcheck_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(800, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_codec:integer_now() + 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL1),
    {async, HTFolder1} = book_returnfolder(Bookie1,
                                                {hashtree_query,
                                                    ?STD_TAG,
                                                    false}),
    KeyHashList = HTFolder1(),
    {async, HTFolder2} = book_returnfolder(Bookie1,
                                                {hashtree_query,
                                                    ?STD_TAG,
                                                    check_presence}),
    ?assertMatch(KeyHashList, HTFolder2()),
    ok = book_close(Bookie1),
    reset_filestructure().

foldobjects_vs_hashtree_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(800, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_codec:integer_now() + 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL1),
    {async, HTFolder1} = book_returnfolder(Bookie1,
                                                {hashtree_query,
                                                    ?STD_TAG,
                                                    false}),
    KeyHashList1 = lists:usort(HTFolder1()),
    io:format("First item ~w~n", [lists:nth(1, KeyHashList1)]),
    FoldObjectsFun = fun(B, K, V, Acc) ->
                            [{B, K, erlang:phash2(term_to_binary(V))}|Acc] end,
    {async, HTFolder2} = book_returnfolder(Bookie1,
                                            {foldobjects_allkeys,
                                                ?STD_TAG,
                                                FoldObjectsFun}),
    KeyHashList2 = HTFolder2(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList2)),
    
    ok = book_close(Bookie1),
    reset_filestructure().

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null),
    {noreply, _State2} = handle_cast(null, #state{}).


-endif.
