%% -------- Overview ---------
%%
%% The eleveleddb is based on the LSM-tree similar to leveldb, except that:
%% - Keys, Metadata and Values are not persisted together - the Keys and
%% Metadata are kept in a tree-based ledger, whereas the values are stored
%% only in a sequential Journal.
%% - Different file formats are used for Journal (based on DJ Bernstein
%% constant database), and the ledger (based on sst)
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
            get_opt/3,
            empty_ledgercache/0,
            loadqueue_ledgercache/1,
            push_ledgercache/2,
            snapshot_store/5,
            fetch_value/2]).  

-include_lib("eunit/include/eunit.hrl").

-define(CACHE_SIZE, 2500).
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").
-define(SNAPSHOT_TIMEOUT, 300000).
-define(CHECKJOURNAL_PROB, 0.2).
-define(CACHE_SIZE_JITTER, 25).
-define(JOURNAL_SIZE_JITTER, 20).
-define(LONG_RUNNING, 80000).
-define(RECENT_AAE, false).

-record(ledger_cache, {mem :: ets:tab(),
                        loader = leveled_tree:empty(?CACHE_TYPE)
                                    :: tuple()|empty_cache,
                        load_queue = [] :: list(),
                        index = leveled_pmem:new_index(), % array or empty_index
                        min_sqn = infinity :: integer()|infinity,
                        max_sqn = 0 :: integer()}).

-record(state, {inker :: pid() | undefined,
                penciller :: pid() | undefined,
                cache_size :: integer() | undefined,
                recent_aae :: false | #recent_aae{} | undefined,
                ledger_cache = #ledger_cache{},
                is_snapshot :: boolean() | undefined,
                slow_offer = false :: boolean(),
                put_timing :: tuple() | undefined,
                get_timing :: tuple() | undefined}).


%%%============================================================================
%%% API
%%%============================================================================

%% @doc Start a Leveled Key/Value store - limited options support.
%%
%% The most common startup parameters are extracted out from the options to
%% provide this startup method.  This will start a KV store from the previous
%% store at root path - or an empty one if there is no store at the path.
%%
%% Fiddling with the LedgerCacheSize and JournalSize may improve performance, 
%% but these are primarily exposed to support special situations (e.g. very
%% low memory installations), there should not be huge variance in outcomes
%% from modifying these numbers.
%%
%% The sync_strategy determines if the store is going to flush writes to disk
%% before returning an ack.  There are three settings currrently supported:
%% - sync - sync to disk by passing the sync flag to the file writer (only
%% works in OTP 18)
%% - riak_sync - sync to disk by explicitly calling data_sync after the write
%% - none - leave it to the operating system to control flushing
%%
%% On startup the Bookie must restart both the Inker to load the Journal, and
%% the Penciller to load the Ledger.  Once the Penciller has started, the
%% Bookie should request the highest sequence number in the Ledger, and then
%% and try and rebuild any missing information from the Journal.
%%
%% To rebuild the Ledger it requests the Inker to scan over the files from
%% the sequence number and re-generate the Ledger changes - pushing the changes
%% directly back into the Ledger.

book_start(RootPath, LedgerCacheSize, JournalSize, SyncStrategy) ->
    book_start([{root_path, RootPath},
                    {cache_size, LedgerCacheSize},
                    {max_journalsize, JournalSize},
                    {sync_strategy, SyncStrategy}]).

%% @doc Start a Leveled Key/Value store - full options support.
%%
%% Allows an options proplists to be passed for setting options.  There are
%% two primary additional options this allows over book_start/4:
%% - retain_strategy
%% - waste_retention_period
%%
%% Both of these relate to compaction in the Journal.  The retain_strategy
%% determines if a skinny record of the object should be retained following
%% compaction, and how thta should be used when recovering lost state in the
%% Ledger.
%%
%% Currently compacted records no longer in use are not removed but moved to
%% a journal_waste folder, and the waste_retention_period determines how long
%% this history should be kept for (for example to allow for it to be backed
%% up before deletion)
%%
%% TODO:
%% The reload_strategy is exposed as currently no firm decision has been made 
%% about how recovery should work.  For instance if we were to trust everything
%% as permanent in the Ledger once it is persisted, then there would be no
%% need to retain a skinny history of key changes in the Journal after
%% compaction.  If, as an alternative we assume the Ledger is never permanent,
%% and retain the skinny hisory - then backups need only be made against the
%% Journal.  The skinny history of key changes is primarily related to the
%% issue of supporting secondary indexes in Riak.
%%
%% These two strategies are referred to as recovr (assume we can recover any
%% deltas from a lost ledger and a lost history through resilience outside of
%% the store), or retain (retain a history of key changes, even when the object
%% value has been compacted).  There is a third, unimplemented strategy, which
%% is recalc - which would require when reloading the Ledger from the Journal,
%% to recalculate the index changes based on the current state of the Ledger
%% and the object metadata.

book_start(Opts) ->
    gen_server:start(?MODULE, [Opts], []).

%% @doc Put an object with an expiry time
%%
%% Put an item in the store but with a Time To Live - the time when the object
%% should expire, in gregorian_seconds (add the required number of seconds to
%% leveled_codec:integer_time/1).
%%
%% There exists the possibility of per object expiry times, not just whole
%% store expiry times as has traditionally been the feature in Riak.  Care
%% will need to be taken if implementing per-object times about the choice of
%% reload_strategy.  If expired objects are to be compacted entirely, then the
%% history of KeyChanges will be lost on reload.

book_tempput(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL)
                                                    when is_integer(TTL) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL).

%% @doc - Standard PUT
%%
%% A PUT request consists of
%% - A Primary Key and a Value
%% - IndexSpecs - a set of secondary key changes associated with the
%% transaction
%% - A tag indictaing the type of object.  Behaviour for metadata extraction, 
%% and ledger compaction will vary by type.  There are three currently
%% implemented types i (Index), o (Standard), o_rkv (Riak).  Keys added with
%% Index tags are not fetchable (as they will not be hashed), but are
%% extractable via range query.
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
%% Ledger Key cache.
%%
%% The PUT can now be acknowledged.  In the background the Bookie may then
%% choose to push the cache to the Penciller for eventual persistence within
%% the ledger.  This push will either be acccepted or returned (if the
%% Penciller has a backlog of key changes).  The back-pressure should lead to
%% the Bookie entering into a slow-offer status whereby the next PUT will be
%% acknowledged by a PAUSE signal - with the expectation that the this will
%% lead to a back-off behaviour.

book_put(Pid, Bucket, Key, Object, IndexSpecs) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, ?STD_TAG).

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, infinity).

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL) ->
    gen_server:call(Pid,
                    {put, Bucket, Key, Object, IndexSpecs, Tag, TTL},
                    infinity).

%% @doc - Standard PUT
%%
%% A thin wrap around the put of a special tombstone object.  There is no
%% immediate reclaim of space, simply the addition of a more recent tombstone.

book_delete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?STD_TAG).

%% @doc - GET and HAD requests
%%
%% The Bookie supports both GET and HEAD requests, with the HEAD request
%% returning only the metadata and not the actual object value.  The HEAD
%% requets cna be serviced by reference to the Ledger Cache and the Penciller.
%%
%% GET requests first follow the path of a HEAD request, and if an object is
%% found, then fetch the value from the Journal via the Inker.

book_get(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?STD_TAG).

book_head(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?STD_TAG).

book_get(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {get, Bucket, Key, Tag}, infinity).

book_head(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag}, infinity).

%% @doc Snapshots/Clones
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% may request a clone of the Penciller, or clones of both the Penciller and
%% the Inker should values also need to be accessed.
%%
%% The clone is seeded with the manifest SQN.  The clone should be registered
%% with the real Inker/Penciller, so that the real Inker/Penciller may prevent
%% the deletion of files still in use by a snapshot clone.
%%
%% Iterators should de-register themselves from the Penciller on completion.
%% Iterators should be automatically release after a timeout period.  A file
%% can only be deleted from the Ledger if it is no longer in the manifest, and
%% there are no registered iterators from before the point the file was
%% removed from the manifest.
%%
%% Clones are simply new gen_servers with copies of the relevant
%% StateData.
%%
%% There are a series of specific folders implemented that provide pre-canned
%% snapshot functionality:
%%
%% {bucket_stats, Bucket}  -> return a key count and total object size within
%% a bucket
%% {riakbucket_stats, Bucket} -> as above, but for buckets with the Riak Tag
%% {binary_bucketlist, Tag, {FoldKeysFun, Acc}} -> if we assume buckets and
%% keys are binaries, provides a fast bucket list function
%% {index_query,
%%        Constraint,
%%        {FoldKeysFun, Acc},
%%        {IdxField, StartValue, EndValue},
%%        {ReturnTerms, TermRegex}} -> secondray index query
%% {keylist, Tag, {FoldKeysFun, Acc}} -> list all keys with tag
%% {keylist, Tag, Bucket, {FoldKeysFun, Acc}} -> list all keys within given
%% bucket
%% {hashlist_query, Tag, JournalCheck} -> return keys and hashes for all
%% objects with a given tag
%% {tictactree_idx,
%%      {Bucket, IdxField, StartValue, EndValue},
%%      TreeSize, 
%%      PartitionFilter}
%% -> compile a hashtree for the items on the index.  A partition filter is 
%% required to avoid adding an index entry in this vnode as a fallback.
%% There is no de-duplicate of results, duplicate reuslts corrupt the tree.
%% {tictactree_obj,
%%      {Bucket, StartKey, EndKey, CheckPresence},
%%      TreeSize,
%%      PartitionFilter}
%% -> compile a hashtree for all the objects in the range.  A partition filter
%% may be passed to restrict the query to a given partition on this vnode.  The
%% filter should bea function that takes (Bucket, Key) as inputs and outputs
%% one of the atoms accumulate or pass.  There is no de-duplicate of results,
%% duplicate reuslts corrupt the tree.
%% CheckPresence can be used if there is a need to do a deeper check to ensure
%% that the object is in the Journal (or at least indexed within the Journal).
%% {foldobjects_bybucket, Tag, Bucket, FoldObjectsFun} -> fold over all objects
%% in a given bucket
%% {foldobjects_byindex,
%%        Tag,
%%        Bucket,
%%        {Field, FromTerm, ToTerm},
%%        FoldObjectsFun} -> fold over all objects with an entry in a given
%% range on a given index

book_returnfolder(Pid, FolderType) ->
    gen_server:call(Pid, {return_folder, FolderType}, infinity).

book_snapshotstore(Pid, Requestor, Timeout) ->
    gen_server:call(Pid, {snapshot, Requestor, store, Timeout}, infinity).

book_snapshotledger(Pid, Requestor, Timeout) ->
    gen_server:call(Pid, {snapshot, Requestor, ledger, Timeout}, infinity).

%% @doc Call for compaction of the Journal
%%
%% the scheduling of Journla compaction is called externally, so it is assumed
%% in Riak it will be triggered by a vnode callback.

book_compactjournal(Pid, Timeout) ->
    gen_server:call(Pid, {compact_journal, Timeout}, infinity).

%% @doc Check on progress of the last compaction

book_islastcompactionpending(Pid) ->
    gen_server:call(Pid, confirm_compact, infinity).

%% @doc Clean shutdown
%%
%% A clean shutdown will persist all the information in the Penciller memory
%% before closing, so shutdown is not instantaneous.

book_close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% @doc Close and clean-out files

book_destroy(Pid) ->
    gen_server:call(Pid, destroy, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    leveled_rand:seed(),
    case get_opt(snapshot_bookie, Opts) of
        undefined ->
            % Start from file not snapshot
            {InkerOpts, PencillerOpts} = set_options(Opts),
            
            CacheJitter = ?CACHE_SIZE div (100 div ?CACHE_SIZE_JITTER),
            CacheSize = get_opt(cache_size, Opts, ?CACHE_SIZE)
                        + erlang:phash2(self()) rem CacheJitter,
            RecentAAE =
                case get_opt(recent_aae, Opts, ?RECENT_AAE) of
                    false ->
                        false;
                    {FilterType, BucketList, LimitMinutes, UnitMinutes} ->
                        #recent_aae{filter = FilterType,
                                    buckets = BucketList,
                                    limit_minutes = LimitMinutes,
                                    unit_minutes = UnitMinutes}
                end,
                
            {Inker, Penciller} = startup(InkerOpts, PencillerOpts, RecentAAE),
            
            NewETS = ets:new(mem, [ordered_set]),
            leveled_log:log("B0001", [Inker, Penciller]),
            {ok, #state{inker=Inker,
                        penciller=Penciller,
                        cache_size=CacheSize,
                        recent_aae=RecentAAE,
                        ledger_cache=#ledger_cache{mem = NewETS},
                        is_snapshot=false}};
        Bookie ->
            {ok, Penciller, Inker} = book_snapshotstore(Bookie,
                                                        self(),
                                                        ?SNAPSHOT_TIMEOUT),
            leveled_log:log("B0002", [Inker, Penciller]),
            {ok, #state{penciller=Penciller,
                        inker=Inker,
                        is_snapshot=true}}
    end.


handle_call({put, Bucket, Key, Object, IndexSpecs, Tag, TTL}, From, State) ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    SW = os:timestamp(),
    {ok, SQN, ObjSize} = leveled_inker:ink_put(State#state.inker,
                                                LedgerKey,
                                                Object,
                                                {IndexSpecs, TTL}),
    T0 = timer:now_diff(os:timestamp(), SW),
    Changes = preparefor_ledgercache(no_type_assigned,
                                        LedgerKey,
                                        SQN,
                                        Object,
                                        ObjSize,
                                        {IndexSpecs, TTL},
                                        State#state.recent_aae),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    T1 = timer:now_diff(os:timestamp(), SW) - T0,
    PutTimes = leveled_log:put_timing(bookie, State#state.put_timing, T0, T1),
    % If the previous push to memory was returned then punish this PUT with a
    % delay.  If the back-pressure in the Penciller continues, these delays
    % will beocme more frequent
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    maybe_longrunning(SW, overall_put),
    case maybepush_ledgercache(State#state.cache_size,
                                    Cache0,
                                    State#state.penciller) of
        {ok, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache,
                                    put_timing=PutTimes,
                                    slow_offer=false}};
        {returned, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache,
                                    put_timing=PutTimes,
                                    slow_offer=true}}
    end;
handle_call({get, Bucket, Key, Tag}, _From, State) ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    SWh = os:timestamp(),
    case fetch_head(LedgerKey,
                    State#state.penciller,
                    State#state.ledger_cache) of
        not_present ->
            GT0 = leveled_log:get_timing(State#state.get_timing, 
                                            SWh,
                                            head_not_present),
            {reply, not_found, State#state{get_timing=GT0}};
        Head ->
            GT0 = leveled_log:get_timing(State#state.get_timing, 
                                            SWh,
                                            head_found),
            SWg = os:timestamp(),
            {Seqn, Status, _MH, _MD} = leveled_codec:striphead_to_details(Head),
            case Status of
                tomb ->
                    {reply, not_found, State};
                {active, TS} ->
                    Active = TS >= leveled_codec:integer_now(),
                    Object = fetch_value(State#state.inker, {LedgerKey, Seqn}),
                    GT1 = leveled_log:get_timing(GT0, SWg, fetch),
                    case {Active, Object} of
                        {_, not_present} ->
                            {reply, not_found, State#state{get_timing=GT1}};
                        {true, Object} ->
                            {reply, {ok, Object}, State#state{get_timing=GT1}};
                        _ ->
                            {reply, not_found, State#state{get_timing=GT1}}
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
            case leveled_codec:striphead_to_details(Head) of
                {_SeqN, tomb, _MH, _MD} ->
                    {reply, not_found, State};
                {_SeqN, {active, TS}, _MH, MD} ->
                    case TS >= leveled_codec:integer_now() of
                        true ->
                            OMD =
                                leveled_codec:build_metadata_object(LedgerKey,
                                                                    MD),
                            {reply, {ok, OMD}, State};
                        false ->
                            {reply, not_found, State}
                    end
            end
    end;
handle_call({snapshot, _Requestor, SnapType, _Timeout}, _From, State) ->
    % TODO: clean-up passing of Requestor (which was previously just used in 
    % logs) and so can now be ignored, and timeout which is ignored - but 
    % probably shouldn't be.
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
            {Bucket, StartObjKey} =
                case Constraint of
                    {B, SK} ->
                        {B, SK};
                    B ->
                        {B, null}
                end,
            {reply,
                index_query(State,
                                Bucket,
                                StartObjKey,
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
        {hashlist_query, Tag, JournalCheck} ->
            {reply,
                hashlist_query(State, Tag, JournalCheck),
                State};
        {tictactree_obj,
            {Tag, Bucket, StartKey, EndKey, CheckPresence},
            TreeSize,
            PartitionFilter} ->
            {reply,
                tictactree(State,
                            Tag,
                            Bucket,
                            {StartKey, EndKey},
                            CheckPresence,
                            TreeSize,
                            PartitionFilter),
                State};
        {tictactree_idx,
            {Bucket, IdxField, StartValue, EndValue},
            TreeSize,
            PartitionFilter} ->
            {reply,
                tictactree(State,
                            ?IDX_TAG,
                            Bucket,
                            {IdxField, StartValue, EndValue},
                            false,
                            TreeSize,
                            PartitionFilter),
                State};
        {foldheads_allkeys, Tag, FoldHeadsFun} ->
            {reply,
                foldheads_allkeys(State, Tag, FoldHeadsFun),
                State};
        {foldheads_bybucket, Tag, Bucket, FoldHeadsFun} ->
            {reply,
                foldheads_bybucket(State, Tag, Bucket, FoldHeadsFun),
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
%%% External functions
%%%============================================================================

%% @doc Empty the ledger cache table following a push
empty_ledgercache() ->
    #ledger_cache{mem = ets:new(empty, [ordered_set])}.

%% @doc push the ledgercache to the Penciller - which should respond ok or
%% returned.  If the response is ok the cache can be flushed, but if the
%% response is returned the cache should continue to build and it should try
%% to flush at a later date
push_ledgercache(Penciller, Cache) ->
    CacheToLoad = {Cache#ledger_cache.loader,
                    Cache#ledger_cache.index,
                    Cache#ledger_cache.min_sqn,
                    Cache#ledger_cache.max_sqn},
    leveled_penciller:pcl_pushmem(Penciller, CacheToLoad).

%% @doc the ledger cache can be built from a queue, for example when
%% loading the ledger from the head of the journal on startup
%%
%% The queue should be build using [NewKey|Acc] so that the most recent
%% key is kept in the sort
loadqueue_ledgercache(Cache) ->
    SL = lists:ukeysort(1, Cache#ledger_cache.load_queue),
    T = leveled_tree:from_orderedlist(SL, ?CACHE_TYPE),
    Cache#ledger_cache{load_queue = [], loader = T}.

%% @doc Allow all a snapshot to be created from part of the store, preferably
%% passing in a query filter so that all of the LoopState does not need to
%% be copied from the real actor to the clone
%%
%% SnapType can be store (requires journal and ledger) or ledger (requires
%% ledger only)
%%
%% Query can be no_lookup, indicating the snapshot will be used for non-specific
%% range queries and not direct fetch requests.  {StartKey, EndKey} if the the
%% snapshot is to be used for one specific query only (this is much quicker to
%% setup, assuming the range is a small subset of the overall key space).
snapshot_store(LedgerCache0, Penciller, Inker, SnapType, Query) ->
    LedgerCache = readycache_forsnapshot(LedgerCache0, Query),
    BookiesMem = {LedgerCache#ledger_cache.loader,
                    LedgerCache#ledger_cache.index,
                    LedgerCache#ledger_cache.min_sqn,
                    LedgerCache#ledger_cache.max_sqn},
    LongRunning = 
        case Query of 
            undefined -> 
                true;
            no_lookup ->
                true;
            _ ->
                % If a specific query has been defined, then not expected
                % to be long running
                false
        end,
    PCLopts = #penciller_options{start_snapshot = true,
                                    source_penciller = Penciller,
                                    snapshot_query = Query,
                                    snapshot_longrunning = LongRunning,
                                    bookies_mem = BookiesMem},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    case SnapType of
        store ->
            InkerOpts = #inker_options{start_snapshot=true,
                                        source_inker=Inker},
            {ok, JournalSnapshot} = leveled_inker:ink_start(InkerOpts),
            {ok, LedgerSnapshot, JournalSnapshot};
        ledger ->
            {ok, LedgerSnapshot, null}
    end.    

snapshot_store(State, SnapType) ->
    snapshot_store(State, SnapType, undefined).

snapshot_store(State, SnapType, Query) ->
    snapshot_store(State#state.ledger_cache,
                    State#state.penciller,
                    State#state.inker,
                    SnapType,
                    Query).

fetch_value(Inker, {Key, SQN}) ->
    SW = os:timestamp(),
    case leveled_inker:ink_fetch(Inker, Key, SQN) of
        {ok, Value} ->
            maybe_longrunning(SW, inker_fetch),
            Value;
        not_present ->
            not_present
    end.


%%%============================================================================
%%% Internal functions
%%%============================================================================

maybe_longrunning(SW, Aspect) ->
    case timer:now_diff(os:timestamp(), SW) of
        N when N > ?LONG_RUNNING ->
            leveled_log:log("B0013", [N, Aspect]);
        _ ->
            ok
    end.

bucket_stats(State, Bucket, Tag) ->
    {ok, LedgerSnapshot, _JournalSnapshot} = snapshot_store(State,
                                                            ledger,
                                                            no_lookup),
    Folder = fun() ->
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
    {ok, LedgerSnapshot, _JournalSnapshot} = snapshot_store(State,
                                                            ledger,
                                                            no_lookup),
    Folder = fun() ->
                BucketAcc = get_nextbucket(null,
                                            null,
                                            Tag,
                                            LedgerSnapshot,
                                            []),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                lists:foldl(fun({B, _K}, Acc) -> FoldBucketsFun(B, Acc) end,
                                InitAcc,
                                BucketAcc)
                end,
    {async, Folder}.

get_nextbucket(NextBucket, NextKey, Tag, LedgerSnapshot, BKList) ->
    Now = leveled_codec:integer_now(),
    StartKey = leveled_codec:to_ledgerkey(NextBucket, NextKey, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    ExtractFun =
        fun(LK, V, _Acc) ->
            {leveled_codec:from_ledgerkey(LK), V}
        end,
    R = leveled_penciller:pcl_fetchnextkey(LedgerSnapshot,
                                                    StartKey,
                                                    EndKey,
                                                    ExtractFun,
                                                    null),
    case R of
        null ->
            leveled_log:log("B0008",[]),
            BKList;
        {{B, K}, V} when is_binary(B), is_binary(K) ->
            case leveled_codec:is_active({B, K}, V, Now) of
                true ->
                    leveled_log:log("B0009",[B]),
                    get_nextbucket(<<B/binary, 0>>,
                                    null,
                                    Tag,
                                    LedgerSnapshot,
                                    [{B, K}|BKList]);
                false ->
                    get_nextbucket(B,
                                    <<K/binary, 0>>,
                                    Tag,
                                    LedgerSnapshot,
                                    BKList)
            end;
        {NB, _V} ->
            leveled_log:log("B0010",[NB]),
            []
    end.
            

index_query(State,
                Bucket,
                StartObjKey,
                {FoldKeysFun, InitAcc},
                {IdxField, StartValue, EndValue},
                {ReturnTerms, TermRegex}) ->
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
    {ok, LedgerSnapshot, _JournalSnapshot} = snapshot_store(State,
                                                            ledger,
                                                            {StartKey,
                                                                EndKey}),
    Folder = fun() ->
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


hashlist_query(State, Tag, JournalCheck) ->
    SnapType = case JournalCheck of
                            false ->
                                ledger;
                            check_presence ->
                                store
                        end,
    {ok, LedgerSnapshot, JournalSnapshot} = snapshot_store(State,
                                                            SnapType,
                                                            no_lookup),
    Folder = fun() ->
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

tictactree(State, Tag, Bucket, Query, JournalCheck, TreeSize, Filter) ->
    % Journal check can be used for object key folds to confirm that the
    % object is still indexed within the journal
    SnapType = case JournalCheck of
                            false ->
                                ledger;
                            check_presence ->
                                store
                        end,
    {ok, LedgerSnapshot, JournalSnapshot} = snapshot_store(State,
                                                            SnapType,
                                                            no_lookup),
    Tree = leveled_tictac:new_tree(temp, TreeSize),
    Folder =
        fun() ->
            % The start key and end key will vary depending on whether the
            % fold is to fold over an index or a key range
            {StartKey, EndKey, HashFun} =
                case Tag of
                    ?IDX_TAG ->
                        {IdxField, StartIdx, EndIdx} = Query,
                        HashIdxValFun =
                            fun(_Key, IdxValue) ->
                                erlang:phash2(IdxValue)
                            end,
                        {leveled_codec:to_ledgerkey(Bucket,
                                                    null,
                                                    ?IDX_TAG,
                                                    IdxField,
                                                    StartIdx),
                            leveled_codec:to_ledgerkey(Bucket,
                                                        null,
                                                        ?IDX_TAG,
                                                        IdxField,
                                                        EndIdx),
                            HashIdxValFun};
                    _ ->
                        {StartObjKey, EndObjKey} = Query,
                        PassHashFun = fun(_Key, Hash) -> Hash end,
                        {leveled_codec:to_ledgerkey(Bucket,
                                                    StartObjKey,
                                                    Tag),
                            leveled_codec:to_ledgerkey(Bucket,
                                                        EndObjKey,
                                                        Tag),
                            PassHashFun}
                end,
            
            AccFun = accumulate_tree(Filter,
                                        JournalCheck,
                                        JournalSnapshot,
                                        HashFun),
            Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                    StartKey,
                                                    EndKey,
                                                    AccFun,
                                                    Tree),
            
            % Close down snapshot when complete so as not to hold removed
            % files open
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
    foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun, false).

foldheads_allkeys(State, Tag, FoldHeadsFun) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    foldobjects(State, Tag, StartKey, EndKey, FoldHeadsFun, true).

foldobjects_bybucket(State, Tag, Bucket, FoldObjectsFun) ->
    StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun, false).

foldheads_bybucket(State, Tag, Bucket, FoldHeadsFun) ->
    StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    foldobjects(State, Tag, StartKey, EndKey, FoldHeadsFun, true).

foldobjects_byindex(State, Tag, Bucket,
                        Field, FromTerm, ToTerm, FoldObjectsFun) ->
    StartKey =
        leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, Field, FromTerm),
    EndKey =
        leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, Field, ToTerm),
    foldobjects(State, Tag, StartKey, EndKey, FoldObjectsFun, false).


foldobjects(_State, Tag, StartKey, EndKey, FoldObjectsFun, DeferredFetch) ->
    {FoldFun, InitAcc} = case is_tuple(FoldObjectsFun) of
                                true ->
                                    FoldObjectsFun;
                                false ->
                                    {FoldObjectsFun, []}
                            end,
    % For fold_objects the snapshot has been moved inside of the Folder
    % function.
    %
    % fold_objects and fold_heads are called by the riak_kv_sweeper in Riak,
    % and the sweeper prompts the fold before checking to see if the fold is
    % ready to be run.  This may lead to the fold being called on an old
    % snapshot.
    Self = self(),
    Folder =
        fun() ->
            {ok,
                LedgerSnapshot,
                JournalSnapshot} = book_snapshotstore(Self, Self, 5400),
                % Timeout will be ignored, as will Requestor
                %
                % This uses the external snapshot - as the snpshot will need 
                % to have consistent state between Bookie and Penciller when
                % it is made.
            AccFun = accumulate_objects(FoldFun,
                                        JournalSnapshot,
                                        Tag,
                                        DeferredFetch),
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
    {ok, LedgerSnapshot, _JournalSnapshot} = snapshot_store(State,
                                                            ledger,
                                                            no_lookup),
    Folder = fun() ->
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


readycache_forsnapshot(LedgerCache, {StartKey, EndKey}) ->
    {KL, MinSQN, MaxSQN} = scan_table(LedgerCache#ledger_cache.mem,
                                        StartKey,
                                        EndKey),
    case KL of
        [] ->
            #ledger_cache{loader=empty_cache,
                            index=empty_index,
                            min_sqn=MinSQN,
                            max_sqn=MaxSQN};
        _ ->
            #ledger_cache{loader=leveled_tree:from_orderedlist(KL,
                                                                ?CACHE_TYPE),
                            index=empty_index,
                            min_sqn=MinSQN,
                            max_sqn=MaxSQN}
    end;
readycache_forsnapshot(LedgerCache, Query) ->
    % Need to convert the Ledger Cache away from using the ETS table
    Tree = leveled_tree:from_orderedset(LedgerCache#ledger_cache.mem,
                                        ?CACHE_TYPE),
    case leveled_tree:tsize(Tree) of
        0 ->
            #ledger_cache{loader=empty_cache,
                            index=empty_index,
                            min_sqn=LedgerCache#ledger_cache.min_sqn,
                            max_sqn=LedgerCache#ledger_cache.max_sqn};
        _ ->
            Idx = 
                case Query of
                    no_lookup ->
                        empty_index;
                    _ ->
                        LedgerCache#ledger_cache.index
                end,
            #ledger_cache{loader=Tree,
                            index=Idx,
                            min_sqn=LedgerCache#ledger_cache.min_sqn,
                            max_sqn=LedgerCache#ledger_cache.max_sqn}
    end.

scan_table(Table, StartKey, EndKey) ->
    scan_table(Table, StartKey, EndKey, [], infinity, 0).

scan_table(Table, StartKey, EndKey, Acc, MinSQN, MaxSQN) ->
    case ets:next(Table, StartKey) of
        '$end_of_table' ->
            {lists:reverse(Acc), MinSQN, MaxSQN};
        NextKey ->
            case leveled_codec:endkey_passed(EndKey, NextKey) of
                true ->
                    {lists:reverse(Acc), MinSQN, MaxSQN};
                false ->
                    [{NextKey, NextVal}] = ets:lookup(Table, NextKey),
                    SQN = leveled_codec:strip_to_seqonly({NextKey, NextVal}),
                    scan_table(Table,
                                NextKey,
                                EndKey,
                                [{NextKey, NextVal}|Acc],
                                min(MinSQN, SQN),
                                max(MaxSQN, SQN))
            end
    end.


set_options(Opts) ->
    MaxJournalSize0 = get_opt(max_journalsize, Opts, 10000000000),
    JournalSizeJitter = MaxJournalSize0 div (100 div ?JOURNAL_SIZE_JITTER),
    MaxJournalSize = MaxJournalSize0 -
                        erlang:phash2(self()) rem JournalSizeJitter,
    
    SyncStrat = get_opt(sync_strategy, Opts, sync),
    WRP = get_opt(waste_retention_period, Opts),
    
    AltStrategy = get_opt(reload_strategy, Opts, []),
    ReloadStrategy = leveled_codec:inker_reload_strategy(AltStrategy),
    
    PCLL0CacheSize = get_opt(max_pencillercachesize, Opts),
    RootPath = get_opt(root_path, Opts),
    
    JournalFP = RootPath ++ "/" ++ ?JOURNAL_FP,
    LedgerFP = RootPath ++ "/" ++ ?LEDGER_FP,
    ok = filelib:ensure_dir(JournalFP),
    ok = filelib:ensure_dir(LedgerFP),
    
    {#inker_options{root_path = JournalFP,
                        reload_strategy = ReloadStrategy,
                        max_run_length = get_opt(max_run_length, Opts),
                        waste_retention_period = WRP,
                        cdb_options = #cdb_options{max_size=MaxJournalSize,
                                                    binary_mode=true,
                                                    sync_strategy=SyncStrat}},
        #penciller_options{root_path = LedgerFP,
                            max_inmemory_tablesize = PCLL0CacheSize,
                            levelzero_cointoss = true}}.

startup(InkerOpts, PencillerOpts, RecentAAE) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    leveled_log:log("B0005", [LedgerSQN]),
    ok = leveled_inker:ink_loadpcl(Inker,
                                    LedgerSQN + 1,
                                    get_loadfun(RecentAAE),
                                    Penciller),
    {Inker, Penciller}.


fetch_head(Key, Penciller, LedgerCache) ->
    SW = os:timestamp(),
    CacheResult = 
        case LedgerCache#ledger_cache.mem of
            undefined ->
                [];
            Tab ->
                ets:lookup(Tab, Key)
        end,
    case CacheResult of
        [{Key, Head}] ->
            Head;
        [] ->
            Hash = leveled_codec:magic_hash(Key),
            case leveled_penciller:pcl_fetch(Penciller, Key, Hash) of
                {Key, Head} ->
                    maybe_longrunning(SW, pcl_head),
                    Head;
                not_present ->
                    maybe_longrunning(SW, pcl_head),
                    not_present
            end
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
    AddKeyFun =
        fun(B, K, H, Acc) ->
            [{B, K, H}|Acc]
        end,
    get_hashaccumulator(JournalCheck,
                            InkerClone,
                            AddKeyFun).

accumulate_tree(FilterFun, JournalCheck, InkerClone, HashFun) ->
    AddKeyFun =
        fun(B, K, H, Tree) ->
            case FilterFun(B, K) of
                accumulate ->
                    leveled_tictac:add_kv(Tree, K, H, HashFun);
                pass ->
                    Tree
            end
        end,
    get_hashaccumulator(JournalCheck,
                        InkerClone,
                        AddKeyFun).
    
get_hashaccumulator(JournalCheck, InkerClone, AddKeyFun) ->
    Now = leveled_codec:integer_now(),
    AccFun =
        fun(LK, V, Acc) ->
            case leveled_codec:is_active(LK, V, Now) of
                true ->
                    {B, K, H} = leveled_codec:get_keyandobjhash(LK, V),
                    Check = leveled_rand:uniform() < ?CHECKJOURNAL_PROB,
                    case {JournalCheck, Check} of
                        {check_presence, true} ->
                            case check_presence(LK, V, InkerClone) of
                                true ->
                                    AddKeyFun(B, K, H, Acc);
                                false ->
                                    Acc
                            end;
                        _ ->    
                            AddKeyFun(B, K, H, Acc)
                    end;
                false ->
                    Acc
            end
        end,
    AccFun.


accumulate_objects(FoldObjectsFun, InkerClone, Tag, DeferredFetch) ->
    Now = leveled_codec:integer_now(),
    AccFun =
        fun(LK, V, Acc) ->
            % The function takes the Ledger Key and the value from the
            % ledger (with the value being the object metadata)
            %
            % Need to check if this is an active object (so TTL has not
            % expired).
            % If this is a deferred_fetch (i.e. the fold is a fold_heads not
            % a fold_objects), then a metadata object needs to be built to be
            % returned - but a quick check that Key is present in the Journal
            % is made first
            case leveled_codec:is_active(LK, V, Now) of
                true ->
                    {SQN, _St, _MH, MD} =
                        leveled_codec:striphead_to_details(V),
                    {B, K} =
                        case leveled_codec:from_ledgerkey(LK) of
                            {B0, K0} ->
                                {B0, K0};
                            {B0, K0, _T0} ->
                                {B0, K0}
                        end,
                    JK = {leveled_codec:to_ledgerkey(B, K, Tag), SQN},
                    case DeferredFetch of
                        true ->
                            InJournal =
                                leveled_inker:ink_keycheck(InkerClone,
                                                            LK,
                                                            SQN),
                            case InJournal of
                                probably ->
                                    Size = leveled_codec:get_size(LK, V),
                                    MDBin =
                                        leveled_codec:build_metadata_object(LK,
                                                                            MD),
                                    Value = {proxy_object,
                                                MDBin,
                                                Size,
                                                {fun fetch_value/2,
                                                    InkerClone,
                                                    JK}},
                                    FoldObjectsFun(B,
                                                    K,
                                                    term_to_binary(Value),
                                                    Acc);
                                missing ->
                                    Acc
                            end;
                        false ->
                            R = fetch_value(InkerClone, JK),
                            case R of
                                not_present ->
                                    Acc;
                                Value ->
                                    FoldObjectsFun(B, K, Value, Acc)
                                
                            end
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
                        LedgerKey, SQN, _Obj, _Size, {IdxSpecs, TTL},
                        _AAE) ->
    {Bucket, Key} = leveled_codec:from_ledgerkey(LedgerKey),
    KeyChanges =
        leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL),
    {no_lookup, SQN, KeyChanges};
preparefor_ledgercache(_InkTag,
                        LedgerKey, SQN, Obj, Size, {IdxSpecs, TTL},
                        AAE) ->
    {Bucket, Key, MetaValue, {KeyH, ObjH}, LastMods} =
        leveled_codec:generate_ledgerkv(LedgerKey, SQN, Obj, Size, TTL),
    KeyChanges =
        [{LedgerKey, MetaValue}] ++
            leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL) ++
            leveled_codec:aae_indexspecs(AAE, Bucket, Key, SQN, ObjH, LastMods),
    {KeyH, SQN, KeyChanges}.


addto_ledgercache({H, SQN, KeyChanges}, Cache) ->
    ets:insert(Cache#ledger_cache.mem, KeyChanges),
    UpdIndex = leveled_pmem:prepare_for_index(Cache#ledger_cache.index, H),
    Cache#ledger_cache{index = UpdIndex,
                        min_sqn=min(SQN, Cache#ledger_cache.min_sqn),
                        max_sqn=max(SQN, Cache#ledger_cache.max_sqn)}.

addto_ledgercache({H, SQN, KeyChanges}, Cache, loader) ->
    UpdQ = KeyChanges ++ Cache#ledger_cache.load_queue,
    UpdIndex = leveled_pmem:prepare_for_index(Cache#ledger_cache.index, H),
    Cache#ledger_cache{index = UpdIndex,
                        load_queue = UpdQ,
                        min_sqn=min(SQN, Cache#ledger_cache.min_sqn),
                        max_sqn=max(SQN, Cache#ledger_cache.max_sqn)}.


maybepush_ledgercache(MaxCacheSize, Cache, Penciller) ->
    Tab = Cache#ledger_cache.mem,
    CacheSize = ets:info(Tab, size),
    TimeToPush = maybe_withjitter(CacheSize, MaxCacheSize),
    if
        TimeToPush ->
            CacheToLoad = {Tab,
                            Cache#ledger_cache.index,
                            Cache#ledger_cache.min_sqn,
                            Cache#ledger_cache.max_sqn},
            case leveled_penciller:pcl_pushmem(Penciller, CacheToLoad) of
                ok ->
                    Cache0 = #ledger_cache{},
                    true = ets:delete(Tab),
                    NewTab = ets:new(mem, [ordered_set]),
                    {ok, Cache0#ledger_cache{mem=NewTab}};
                returned ->
                    {returned, Cache}
            end;
        true ->
             {ok, Cache}
    end.


maybe_withjitter(CacheSize, MaxCacheSize) ->    
    if
        CacheSize > MaxCacheSize ->
            R = leveled_rand:uniform(7 * MaxCacheSize),
            if
                (CacheSize - MaxCacheSize) > R ->
                    true;
                true ->
                    false
            end;
        true ->
            false
    end.


get_loadfun(RecentAAE) ->
    PrepareFun = 
        fun(Tag, PK, SQN, Obj, VS, IdxSpecs) ->
            preparefor_ledgercache(Tag, PK, SQN, Obj, VS, IdxSpecs, RecentAAE)
        end,
    LoadFun =
        fun(KeyInJournal, ValueInJournal, _Pos, Acc0, ExtractFun) ->
            {MinSQN, MaxSQN, OutputTree} = Acc0,
            {SQN, InkTag, PK} = KeyInJournal,
            % VBin may already be a term
            {VBin, VSize} = ExtractFun(ValueInJournal), 
            {Obj, IdxSpecs} = leveled_codec:split_inkvalue(VBin),
            case SQN of
                SQN when SQN < MinSQN ->
                    {loop, Acc0};    
                SQN when SQN < MaxSQN ->
                    Chngs = PrepareFun(InkTag, PK, SQN, Obj, VSize, IdxSpecs),
                    {loop,
                        {MinSQN,
                            MaxSQN,
                            addto_ledgercache(Chngs, OutputTree, loader)}};
                MaxSQN ->
                    leveled_log:log("B0006", [SQN]),
                    Chngs = PrepareFun(InkTag, PK, SQN, Obj, VSize, IdxSpecs),
                    {stop,
                        {MinSQN,
                            MaxSQN,
                            addto_ledgercache(Chngs, OutputTree, loader)}};
                SQN when SQN > MaxSQN ->
                    leveled_log:log("B0007", [MaxSQN, SQN]),
                    {stop, Acc0}
            end
        end,
    LoadFun.


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
    Value = leveled_rand:rand_bytes(256),
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

hashlist_query_test() ->
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
                                                {hashlist_query,
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
                                                {hashlist_query,
                                                    ?STD_TAG,
                                                    false}),
    L0 = length(KeyHashList),
    HTR2 =  HTFolder2(),
    ?assertMatch(L0, length(HTR2)),
    ?assertMatch(KeyHashList, HTR2),
    ok = book_close(Bookie2),
    reset_filestructure().

hashlist_query_withjournalcheck_test() ->
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
                                                {hashlist_query,
                                                    ?STD_TAG,
                                                    false}),
    KeyHashList = HTFolder1(),
    {async, HTFolder2} = book_returnfolder(Bookie1,
                                                {hashlist_query,
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
                                                {hashlist_query,
                                                    ?STD_TAG,
                                                    false}),
    KeyHashList1 = lists:usort(HTFolder1()),
    
    FoldObjectsFun = fun(B, K, V, Acc) ->
                            [{B, K, erlang:phash2(term_to_binary(V))}|Acc] end,
    {async, HTFolder2} =
        book_returnfolder(Bookie1,
                            {foldobjects_allkeys, ?STD_TAG, FoldObjectsFun}),
    KeyHashList2 = HTFolder2(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList2)),
    
    FoldHeadsFun =
        fun(B, K, ProxyV, Acc) ->
            {proxy_object,
                _MDBin,
                _Size,
                {FetchFun, Clone, JK}} = binary_to_term(ProxyV),
            V = FetchFun(Clone, JK),
            [{B, K, erlang:phash2(term_to_binary(V))}|Acc]
        end,
    
    {async, HTFolder3} =
        book_returnfolder(Bookie1,
                            {foldheads_allkeys, ?STD_TAG, FoldHeadsFun}),
    KeyHashList3 = HTFolder3(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList3)),
    
    FoldHeadsFun2 = 
        fun(B, K, ProxyV, Acc) ->
            {proxy_object,
                MD,
                _Size,
                _Fetcher} = binary_to_term(ProxyV),
            {Hash, _Size} = MD,
            [{B, K, Hash}|Acc]
        end,
    
    {async, HTFolder4} =
        book_returnfolder(Bookie1,
                            {foldheads_allkeys, ?STD_TAG, FoldHeadsFun2}),
    KeyHashList4 = HTFolder4(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList4)),
    
    ok = book_close(Bookie1),
    reset_filestructure().


foldobjects_vs_foldheads_bybucket_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(400, 1),
    ObjL2 = generate_multiple_objects(400, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_codec:integer_now() + 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "BucketA", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL1),
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "BucketB", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL2),
    
    FoldObjectsFun = fun(B, K, V, Acc) ->
                            [{B, K, erlang:phash2(term_to_binary(V))}|Acc] end,
    {async, HTFolder1A} =
        book_returnfolder(Bookie1,
                            {foldobjects_bybucket,
                                ?STD_TAG,
                                "BucketA",
                                FoldObjectsFun}),
    KeyHashList1A = HTFolder1A(),
    {async, HTFolder1B} =
        book_returnfolder(Bookie1,
                            {foldobjects_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                FoldObjectsFun}),
    KeyHashList1B = HTFolder1B(),
    ?assertMatch(false,
                    lists:usort(KeyHashList1A) == lists:usort(KeyHashList1B)),
    
    FoldHeadsFun =
        fun(B, K, ProxyV, Acc) ->
            {proxy_object,
                        _MDBin,
                        _Size,
                        {FetchFun, Clone, JK}} = binary_to_term(ProxyV),
            V = FetchFun(Clone, JK),
            [{B, K, erlang:phash2(term_to_binary(V))}|Acc]
        end,
    
    {async, HTFolder2A} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketA",
                                FoldHeadsFun}),
    KeyHashList2A = HTFolder2A(),
    {async, HTFolder2B} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                FoldHeadsFun}),
    KeyHashList2B = HTFolder2B(),
    ?assertMatch(true,
                    lists:usort(KeyHashList1A) == lists:usort(KeyHashList2A)),
    ?assertMatch(true,
                    lists:usort(KeyHashList1B) == lists:usort(KeyHashList2B)),
    
    ok = book_close(Bookie1),
    reset_filestructure().


scan_table_test() ->
    K1 = leveled_codec:to_ledgerkey(<<"B1">>,
                                        <<"K1">>,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AA1">>),
    K2 = leveled_codec:to_ledgerkey(<<"B1">>,
                                        <<"K2">>,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AA1">>),
    K3 = leveled_codec:to_ledgerkey(<<"B1">>,
                                        <<"K3">>,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AB1">>),
    K4 = leveled_codec:to_ledgerkey(<<"B1">>,
                                        <<"K4">>,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AA2">>),
    K5 = leveled_codec:to_ledgerkey(<<"B2">>,
                                        <<"K5">>,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AA2">>),
    Tab0 = ets:new(mem, [ordered_set]),
    
    SK_A0 = leveled_codec:to_ledgerkey(<<"B1">>,
                                        null,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AA0">>),
    EK_A9 = leveled_codec:to_ledgerkey(<<"B1">>,
                                        null,
                                        ?IDX_TAG,
                                        <<"F1-bin">>,
                                        <<"AA9">>),
    Empty = {[], infinity, 0},
    ?assertMatch(Empty,
                    scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K1, {1, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}], 1, 1},
                    scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K2, {2, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}, {K2, _}], 1, 2},
                    scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K3, {3, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}, {K2, _}], 1, 2},
                    scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K4, {4, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}, {K2, _}, {K4, _}], 1, 4},
                    scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K5, {5, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}, {K2, _}, {K4, _}], 1, 4},
                    scan_table(Tab0, SK_A0, EK_A9)).

longrunning_test() ->
    SW = os:timestamp(),
    timer:sleep(100),
    ok = maybe_longrunning(SW, put).

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null),
    {noreply, _State2} = handle_cast(null, #state{}).


-endif.
