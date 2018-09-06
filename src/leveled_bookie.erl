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
        book_mput/2,
        book_mput/3,
        book_delete/4,
        book_get/3,
        book_get/4,
        book_head/3,
        book_head/4,
        book_returnfolder/2,
        book_snapshot/4,
        book_compactjournal/2,
        book_islastcompactionpending/1,
        book_trimjournal/1,
        book_close/1,
        book_destroy/1,
        book_isempty/2]).

-export([empty_ledgercache/0,
            loadqueue_ledgercache/1,
            push_ledgercache/2,
            snapshot_store/6,
            fetch_value/2]).

-include_lib("eunit/include/eunit.hrl").

-define(CACHE_SIZE, 2500).
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").
-define(SNAPSHOT_TIMEOUT, 300000).
-define(CACHE_SIZE_JITTER, 25).
-define(JOURNAL_SIZE_JITTER, 20).
-define(ABSOLUTEMAX_JOURNALSIZE, 4000000000).
-define(LONG_RUNNING, 80000).
-define(RECENT_AAE, false).
-define(COMPRESSION_METHOD, lz4).
-define(COMPRESSION_POINT, on_receipt).
-define(TIMING_SAMPLESIZE, 100).
-define(TIMING_SAMPLECOUNTDOWN, 10000).
-define(DUMMY, dummy). % Dummy key used for mput operations
-define(OPTION_DEFAULTS,
            [{root_path, undefined},
                {snapshot_bookie, undefined},
                {cache_size, ?CACHE_SIZE},
                {max_journalsize, 1000000000},
                {sync_strategy, none},
                {recent_aae, ?RECENT_AAE},
                {head_only, false},
                {waste_retention_period, undefined},
                {max_run_length, undefined},
                {singlefile_compactionpercentage, 50.0},
                {maxrunlength_compactionpercentage, 70.0},
                {reload_strategy, []},
                {max_pencillercachesize, undefined},
                {compression_method, ?COMPRESSION_METHOD},
                {compression_point, ?COMPRESSION_POINT}]).

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
                recent_aae :: recent_aae(),
                ledger_cache = #ledger_cache{},
                is_snapshot :: boolean() | undefined,
                slow_offer = false :: boolean(),

                head_only = false :: boolean(),
                head_lookup = true :: boolean(),

                put_countdown = 0 :: integer(),
                get_countdown = 0 :: integer(),
                fold_countdown = 0 :: integer(),
                head_countdown = 0 :: integer(),
                get_timings = no_timing :: get_timings(),
                put_timings = no_timing :: put_timings(),
                fold_timings = no_timing :: fold_timings(),
                head_timings = no_timing :: head_timings()}).


-record(get_timings, {sample_count = 0 :: integer(),
                        head_time = 0 :: integer(),
                        body_time = 0 :: integer(),
                        fetch_count = 0 :: integer()}).

-record(head_timings, {sample_count = 0 :: integer(),
                        pcl_time = 0 :: integer(),
                        buildhead_time = 0 :: integer()}).

-record(put_timings, {sample_count = 0 :: integer(),
                        mem_time = 0 :: integer(),
                        ink_time = 0 :: integer(),
                        total_size = 0 :: integer()}).

-record(fold_timings, {sample_count = 0 :: integer(),
                        setup_time = 0 :: integer()}).


-type book_state() :: #state{}.
-type sync_mode() :: sync|none|riak_sync.
-type ledger_cache() :: #ledger_cache{}.
-type get_timings() :: no_timing|#get_timings{}.
-type put_timings() :: no_timing|#put_timings{}.
-type fold_timings() :: no_timing|#fold_timings{}.
-type head_timings() :: no_timing|#head_timings{}.
-type timing_types() :: head|get|put|fold.
-type recent_aae() :: false|#recent_aae{}|undefined.
-type key() :: binary()|string().
    % Keys SHOULD be binary()
    % string() support is a legacy of old tests
-type open_options() :: 
    %% For full description of options see ../docs/STARTUP_OPTIONS.md
    [{root_path, string()|undefined} |
            % Folder to be used as the root path for storing all the database
            % information.  May be undefined is snapshot_bookie is a pid()
            % TODO: Some sort of split root path to allow for mixed classes of
            % storage (e.g. like eleveldb tiered storage - only with 
            % separation between ledger and non-current journal)
        {snapshot_bookie, undefined|pid()} |
            % Is the bookie being started required to a be a snapshot of an
            % existing bookie, rather than a new bookie.  The bookie to be
            % snapped should have its pid passed as the startup option in this
            % case
        {cache_size, pos_integer()} |
            % The size of the Bookie's memory, the cache of the recent 
            % additions to the ledger.  Defaults to ?CACHE_SIZE, plus some
            % randomised jitter (randomised jitter will still be added to 
            % configured values
        {max_journalsize, pos_integer()} |
            % The maximum size of a journal file in bytes.  The abolute 
            % maximum must be 4GB due to 4 byte file pointers being used
        {sync_strategy, sync_mode()} |
            % Should be sync if it is necessary to flush to disk after every
            % write, or none if not (allow the OS to schecdule).  This has a
            % significant impact on performance which can be mitigated 
            % partially in hardware (e.g through use of FBWC).
            % riak_sync is used for backwards compatability with OTP16 - and 
            % will manually call sync() after each write (rather than use the
            % O_SYNC option on startup
        {recent_aae, false|{atom(), list(), integer(), integer()}} |
            % DEPRECATED
            % Before working on kv_index_tictactree looked at the possibility
            % of maintaining AAE just for recent changes.  Given the efficiency
            % of the kv_index_tictactree approach this is unecessary.
            % Should be set to false
        {head_only, false|with_lookup|no_lookup} |
            % When set to true, there are three fundamental changes as to how
            % leveled will work:
            % - Compaction of the journalwill be managed by simply removing any
            % journal file thathas a highest sequence number persisted to the
            % ledger;
            % - GETs are not supported, only head requests;
            % - PUTs should arrive batched object specs using the book_mput/2
            % function.
            % head_only mode is disabled with false (default).  There are two
            % different modes in which head_only can run with_lookup or 
            % no_lookup and heaD_only mode is enabled by passing one of these
            % atoms: 
            % - with_lookup assumes that individual objects may need to be
            % fetched;
            % - no_lookup prevents individual objects from being fetched, so 
            % that the store can only be used for folds (without segment list
            % acceleration)
        {waste_retention_period, undefined|pos_integer()} |
            % If a value is not required in the journal (i.e. it has been 
            % replaced and is now to be removed for compaction) for how long
            % should it be retained.  For example should it be kept for a 
            % period until the operator cna be sure a backup has been 
            % completed?
            % If undefined, will not retian waste, otherwise the period is the
            % number of seconds to wait
        {max_run_length, undefined|pos_integer()} |
            % The maximum number of consecutive files that can be compacted in
            % one compaction operation.  
            % Defaults to leveled_iclerk:?MAX_COMPACTION_RUN (if undefined)
        {singlefile_compactionpercentage, float()} |
            % What is the percentage of space to be recovered from compacting
            % a single file, before that file can be a compaction candidate in
            % a compaction run of length 1
        {maxrunlength_compactionpercentage, float()} |
            % What is the percentage of space to be recovered from compacting
            % a run of max_run_length, before that run can be a compaction 
            % candidate.  For runs between 1 and max_run_length, a 
            % proportionate score is calculated
        {reload_strategy, list()} |
            % The reload_strategy is exposed as an option as currently no firm
            % decision has been made about how recovery from failure should
            % work.  For instance if we were to trust everything as permanent
            % in the Ledger once it is persisted, then there would be no need
            % to retain a skinny history of key changes in the Journal after
            % compaction.  If, as an alternative we assume the Ledger is never
            % permanent, and retain the skinny hisory - then backups need only
            % be made against the Journal.  The skinny history of key changes
            % is primarily related to the issue of supporting secondary indexes
            % in Riak.
            %
            % These two strategies are referred to as recovr (assume we can
            % recover any deltas from a lost ledger and a lost history through
            % resilience outside of the store), or retain (retain a history of
            % key changes, even when the object value has been compacted). 
            %
            % There is a third, theoretical and untested strategy, which is
            % recalc - which would require when reloading the Ledger from the
            % Journal, to recalculate the index changes based on the current
            % state of the Ledger and the object metadata.
            %
            % reload_strategy ptions are a list - to map from a tag to the
            % strategy (recovr|retain|recalc).  Defualt strategies are:
            % [{?RIAK_TAG, retain}, {?STD_TAG, retain}]
        {max_pencillercachesize, pos_integer()|undefined} |
            % How many ledger keys should the penciller retain in memory
            % between flushing new level zero files.
            % Defaults ot leveled_penciller:?MAX_TABLESIZE when undefined
        {compression_method, native|lz4} |
            % Compression method and point allow Leveled to be switched from
            % using bif based compression (zlib) to using nif based compression
            % (lz4).
            % Defaults to ?COMPRESSION_METHOD
        {compression_point, on_compact|on_receipt}
            % The =compression point can be changed between on_receipt (all
            % values are compressed as they are received), to on_compact where
            % values are originally stored uncompressed (speeding PUT times),
            % and are only compressed when they are first subject to compaction
            % Defaults to ?COMPRESSION_POINT
        ].

-export_type([key/0]).


%%%============================================================================
%%% API
%%%============================================================================

-spec book_start(string(), integer(), integer(), sync_mode()) -> {ok, pid()}.

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
    book_start(set_defaults([{root_path, RootPath},
                                {cache_size, LedgerCacheSize},
                                {max_journalsize, JournalSize},
                                {sync_strategy, SyncStrategy}])).

-spec book_start(list(tuple())) -> {ok, pid()}.

%% @doc Start a Leveled Key/Value store - full options support.
%%
%% For full description of options see ../docs/STARTUP_OPTIONS.md and also
%% comments on the open_options() type

book_start(Opts) ->
    gen_server:start_link(?MODULE, [set_defaults(Opts)], []).


-spec book_tempput(pid(), key(), key(), any(), 
                    leveled_codec:index_specs(), 
                    leveled_codec:tag(), integer()) -> ok|pause.

%% @doc Put an object with an expiry time
%%
%% Put an item in the store but with a Time To Live - the time when the object
%% should expire, in gregorian_seconds (add the required number of seconds to
%% leveled_util:integer_time/1).
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

-spec book_put(pid(), key(), key(), any(), 
                leveled_codec:index_specs(), 
                leveled_codec:tag(), infinity|integer()) -> ok|pause.

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL) ->
    gen_server:call(Pid,
                    {put, Bucket, Key, Object, IndexSpecs, Tag, TTL},
                    infinity).


-spec book_mput(pid(), list(tuple())) -> ok|pause.
%% @doc
%%
%% When the store is being run in head_only mode, batches of object specs may
%% be inserted in to the store using book_mput/2.  ObjectSpecs should be 
%% of the form {ObjectOp, Bucket, Key, SubKey, Value}.  The Value will be 
%% stored within the HEAD of the object (in the Ledger), so the full object 
%% is retrievable using a HEAD request.  The ObjectOp is either add or remove.
%%
%% The list should be de-duplicated before it is passed to the bookie.
book_mput(Pid, ObjectSpecs) ->
    book_mput(Pid, ObjectSpecs, infinity).

-spec book_mput(pid(), list(tuple()), infinity|integer()) -> ok|pause.
%% @doc
%%
%% When the store is being run in head_only mode, batches of object specs may
%% be inserted in to the store using book_mput/2.  ObjectSpecs should be 
%% of the form {action, Bucket, Key, SubKey, Value}.  The Value will be 
%% stored within the HEAD of the object (in the Ledger), so the full object 
%% is retrievable using a HEAD request.
%%
%% The list should be de-duplicated before it is passed to the bookie.
book_mput(Pid, ObjectSpecs, TTL) ->
    gen_server:call(Pid, {mput, ObjectSpecs, TTL}, infinity).

-spec book_delete(pid(), key(), key(), leveled_codec:index_specs()) 
                                                                -> ok|pause.

%% @doc 
%%
%% A thin wrap around the put of a special tombstone object.  There is no
%% immediate reclaim of space, simply the addition of a more recent tombstone.

book_delete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?STD_TAG).


-spec book_get(pid(), key(), key(), leveled_codec:tag()) 
                                                    -> {ok, any()}|not_found.
-spec book_head(pid(), key(), key(), leveled_codec:tag())
                                                    -> {ok, any()}|not_found.

%% @doc - GET and HEAD requests
%%
%% The Bookie supports both GET and HEAD requests, with the HEAD request
%% returning only the metadata and not the actual object value.  The HEAD
%% requets cna be serviced by reference to the Ledger Cache and the Penciller.
%%
%% GET requests first follow the path of a HEAD request, and if an object is
%% found, then fetch the value from the Journal via the Inker.


book_get(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {get, Bucket, Key, Tag}, infinity).

book_head(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag}, infinity).

book_get(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?STD_TAG).

book_head(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?STD_TAG).


-spec book_returnfolder(pid(), tuple()) -> {async, fun()}.

%% @doc Snapshots/Clones
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% may request a clone of the Penciller, or clones of both the Penciller and
%% the Inker should values also need to be accessed.  The snapshot clone is 
%% made available through a "runner" - a new trasnportable PID through which
%% the previous state of the store can be queried.  So, for example, a 
%% riak_kv_vnode_worker in the pool could host the runner.
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
%% snapshot functionality, more folders can be seen in the get_runner/2 
%% function:
%%
%% {bucket_stats, Bucket}  -> return a key count and total object size within
%% a bucket
%% {riakbucket_stats, Bucket} -> as above, but for buckets with the Riak Tag
%% {bucket_list, Tag, {FoldKeysFun, Acc}} -> if we assume buckets and
%% keys are binaries, provides a fast bucket list function
%% {index_query,
%%        Constraint,
%%        {FoldKeysFun, Acc},
%%        {IdxField, StartValue, EndValue},
%%        {ReturnTerms, TermRegex}} -> secondray index query
%% {keylist, Tag, {FoldKeysFun, Acc}} -> list all keys with tag
%% {keylist, Tag, Bucket, {FoldKeysFun, Acc}} -> list all keys within given
%% bucket
%% {foldobjects_bybucket, Tag, Bucket, FoldObjectsFun} -> fold over all objects
%% in a given bucket
%% {foldobjects_byindex,
%%        Tag,
%%        Bucket,
%%        {Field, FromTerm, ToTerm},
%%        FoldObjectsFun} -> fold over all objects with an entry in a given
%% range on a given index

book_returnfolder(Pid, RunnerType) ->
    gen_server:call(Pid, {return_runner, RunnerType}, infinity).


-spec book_snapshot(pid(), 
                    store|ledger, 
                    tuple()|undefined, 
                    boolean()|undefined) -> {ok, pid(), pid()|null}.

%% @doc create a snapshot of the store
%%
%% Snapshot can be based on a pre-defined query (which will be used to filter 
%% caches prior to copying for the snapshot), and can be defined as long 
%% running to avoid timeouts (snapshots are generally expected to be required
%% for < 60s)

book_snapshot(Pid, SnapType, Query, LongRunning) ->
    gen_server:call(Pid, {snapshot, SnapType, Query, LongRunning}, infinity).


-spec book_compactjournal(pid(), integer()) -> ok.
-spec book_islastcompactionpending(pid()) -> boolean().
-spec book_trimjournal(pid()) -> ok.

%% @doc Call for compaction of the Journal
%%
%% the scheduling of Journla compaction is called externally, so it is assumed
%% in Riak it will be triggered by a vnode callback.

book_compactjournal(Pid, Timeout) ->
    gen_server:call(Pid, {compact_journal, Timeout}, infinity).

%% @doc Check on progress of the last compaction

book_islastcompactionpending(Pid) ->
    gen_server:call(Pid, confirm_compact, infinity).

%% @doc Trim the journal when in head_only mode
%%
%% In head_only mode the journlacna be trimmed of entries which are before the 
%% persisted SQN.  This is much quicker than compacting the journal

book_trimjournal(Pid) ->
    gen_server:call(Pid, trim, infinity).

-spec book_close(pid()) -> ok.
-spec book_destroy(pid()) -> ok.

%% @doc Clean shutdown
%%
%% A clean shutdown will persist all the information in the Penciller memory
%% before closing, so shutdown is not instantaneous.
book_close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% @doc Close and clean-out files
book_destroy(Pid) ->
    gen_server:call(Pid, destroy, infinity).


-spec book_isempty(pid(), leveled_codec:tag()) -> boolean().
%% @doc 
%% Confirm if the store is empty, or if it contains a Key and Value for a 
%% given tag
book_isempty(Pid, Tag) ->
    FoldAccT = {fun(_B, _Acc) -> false end, true},
    {async, Runner} = 
        gen_server:call(Pid, {return_runner, {first_bucket, Tag, FoldAccT}}),
    Runner().

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

-spec init([open_options()]) -> {ok, book_state()}.
init([Opts]) ->
    leveled_rand:seed(),
    case proplists:get_value(snapshot_bookie, Opts) of
        undefined ->
            % Start from file not snapshot
            {InkerOpts, PencillerOpts} = set_options(Opts),

            CacheJitter = 
                proplists:get_value(cache_size, Opts) 
                    div (100 div ?CACHE_SIZE_JITTER),
            CacheSize = 
                proplists:get_value(cache_size, Opts)
                    + erlang:phash2(self()) rem CacheJitter,
            RecentAAE =
                case proplists:get_value(recent_aae, Opts) of
                    false ->
                        false;
                    {FilterType, BucketList, LimitMinutes, UnitMinutes} ->
                        #recent_aae{filter = FilterType,
                                    buckets = BucketList,
                                    limit_minutes = LimitMinutes,
                                    unit_minutes = UnitMinutes}
                end,
            
            {HeadOnly, HeadLookup} = 
                case proplists:get_value(head_only, Opts) of 
                    false ->
                        {false, true};
                    with_lookup ->
                        {true, true};
                    no_lookup ->
                        {true, false}
                end,
            
            State0 = #state{cache_size=CacheSize,
                                recent_aae=RecentAAE,
                                is_snapshot=false,
                                head_only=HeadOnly,
                                head_lookup = HeadLookup},

            {Inker, Penciller} = 
                startup(InkerOpts, PencillerOpts, State0),

            NewETS = ets:new(mem, [ordered_set]),
            leveled_log:log("B0001", [Inker, Penciller]),
            {ok, State0#state{inker=Inker,
                                penciller=Penciller,
                                ledger_cache=#ledger_cache{mem = NewETS}}};
        Bookie ->
            {ok, Penciller, Inker} = 
                book_snapshot(Bookie, store, undefined, true),
            leveled_log:log("B0002", [Inker, Penciller]),
            {ok, #state{penciller=Penciller,
                        inker=Inker,
                        is_snapshot=true}}
    end.


handle_call({put, Bucket, Key, Object, IndexSpecs, Tag, TTL}, From, State)
                                        when State#state.head_only == false ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    SW0 = os:timestamp(),
    {ok, SQN, ObjSize} = leveled_inker:ink_put(State#state.inker,
                                               LedgerKey,
                                               Object,
                                               {IndexSpecs, TTL}),
    {SW1, Timings1} = 
        update_timings(SW0, {put, {inker, ObjSize}}, State#state.put_timings),
    Changes = preparefor_ledgercache(null,
                                        LedgerKey,
                                        SQN,
                                        Object,
                                        ObjSize,
                                        {IndexSpecs, TTL},
                                        State),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    {_SW2, Timings2} = update_timings(SW1, {put, mem}, Timings1),

    {Timings, CountDown} = 
        update_statetimings(put, Timings2, State#state.put_countdown),
        % If the previous push to memory was returned then punish this PUT with
        % a delay.  If the back-pressure in the Penciller continues, these 
        % delays will beocme more frequent
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    maybe_longrunning(SW0, overall_put),
    case maybepush_ledgercache(State#state.cache_size,
                               Cache0,
                               State#state.penciller) of
        {ok, NewCache} ->
            {noreply, State#state{ledger_cache = NewCache,
                                  put_timings = Timings,
                                  put_countdown = CountDown,
                                  slow_offer = false}};
        {returned, NewCache} ->
            {noreply, State#state{ledger_cache = NewCache,
                                  put_timings = Timings,
                                  put_countdown = CountDown,
                                  slow_offer = true}}
    end;
handle_call({mput, ObjectSpecs, TTL}, From, State) 
                                        when State#state.head_only == true ->
    {ok, SQN} = 
        leveled_inker:ink_mput(State#state.inker, dummy, {ObjectSpecs, TTL}),
    Changes = 
        preparefor_ledgercache(?INKT_MPUT, ?DUMMY, 
                                SQN, null, length(ObjectSpecs), 
                                {ObjectSpecs, TTL}, 
                                State),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    case maybepush_ledgercache(State#state.cache_size,
                                    Cache0,
                                    State#state.penciller) of
        {ok, NewCache} ->
            {noreply, State#state{ledger_cache = NewCache,
                                    slow_offer = false}};
        {returned, NewCache} ->
            {noreply, State#state{ledger_cache = NewCache,
                                    slow_offer = true}}
    end;
handle_call({get, Bucket, Key, Tag}, _From, State) 
                                        when State#state.head_only == false ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    SWh = os:timestamp(),
    HeadResult = 
        case fetch_head(LedgerKey,
                        State#state.penciller,
                        State#state.ledger_cache) of
            not_present ->
                not_found;
            Head ->
                {Seqn, Status, _MH, _MD} = 
                    leveled_codec:striphead_to_details(Head),
                case Status of
                    tomb ->
                        not_found;
                    {active, TS} ->
                        case TS >= leveled_util:integer_now() of
                            false ->
                                not_found;
                            true ->
                                {LedgerKey, Seqn}
                        end
                end
        end,
    {SWb, Timings1} = 
        update_timings(SWh, {get, head}, State#state.get_timings),
    {Reply, Timings2} = 
        case HeadResult of 
            not_found -> 
                {not_found, Timings1};
            {LK, SQN} ->
                Object = fetch_value(State#state.inker, {LK, SQN}),
                {_SW, UpdTimingsB} =
                    update_timings(SWb, {get, body}, Timings1),
                case Object of 
                    not_present ->
                        {not_found, UpdTimingsB};
                    _ ->
                        {{ok, Object}, UpdTimingsB} 
                end 
        end,
    {Timings, CountDown} = 
        update_statetimings(get, Timings2, State#state.get_countdown),
    {reply, Reply, State#state{get_timings = Timings, 
                                get_countdown = CountDown}};
handle_call({head, Bucket, Key, Tag}, _From, State) 
                                        when State#state.head_lookup == true ->
    SWp = os:timestamp(),
    LK = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    case fetch_head(LK, 
                    State#state.penciller, 
                    State#state.ledger_cache, 
                    State#state.head_only) of
        not_present ->
            {reply, not_found, State};
        Head ->
            case leveled_codec:striphead_to_details(Head) of
                {_SeqN, tomb, _MH, _MD} ->
                    {reply, not_found, State};
                {_SeqN, {active, TS}, _MH, MD} ->
                    case TS >= leveled_util:integer_now() of
                        true ->
                            {SWr, UpdTimingsP} = 
                                update_timings(SWp, 
                                               {head, pcl}, 
                                               State#state.head_timings),
                            OMD = leveled_codec:build_metadata_object(LK, MD),
                            {_SW, UpdTimingsR} = 
                                update_timings(SWr, {head, rsp}, UpdTimingsP),
                            {UpdTimings, CountDown} =
                                update_statetimings(head, 
                                                    UpdTimingsR, 
                                                    State#state.head_countdown),
                            {reply, 
                             {ok, OMD}, 
                             State#state{head_timings = UpdTimings,
                                         head_countdown = CountDown}};
                        false ->
                            {reply, not_found, State}
                    end
            end
    end;
handle_call({snapshot, SnapType, Query, LongRunning}, _From, State) ->
    % Snapshot the store, specifying if the snapshot should be long running 
    % (i.e. will the snapshot be queued or be required for an extended period 
    % e.g. many minutes)
    Reply = snapshot_store(State, SnapType, Query, LongRunning),
    {reply, Reply, State};
handle_call({return_runner, QueryType}, _From, State) ->
    SW = os:timestamp(),
    Runner = get_runner(State, QueryType),
    {_SW, Timings1} = 
        update_timings(SW, {fold, setup}, State#state.fold_timings),
    {Timings, CountDown} = 
        update_statetimings(fold, Timings1, State#state.fold_countdown),
    {reply, Runner, State#state{fold_timings = Timings, 
                                fold_countdown = CountDown}};
handle_call({compact_journal, Timeout}, _From, State)
                                        when State#state.head_only == false ->
    ok = leveled_inker:ink_compactjournal(State#state.inker,
                                          self(),
                                          Timeout),
    {reply, ok, State};
handle_call(confirm_compact, _From, State)
                                        when State#state.head_only == false ->
    {reply, leveled_inker:ink_compactionpending(State#state.inker), State};
handle_call(trim, _From, State) when State#state.head_only == true ->
    PSQN = leveled_penciller:pcl_persistedsqn(State#state.penciller),
    {reply, leveled_inker:ink_trim(State#state.inker, PSQN), State};
handle_call(close, _From, State) ->
    leveled_inker:ink_close(State#state.inker),
    leveled_penciller:pcl_close(State#state.penciller),
    {stop, normal, ok, State};
handle_call(destroy, _From, State=#state{is_snapshot=Snp}) when Snp == false ->
    leveled_log:log("B0011", []),
    {ok, InkPathList} = leveled_inker:ink_doom(State#state.inker),
    {ok, PCLPathList} = leveled_penciller:pcl_doom(State#state.penciller),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, InkPathList),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, PCLPathList),
    {stop, normal, ok, State};
handle_call(Msg, _From, State) ->
    {reply, {unsupported_message, element(1, Msg)}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    leveled_log:log("B0003", [Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% External functions
%%%============================================================================

-spec empty_ledgercache() -> ledger_cache().
%% @doc 
%% Empty the ledger cache table following a push
empty_ledgercache() ->
    #ledger_cache{mem = ets:new(empty, [ordered_set])}.

-spec push_ledgercache(pid(), ledger_cache()) -> ok|returned.
%% @doc 
%% Push the ledgercache to the Penciller - which should respond ok or
%% returned.  If the response is ok the cache can be flushed, but if the
%% response is returned the cache should continue to build and it should try
%% to flush at a later date
push_ledgercache(Penciller, Cache) ->
    CacheToLoad = {Cache#ledger_cache.loader,
                    Cache#ledger_cache.index,
                    Cache#ledger_cache.min_sqn,
                    Cache#ledger_cache.max_sqn},
    leveled_penciller:pcl_pushmem(Penciller, CacheToLoad).

-spec loadqueue_ledgercache(ledger_cache()) -> ledger_cache().
%% @doc 
%% The ledger cache can be built from a queue, for example when loading the 
%% ledger from the head of the journal on startup
%%
%% The queue should be build using [NewKey|Acc] so that the most recent
%% key is kept in the sort
loadqueue_ledgercache(Cache) ->
    SL = lists:ukeysort(1, Cache#ledger_cache.load_queue),
    T = leveled_tree:from_orderedlist(SL, ?CACHE_TYPE),
    Cache#ledger_cache{load_queue = [], loader = T}.

-spec snapshot_store(ledger_cache(), 
                        pid(), null|pid(), store|ledger, 
                        undefined|tuple(), undefined|boolean()) ->
                                                {ok, pid(), pid()|null}.
%% @doc 
%% Allow all a snapshot to be created from part of the store, preferably
%% passing in a query filter so that all of the LoopState does not need to
%% be copied from the real actor to the clone
%%
%% SnapType can be store (requires journal and ledger) or ledger (requires
%% ledger only)
%%
%% Query can be no_lookup, indicating the snapshot will be used for non-specific
%% range queries and not direct fetch requests.  {StartKey, EndKey} if the the
%% snapshot is to be used for one specific query only (this is much quicker to
%% setup, assuming the range is a small subset of the overall key space).  If 
%% lookup is required but the range isn't defined then 'undefined' should be 
%% passed as the query
snapshot_store(LedgerCache, Penciller, Inker, SnapType, Query, LongRunning) ->
    LedgerCacheReady = readycache_forsnapshot(LedgerCache, Query),
    BookiesMem = {LedgerCacheReady#ledger_cache.loader,
                    LedgerCacheReady#ledger_cache.index,
                    LedgerCacheReady#ledger_cache.min_sqn,
                    LedgerCacheReady#ledger_cache.max_sqn},
    PCLopts = #penciller_options{start_snapshot = true,
                                    source_penciller = Penciller,
                                    snapshot_query = Query,
                                    snapshot_longrunning = LongRunning,
				    bookies_pid = self(),
                                    bookies_mem = BookiesMem},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_snapstart(PCLopts),
    case SnapType of
        store ->
            InkerOpts = #inker_options{start_snapshot=true,
                                       bookies_pid = self(),
                                       source_inker=Inker},
            {ok, JournalSnapshot} = leveled_inker:ink_snapstart(InkerOpts),
            {ok, LedgerSnapshot, JournalSnapshot};
        ledger ->
            {ok, LedgerSnapshot, null}
    end.

snapshot_store(State, SnapType, Query, LongRunning) ->
    snapshot_store(State#state.ledger_cache,
                    State#state.penciller,
                    State#state.inker,
                    SnapType,
                    Query,
                    LongRunning).

-spec fetch_value(pid(), {any(), integer()}) -> not_present|any().
%% @doc
%% Fetch a value from the Journal
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

-spec startup(#inker_options{}, #penciller_options{}, book_state()) 
                                                            -> {pid(), pid()}.
%% @doc
%% Startup the Inker and the Penciller, and prompt the loading of the Penciller
%% from the Inker.  The Penciller may be shutdown without the latest data 
%% having been persisted: and so the Iker must be able to update the Penciller
%% on startup with anything that happened but wasn't flushed to disk.
startup(InkerOpts, PencillerOpts, State) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    leveled_log:log("B0005", [LedgerSQN]),
    ok = leveled_inker:ink_loadpcl(Inker,
                                    LedgerSQN + 1,
                                    get_loadfun(State),
                                    Penciller),
    {Inker, Penciller}.


-spec set_defaults(list()) -> open_options().
%% @doc
%% Set any pre-defined defaults for options if the option is not present in
%% the passed in options
set_defaults(Opts) ->
    lists:ukeymerge(1, 
                    lists:ukeysort(1, Opts), 
                    lists:ukeysort(1, ?OPTION_DEFAULTS)).

-spec set_options(open_options()) -> {#inker_options{}, #penciller_options{}}.
%% @doc
%% Take the passed in property list of operations and extract out any relevant
%% options to the Inker or the Penciller
set_options(Opts) ->
    MaxJournalSize0 = 
        min(?ABSOLUTEMAX_JOURNALSIZE, 
            proplists:get_value(max_journalsize, Opts)),
    JournalSizeJitter = MaxJournalSize0 div (100 div ?JOURNAL_SIZE_JITTER),
    MaxJournalSize = 
        min(?ABSOLUTEMAX_JOURNALSIZE, 
                MaxJournalSize0 - erlang:phash2(self()) rem JournalSizeJitter),

    SyncStrat = proplists:get_value(sync_strategy, Opts),
    WRP = proplists:get_value(waste_retention_period, Opts),

    AltStrategy = proplists:get_value(reload_strategy, Opts),
    ReloadStrategy = leveled_codec:inker_reload_strategy(AltStrategy),

    PCLL0CacheSize = proplists:get_value(max_pencillercachesize, Opts),
    RootPath = proplists:get_value(root_path, Opts),

    JournalFP = RootPath ++ "/" ++ ?JOURNAL_FP,
    LedgerFP = RootPath ++ "/" ++ ?LEDGER_FP,
    ok = filelib:ensure_dir(JournalFP),
    ok = filelib:ensure_dir(LedgerFP),

    SFL_CompPerc = 
        proplists:get_value(singlefile_compactionpercentage, Opts),
    MRL_CompPerc = 
        proplists:get_value(maxrunlength_compactionpercentage, Opts),
    true = MRL_CompPerc >= SFL_CompPerc,
    true = 100.0 >= MRL_CompPerc,
    true = SFL_CompPerc >= 0.0,

    CompressionMethod = proplists:get_value(compression_method, Opts),
    CompressOnReceipt = 
        case proplists:get_value(compression_point, Opts) of 
            on_receipt ->
                % Note this will add measurable delay to PUT time
                % https://github.com/martinsumner/leveled/issues/95
                true;
            on_compact ->
                % If using lz4 this is not recommended
                false 
        end,

    {#inker_options{root_path = JournalFP,
                        reload_strategy = ReloadStrategy,
                        max_run_length = proplists:get_value(max_run_length, Opts),
                        singlefile_compactionperc = SFL_CompPerc,
                        maxrunlength_compactionperc = MRL_CompPerc,
                        waste_retention_period = WRP,
                        compression_method = CompressionMethod,
                        compress_on_receipt = CompressOnReceipt,
                        cdb_options = 
                            #cdb_options{max_size=MaxJournalSize,
                                            binary_mode=true,
                                            sync_strategy=SyncStrat}},
        #penciller_options{root_path = LedgerFP,
                            max_inmemory_tablesize = PCLL0CacheSize,
                            levelzero_cointoss = true,
                            compression_method = CompressionMethod}}.


-spec return_snapfun(book_state(), store|ledger, 
                        tuple()|no_lookup|undefined, 
                        boolean(), boolean()) -> fun().
%% @doc
%% Generates a function from which a snapshot can be created.  The primary
%% factor here is the SnapPreFold boolean.  If this is true then the snapshot
%% will be taken before the Fold function is returned.  If SnapPreFold is
%% false then the snapshot will be taken when the Fold function is called.
%%
%% SnapPrefold is to be used when the intention is to queue the fold, and so
%% claling of the fold may be delayed, but it is still desired that the fold
%% represent the point in time that the query was requested.
return_snapfun(State, SnapType, Query, LongRunning, SnapPreFold) ->
    case SnapPreFold of
        true ->
            {ok, LS, JS} = snapshot_store(State, SnapType, Query, LongRunning),
            fun() -> {ok, LS, JS} end;
        false ->
            Self = self(),
            % Timeout will be ignored, as will Requestor
            %
            % This uses the external snapshot - as the snapshot will need
            % to have consistent state between Bookie and Penciller when
            % it is made.
            fun() -> book_snapshot(Self, SnapType, Query, LongRunning) end
    end.

-spec snaptype_by_presence(boolean()) -> store|ledger.
%% @doc
%% Folds that traverse over object heads, may also either require to return 
%% the object,or at least confirm th eobject is present in the Ledger.  This
%% is achieved by enabling presence - and this will change the type of 
%% snapshot to one that covers the whole store (i.e. both ledger and journal),
%% rather than just the ledger.
snaptype_by_presence(true) ->
    store;
snaptype_by_presence(false) -> 
    ledger.

-spec get_runner(book_state(), tuple()) -> {async, fun()}.
%% @doc
%% Getan {async, Runner} for a given fold type.  Fold types have different 
%% tuple inputs
get_runner(State, {index_query, Constraint, FoldAccT, Range, TermHandling}) ->
    {IdxFld, StartT, EndT} = Range,
    {Bucket, ObjKey0} =
        case Constraint of
            {B, SK} ->
                {B, SK};
            B ->
                {B, null}
        end,
    StartKey = 
        leveled_codec:to_ledgerkey(Bucket, ObjKey0, ?IDX_TAG, IdxFld, StartT),
    EndKey = 
        leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, IdxFld, EndT),
    SnapFun = return_snapfun(State, ledger, {StartKey, EndKey}, false, false),
    leveled_runner:index_query(SnapFun, 
                                {StartKey, EndKey, TermHandling}, 
                                FoldAccT);
get_runner(State, {keylist, Tag, FoldAccT}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(SnapFun, Tag, null, FoldAccT);
get_runner(State, {keylist, Tag, Bucket, FoldAccT}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(SnapFun, Tag, Bucket, FoldAccT);
get_runner(State, {keylist, Tag, Bucket, KeyRange, FoldAccT}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(SnapFun, Tag, Bucket, KeyRange, FoldAccT);
%% Set of runners for object or metadata folds
get_runner(State, 
            {foldheads_allkeys, 
                Tag, FoldFun, 
                JournalCheck, SnapPreFold, SegmentList}) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, SnapPreFold),
    leveled_runner:foldheads_allkeys(SnapFun, 
                                        Tag, FoldFun, 
                                        JournalCheck, SegmentList);
get_runner(State,
            {foldobjects_allkeys, Tag, FoldFun, SnapPreFold}) ->
    get_runner(State, 
                {foldobjects_allkeys, Tag, FoldFun, SnapPreFold, key_order});
get_runner(State, 
            {foldobjects_allkeys, Tag, FoldFun, SnapPreFold, key_order}) ->
    SnapFun = return_snapfun(State, store, no_lookup, true, SnapPreFold),
    leveled_runner:foldobjects_allkeys(SnapFun, Tag, FoldFun, key_order);
get_runner(State,
            {foldobjects_allkeys, Tag, FoldFun, SnapPreFold, sqn_order}) ->
    SnapFun = return_snapfun(State, store, undefined, true, SnapPreFold),
    leveled_runner:foldobjects_allkeys(SnapFun, Tag, FoldFun, sqn_order);
get_runner(State,
            {foldheads_bybucket,
                Tag, 
                BucketList, bucket_list,
                FoldFun,
                JournalCheck, SnapPreFold, SegmentList}) ->
    KeyRangeFun = 
        fun(Bucket) ->
            {StartKey, EndKey, _} = return_ledger_keyrange(Tag, Bucket, all),
            {StartKey, EndKey}
        end,
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, SnapPreFold),
    leveled_runner:foldheads_bybucket(SnapFun,
                                        Tag,
                                        lists:map(KeyRangeFun, BucketList),
                                        FoldFun,
                                        JournalCheck, SegmentList);
get_runner(State,
            {foldheads_bybucket, 
                Tag, 
                Bucket, KeyRange, 
                FoldFun, 
                JournalCheck, SnapPreFold, SegmentList}) ->
    {StartKey, EndKey, SnapQ} = return_ledger_keyrange(Tag, Bucket, KeyRange),
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, SnapQ, true, SnapPreFold),
    leveled_runner:foldheads_bybucket(SnapFun, 
                                        Tag, 
                                        [{StartKey, EndKey}], 
                                        FoldFun, 
                                        JournalCheck, SegmentList);
get_runner(State,
            {foldobjects_bybucket, 
                Tag, Bucket, KeyRange, 
                FoldFun, 
                SnapPreFold}) ->
    {StartKey, EndKey, SnapQ} = return_ledger_keyrange(Tag, Bucket, KeyRange),
    SnapFun = return_snapfun(State, store, SnapQ, true, SnapPreFold),
    leveled_runner:foldobjects_bybucket(SnapFun, 
                                        Tag, 
                                        [{StartKey, EndKey}], 
                                        FoldFun);
get_runner(State, 
            {foldobjects_byindex,
                Tag, Bucket, {Field, FromTerm, ToTerm},
                FoldObjectsFun,
                SnapPreFold}) ->
    SnapFun = return_snapfun(State, store, no_lookup, true, SnapPreFold),
    leveled_runner:foldobjects_byindex(SnapFun, 
                                        {Tag, Bucket, Field, FromTerm, ToTerm},
                                        FoldObjectsFun);
get_runner(State, {bucket_list, Tag, FoldAccT}) ->
    {FoldBucketsFun, Acc} = FoldAccT,
    SnapFun = return_snapfun(State, ledger, no_lookup, false, false),
    leveled_runner:bucket_list(SnapFun, Tag, FoldBucketsFun, Acc);
get_runner(State, {first_bucket, Tag, FoldAccT}) ->
    {FoldBucketsFun, Acc} = FoldAccT,
    SnapFun = return_snapfun(State, ledger, no_lookup, false, false),
    leveled_runner:bucket_list(SnapFun, Tag, FoldBucketsFun, Acc, 1);
%% Set of specific runners, primarily used as exmaples for tests
get_runner(State, DeprecatedQuery) ->
    get_deprecatedrunner(State, DeprecatedQuery).


-spec get_deprecatedrunner(book_state(), tuple()) -> {async, fun()}.
%% @doc
%% Get an {async, Runner} for a given fold type.  Fold types have different 
%% tuple inputs.  These folds are currently used in tests, but are deprecated.
%% Most of these folds should be achievable through other available folds.
get_deprecatedrunner(State, {bucket_stats, Bucket}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucket_sizestats(SnapFun, Bucket, ?STD_TAG);
get_deprecatedrunner(State, {riakbucket_stats, Bucket}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucket_sizestats(SnapFun, Bucket, ?RIAK_TAG);
get_deprecatedrunner(State, {hashlist_query, Tag, JournalCheck}) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, true),    
    leveled_runner:hashlist_query(SnapFun, Tag, JournalCheck);
get_deprecatedrunner(State, 
                        {tictactree_obj,
                        {Tag, Bucket, StartK, EndK, JournalCheck},
                        TreeSize,
                        PartitionFilter}) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, true),  
    leveled_runner:tictactree(SnapFun, 
                                {Tag, Bucket, {StartK, EndK}}, 
                                JournalCheck,
                                TreeSize,
                                PartitionFilter);
get_deprecatedrunner(State, 
                        {tictactree_idx,
                        {Bucket, IdxField, StartK, EndK},
                        TreeSize,
                        PartitionFilter}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),  
    leveled_runner:tictactree(SnapFun, 
                                {?IDX_TAG, Bucket, {IdxField, StartK, EndK}}, 
                                false,
                                TreeSize,
                                PartitionFilter).


-spec return_ledger_keyrange(atom(), any(), tuple()|all) -> 
                                        {tuple(), tuple(), tuple()|no_lookup}.
%% @doc
%% Convert a range of binary keys into a ledger key range, returning 
%% {StartLK, EndLK, Query} where Query is  to indicate whether the query 
%% range is worth using to minimise the cost of the snapshot 
return_ledger_keyrange(Tag, Bucket, KeyRange) ->
    {StartKey, EndKey, Snap} =
        case KeyRange of 
            all -> 
                {leveled_codec:to_ledgerkey(Bucket, null, Tag),
                    leveled_codec:to_ledgerkey(Bucket, null, Tag),
                    false};
            {StartTerm, <<"$all">>} ->
                {leveled_codec:to_ledgerkey(Bucket, StartTerm, Tag),
                    leveled_codec:to_ledgerkey(Bucket, null, Tag),
                    false};
            {StartTerm, EndTerm} ->
                {leveled_codec:to_ledgerkey(Bucket, StartTerm, Tag),
                    leveled_codec:to_ledgerkey(Bucket, EndTerm, Tag),
                    true}
        end,
    SnapQuery = 
        case Snap of 
            true ->
                {StartKey, EndKey};
            false ->
                no_lookup
        end,
    {StartKey, EndKey, SnapQuery}.


-spec maybe_longrunning(erlang:timestamp(), atom()) -> ok.
%% @doc
%% Check the length of time an operation (named by Aspect) has taken, and 
%% see if it has crossed the long running threshold.  If so log to indicate 
%% a long running event has occurred.
maybe_longrunning(SW, Aspect) ->
    case timer:now_diff(os:timestamp(), SW) of
        N when N > ?LONG_RUNNING ->
            leveled_log:log("B0013", [N, Aspect]);
        _ ->
            ok
    end.

-spec readycache_forsnapshot(ledger_cache(), tuple()|no_lookup|undefined) 
                                                        -> ledger_cache().
%% @doc
%% Strip the ledger cach back to only the relevant informaiton needed in 
%% the query, and to make the cache a snapshot (and so not subject to changes
%% such as additions to the ets table)
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

-spec scan_table(ets:tab(), 
                    leveled_codec:ledger_key(), leveled_codec:ledger_key()) 
                        ->  {list(leveled_codec:ledger_kv()), 
                                non_neg_integer()|infinity, 
                                non_neg_integer()}.
%% @doc
%% Query the ETS table to find a range of keys (start inclusive).  Should also
%% return the miniumum and maximum sequence number found in the query.  This 
%% is just then used as a safety check when loading these results into the 
%% penciller snapshot
scan_table(Table, StartKey, EndKey) ->
    case ets:lookup(Table, StartKey) of
        [] ->
            scan_table(Table, StartKey, EndKey, [], infinity, 0);
        [{StartKey, StartVal}] ->
            SQN = leveled_codec:strip_to_seqonly({StartKey, StartVal}),
            scan_table(Table, StartKey, EndKey, 
                        [{StartKey, StartVal}], SQN, SQN)
    end.

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


-spec fetch_head(leveled_codec:ledger_key(), pid(), ledger_cache())
                                    -> not_present|leveled_codec:ledger_value().
%% @doc
%% Fetch only the head of the object from the Ledger (or the bookie's recent
%% ledger cache if it has just been updated).  not_present is returned if the 
%% Key is not found
fetch_head(Key, Penciller, LedgerCache) ->
    fetch_head(Key, Penciller, LedgerCache, false).

-spec fetch_head(leveled_codec:ledger_key(), pid(), ledger_cache(), boolean())
                                    -> not_present|leveled_codec:ledger_value().
%% doc
%% The L0Index needs to be bypassed when running head_only
fetch_head(Key, Penciller, LedgerCache, HeadOnly) ->
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
            Hash = leveled_codec:segment_hash(Key),
            UseL0Idx = not HeadOnly, 
                % don't use the L0Index in head only mode. Object specs don't 
                % get an addition on the L0 index
            case leveled_penciller:pcl_fetch(Penciller, Key, Hash, UseL0Idx) of
                {Key, Head} ->
                    maybe_longrunning(SW, pcl_head),
                    Head;
                not_present ->
                    maybe_longrunning(SW, pcl_head),
                    not_present
            end
    end.


-spec preparefor_ledgercache(leveled_codec:journal_key_tag()|null, 
                                leveled_codec:ledger_key()|?DUMMY,
                                integer(), any(), integer(), 
                                leveled_codec:journal_keychanges(), 
                                book_state())
                                    -> {integer()|no_lookup, 
                                            integer(), 
                                            list(leveled_codec:ledger_kv())}.
%% @doc
%% Prepare an object and its related key changes for addition to the Ledger 
%% via the Ledger Cache.
preparefor_ledgercache(?INKT_MPUT, 
                        ?DUMMY, SQN, _O, _S, {ObjSpecs, TTL}, 
                        _State) ->
    ObjChanges = leveled_codec:obj_objectspecs(ObjSpecs, SQN, TTL),
    {no_lookup, SQN, ObjChanges};
preparefor_ledgercache(?INKT_KEYD,
                        LedgerKey, SQN, _Obj, _Size, {IdxSpecs, TTL},
                        _State) ->
    {Bucket, Key} = leveled_codec:from_ledgerkey(LedgerKey),
    KeyChanges =
        leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL),
    {no_lookup, SQN, KeyChanges};
preparefor_ledgercache(_InkTag,
                        LedgerKey, SQN, Obj, Size, {IdxSpecs, TTL},
                        State) ->
    {Bucket, Key, MetaValue, {KeyH, ObjH}, LastMods} =
        leveled_codec:generate_ledgerkv(LedgerKey, SQN, Obj, Size, TTL),
    KeyChanges =
        [{LedgerKey, MetaValue}] ++
            leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL) ++
            leveled_codec:aae_indexspecs(State#state.recent_aae, 
                                            Bucket, Key, SQN, ObjH, LastMods),
    {KeyH, SQN, KeyChanges}.


-spec addto_ledgercache({integer()|no_lookup, 
                                integer(), 
                                list(leveled_codec:ledger_kv())}, 
                            ledger_cache()) 
                                    -> ledger_cache().
%% @doc
%% Add a set of changes associated with a single sequence number (journal 
%% update) and key to the ledger cache.  If the changes are not to be looked
%% up directly, then they will not be indexed to accelerate lookup
addto_ledgercache({H, SQN, KeyChanges}, Cache) ->
    ets:insert(Cache#ledger_cache.mem, KeyChanges),
    UpdIndex = leveled_pmem:prepare_for_index(Cache#ledger_cache.index, H),
    Cache#ledger_cache{index = UpdIndex,
                        min_sqn=min(SQN, Cache#ledger_cache.min_sqn),
                        max_sqn=max(SQN, Cache#ledger_cache.max_sqn)}.

-spec addto_ledgercache({integer()|no_lookup, 
                                integer(), 
                                list(leveled_codec:ledger_kv())}, 
                            ledger_cache(),
                            loader) 
                                    -> ledger_cache().
%% @doc
%% Add a set of changes associated witha single sequence number (journal 
%% update) to the ledger cache.  This is used explicitly when laoding the
%% ledger from the Journal (i.e. at startup) - and in this case the ETS insert
%% can be bypassed, as all changes will be flushed to the Penciller before the
%% load is complete.
addto_ledgercache({H, SQN, KeyChanges}, Cache, loader) ->
    UpdQ = KeyChanges ++ Cache#ledger_cache.load_queue,
    UpdIndex = leveled_pmem:prepare_for_index(Cache#ledger_cache.index, H),
    Cache#ledger_cache{index = UpdIndex,
                        load_queue = UpdQ,
                        min_sqn=min(SQN, Cache#ledger_cache.min_sqn),
                        max_sqn=max(SQN, Cache#ledger_cache.max_sqn)}.


-spec maybepush_ledgercache(integer(), ledger_cache(), pid()) 
                                            -> {ok|returned, ledger_cache()}.
%% @doc
%% Following an update to the ledger cache, check if this now big enough to be 
%% pushed down to the Penciller.  There is some random jittering here, to 
%% prevent coordination across leveled instances (e.g. when running in Riak).
%% 
%% The penciller may be too busy, as the LSM tree is backed up with merge
%% activity.  In this case the update is not made and 'returned' not ok is set
%% in the reply.  Try again later when it isn't busy (and also potentially 
%% implement a slow_offer state to slow down the pace at which PUTs are being
%% received)
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

-spec maybe_withjitter(integer(), integer()) -> boolean().
%% @doc
%% Push down randomly, but the closer to the maximum size, the more likely a 
%% push should be
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


-spec get_loadfun(book_state()) -> fun().
%% @doc
%% The LoadFun will be sued by the Inker when walking across the Journal to 
%% load the Penciller at startup
get_loadfun(State) ->
    PrepareFun =
        fun(Tag, PK, SQN, Obj, VS, IdxSpecs) ->
            preparefor_ledgercache(Tag, PK, SQN, Obj, VS, IdxSpecs, State)
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


delete_path(DirPath) ->
    ok = filelib:ensure_dir(DirPath),
    {ok, Files} = file:list_dir(DirPath),
    [file:delete(filename:join([DirPath, File])) || File <- Files],
    file:del_dir(DirPath).



%%%============================================================================
%%% Timing Functions
%%%============================================================================

-spec update_statetimings(timing_types(), 
                    put_timings()|get_timings()|fold_timings()|head_timings(), 
                    integer()) 
                    -> 
                    {put_timings()|get_timings()|fold_timings()|head_timings(), 
                    integer()}.
%% @doc
%%
%% The timings state is either in countdown to the next set of samples of
%% we are actively collecting a sample.  Active collection take place 
%% when the countdown is 0.  Once the sample has reached the expected count
%% then there is a log of that sample, and the countdown is restarted.
%%
%% Outside of sample windows the timings object should be set to the atom
%% no_timing.  no_timing is a valid state for each timings type.
update_statetimings(head, no_timing, 0) ->
    {#head_timings{}, 0};
update_statetimings(put, no_timing, 0) ->
    {#put_timings{}, 0};
update_statetimings(get, no_timing, 0) ->
    {#get_timings{}, 0};
update_statetimings(fold, no_timing, 0) ->
    {#fold_timings{}, 0};
update_statetimings(head, Timings, 0) ->
    case Timings#head_timings.sample_count of 
        SC when SC >= ?TIMING_SAMPLESIZE ->
            log_timings(head, Timings),
            {no_timing, leveled_rand:uniform(10 * ?TIMING_SAMPLECOUNTDOWN)};
        _SC ->
            {Timings, 0}
    end;
update_statetimings(put, Timings, 0) ->
    case Timings#put_timings.sample_count of 
        SC when SC >= ?TIMING_SAMPLESIZE ->
            log_timings(put, Timings),
            {no_timing, leveled_rand:uniform(2 * ?TIMING_SAMPLECOUNTDOWN)};
        _SC ->
            {Timings, 0}
    end;
update_statetimings(get, Timings, 0) ->
    case Timings#get_timings.sample_count of 
        SC when SC >= ?TIMING_SAMPLESIZE ->
            log_timings(get, Timings),
            {no_timing, leveled_rand:uniform(2 * ?TIMING_SAMPLECOUNTDOWN)};
        _SC ->
            {Timings, 0}
    end;
update_statetimings(fold, Timings, 0) ->
    case Timings#fold_timings.sample_count of 
        SC when SC >= (?TIMING_SAMPLESIZE div 10) ->
            log_timings(fold, Timings),
            {no_timing, 
                leveled_rand:uniform(2 * (?TIMING_SAMPLECOUNTDOWN div 10))};
        _SC ->
            {Timings, 0}
    end;
update_statetimings(_, no_timing, N) ->
    {no_timing, N - 1}.

log_timings(head, Timings) ->
    leveled_log:log("B0018", 
                        [Timings#head_timings.sample_count,
                            Timings#head_timings.pcl_time,
                            Timings#head_timings.buildhead_time]);
log_timings(put, Timings) ->
    leveled_log:log("B0015", [Timings#put_timings.sample_count, 
                                Timings#put_timings.mem_time,
                                Timings#put_timings.ink_time,
                                Timings#put_timings.total_size]);
log_timings(get, Timings) ->
    leveled_log:log("B0016", [Timings#get_timings.sample_count, 
                                Timings#get_timings.head_time,
                                Timings#get_timings.body_time,
                                Timings#get_timings.fetch_count]);
log_timings(fold, Timings) ->    
    leveled_log:log("B0017", [Timings#fold_timings.sample_count, 
                                Timings#fold_timings.setup_time]).


update_timings(_SW, _Stage, no_timing) ->
    {no_timing, no_timing};
update_timings(SW, {head, Stage}, Timings) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    Timings0 = 
        case Stage of 
            pcl ->
                PCT = Timings#head_timings.pcl_time + Timer,
                Timings#head_timings{pcl_time = PCT};
            rsp ->
                BHT = Timings#head_timings.buildhead_time + Timer,
                CNT = Timings#head_timings.sample_count + 1,
                Timings#head_timings{buildhead_time = BHT, sample_count = CNT}
        end,
    {os:timestamp(), Timings0};
update_timings(SW, {put, Stage}, Timings) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    Timings0 = 
        case Stage of 
            {inker, ObjectSize} ->
                INT = Timings#put_timings.ink_time + Timer,
                TSZ = Timings#put_timings.total_size + ObjectSize,
                Timings#put_timings{ink_time = INT, total_size = TSZ};
            mem ->
                PCT = Timings#put_timings.mem_time + Timer,
                CNT = Timings#put_timings.sample_count + 1,
                Timings#put_timings{mem_time = PCT, sample_count = CNT}
        end,
    {os:timestamp(), Timings0};
update_timings(SW, {get, head}, Timings) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    GHT = Timings#get_timings.head_time + Timer,
    CNT = Timings#get_timings.sample_count + 1,
    Timings0 = Timings#get_timings{head_time = GHT, sample_count = CNT},
    {os:timestamp(), Timings0};
update_timings(SW, {get, body}, Timings) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    GBT = Timings#get_timings.body_time + Timer,
    FCNT = Timings#get_timings.fetch_count + 1,
    Timings0 = Timings#get_timings{body_time = GBT, fetch_count = FCNT},
    {no_timing, Timings0};
update_timings(SW, {fold, setup}, Timings) ->
    Timer = timer:now_diff(os:timestamp(), SW),
    FST = Timings#fold_timings.setup_time + Timer,
    CNT = Timings#fold_timings.sample_count + 1,
    Timings0 = Timings#fold_timings{setup_time = FST, sample_count = CNT},
    {no_timing, Timings0}.

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
    Future = leveled_util:integer_now() + 300,
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
    Past = leveled_util:integer_now() - 300,
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

hashlist_query_test_() ->
    {timeout, 60, fun hashlist_query_testto/0}.

hashlist_query_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                {max_journalsize, 1000000},
                                {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(1200, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    lists:foreach(fun({K, V, S}) -> ok = book_tempput(Bookie1,
                                                        "Bucket", K, V, S,
                                                        ?STD_TAG,
                                                        Future) end,
                    ObjL1),
    ObjL2 = generate_multiple_objects(20, 1201),
    % Put in a few objects with a TTL in the past
    Past = leveled_util:integer_now() - 300,
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


hashlist_query_withjournalcheck_test_() ->
    {timeout, 60, fun hashlist_query_withjournalcheck_testto/0}.

hashlist_query_withjournalcheck_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(800, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
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
                                                    true}),
    ?assertMatch(KeyHashList, HTFolder2()),
    ok = book_close(Bookie1),
    reset_filestructure().

foldobjects_vs_hashtree_test_() ->
    {timeout, 60, fun foldobjects_vs_hashtree_testto/0}.

foldobjects_vs_hashtree_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ObjL1 = generate_multiple_objects(800, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
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
    {async, HTFolder2} = book_returnfolder(Bookie1,
                                            {foldobjects_allkeys, 
                                                ?STD_TAG, 
                                                FoldObjectsFun, 
                                                true}),
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
                            {foldheads_allkeys, 
                                ?STD_TAG, 
                                FoldHeadsFun, 
                                true, true, false}),
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
                            {foldheads_allkeys, 
                                ?STD_TAG, 
                                FoldHeadsFun2,
                                false, false, false}),
    KeyHashList4 = HTFolder4(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList4)),

    ok = book_close(Bookie1),
    reset_filestructure().

foldobjects_vs_foldheads_bybucket_test_() ->
    {timeout, 60, fun foldobjects_vs_foldheads_bybucket_testto/0}.

foldobjects_vs_foldheads_bybucket_testto() ->
    folder_cache_test(10),
    folder_cache_test(100),
    folder_cache_test(300),
    folder_cache_test(1000).

folder_cache_test(CacheSize) ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, CacheSize}]),
    ObjL1 = generate_multiple_objects(400, 1),
    ObjL2 = generate_multiple_objects(400, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
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
                                all,
                                FoldObjectsFun,
                                false}),
    KeyHashList1A = HTFolder1A(),
    {async, HTFolder1B} =
        book_returnfolder(Bookie1,
                            {foldobjects_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                all,
                                FoldObjectsFun,
                                true}),
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
                                all,
                                FoldHeadsFun,
                                true, true, false}),
    KeyHashList2A = HTFolder2A(),
    {async, HTFolder2B} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                all,
                                FoldHeadsFun,
                                true, false, false}),
    KeyHashList2B = HTFolder2B(),
    
    ?assertMatch(true,
                    lists:usort(KeyHashList1A) == lists:usort(KeyHashList2A)),
    ?assertMatch(true,
                    lists:usort(KeyHashList1B) == lists:usort(KeyHashList2B)),

    {async, HTFolder2C} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                {"Key", <<"$all">>},
                                FoldHeadsFun,
                                true, false, false}),
    KeyHashList2C = HTFolder2C(),
    {async, HTFolder2D} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                {"Key", "Keyzzzzz"},
                                FoldHeadsFun,
                                true, true, false}),
    KeyHashList2D = HTFolder2D(),
    ?assertMatch(true,
                    lists:usort(KeyHashList2B) == lists:usort(KeyHashList2C)),
    ?assertMatch(true,
                    lists:usort(KeyHashList2B) == lists:usort(KeyHashList2D)),
    

    CheckSplitQueryFun = 
        fun(SplitInt) ->
            io:format("Testing SplitInt ~w~n", [SplitInt]),
            SplitIntEnd = "Key" ++ integer_to_list(SplitInt) ++ "|",
            SplitIntStart = "Key" ++ integer_to_list(SplitInt + 1),
            {async, HTFolder2E} =
                book_returnfolder(Bookie1,
                                    {foldheads_bybucket,
                                        ?STD_TAG,
                                        "BucketB",
                                        {"Key", SplitIntEnd},
                                        FoldHeadsFun,
                                        true, false, false}),
            KeyHashList2E = HTFolder2E(),
            {async, HTFolder2F} =
                book_returnfolder(Bookie1,
                                    {foldheads_bybucket,
                                        ?STD_TAG,
                                        "BucketB",
                                        {SplitIntStart, "Key|"},
                                        FoldHeadsFun,
                                        true, false, false}),
            KeyHashList2F = HTFolder2F(),

            ?assertMatch(true, length(KeyHashList2E) > 0),
            ?assertMatch(true, length(KeyHashList2F) > 0),
            io:format("Length of 2B ~w 2E ~w 2F ~w~n", 
                        [length(KeyHashList2B), 
                            length(KeyHashList2E), 
                            length(KeyHashList2F)]),
            CompareL = lists:usort(KeyHashList2E ++ KeyHashList2F),
            ?assertMatch(true, lists:usort(KeyHashList2B) == CompareL)
        end,
    
    lists:foreach(CheckSplitQueryFun, [1, 4, 8, 300, 100, 400, 200, 600]),

    ok = book_close(Bookie1),
    reset_filestructure().

is_empty_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    % Put in an object with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    ?assertMatch(true, leveled_bookie:book_isempty(Bookie1, ?STD_TAG)),
    ok = book_tempput(Bookie1, 
                        <<"B">>, <<"K">>, {value, <<"V">>}, [], 
                        ?STD_TAG, Future),
    ?assertMatch(false, leveled_bookie:book_isempty(Bookie1, ?STD_TAG)),
    ?assertMatch(true, leveled_bookie:book_isempty(Bookie1, ?RIAK_TAG)),

    ok = leveled_bookie:book_close(Bookie1).

is_empty_headonly_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500},
                                    {head_only, no_lookup}]),
    ?assertMatch(true, book_isempty(Bookie1, ?HEAD_TAG)),
    ObjSpecs = 
        [{add, <<"B1">>, <<"K1">>, <<1:8/integer>>, 100},
            {remove, <<"B1">>, <<"K1">>, <<0:8/integer>>, null}],
    ok = book_mput(Bookie1, ObjSpecs),
    ?assertMatch(false, book_isempty(Bookie1, ?HEAD_TAG)),
    ok = book_close(Bookie1).


foldkeys_headonly_test() ->
    foldkeys_headonly_tester(5000, 25, "BucketStr"),
    foldkeys_headonly_tester(2000, 25, <<"B0">>).


foldkeys_headonly_tester(ObjectCount, BlockSize, BStr) ->
    RootPath = reset_filestructure(),
    
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500},
                                    {head_only, no_lookup}]),
    GenObjSpecFun =
        fun(I) ->
            Key = I rem 6,
            {add, BStr, <<Key:8/integer>>, integer_to_list(I), I}
        end,
    ObjSpecs = lists:map(GenObjSpecFun, lists:seq(1, ObjectCount)),
    ObjSpecBlocks = 
        lists:map(fun(I) ->
                        lists:sublist(ObjSpecs, I * BlockSize + 1, BlockSize)
                    end,
                    lists:seq(0, ObjectCount div BlockSize - 1)),
    lists:map(fun(Block) -> book_mput(Bookie1, Block) end, ObjSpecBlocks),
    ?assertMatch(false, book_isempty(Bookie1, ?HEAD_TAG)),
    
    FolderT = 
        {keylist, 
            ?HEAD_TAG, BStr, 
            {fun(_B, {K, SK}, Acc) -> [{K, SK}|Acc] end, []}
        },
    {async, Folder1} = book_returnfolder(Bookie1, FolderT),
    Key_SKL1 = lists:reverse(Folder1()),
    Key_SKL_Compare = 
        lists:usort(lists:map(fun({add, _B, K, SK, _V}) -> {K, SK} end, ObjSpecs)),
    ?assertMatch(Key_SKL_Compare, Key_SKL1),

    ok = book_close(Bookie1),
    
    {ok, Bookie2} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500},
                                    {head_only, no_lookup}]),
    {async, Folder2} = book_returnfolder(Bookie2, FolderT),
    Key_SKL2 = lists:reverse(Folder2()),
    ?assertMatch(Key_SKL_Compare, Key_SKL2),

    ok = book_close(Bookie2).


is_empty_stringkey_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ?assertMatch(true, book_isempty(Bookie1, ?STD_TAG)),
    Past = leveled_util:integer_now() - 300,
    ?assertMatch(true, leveled_bookie:book_isempty(Bookie1, ?STD_TAG)),
    ok = book_tempput(Bookie1, 
                        "B", "K", {value, <<"V">>}, [], 
                        ?STD_TAG, Past),
    ok = book_put(Bookie1, 
                    "B", "K0", {value, <<"V">>}, [], 
                    ?STD_TAG),
    ?assertMatch(false, book_isempty(Bookie1, ?STD_TAG)),
    ok = book_close(Bookie1).

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
