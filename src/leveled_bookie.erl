%% -------- Overview ---------
%%
%% Leveled is based on the LSM-tree similar to leveldb, except that:
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
%% - The ledger is a merge tree, where the key is the actual object key, and
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
        book_plainstart/1,
        book_put/5,
        book_put/6,
        book_put/8,
        book_tempput/7,
        book_mput/2,
        book_mput/3,
        book_delete/4,
        book_get/3,
        book_get/4,
        book_head/3,
        book_head/4,
        book_sqn/3,
        book_sqn/4,
        book_headonly/4,
        book_snapshot/4,
        book_compactjournal/2,
        book_islastcompactionpending/1,
        book_trimjournal/1,
        book_hotbackup/1,
        book_close/1,
        book_destroy/1,
        book_isempty/2,
        book_logsettings/1,
        book_loglevel/2,
        book_addlogs/2,
        book_removelogs/2,
        book_headstatus/1
    ]).

%% folding API
-export([
         book_returnfolder/2,
         book_indexfold/5,
         book_bucketlist/4,
         book_keylist/3,
         book_keylist/4,
         book_keylist/5,
         book_keylist/6,
         book_objectfold/4,
         book_objectfold/5,
         book_objectfold/6,
         book_headfold/6,
         book_headfold/7,
         book_headfold/9
        ]).

-export([empty_ledgercache/0,
            snapshot_store/7,
            fetch_value/2,
            journal_notfound/4]).

-ifdef(TEST).
-export([book_returnactors/1]).
-endif.

-include_lib("eunit/include/eunit.hrl").

-define(DUMMY, dummy). % Dummy key used for mput operations

-define(OPTION_DEFAULTS,
            [{root_path, undefined},
                {snapshot_bookie, undefined},
                {cache_size, ?CACHE_SIZE},
                {cache_multiple, ?MAX_CACHE_MULTTIPLE},
                {max_journalsize, 1000000000},
                {max_journalobjectcount, 200000},
                {max_sstslots, 256},
                {sync_strategy, ?DEFAULT_SYNC_STRATEGY},
                {head_only, false},
                {waste_retention_period, undefined},
                {max_run_length, undefined},
                {singlefile_compactionpercentage, 30.0},
                {maxrunlength_compactionpercentage, 70.0},
                {journalcompaction_scoreonein, 1},
                {reload_strategy, []},
                {max_pencillercachesize, ?MAX_PCL_CACHE_SIZE},
                {ledger_preloadpagecache_level, ?SST_PAGECACHELEVEL_LOOKUP},
                {compression_method, ?COMPRESSION_METHOD},
                {compression_point, ?COMPRESSION_POINT},
                {compression_level, ?COMPRESSION_LEVEL},
                {log_level, ?LOG_LEVEL},
                {forced_logs, []},
                {database_id, ?DEFAULT_DBID},
                {override_functions, []},
                {snapshot_timeout_short, ?SNAPTIMEOUT_SHORT},
                {snapshot_timeout_long, ?SNAPTIMEOUT_LONG},
                {stats_percentage, ?DEFAULT_STATS_PERC},
                {stats_logfrequency,
                    element(1, leveled_monitor:get_defaults())},
                {monitor_loglist,
                    element(2, leveled_monitor:get_defaults())}]).

-record(ledger_cache, {mem :: ets:tab(),
                        loader = leveled_tree:empty(?CACHE_TYPE)
                                    :: tuple()|empty_cache,
                        load_queue = [] :: list(),
                        index = leveled_pmem:new_index(),
                        min_sqn = infinity :: integer()|infinity,
                        max_sqn = 0 :: integer()}).

-record(state, {inker :: pid() | undefined,
                penciller :: pid() | undefined,
                cache_size :: pos_integer() | undefined,
                cache_multiple :: pos_integer() | undefined,
                ledger_cache = #ledger_cache{} :: ledger_cache(),
                is_snapshot :: boolean() | undefined,
                slow_offer = false :: boolean(),
                head_only = false :: boolean(),
                head_lookup = true :: boolean(),
                ink_checking = ?MAX_KEYCHECK_FREQUENCY :: integer(),
                bookie_monref :: reference() | undefined,
                monitor = {no_monitor, 0} :: leveled_monitor:monitor()}).


-type book_state() :: #state{}.
-type sync_mode() :: sync|none|riak_sync.
-type ledger_cache() :: #ledger_cache{}.


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
            % configured values)
            % The minimum value is 100 - any lower value will be ignored
        {cache_multiple, pos_integer()} |
            % A multiple of the cache size beyond which the cache should not
            % grow even if the penciller is busy.  A pasue will be returned for
            % every PUT when this multiple of the cache_size is reached
        {max_journalsize, pos_integer()} |
            % The maximum size of a journal file in bytes.  The absolute 
            % maximum must be 4GB due to 4 byte file pointers being used
        {max_journalobjectcount, pos_integer()} |
            % The maximum size of the journal by count of the objects.  The
            % journal must remain within the limit set by both this figures and
            % the max_journalsize
        {max_sstslots, pos_integer()} |
            % The maximum number of slots in a SST file.  All testing is done
            % at a size of 256 (except for Quickcheck tests}, altering this
            % value is not recommended
        {sync_strategy, sync_mode()} |
            % Should be sync if it is necessary to flush to disk after every
            % write, or none if not (allow the OS to schecdule).  This has a
            % significant impact on performance which can be mitigated 
            % partially in hardware (e.g through use of FBWC).
            % riak_sync is used for backwards compatability with OTP16 - and 
            % will manually call sync() after each write (rather than use the
            % O_SYNC option on startup)
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
        {journalcompaction_scoreonein, pos_integer()} |
            % When scoring for compaction run a probability (1 in x) of whether
            % any file will be scored this run.  If not scored a cached score
            % will be used, and the cached score is the average of the latest
            % score and the rolling average of previous scores
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
            % There is a third strategy, which is recalc, where on reloading 
            % the Ledger from the Journal, the key changes are recalculated by
            % comparing the extracted metadata from the Journal object, with the
            % extracted metadata from the current Ledger object it is set to
            % replace (should one be present).  Implementing the recalc
            % strategy requires a override function for 
            % `leveled_head:diff_indexspecs/3`.
            % A function for the ?RIAK_TAG is provided and tested.
            %
            % reload_strategy options are a list - to map from a tag to the
            % strategy (recovr|retain|recalc).  Defualt strategies are:
            % [{?RIAK_TAG, retain}, {?STD_TAG, retain}]
        {max_pencillercachesize, pos_integer()|undefined} |
            % How many ledger keys should the penciller retain in memory
            % between flushing new level zero files.
            % Defaults to ?MAX_PCL_CACHE_SIZE when undefined
            % The minimum size 400 - attempt to set this vlaue lower will be 
            % ignored.  As a rule the value should be at least 4 x the Bookie's
            % cache size
        {ledger_preloadpagecache_level, pos_integer()} |
            % To which level of the ledger should the ledger contents be
            % pre-loaded into the pagecache (using fadvise on creation and
            % startup)
        {compression_method, native|lz4|none} |
            % Compression method and point allow Leveled to be switched from
            % using bif based compression (zlib) to using nif based compression
            % (lz4).  To disable compression use none.  This will disable in
            % the ledger as well as the journla (both on_receipt and
            % on_compact).
            % Defaults to ?COMPRESSION_METHOD
        {compression_point, on_compact|on_receipt} |
            % The =compression point can be changed between on_receipt (all
            % values are compressed as they are received), to on_compact where
            % values are originally stored uncompressed (speeding PUT times),
            % and are only compressed when they are first subject to compaction
            % Defaults to ?COMPRESSION_POINT
        {compression_level, 0..7} |
            % At what level of the LSM tree in the ledger should compression be
            % enabled.
            % Defaults to ?COMPRESSION_LEVEL
        {log_level, debug|info|warn|error|critical} |
            % Set the log level.  The default log_level of info is noisy - the
            % current implementation was targetted at environments that have
            % facilities to index large proportions of logs and allow for
            % dynamic querying of those indexes to output relevant stats.
            %
            % As an alternative a higher log_level can be used to reduce this
            % 'noise', however, there is currently no separate stats facility
            % to gather relevant information outside of info level logs.  So
            % moving to higher log levels will at present make the operator
            % blind to sample performance statistics of leveled sub-components
            % etc
        {forced_logs, list(atom())} |
            % Forced logs allow for specific info level logs, such as those
            % logging stats to be logged even when the default log level has
            % been set to a higher log level.  Using:
            % {forced_logs, 
            %   [b0015, b0016, b0017, b0018, p0032, sst12]}
            % Will log all timing points even when log_level is not set to
            % support info
        {database_id, non_neg_integer()} |
            % Integer database ID to be used in logs
        {override_functions, list(leveled_head:appdefinable_function_tuple())} |
            % Provide a list of override functions that will be used for
            % user-defined tags
        {snapshot_timeout_short, pos_integer()} |
            % Time in seconds before a snapshot that has not been shutdown is
            % assumed to have failed, and so requires to be torndown.  The
            % short timeout is applied to queries where long_running is set to
            % false
        {snapshot_timeout_long, pos_integer()} |
            % Time in seconds before a snapshot that has not been shutdown is
            % assumed to have failed, and so requires to be torndown.  The
            % short timeout is applied to queries where long_running is set to
            % true
        {stats_percentage, 0..100} |
            % Probability that stats will be collected for an individual
            % request.
        {stats_logfrequency, pos_integer()} |
            % Time in seconds before logging the next timing log. This covers
            % the logs associated with the timing of GET/PUTs in various parts
            % of the system.  There are 7 such logs - so setting to 30s will
            % mean that each inidividual log will occur every 210s
        {monitor_loglist, list(leveled_monitor:log_type())}
        ].

-type load_item() ::
    {leveled_codec:journal_key_tag()|null, 
        leveled_codec:ledger_key()|?DUMMY,
        non_neg_integer(), any(), integer(), 
        leveled_codec:journal_keychanges()}.

-type initial_loadfun() ::
    fun((leveled_codec:journal_key(),
            any(),
            non_neg_integer(),
            {non_neg_integer(), non_neg_integer(), ledger_cache()},
            fun((any()) -> {binary(), non_neg_integer()})) ->
                {loop|stop, list(load_item())}).

-export_type([initial_loadfun/0, ledger_cache/0]).

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


-spec book_plainstart(list(tuple())) -> {ok, pid()}.

%% @doc
%% Start used in tests to start without linking
book_plainstart(Opts) ->
    gen_server:start(?MODULE, [set_defaults(Opts)], []).


-spec book_tempput(pid(), leveled_codec:key(), leveled_codec:key(), any(), 
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
%% - A tag indicating the type of object.  Behaviour for metadata extraction,
%% and ledger compaction will vary by type.  There are three currently
%% implemented types i (Index), o (Standard), o_rkv (Riak).  Keys added with
%% Index tags are not fetchable (as they will not be hashed), but are
%% extractable via range query.
%%
%% The extended-arity book_put functions support the addition of an object
%% TTL and a `sync` boolean to flush this PUT (and any other buffered PUTs to
%% disk when the sync_stategy is `none`.
%%
%% The Bookie takes the request and passes it first to the Inker to add the
%% request to the journal.
%%
%% The inker will pass the PK/Value/IndexSpecs to the current (append only)
%% CDB journal file to persist the change.  The call should return either 'ok'
%% or 'roll'. 'roll' indicates that the CDB file has insufficient capacity for
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

-spec book_put(pid(), leveled_codec:key(), leveled_codec:key(), any(), 
                leveled_codec:index_specs(), 
                leveled_codec:tag(), infinity|integer()) -> ok|pause.

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL) when is_atom(Tag) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, false).

-spec book_put(pid(), leveled_codec:key(), leveled_codec:key(), any(), 
                leveled_codec:index_specs(), 
                leveled_codec:tag(), infinity|integer(),
                boolean()) -> ok|pause.
book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    gen_server:call(Pid,
                    {put, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync},
                    infinity).


-spec book_mput(pid(), list(leveled_codec:object_spec())) -> ok|pause.
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

-spec book_mput(pid(), list(leveled_codec:object_spec()), infinity|integer())
                                                                -> ok|pause.
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

-spec book_delete(pid(), 
                    leveled_codec:key(), leveled_codec:key(), 
                    leveled_codec:index_specs()) -> ok|pause.

%% @doc 
%%
%% A thin wrap around the put of a special tombstone object.  There is no
%% immediate reclaim of space, simply the addition of a more recent tombstone.

book_delete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?STD_TAG).


-spec book_get(pid(), 
                leveled_codec:key(), leveled_codec:key(), leveled_codec:tag())
                                                    -> {ok, any()}|not_found.
-spec book_head(pid(), 
                leveled_codec:key(), leveled_codec:key(), leveled_codec:tag())
                                                    -> {ok, any()}|not_found.

-spec book_sqn(pid(), 
                leveled_codec:key(), leveled_codec:key(), leveled_codec:tag())
                                        -> {ok, non_neg_integer()}|not_found.

-spec book_headonly(pid(), 
                    leveled_codec:key(), leveled_codec:key(), leveled_codec:key())
                                                    -> {ok, any()}|not_found.

%% @doc - GET and HEAD requests
%%
%% The Bookie supports both GET and HEAD requests, with the HEAD request
%% returning only the metadata and not the actual object value.  The HEAD
%% requets cna be serviced by reference to the Ledger Cache and the Penciller.
%%
%% GET requests first follow the path of a HEAD request, and if an object is
%% found, then fetch the value from the Journal via the Inker.
%% 
%% to perform a head request in head_only mode with_lookup, book_headonly/4 
%% should be used.  Not if head_only mode is false or no_lookup, then this
%% request would not be supported


book_get(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {get, Bucket, Key, Tag}, infinity).

book_head(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag, false}, infinity).

book_get(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?STD_TAG).

book_head(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?STD_TAG).

book_headonly(Pid, Bucket, Key, SubKey) ->
    gen_server:call(Pid,
                    {head, Bucket, {Key, SubKey}, ?HEAD_TAG, false},
                    infinity).


book_sqn(Pid, Bucket, Key) ->
    book_sqn(Pid, Bucket, Key, ?STD_TAG).

book_sqn(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag, true}, infinity).

-spec book_returnfolder(pid(), tuple()) -> {async, fun(() -> term())}.

%% @doc Folds over store - deprecated
%% The tuple() is a query, and book_returnfolder will return an {async, Folder}
%% whereby calling Folder() will run a particular fold over a snapshot of the
%% store, and close the snapshot when complete
%%
%% For any new application requiring a fold - use the API below instead, and
%% one of:
%% - book_indexfold
%% - book_bucketlist
%% - book_keylist
%% - book_headfold
%% - book_objectfold

book_returnfolder(Pid, RunnerType) ->
    gen_server:call(Pid, {return_runner, RunnerType}, infinity).

%% Different runner types for async queries:
%% - book_indexfold
%% - book_bucketlist
%% - book_keylist
%% - book_headfold
%% - book_objectfold
%%
%% See individual instructions for each one.  All folds can be completed early
%% by using a fold_function that throws an exception when some threshold is
%% reached - and a worker that catches that exception.
%%
%% See test/end_to_end/iterator_SUITE:breaking_folds/1

%% @doc Builds and returns an `{async, Runner}' pair for secondary
%% index queries. Calling `Runner' will fold over keys (ledger) tagged
%% with the index `?IDX_TAG' and Constrain the fold to a specific
%% `Bucket''s index fields, as specified by the `Constraint'
%% argument. If `Constraint' is a tuple of `{Bucket, Key}' the fold
%% starts at `Key', meaning any keys lower than `Key' and which match
%% the start of the range query, will not be folded over (this is
%% useful for implementing pagination, for example.)
%%
%% Provide a `FoldAccT' tuple of fold fun ( which is 3 arity fun that
%% will be called once per-matching index entry, with the Bucket,
%% Primary Key (or {IndexVal and Primary key} if `ReturnTerms' is
%% true)) and an initial Accumulator, which will be passed as the 3rd
%% argument in the initial call to FoldFun. Subsequent calls to
%% FoldFun will use the previous return of FoldFun as the 3rd
%% argument, and the final return of `Runner' is the final return of
%% `FoldFun', the final Accumulator value. The query can filter inputs
%% based on `Range' and `TermHandling'.  `Range' specifies the name of
%% `IndexField' to query, and `Start' and `End' optionally provide the
%% range to query over.  `TermHandling' is a 2-tuple, the first
%% element is a `boolean()', `true' meaning return terms, (see fold
%% fun above), `false' meaning just return primary keys. `TermRegex'
%% is either a regular expression of type `re:mp()' (that will be run
%% against each index term value, and only those that match will be
%% accumulated) or `undefined', which means no regular expression
%% filtering of index values. NOTE: Regular Expressions can ONLY be
%% run on indexes that have binary or string values, NOT integer
%% values. In the Riak sense of secondary indexes, there are two types
%% of indexes `_bin' indexes and `_int' indexes. Term regex may only
%% be run against the `_bin' type.
%%
%% Any book_indexfold query will fold over the snapshot under the control
%% of the worker process controlling the function - and that process can 
%% be interrupted by a throw, which will be forwarded to the worker (whilst
%% still closing down the snapshot).  This may be used, for example, to
%% curtail a fold in the application at max_results
-spec book_indexfold(pid(),
                     Constraint:: {Bucket, StartKey},
                     FoldAccT :: {FoldFun, Acc},
                     Range :: {IndexField, Start, End},
                     TermHandling :: {ReturnTerms, TermRegex}) ->
                            {async, Runner::fun(() -> term())}
                                when Bucket::term(),
                                     StartKey::term(),
                                     FoldFun::fun((Bucket, Key | {IndexVal, Key}, Acc) -> Acc),
                                     Acc::term(),
                                     IndexField::term(),
                                     IndexVal::term(),
                                     Start::IndexVal,
                                     End::IndexVal,
                                     ReturnTerms::boolean(),
                                     TermRegex :: leveled_codec:regular_expression().

book_indexfold(Pid, Constraint, FoldAccT, Range, TermHandling)
                                                when is_tuple(Constraint) ->
    RunnerType = 
        {index_query, Constraint, FoldAccT, Range, TermHandling},
    book_returnfolder(Pid, RunnerType);
book_indexfold(Pid, Bucket, FoldAccT, Range, TermHandling) ->
    % StartKey must be specified to avoid confusion when bucket is a tuple.
    % Use an empty StartKey if no StartKey is required (e.g. <<>>).  In a
    % future release this code branch may be removed, and such queries may
    % instead return `error`.  For now null is assumed to be lower than any
    % key
    leveled_log:log(b0019, [Bucket]),
    book_indexfold(Pid, {Bucket, null}, FoldAccT, Range, TermHandling).


%% @doc list buckets. Folds over the ledger only. Given a `Tag' folds
%% over the keyspace calling `FoldFun' from `FoldAccT' for each
%% `Bucket'. `FoldFun' is a 2-arity function that is passed `Bucket'
%% and `Acc'. On first call `Acc' is the initial `Acc' from
%% `FoldAccT', thereafter the result of the previous call to
%% `FoldFun'. `Constraint' can be either atom `all' or `first' meaning
%% return all buckets, or just the first one found. Returns `{async,
%% Runner}' where `Runner' is a fun that returns the final value of
%% `FoldFun', the final `Acc' accumulator.
-spec book_bucketlist(pid(), Tag, FoldAccT, Constraint) ->
                             {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Acc) -> Acc),
      Acc :: term(),
      Constraint :: first | all,
      Bucket :: term(),
      Acc :: term(),
      Runner :: fun(() -> Acc).
book_bucketlist(Pid, Tag, FoldAccT, Constraint) ->
    RunnerType=
        case Constraint of
            first-> {first_bucket, Tag, FoldAccT};
            all -> {bucket_list, Tag, FoldAccT}
        end,
    book_returnfolder(Pid, RunnerType).


%% @doc fold over the keys (ledger only) for a given `Tag'. Each key
%% will result in a call to `FoldFun' from `FoldAccT'. `FoldFun' is a
%% 3-arity function, called with `Bucket', `Key' and `Acc'. The
%% initial value of `Acc' is the second element of `FoldAccT'. Returns
%% `{async, Runner}' where `Runner' is a function that will run the
%% fold and return the final value of `Acc'
%%
%% Any book_keylist query will fold over the snapshot under the control
%% of the worker process controlling the function - and that process can 
%% be interrupted by a throw, which will be forwarded to the worker (whilst
%% still closing down the snapshot).  This may be used, for example, to
%% curtail a fold in the application at max_results
-spec book_keylist(pid(), Tag, FoldAccT) -> {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, FoldAccT) ->
    RunnerType = {keylist, Tag, FoldAccT},
    book_returnfolder(Pid, RunnerType).

%% @doc as for book_keylist/3 but constrained to only those keys in
%% `Bucket'
-spec book_keylist(pid(), Tag, Bucket, FoldAccT) -> {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, Bucket, FoldAccT) ->
    RunnerType = {keylist, Tag, Bucket, FoldAccT},
    book_returnfolder(Pid, RunnerType).

%% @doc as for book_keylist/4 with additional constraint that only
%% keys in the `KeyRange' tuple will be folder over, where `KeyRange'
%% is `StartKey', the first key in the range and `EndKey' the last,
%% (inclusive.) Or the atom `all', which will return all keys in the
%% `Bucket'.
-spec book_keylist(pid(), Tag, Bucket, KeyRange, FoldAccT) ->
                                                    {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      KeyRange :: {StartKey, EndKey} | all,
      StartKey :: Key,
      EndKey :: Key,
      Key :: term(),
      Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, Bucket, KeyRange, FoldAccT) ->
    RunnerType = {keylist, Tag, Bucket, KeyRange, FoldAccT, undefined},
    book_returnfolder(Pid, RunnerType).

%% @doc as for book_keylist/5 with additional constraint that a compile regular
%% expression is passed to be applied against any key that is in the range.
%% This is always applied to the Key and only the Key, not to any SubKey.
-spec book_keylist(pid(), Tag, Bucket, KeyRange, FoldAccT, TermRegex) ->
                                                    {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      KeyRange :: {StartKey, EndKey} | all,
      StartKey :: Key,
      EndKey :: Key,
      Key :: term(),
      TermRegex :: leveled_codec:regular_expression(),
      Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, Bucket, KeyRange, FoldAccT, TermRegex) ->
    RunnerType = {keylist, Tag, Bucket, KeyRange, FoldAccT, TermRegex},
    book_returnfolder(Pid, RunnerType).


%% @doc fold over all the objects/values in the store in key
%% order. `Tag' is the tagged type of object. `FoldAccT' is a 2-tuple,
%% the first element being a 4-arity fun, that is called once for each
%% key with the arguments `Bucket', `Key', `Value', `Acc'. The 2nd
%% element is the initial accumulator `Acc' which is passed to
%% `FoldFun' on it's first call. Thereafter the return value from
%% `FoldFun' is the 4th argument to the next call of
%% `FoldFun'. `SnapPreFold' is a boolean where `true' means take the
%% snapshot at once, and `false' means take the snapshot when the
%% returned `Runner' is executed. Return `{async, Runner}' where
%% `Runner' is a 0-arity function that returns the final accumulator
%% from `FoldFun'
-spec book_objectfold(pid(), Tag, FoldAccT, SnapPreFold) -> {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Value :: term(),
      SnapPreFold :: boolean(),
      Runner :: fun(() -> Acc).
book_objectfold(Pid, Tag, FoldAccT, SnapPreFold) ->
    RunnerType = {foldobjects_allkeys, Tag, FoldAccT, SnapPreFold},
    book_returnfolder(Pid, RunnerType).

%% @doc exactly as book_objectfold/4 with the additional parameter
%% `Order'. `Order' can be `sqn_order' or `key_order'. In
%% book_objectfold/4 and book_objectfold/6 `key_order' is
%% implied. This function called with `Option == key_order' is
%% identical to book_objectfold/4. NOTE: if you most fold over ALL
%% objects, this is quicker than `key_order' due to accessing the
%% journal objects in thei ron disk order, not via a fold over the
%% ledger.
-spec book_objectfold(pid(), Tag, FoldAccT, SnapPreFold, Order) -> {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Value :: term(),
      SnapPreFold :: boolean(),
      Runner :: fun(() -> Acc),
      Order :: key_order | sqn_order.
book_objectfold(Pid, Tag, FoldAccT, SnapPreFold, Order) ->
    RunnerType = {foldobjects_allkeys, Tag, FoldAccT, SnapPreFold, Order},
    book_returnfolder(Pid, RunnerType).

%% @doc as book_objectfold/4, with the addition of some constraints on
%% the range of objects folded over. The 3rd argument `Bucket' limits
%% ths fold to that specific bucket only. The 4th argument `Limiter'
%% further constrains the fold. `Limiter' can be either a `Range' or
%% `Index' query. `Range' is either that atom `all', meaning {min,
%% max}, or, a two tuple of start key and end key, inclusive. Index
%% Query is a 3-tuple of `{IndexField, StartTerm, EndTerm}`, just as
%% in book_indexfold/5
-spec book_objectfold(pid(), Tag, Bucket, Limiter, FoldAccT, SnapPreFold) ->
                             {async, Runner} when
      Tag :: leveled_codec:tag(),
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Value :: term(),
      Limiter :: Range | Index,
      Range :: {StartKey, EndKey} | all,
      Index :: {IndexField, Start, End},
      IndexField::term(),
      IndexVal::term(),
      Start::IndexVal,
      End::IndexVal,
      StartKey :: Key,
      EndKey :: Key,
      SnapPreFold :: boolean(),
      Runner :: fun(() -> Acc).
book_objectfold(Pid, Tag, Bucket, Limiter, FoldAccT, SnapPreFold) ->
    RunnerType =
        case Limiter of
            all ->
                {foldobjects_bybucket, Tag, Bucket, all, FoldAccT, SnapPreFold};
            Range when is_tuple(Range) andalso size(Range) == 2 ->
                {foldobjects_bybucket, Tag, Bucket, Range, FoldAccT, SnapPreFold};
            IndexQuery when is_tuple(IndexQuery) andalso size(IndexQuery) == 3 ->
                IndexQuery = Limiter,
                {foldobjects_byindex, Tag, Bucket, IndexQuery, FoldAccT, SnapPreFold}
        end,
    book_returnfolder(Pid, RunnerType).


%% @doc LevelEd stores not just Keys in the ledger, but also may store
%% object metadata, referred to as heads (after Riak head request for
%% object metadata) Often when folding over objects all that is really
%% required is the object metadata. These "headfolds" are an efficient
%% way to fold over the ledger (possibly wholly in memory) and get
%% object metadata.
%%
%% Fold over the object's head. `Tag' is the tagged type of the
%% objects to fold over. `FoldAccT' is a 2-tuple. The 1st element is a
%% 4-arity fold fun, that takes a Bucket, Key, ProxyObject, and the
%% `Acc'. The ProxyObject is an object that only contains the
%% head/metadata, and no object data from the journal. The `Acc' in
%% the first call is that provided as the second element of `FoldAccT'
%% and thereafter the return of the previous all to the fold fun. If
%% `JournalCheck' is `true' then the journal is checked to see if the
%% object in the ledger is present, which means a snapshot of the
%% whole store is required, if `false', then no such check is
%% performed, and onlt ledger need be snapshotted. `SnapPreFold' is a
%% boolean that determines if the snapshot is taken when the folder is
%% requested `true', or when when run `false'. `SegmentList' can be
%% `false' meaning, all heads, or a list of integers that designate
%% segments in a TicTac Tree.
-spec book_headfold(pid(), Tag, FoldAccT, JournalCheck, SnapPreFold, SegmentList) ->
                           {async, Runner} when
      Tag :: leveled_codec:tag(),
            FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Value :: term(),
      JournalCheck :: boolean(),
      SnapPreFold :: boolean(),
      SegmentList :: false | list(integer()),
      Runner :: fun(() -> Acc).
book_headfold(Pid, Tag, FoldAccT, JournalCheck, SnapPreFold, SegmentList) ->
    book_headfold(Pid, Tag, all, 
                    FoldAccT, JournalCheck, SnapPreFold, 
                    SegmentList, false, false).

%% @doc as book_headfold/6, but with the addition of a `Limiter' that
%% restricts the set of objects folded over. `Limiter' can either be a
%% bucket list, or a key range of a single bucket. For bucket list,
%% the `Limiter' should be a 2-tuple, the first element the tag
%% `bucket_list' and the second a `list()' of `Bucket'. Only heads
%% from the listed buckets will be folded over. A single bucket key
%% range may also be used as a `Limiter', in which case the argument
%% is a 3-tuple of `{range ,Bucket, Range}' where `Bucket' is a
%% bucket, and `Range' is a 2-tuple of start key and end key,
%% inclusive, or the atom `all'. The rest of the arguments are as
%% `book_headfold/6'
-spec book_headfold(pid(), Tag, Limiter, FoldAccT, JournalCheck, SnapPreFold, SegmentList) ->
                           {async, Runner} when
      Tag :: leveled_codec:tag(),
      Limiter :: BucketList | BucketKeyRange,
      BucketList :: {bucket_list, list(Bucket)},
      BucketKeyRange :: {range, Bucket, KeyRange},
      KeyRange :: {StartKey, EndKey} | all,
      StartKey :: Key,
      EndKey :: Key,
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Value :: term(),
      JournalCheck :: boolean(),
      SnapPreFold :: boolean(),
      SegmentList :: false | list(integer()),
      Runner :: fun(() -> Acc).
book_headfold(Pid, Tag, Limiter, FoldAccT, JournalCheck, SnapPreFold, SegmentList) ->
    book_headfold(Pid, Tag, Limiter, 
                    FoldAccT, JournalCheck, SnapPreFold, 
                    SegmentList, false, false).

%% @doc as book_headfold/7, but with the addition of a Last Modified Date
%% Range and Max Object Count.  For version 2 objects this will filter out
%% all objects with a highest Last Modified Date that is outside of the range.
%% All version 1 objects will be included in the result set regardless of Last
%% Modified Date.
%% The Max Object Count will stop the fold once the count has been reached on
%% this store only.  The Max Object Count if provided will mean that the runner
%% will return {RemainingCount, Acc} not just Acc
-spec book_headfold(pid(), Tag, Limiter, FoldAccT, JournalCheck, SnapPreFold,
                        SegmentList, LastModRange, MaxObjectCount) ->
                           {async, Runner} when
      Tag :: leveled_codec:tag(),
      Limiter :: BucketList | BucketKeyRange | all,
      BucketList :: {bucket_list, list(Bucket)},
      BucketKeyRange :: {range, Bucket, KeyRange},
      KeyRange :: {StartKey, EndKey} | all,
      StartKey :: Key,
      EndKey :: Key,
      FoldAccT :: {FoldFun, Acc},
      FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
      Acc :: term(),
      Bucket :: term(),
      Key :: term(),
      Value :: term(),
      JournalCheck :: boolean(),
      SnapPreFold :: boolean(),
      SegmentList :: false | list(integer()),
      LastModRange :: false | leveled_codec:lastmod_range(),
      MaxObjectCount :: false | pos_integer(),
      Runner :: fun(() -> ResultingAcc),
      ResultingAcc :: Acc | {non_neg_integer(), Acc}.
book_headfold(Pid, Tag, {bucket_list, BucketList}, FoldAccT, JournalCheck, SnapPreFold,
                SegmentList, LastModRange, MaxObjectCount) ->
    RunnerType = 
        {foldheads_bybucket, Tag, BucketList, bucket_list, FoldAccT, 
            JournalCheck, SnapPreFold, 
            SegmentList, LastModRange, MaxObjectCount},
    book_returnfolder(Pid, RunnerType);
book_headfold(Pid, Tag, {range, Bucket, KeyRange}, FoldAccT, JournalCheck, SnapPreFold,
                SegmentList, LastModRange, MaxObjectCount) ->
    RunnerType = 
        {foldheads_bybucket, Tag, Bucket, KeyRange, FoldAccT,
            JournalCheck, SnapPreFold,
            SegmentList, LastModRange, MaxObjectCount},
    book_returnfolder(Pid, RunnerType);
book_headfold(Pid, Tag, all, FoldAccT, JournalCheck, SnapPreFold,
                SegmentList, LastModRange, MaxObjectCount) ->
    RunnerType = {foldheads_allkeys, Tag, FoldAccT,
                    JournalCheck, SnapPreFold, 
                    SegmentList, LastModRange, MaxObjectCount},
    book_returnfolder(Pid, RunnerType).

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

-spec book_compactjournal(pid(), integer()) -> ok|busy.
-spec book_islastcompactionpending(pid()) -> boolean().
-spec book_trimjournal(pid()) -> ok.

%% @doc Call for compaction of the Journal
%%
%% the scheduling of Journla compaction is called externally, so it is assumed
%% in Riak it will be triggered by a vnode callback.

book_compactjournal(Pid, Timeout) ->
    {R, _P} = gen_server:call(Pid, {compact_journal, Timeout}, infinity),
    R.

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


-spec book_hotbackup(pid()) -> {async, fun((string()) -> ok)}.
%% @doc Backup the Bookie
%% Return a function that will take a backup of a snapshot of the Journal.
%% The function will be 1-arity, and can be passed the absolute folder name
%% to store the backup.
%% 
%% Backup files are hard-linked.  Does not work in head_only mode, or if
%% index changes are used with a `recovr` compaction/reload strategy
book_hotbackup(Pid) ->
    gen_server:call(Pid, hot_backup, infinity). 

-spec book_isempty(pid(), leveled_codec:tag()) -> boolean().
%% @doc 
%% Confirm if the store is empty, or if it contains a Key and Value for a 
%% given tag
book_isempty(Pid, Tag) ->
    FoldAccT = {fun(_B, _Acc) -> false end, true},
    {async, Runner} = book_bucketlist(Pid, Tag, FoldAccT, first),
    Runner().

-spec book_logsettings(pid()) -> {leveled_log:log_level(), list(string())}.
%% @doc
%% Retrieve the current log settings
book_logsettings(Pid) ->
    gen_server:call(Pid, log_settings, infinity).

-spec book_loglevel(pid(), leveled_log:log_level()) -> ok.
%% @doc
%% Change the log level of the store
book_loglevel(Pid, LogLevel) ->
    gen_server:cast(Pid, {log_level, LogLevel}).

-spec book_addlogs(pid(), list(string())) -> ok.
%% @doc
%% Add to the list of forced logs, a list of more forced logs
book_addlogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {add_logs, ForcedLogs}).

-spec book_removelogs(pid(), list(string())) -> ok.
%% @doc
%% Remove from the list of forced logs, a list of forced logs
book_removelogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {remove_logs, ForcedLogs}).

-spec book_returnactors(pid()) -> {ok, pid(), pid()}.
%% @doc
%% Return the Inker and Penciller - {ok, Inker, Penciller}.  Used only in tests
book_returnactors(Pid) ->
    gen_server:call(Pid, return_actors, infinity).

-spec book_headstatus(pid()) -> {boolean(), boolean()}.
%% @doc
%% Return booleans to state the bookie is in head_only mode, and supporting
%% lookups
book_headstatus(Pid) ->
    gen_server:call(Pid, head_status, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

-spec init([open_options()]) -> {ok, book_state()}.
init([Opts]) ->
    leveled_rand:seed(),
    case {proplists:get_value(snapshot_bookie, Opts), 
            proplists:get_value(root_path, Opts)} of
        {undefined, undefined} ->
            {stop, no_root_path};
        {undefined, _RP} ->
            % Start from file not snapshot

            % Must set log level first - as log level will be fetched within
            % set_options/1.  Also logs can now be added to set_options/1
            LogLevel = proplists:get_value(log_level, Opts),
            leveled_log:set_loglevel(LogLevel),
            ForcedLogs = proplists:get_value(forced_logs, Opts),
            leveled_log:add_forcedlogs(ForcedLogs),
            DatabaseID = proplists:get_value(database_id, Opts),
            leveled_log:set_databaseid(DatabaseID),

            {ok, Monitor} =
                leveled_monitor:monitor_start(
                    proplists:get_value(stats_logfrequency, Opts),
                    proplists:get_value(monitor_loglist, Opts)
                ),
            StatLogFrequency = proplists:get_value(stats_percentage, Opts),

            {InkerOpts, PencillerOpts} =
                set_options(Opts, {Monitor, StatLogFrequency}),

            OverrideFunctions = proplists:get_value(override_functions, Opts),
            SetFun =
                fun({FuncName, Func}) ->
                    application:set_env(leveled, FuncName, Func)
                end,
            lists:foreach(SetFun, OverrideFunctions),

            ConfiguredCacheSize = 
                max(proplists:get_value(cache_size, Opts), ?MIN_CACHE_SIZE),
            CacheJitter = 
                max(1, ConfiguredCacheSize div (100 div ?CACHE_SIZE_JITTER)),
            CacheSize = 
                ConfiguredCacheSize + erlang:phash2(self()) rem CacheJitter,
            MaxCacheMultiple =
                proplists:get_value(cache_multiple, Opts),
            PCLMaxSize =
                PencillerOpts#penciller_options.max_inmemory_tablesize,
            CacheRatio = PCLMaxSize div ConfiguredCacheSize,
                % It is expected that the maximum size of the penciller
                % in-memory store should not be more than about 10 x the size
                % of the ledger cache.  In this case there will be a larger
                % than tested list of ledger_caches in the penciller memory,
                % and performance may be unpredictable
            case CacheRatio > 32 of
                true ->
                    leveled_log:log(b0020, [PCLMaxSize, ConfiguredCacheSize]);
                false ->
                    ok
            end,

            PageCacheLevel = proplists:get_value(ledger_preloadpagecache_level, Opts),    

            {HeadOnly, HeadLookup, SSTPageCacheLevel} = 
                case proplists:get_value(head_only, Opts) of 
                    false ->
                        {false, true, PageCacheLevel};
                    with_lookup ->
                        {true, true, PageCacheLevel};
                    no_lookup ->
                        {true, false, ?SST_PAGECACHELEVEL_NOLOOKUP}
                end,
            % Override the default page cache level - we want to load into the
            % page cache many levels if we intend to support lookups, and only
            % levels 0 and 1 otherwise
            SSTOpts = PencillerOpts#penciller_options.sst_options,
            SSTOpts0 = SSTOpts#sst_options{pagecache_level = SSTPageCacheLevel},
            PencillerOpts0 =
                PencillerOpts#penciller_options{sst_options = SSTOpts0},            

            {Inker, Penciller} =  startup(InkerOpts, PencillerOpts0),

            NewETS = ets:new(mem, [ordered_set]),
            leveled_log:log(b0001, [Inker, Penciller]),
            {ok, 
                #state{
                    cache_size = CacheSize,
                    cache_multiple = MaxCacheMultiple,
                    is_snapshot = false,
                    head_only = HeadOnly,
                    head_lookup = HeadLookup,
                    inker = Inker,
                    penciller = Penciller,
                    ledger_cache = #ledger_cache{mem = NewETS},
                    monitor = {Monitor, StatLogFrequency}}};
        {Bookie, undefined} ->
            {ok, Penciller, Inker} = 
                book_snapshot(Bookie, store, undefined, true),
            BookieMonitor = erlang:monitor(process, Bookie),
            NewETS = ets:new(mem, [ordered_set]),
            {HeadOnly, Lookup} = leveled_bookie:book_headstatus(Bookie),
            leveled_log:log(b0002, [Inker, Penciller]),
            {ok,
                #state{penciller = Penciller,
                        inker = Inker,
                        ledger_cache = #ledger_cache{mem = NewETS},
                        head_only = HeadOnly,
                        head_lookup = Lookup,
                        bookie_monref = BookieMonitor,
                        is_snapshot = true}}
    end.


handle_call({put, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync},
                From, State) when State#state.head_only == false ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    SWLR = os:timestamp(),
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    {ok, SQN, ObjSize} = leveled_inker:ink_put(State#state.inker,
                                               LedgerKey,
                                               Object,
                                               {IndexSpecs, TTL},
                                               DataSync),
    {T0, SW1} = leveled_monitor:step_time(SW0),
    Changes =
        preparefor_ledgercache(
            null, LedgerKey, SQN, Object, ObjSize, {IndexSpecs, TTL}),
    {T1, SW2} = leveled_monitor:step_time(SW1),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    {T2, _SW3} = leveled_monitor:step_time(SW2),
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    maybe_longrunning(SWLR, overall_put),
    maybelog_put_timing(State#state.monitor, T0, T1, T2, ObjSize),
    case maybepush_ledgercache(
            State#state.cache_size,
            State#state.cache_multiple,
            Cache0,
            State#state.penciller) of
        {ok, Cache} ->
            {noreply, State#state{slow_offer = false, ledger_cache = Cache}};
        {returned, Cache} ->
            {noreply, State#state{slow_offer = true, ledger_cache = Cache}}
    end;
handle_call({mput, ObjectSpecs, TTL}, From, State) 
                                        when State#state.head_only == true ->
    {ok, SQN} = 
        leveled_inker:ink_mput(State#state.inker, dummy, {ObjectSpecs, TTL}),
    Changes = 
        preparefor_ledgercache(?INKT_MPUT, ?DUMMY, 
                                SQN, null, length(ObjectSpecs), 
                                {ObjectSpecs, TTL}),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    case maybepush_ledgercache(
            State#state.cache_size,
            State#state.cache_multiple,
            Cache0,
            State#state.penciller) of
        {ok, Cache} ->
            {noreply, State#state{ledger_cache = Cache, slow_offer = false}};
        {returned, Cache} ->
            {noreply, State#state{ledger_cache = Cache, slow_offer = true}}
    end;
handle_call({get, Bucket, Key, Tag}, _From, State) 
                                        when State#state.head_only == false ->
    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    {H0, _CacheHit} =
        fetch_head(LedgerKey,
                    State#state.penciller,
                    State#state.ledger_cache),
    HeadResult = 
        case H0 of
            not_present ->
                not_found;
            Head ->
                {Seqn, Status, _MH, _MD} = 
                    leveled_codec:striphead_to_v1details(Head),
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
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    GetResult = 
        case HeadResult of 
            not_found -> 
                not_found;
            {LK, SQN} ->
                Object = fetch_value(State#state.inker, {LK, SQN}),
                case Object of 
                    not_present ->
                        not_found;
                    _ ->
                        {ok, Object}
                end 
        end,
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    maybelog_get_timing(
        State#state.monitor, TS0, TS1, GetResult == not_found),
    {reply, GetResult, State};
handle_call({head, Bucket, Key, Tag, SQNOnly}, _From, State) 
                                        when State#state.head_lookup == true ->
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    LK = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
    {Head, CacheHit} =
        fetch_head(LK, 
                    State#state.penciller, 
                    State#state.ledger_cache,
                    State#state.head_only),
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    JrnalCheckFreq =
        case State#state.head_only of
            true ->
                0;
            false ->
                State#state.ink_checking
        end,
    {LedgerMD, SQN, UpdJrnalCheckFreq} =
        case Head of
            not_present ->
                {not_found, null, JrnalCheckFreq};
            Head ->
                case leveled_codec:striphead_to_v1details(Head) of
                    {_SeqN, tomb, _MH, _MD} ->
                        {not_found, null, JrnalCheckFreq};
                    {SeqN, {active, TS}, _MH, MD} ->
                        case TS >= leveled_util:integer_now() of
                            true ->
                                I = State#state.inker,
                                case journal_notfound(
                                        JrnalCheckFreq, I, LK, SeqN) of
                                    {true, UppedFrequency} ->
                                        {not_found, null, UppedFrequency};
                                    {false, ReducedFrequency} ->
                                        {MD, SeqN, ReducedFrequency}
                                end;
                            false ->
                                {not_found, null, JrnalCheckFreq}
                        end
                end
        end,
    Reply = 
        case {LedgerMD, SQNOnly} of
            {not_found, _} ->
                not_found;
            {_, false} ->
                {ok, leveled_head:build_head(Tag, LedgerMD)};
            {_, true} ->
                {ok, SQN}
        end,
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    maybelog_head_timing(
        State#state.monitor, TS0, TS1, LedgerMD == not_found, CacheHit),
    case UpdJrnalCheckFreq of
        JrnalCheckFreq ->
            {reply, Reply, State};
        UpdJrnalCheckFreq ->
            {reply, Reply, State#state{ink_checking = UpdJrnalCheckFreq}}
    end;
handle_call({snapshot, SnapType, Query, LongRunning}, _From, State) ->
    % Snapshot the store, specifying if the snapshot should be long running 
    % (i.e. will the snapshot be queued or be required for an extended period 
    % e.g. many minutes)
    {ok, PclSnap, InkSnap} =
        snapshot_store(
            State#state.ledger_cache,
            State#state.penciller,
            State#state.inker,
            State#state.monitor,
            SnapType,
            Query,
            LongRunning),
    {reply, {ok, PclSnap, InkSnap},State};
handle_call(log_settings, _From, State) ->
    {reply, leveled_log:return_settings(), State};
handle_call({return_runner, QueryType}, _From, State) ->
    Runner = get_runner(State, QueryType),
    {reply, Runner, State};
handle_call({compact_journal, Timeout}, From, State)
                                        when State#state.head_only == false ->
    case leveled_inker:ink_compactionpending(State#state.inker) of
        true ->
            {reply, {busy, undefined}, State};
        false ->
            {ok, PclSnap, null} =
            snapshot_store(
                State#state.ledger_cache,
                State#state.penciller,
                State#state.inker,
                State#state.monitor,
                ledger,
                undefined,
                true),
            R = leveled_inker:ink_compactjournal(State#state.inker,
                                                    PclSnap,
                                                    Timeout),
            gen_server:reply(From, R),
            case maybepush_ledgercache(
                    State#state.cache_size,
                    State#state.cache_multiple,
                    State#state.ledger_cache,
                    State#state.penciller) of
                {_, NewCache} ->
                    {noreply, State#state{ledger_cache = NewCache}}
            end
    end;
handle_call(confirm_compact, _From, State)
                                        when State#state.head_only == false ->
    {reply, leveled_inker:ink_compactionpending(State#state.inker), State};
handle_call(trim, _From, State) when State#state.head_only == true ->
    PSQN = leveled_penciller:pcl_persistedsqn(State#state.penciller),
    {reply, leveled_inker:ink_trim(State#state.inker, PSQN), State};
handle_call(hot_backup, _From, State) when State#state.head_only == false ->
    ok = leveled_inker:ink_roll(State#state.inker),
    BackupFun = 
        fun(InkerSnapshot) ->
            fun(BackupPath) ->
                ok = leveled_inker:ink_backup(InkerSnapshot, BackupPath),
                ok = leveled_inker:ink_close(InkerSnapshot)
            end
        end,
    InkerOpts = 
        #inker_options{start_snapshot = true, 
                        source_inker = State#state.inker,
                        bookies_pid = self()},
    {ok, Snapshot} = leveled_inker:ink_snapstart(InkerOpts),
    {reply, {async, BackupFun(Snapshot)}, State};
handle_call(close, _From, State) ->
    leveled_inker:ink_close(State#state.inker),
    leveled_penciller:pcl_close(State#state.penciller),
    leveled_monitor:monitor_close(element(1, State#state.monitor)),
    {stop, normal, ok, State};
handle_call(destroy, _From, State=#state{is_snapshot=Snp}) when Snp == false ->
    leveled_log:log(b0011, []),
    {ok, InkPathList} = leveled_inker:ink_doom(State#state.inker),
    {ok, PCLPathList} = leveled_penciller:pcl_doom(State#state.penciller),
    leveled_monitor:monitor_close(element(1, State#state.monitor)),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, InkPathList),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, PCLPathList),
    {stop, normal, ok, State};
handle_call(return_actors, _From, State) ->
    {reply, {ok, State#state.inker, State#state.penciller}, State};
handle_call(head_status, _From, State) ->
    {reply, {State#state.head_only, State#state.head_lookup}, State};
handle_call(Msg, _From, State) ->
    {reply, {unsupported_message, element(1, Msg)}, State}.


handle_cast({log_level, LogLevel}, State) ->
    PCL = State#state.penciller,
    INK = State#state.inker,
    ok = leveled_penciller:pcl_loglevel(PCL, LogLevel),
    ok = leveled_inker:ink_loglevel(INK, LogLevel),
    case element(1, State#state.monitor) of
        no_monitor ->
            ok;
        Monitor ->
            leveled_monitor:log_level(Monitor, LogLevel)
    end,
    ok = leveled_log:set_loglevel(LogLevel),
    {noreply, State};
handle_cast({add_logs, ForcedLogs}, State) ->
    PCL = State#state.penciller,
    INK = State#state.inker,
    ok = leveled_penciller:pcl_addlogs(PCL, ForcedLogs),
    ok = leveled_inker:ink_addlogs(INK, ForcedLogs),
    case element(1, State#state.monitor) of
        no_monitor ->
            ok;
        Monitor ->
            leveled_monitor:log_add(Monitor, ForcedLogs)
    end,
    ok = leveled_log:add_forcedlogs(ForcedLogs),
    {noreply, State};
handle_cast({remove_logs, ForcedLogs}, State) ->
    PCL = State#state.penciller,
    INK = State#state.inker,
    ok = leveled_penciller:pcl_removelogs(PCL, ForcedLogs),
    ok = leveled_inker:ink_removelogs(INK, ForcedLogs),
    case element(1, State#state.monitor) of
        no_monitor ->
            ok;
        Monitor ->
            leveled_monitor:log_remove(Monitor, ForcedLogs)
    end,
    ok = leveled_log:remove_forcedlogs(ForcedLogs),
    {noreply, State}.


%% handle the bookie stopping and stop this snapshot
handle_info({'DOWN', BookieMonRef, process, BookiePid, Info},
	    State=#state{bookie_monref = BookieMonRef, is_snapshot = true}) ->
    leveled_log:log(b0004, [BookiePid, Info]),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.


terminate(Reason, _State) ->
    leveled_log:log(b0003, [Reason]).

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


-spec push_to_penciller(
    pid(),
    list(load_item()), 
    ledger_cache(),
    leveled_codec:compaction_strategy())
        -> ledger_cache().
%% @doc
%% The push to penciller must start as a tree to correctly de-duplicate
%% the list by order before becoming a de-duplicated list for loading  
push_to_penciller(Penciller, LoadItemList, LedgerCache, ReloadStrategy) ->
    UpdLedgerCache =
        lists:foldl(
            fun({InkTag, PK, SQN, Obj, IndexSpecs, ValSize}, AccLC) ->
                Chngs =
                    case leveled_codec:get_tagstrategy(PK, ReloadStrategy) of
                        recalc ->
                            recalcfor_ledgercache(
                                InkTag, PK, SQN, Obj, ValSize, IndexSpecs,
                                AccLC, Penciller);
                        _ ->
                            preparefor_ledgercache(
                                InkTag, PK, SQN, Obj, ValSize, IndexSpecs)
                    end,
                addto_ledgercache(Chngs, AccLC, loader)
            end,
            LedgerCache,
            lists:reverse(LoadItemList)
        ),
    case length(UpdLedgerCache#ledger_cache.load_queue) of
        N when N > ?LOADING_BATCH ->
            leveled_log:log(b0006, [UpdLedgerCache#ledger_cache.max_sqn]),
            ok =
                push_to_penciller_loop(
                    Penciller, loadqueue_ledgercache(UpdLedgerCache)),
            empty_ledgercache();
        _ ->
            UpdLedgerCache
    end.

-spec push_to_penciller_loop(pid(), ledger_cache()) -> ok.
push_to_penciller_loop(Penciller, LedgerCache) ->
    case push_ledgercache(Penciller, LedgerCache) of
        returned ->
            timer:sleep(?LOADING_PAUSE),
            push_to_penciller_loop(Penciller, LedgerCache);
        ok ->
            ok
    end.

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
                        pid(),
                        null|pid(),
                        leveled_monitor:monitor(),
                        store|ledger, 
                        undefined|tuple(),
                        undefined|boolean()) ->
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
snapshot_store(
        LedgerCache, Penciller, Inker, Monitor, SnapType, Query, LongRunning) ->
    SW0 = leveled_monitor:maybe_time(Monitor),
    LedgerCacheReady = readycache_forsnapshot(LedgerCache, Query),
    BookiesMem = {LedgerCacheReady#ledger_cache.loader,
                    LedgerCacheReady#ledger_cache.index,
                    LedgerCacheReady#ledger_cache.min_sqn,
                    LedgerCacheReady#ledger_cache.max_sqn},
    PCLopts = 
        #penciller_options{start_snapshot = true,
                            source_penciller = Penciller,
                            snapshot_query = Query,
                            snapshot_longrunning = LongRunning,
				            bookies_pid = self(),
                            bookies_mem = BookiesMem},
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    {ok, LedgerSnapshot} = leveled_penciller:pcl_snapstart(PCLopts),
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    ok = maybelog_snap_timing(Monitor, TS0, TS1),
    case SnapType of
        store ->
            InkerOpts = #inker_options{start_snapshot = true,
                                       bookies_pid = self(),
                                       source_inker = Inker},
            {ok, JournalSnapshot} = leveled_inker:ink_snapstart(InkerOpts),
            {ok, LedgerSnapshot, JournalSnapshot};
        ledger ->
            {ok, LedgerSnapshot, null}
    end.


-spec fetch_value(pid(), leveled_codec:journal_ref()) -> not_present|any().
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

-spec startup(#inker_options{}, #penciller_options{}) -> {pid(), pid()}.
%% @doc
%% Startup the Inker and the Penciller, and prompt the loading of the Penciller
%% from the Inker.  The Penciller may be shutdown without the latest data 
%% having been persisted: and so the Iker must be able to update the Penciller
%% on startup with anything that happened but wasn't flushed to disk.
startup(InkerOpts, PencillerOpts) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    leveled_log:log(b0005, [LedgerSQN]),
    ReloadStrategy = InkerOpts#inker_options.reload_strategy,
    LoadFun = get_loadfun(),
    BatchFun = 
        fun(BatchAcc, Acc) ->
            push_to_penciller(
                Penciller, BatchAcc, Acc, ReloadStrategy)
        end,
    InitAccFun =
        fun(FN, CurrentMinSQN) ->
            leveled_log:log(i0014, [FN, CurrentMinSQN]),
            []
        end,
    FinalAcc =
        leveled_inker:ink_loadpcl(
            Inker, LedgerSQN + 1, LoadFun, InitAccFun, BatchFun),
    ok = push_to_penciller_loop(Penciller, loadqueue_ledgercache(FinalAcc)),
    ok = leveled_inker:ink_checksqn(Inker, LedgerSQN),
    {Inker, Penciller}.


-spec set_defaults(list()) -> open_options().
%% @doc
%% Set any pre-defined defaults for options if the option is not present in
%% the passed in options
set_defaults(Opts) ->
    lists:ukeymerge(1, 
                    lists:ukeysort(1, Opts), 
                    lists:ukeysort(1, ?OPTION_DEFAULTS)).

-spec set_options(
    open_options(), leveled_monitor:monitor()) ->
        {#inker_options{}, #penciller_options{}}.
%% @doc
%% Take the passed in property list of operations and extract out any relevant
%% options to the Inker or the Penciller
set_options(Opts, Monitor) ->
    MaxJournalSize0 = 
        min(?ABSOLUTEMAX_JOURNALSIZE, 
            proplists:get_value(max_journalsize, Opts)),
    JournalSizeJitter = MaxJournalSize0 div (100 div ?JOURNAL_SIZE_JITTER),
    MaxJournalSize = 
        min(?ABSOLUTEMAX_JOURNALSIZE, 
                MaxJournalSize0 - erlang:phash2(self()) rem JournalSizeJitter),
    MaxJournalCount0 =
        proplists:get_value(max_journalobjectcount, Opts),
    JournalCountJitter = MaxJournalCount0 div (100 div ?JOURNAL_SIZE_JITTER),
    MaxJournalCount = 
        MaxJournalCount0 - erlang:phash2(self()) rem JournalCountJitter,

    SyncStrat = proplists:get_value(sync_strategy, Opts),
    WRP = proplists:get_value(waste_retention_period, Opts),

    SnapTimeoutShort = proplists:get_value(snapshot_timeout_short, Opts),
    SnapTimeoutLong = proplists:get_value(snapshot_timeout_long, Opts),

    AltStrategy = proplists:get_value(reload_strategy, Opts),
    ReloadStrategy = leveled_codec:inker_reload_strategy(AltStrategy),

    PCLL0CacheSize = 
        max(?MIN_PCL_CACHE_SIZE, 
            proplists:get_value(max_pencillercachesize, Opts)),
    RootPath = proplists:get_value(root_path, Opts),

    JournalFP = filename:join(RootPath, ?JOURNAL_FP),
    LedgerFP = filename:join(RootPath, ?LEDGER_FP),
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
    CompressionLevel = proplists:get_value(compression_level, Opts),
    
    MaxSSTSlots = proplists:get_value(max_sstslots, Opts),

    ScoreOneIn = proplists:get_value(journalcompaction_scoreonein, Opts),

    {#inker_options{root_path = JournalFP,
                    reload_strategy = ReloadStrategy,
                    max_run_length = proplists:get_value(max_run_length, Opts),
                    singlefile_compactionperc = SFL_CompPerc,
                    maxrunlength_compactionperc = MRL_CompPerc,
                    waste_retention_period = WRP,
                    snaptimeout_long = SnapTimeoutLong,
                    compression_method = CompressionMethod,
                    compress_on_receipt = CompressOnReceipt,
                    score_onein = ScoreOneIn,
                    cdb_options = 
                        #cdb_options{
                            max_size = MaxJournalSize,
                            max_count = MaxJournalCount,
                            binary_mode = true,
                            sync_strategy = SyncStrat,
                            log_options = leveled_log:get_opts(),
                            monitor = Monitor},
                    monitor = Monitor},
        #penciller_options{root_path = LedgerFP,
                            max_inmemory_tablesize = PCLL0CacheSize,
                            levelzero_cointoss = true,
                            snaptimeout_short = SnapTimeoutShort,
                            snaptimeout_long = SnapTimeoutLong,
                            sst_options =
                                #sst_options{
                                    press_method = CompressionMethod,
                                    press_level = CompressionLevel,
                                    log_options = leveled_log:get_opts(),
                                    max_sstslots = MaxSSTSlots,
                                    monitor = Monitor},
                            monitor = Monitor}
        }.


-spec return_snapfun(
    book_state(), store|ledger,
    tuple()|no_lookup|undefined,
    boolean(), boolean()) 
        -> fun(() -> {ok, pid(), pid()|null, fun(() -> ok)}).
%% @doc
%% Generates a function from which a snapshot can be created.  The primary
%% factor here is the SnapPreFold boolean.  If this is true then the snapshot
%% will be taken before the Fold function is returned.  If SnapPreFold is
%% false then the snapshot will be taken when the Fold function is called.
%%
%% SnapPrefold is to be used when the intention is to queue the fold, and so
%% calling of the fold may be delayed, but it is still desired that the fold
%% represent the point in time that the query was requested.
%% 
%% Also returns a function which will close any snapshots to be used in the
%% runners post-query cleanup action
%% 
%% When the bookie is a snapshot, a fresh snapshot should not be taken, the
%% previous snapshot should be used instead.  Also the snapshot should not be
%% closed as part of the post-query activity as the snapshot may be reused, and
%% should be manually closed.
return_snapfun(State, SnapType, Query, LongRunning, SnapPreFold) ->
    CloseFun =
        fun(LS0, JS0) ->
            fun() ->
                ok = leveled_penciller:pcl_close(LS0),
                case JS0 of
                    JS0 when is_pid(JS0) ->
                        leveled_inker:ink_close(JS0);
                    _ ->
                        ok
                end
            end
        end,
    case {SnapPreFold, State#state.is_snapshot} of
        {true, false} ->
            {ok, LS, JS} =
                snapshot_store(
                    State#state.ledger_cache,
                    State#state.penciller,
                    State#state.inker,
                    State#state.monitor,
                    SnapType,
                    Query,
                    LongRunning),
            fun() -> {ok, LS, JS, CloseFun(LS, JS)} end;
        {false, false} ->
            Self = self(),
            % Timeout will be ignored, as will Requestor
            %
            % This uses the external snapshot - as the snapshot will need
            % to have consistent state between Bookie and Penciller when
            % it is made.
            fun() -> 
                {ok, LS, JS} = 
                    book_snapshot(Self, SnapType, Query, LongRunning),
                {ok, LS, JS, CloseFun(LS, JS)}
            end;
        {_ , true} ->
            LS = State#state.penciller,
            JS = State#state.inker,
            fun() -> {ok, LS, JS, fun() -> ok end} end
    end.

-spec snaptype_by_presence(boolean()) -> store|ledger.
%% @doc
%% Folds that traverse over object heads, may also either require to return 
%% the object, or at least confirm the object is present in the Ledger.  This
%% is achieved by enabling presence - and this will change the type of 
%% snapshot to one that covers the whole store (i.e. both ledger and journal),
%% rather than just the ledger.
snaptype_by_presence(true) ->
    store;
snaptype_by_presence(false) -> 
    ledger.

-spec get_runner(book_state(), tuple()) -> {async, fun(() -> term())}.
%% @doc
%% Get an {async, Runner} for a given fold type.  Fold types have different 
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
get_runner(State, {keylist, Tag, Bucket, KeyRange, FoldAccT, TermRegex}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(SnapFun, 
                                    Tag, Bucket, KeyRange, 
                                    FoldAccT, TermRegex);
%% Set of runners for object or metadata folds
get_runner(State, 
            {foldheads_allkeys, 
                Tag, FoldFun, 
                JournalCheck, SnapPreFold, SegmentList,
                LastModRange, MaxObjectCount}) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, SnapPreFold),
    leveled_runner:foldheads_allkeys(SnapFun, 
                                        Tag, FoldFun, 
                                        JournalCheck, SegmentList,
                                        LastModRange, MaxObjectCount);
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
                JournalCheck, SnapPreFold,
                SegmentList, LastModRange, MaxObjectCount}) ->
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
                                        JournalCheck,
                                        SegmentList,
                                        LastModRange, MaxObjectCount);
get_runner(State,
            {foldheads_bybucket, 
                Tag, 
                Bucket, KeyRange, 
                FoldFun, 
                JournalCheck, SnapPreFold,
                SegmentList, LastModRange, MaxObjectCount}) ->
    {StartKey, EndKey, SnapQ} = return_ledger_keyrange(Tag, Bucket, KeyRange),
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, SnapQ, true, SnapPreFold),
    leveled_runner:foldheads_bybucket(SnapFun, 
                                        Tag, 
                                        [{StartKey, EndKey}], 
                                        FoldFun, 
                                        JournalCheck,
                                        SegmentList,
                                        LastModRange, MaxObjectCount);
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


-spec get_deprecatedrunner(book_state(), tuple()) ->
                                                {async, fun(() -> term())}.
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
%% {StartLK, EndLK, Query} where Query is to indicate whether the query 
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
            leveled_log:log(b0013, [N, Aspect]);
        _ ->
            ok
    end.

-spec readycache_forsnapshot(ledger_cache(), tuple()|no_lookup|undefined) 
                                                        -> ledger_cache().
%% @doc
%% Strip the ledger cach back to only the relevant information needed in 
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
                    -> {not_present|leveled_codec:ledger_value(), boolean()}.
%% @doc
%% Fetch only the head of the object from the Ledger (or the bookie's recent
%% ledger cache if it has just been updated).  not_present is returned if the 
%% Key is not found
fetch_head(Key, Penciller, LedgerCache) ->
    fetch_head(Key, Penciller, LedgerCache, false).

-spec fetch_head(leveled_codec:ledger_key(), pid(), ledger_cache(), boolean())
                    -> {not_present|leveled_codec:ledger_value(), boolean()}.
%% doc
%% The L0Index needs to be bypassed when running head_only
fetch_head(Key, Penciller, LedgerCache, HeadOnly) ->
    SW = os:timestamp(),
    case ets:lookup(LedgerCache#ledger_cache.mem, Key) of
        [{Key, Head}] ->
            {Head, true};
        [] ->
            Hash = leveled_codec:segment_hash(Key),
            UseL0Idx = not HeadOnly, 
                % don't use the L0Index in head only mode. Object specs don't 
                % get an addition on the L0 index
            case leveled_penciller:pcl_fetch(Penciller, Key, Hash, UseL0Idx) of
                {Key, Head} ->
                    maybe_longrunning(SW, pcl_head),
                    {Head, false};
                not_present ->
                    maybe_longrunning(SW, pcl_head),
                    {not_present, false}
            end
    end.


-spec journal_notfound(integer(), pid(), leveled_codec:ledger_key(), integer())
                                                    -> {boolean(), integer()}.
%% @doc Check to see if the item is not_found in the journal.  If it is found
%% return false, and drop the counter that represents the frequency this check
%% should be made.  If it is not_found, this is not expected so up the check
%% frequency to the maximum value
journal_notfound(CheckFrequency, Inker, LK, SQN) ->
    check_notfound(CheckFrequency, 
                    fun() ->
                        leveled_inker:ink_keycheck(Inker, LK, SQN)
                    end).


-spec check_notfound(integer(), fun(() -> probably|missing)) ->
                                                    {boolean(), integer()}.
%% @doc Use a function to check if an item is found
check_notfound(CheckFrequency, CheckFun) ->
    case leveled_rand:uniform(?MAX_KEYCHECK_FREQUENCY) of 
        X when X =< CheckFrequency ->
            case CheckFun() of
                probably ->
                    {false, max(?MIN_KEYCHECK_FREQUENCY, CheckFrequency - 1)};
                missing ->
                    {true, ?MAX_KEYCHECK_FREQUENCY}
            end;
        _X ->
            {false, CheckFrequency}
    end.


-spec preparefor_ledgercache(leveled_codec:journal_key_tag()|null, 
                                leveled_codec:ledger_key()|?DUMMY,
                                non_neg_integer(), any(), integer(), 
                                leveled_codec:journal_keychanges())
                                    -> {leveled_codec:segment_hash(), 
                                            non_neg_integer(), 
                                            list(leveled_codec:ledger_kv())}.
%% @doc
%% Prepare an object and its related key changes for addition to the Ledger 
%% via the Ledger Cache.
preparefor_ledgercache(?INKT_MPUT, 
                        ?DUMMY, SQN, _O, _S, {ObjSpecs, TTL}) ->
    ObjChanges = leveled_codec:obj_objectspecs(ObjSpecs, SQN, TTL),
    {no_lookup, SQN, ObjChanges};
preparefor_ledgercache(?INKT_KEYD,
                        LedgerKey, SQN, _Obj, _Size, {IdxSpecs, TTL}) ->
    {Bucket, Key} = leveled_codec:from_ledgerkey(LedgerKey),
    KeyChanges =
        leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL),
    {no_lookup, SQN, KeyChanges};
preparefor_ledgercache(_InkTag,
                        LedgerKey, SQN, Obj, Size, {IdxSpecs, TTL}) ->
    {Bucket, Key, MetaValue, {KeyH, _ObjH}, _LastMods} =
        leveled_codec:generate_ledgerkv(LedgerKey, SQN, Obj, Size, TTL),
    KeyChanges =
        [{LedgerKey, MetaValue}] ++
            leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL),
    {KeyH, SQN, KeyChanges}.


-spec recalcfor_ledgercache(leveled_codec:journal_key_tag()|null, 
                                leveled_codec:ledger_key()|?DUMMY,
                                non_neg_integer(), any(), integer(), 
                                leveled_codec:journal_keychanges(),
                                ledger_cache(),
                                pid())
                                    -> {leveled_codec:segment_hash(), 
                                            non_neg_integer(), 
                                            list(leveled_codec:ledger_kv())}.
%% @doc
%% When loading from the journal to the ledger, may hit a key which has the
%% `recalc` strategy.  Such a key needs to recalculate the key changes by
%% comparison with the current state of the ledger, assuming it is a full
%% journal entry (i.e. KeyDeltas which may be a result of previously running
%% with a retain strategy should be ignored).  
recalcfor_ledgercache(InkTag,
                        _LedgerKey, SQN, _Obj, _Size, {_IdxSpecs, _TTL},
                        _LedgerCache,
                        _Penciller)
                            when InkTag == ?INKT_MPUT; InkTag == ?INKT_KEYD ->
    {no_lookup, SQN, []};
recalcfor_ledgercache(_InkTag,
                        LK, SQN, Obj, Size, {_IgnoreJournalIdxSpecs, TTL},
                        LedgerCache,
                        Penciller) ->
    {Bucket, Key, MetaValue, {KeyH, _ObjH}, _LastMods} =
        leveled_codec:generate_ledgerkv(LK, SQN, Obj, Size, TTL),
    OldObject = 
        case check_in_ledgercache(LK, KeyH, LedgerCache, loader) of
            false ->
                leveled_penciller:pcl_fetch(Penciller, LK, KeyH, true);
            KV ->
                KV
        end,
    OldMetadata =
        case OldObject of
            not_present ->
                not_present;
            {LK, LV} ->
                leveled_codec:get_metadata(LV)
        end,
    UpdMetadata = leveled_codec:get_metadata(MetaValue),
    IdxSpecs =
        leveled_head:diff_indexspecs(element(1, LK), UpdMetadata, OldMetadata),
    {KeyH,
        SQN,
        [{LK, MetaValue}]
            ++ leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL)}.


-spec addto_ledgercache({leveled_codec:segment_hash(), 
                                non_neg_integer(), 
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
%% Add a set of changes associated with a single sequence number (journal 
%% update) to the ledger cache.  This is used explicitly when loading the
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


-spec check_in_ledgercache(leveled_codec:ledger_key(),
                            leveled_codec:segment_hash(),
                            ledger_cache(),
                            loader) ->
                                false | leveled_codec:ledger_kv().
%% @doc
%% Check the ledger cache for a Key, when the ledger cache is in loader mode
%% and so is populating a queue not an ETS table
check_in_ledgercache(PK, Hash, Cache, loader) ->
    case leveled_pmem:check_index(Hash, [Cache#ledger_cache.index]) of
        [] ->
            false;
        _ ->
            lists:keyfind(PK, 1, Cache#ledger_cache.load_queue)
    end.


-spec maybepush_ledgercache(
    pos_integer(), pos_integer(), ledger_cache(), pid()) 
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
maybepush_ledgercache(MaxCacheSize, MaxCacheMult, Cache, Penciller) ->
    Tab = Cache#ledger_cache.mem,
    CacheSize = ets:info(Tab, size),
    TimeToPush = maybe_withjitter(CacheSize, MaxCacheSize, MaxCacheMult),
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

-spec maybe_withjitter(
    non_neg_integer(), pos_integer(), pos_integer()) -> boolean().
%% @doc
%% Push down randomly, but the closer to 4 * the maximum size, the more likely
%% a push should be
maybe_withjitter(
    CacheSize, MaxCacheSize, MaxCacheMult) when CacheSize > MaxCacheSize ->
    R = leveled_rand:uniform(MaxCacheMult * MaxCacheSize),
    (CacheSize - MaxCacheSize) > R;
maybe_withjitter(_CacheSize, _MaxCacheSize, _MaxCacheMult) ->
    false.


-spec get_loadfun() -> initial_loadfun().
%% @doc
%% The LoadFun will be used by the Inker when walking across the Journal to 
%% load the Penciller at startup.  
get_loadfun() ->
    fun(KeyInJournal, ValueInJournal, _Pos, Acc0, ExtractFun) ->
        {MinSQN, MaxSQN, LoadItems} = Acc0,
        {SQN, InkTag, PK} = KeyInJournal,
        case SQN of
            SQN when SQN < MinSQN ->
                {loop, Acc0};
            SQN when SQN > MaxSQN ->
                {stop, Acc0};
            _ ->
                {VBin, ValSize} = ExtractFun(ValueInJournal),
                % VBin may already be a term
                {Obj, IdxSpecs} = leveled_codec:split_inkvalue(VBin),
                case SQN of
                    MaxSQN ->
                        {stop,
                            {MinSQN,
                            MaxSQN,
                            [{InkTag, PK, SQN, Obj, IdxSpecs, ValSize}
                                |LoadItems]}};
                    _ ->
                        {loop,
                            {MinSQN,
                            MaxSQN,
                            [{InkTag, PK, SQN, Obj, IdxSpecs, ValSize}
                                |LoadItems]}}
                end
        end
    end.


delete_path(DirPath) ->
    ok = filelib:ensure_dir(DirPath),
    {ok, Files} = file:list_dir(DirPath),
    [file:delete(filename:join([DirPath, File])) || File <- Files],
    file:del_dir(DirPath).

-spec maybelog_put_timing(
        leveled_monitor:monitor(),
        leveled_monitor:timing(),
        leveled_monitor:timing(),
        leveled_monitor:timing(),
        pos_integer()) -> ok.
maybelog_put_timing(_Monitor, no_timing, no_timing, no_timing, _Size) ->
    ok;
maybelog_put_timing({Pid, _StatsFreq}, InkTime, PrepTime, MemTime, Size) ->
    leveled_monitor:add_stat(
        Pid, {bookie_put_update, InkTime, PrepTime, MemTime, Size}).

-spec maybelog_head_timing(
        leveled_monitor:monitor(),
        leveled_monitor:timing(),
        leveled_monitor:timing(),
        boolean(),
        boolean()) -> ok.
maybelog_head_timing(_Monitor, no_timing, no_timing, _NF, _CH) ->
    ok;
maybelog_head_timing({Pid, _StatsFreq}, FetchTime, _, true, _CH) ->
    leveled_monitor:add_stat(
        Pid, {bookie_head_update, FetchTime, not_found, 0});
maybelog_head_timing({Pid, _StatsFreq}, FetchTime, RspTime, _NF, CH) ->
    CH0 = case CH of true -> 1; false -> 0 end,
    leveled_monitor:add_stat(
        Pid, {bookie_head_update, FetchTime, RspTime, CH0}).

-spec maybelog_get_timing(
    leveled_monitor:monitor(),
    leveled_monitor:timing(),
    leveled_monitor:timing(),
    boolean()) -> ok.
maybelog_get_timing(_Monitor, no_timing, no_timing, _NF) ->
    ok;
maybelog_get_timing({Pid, _StatsFreq}, HeadTime, _BodyTime, true) ->
    leveled_monitor:add_stat(Pid, {bookie_get_update, HeadTime, not_found});
maybelog_get_timing({Pid, _StatsFreq}, HeadTime, BodyTime, false) ->
    leveled_monitor:add_stat(Pid, {bookie_get_update, HeadTime, BodyTime}).


-spec maybelog_snap_timing(
    leveled_monitor:monitor(),
    leveled_monitor:timing(),
    leveled_monitor:timing()) -> ok.
maybelog_snap_timing(_Monitor, no_timing, no_timing) ->
    ok;
maybelog_snap_timing({Pid, _StatsFreq}, BookieTime, PCLTime) ->
    leveled_monitor:add_stat(Pid, {bookie_snap_update, BookieTime, PCLTime}).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

reset_filestructure() ->
    RootPath  = "test/test_area",
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
                                true, true, false, false, false}),
    KeyHashList3 = HTFolder3(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList3)),

    FoldHeadsFun2 =
        fun(B, K, ProxyV, Acc) ->
            {proxy_object,
                MD,
                _Size,
                _Fetcher} = binary_to_term(ProxyV),
            {Hash, _Size, _UserDefinedMD} = MD,
            [{B, K, Hash}|Acc]
        end,

    {async, HTFolder4} =
        book_returnfolder(Bookie1,
                            {foldheads_allkeys, 
                                ?STD_TAG, 
                                FoldHeadsFun2,
                                false, false, false, false, false}),
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
    _ = book_returnactors(Bookie1),
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
                                true, true,
                                false, false, false}),
    KeyHashList2A = HTFolder2A(),
    {async, HTFolder2B} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                all,
                                FoldHeadsFun,
                                true, false,
                                false, false, false}),
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
                                true, false,
                                false, false, false}),
    KeyHashList2C = HTFolder2C(),
    {async, HTFolder2D} =
        book_returnfolder(Bookie1,
                            {foldheads_bybucket,
                                ?STD_TAG,
                                "BucketB",
                                {"Key", "Keyzzzzz"},
                                FoldHeadsFun,
                                true, true,
                                false, false, false}),
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
                                        true, false,
                                        false, false, false}),
            KeyHashList2E = HTFolder2E(),
            {async, HTFolder2F} =
                book_returnfolder(Bookie1,
                                    {foldheads_bybucket,
                                        ?STD_TAG,
                                        "BucketB",
                                        {SplitIntStart, "Key|"},
                                        FoldHeadsFun,
                                        true, false,
                                        false, false, false}),
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

small_cachesize_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 1}]),
    ok = leveled_bookie:book_close(Bookie1).


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

undefined_rootpath_test() ->
    Opts = [{max_journalsize, 1000000}, {cache_size, 500}],
    error_logger:tty(false),
    R = gen_server:start(?MODULE, [set_defaults(Opts)], []),
    ?assertMatch({error, no_root_path}, R),
    error_logger:tty(true).
        

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
    timer:sleep(?LONG_RUNNING div 1000 + 100),
    ok = maybe_longrunning(SW, put).

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).

erase_journal_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}, 
                                {max_journalsize, 50000}, 
                                {cache_size, 100}]),
    ObjL1 = generate_multiple_objects(500, 1),
    % Put in all the objects with a TTL in the future
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, "Bucket", K, V, S, ?STD_TAG)
        end,
        ObjL1),
    lists:foreach(
        fun({K, V, _S}) ->
            {ok, V} = book_get(Bookie1, "Bucket", K, ?STD_TAG)
        end,
        ObjL1),
    
    CheckHeadFun =
        fun(Book) -> 
            fun({K, _V, _S}, Acc) ->
                case book_head(Book, "Bucket", K, ?STD_TAG) of 
                    {ok, _Head} -> Acc;
                    not_found -> Acc + 1
                end
            end
        end,
    HeadsNotFound1 = lists:foldl(CheckHeadFun(Bookie1), 0, ObjL1),
    ?assertMatch(0, HeadsNotFound1),

    ok = book_close(Bookie1),
    io:format("Bookie closed - clearing Journal~n"),
    leveled_inker:clean_testdir(RootPath ++ "/" ++ ?JOURNAL_FP),
    {ok, Bookie2} = book_start([{root_path, RootPath}, 
                                {max_journalsize, 5000}, 
                                {cache_size, 100}]),
    HeadsNotFound2 = lists:foldl(CheckHeadFun(Bookie2), 0, ObjL1),
    ?assertMatch(500, HeadsNotFound2),
    ok = book_destroy(Bookie2).

sqnorder_fold_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ok = book_put(Bookie1, 
                    <<"B">>, <<"K1">>, {value, <<"V1">>}, [], 
                    ?STD_TAG),
    ok = book_put(Bookie1, 
                    <<"B">>, <<"K2">>, {value, <<"V2">>}, [], 
                    ?STD_TAG),
    
    FoldObjectsFun = fun(B, K, V, Acc) -> Acc ++ [{B, K, V}] end,
    {async, ObjFPre} =
        book_objectfold(Bookie1,
                        ?STD_TAG, {FoldObjectsFun, []}, true, sqn_order),
    {async, ObjFPost} =
        book_objectfold(Bookie1,
                        ?STD_TAG, {FoldObjectsFun, []}, false, sqn_order),
    
    ok = book_put(Bookie1, 
                    <<"B">>, <<"K3">>, {value, <<"V3">>}, [], 
                    ?STD_TAG),

    ObjLPre = ObjFPre(),
    ?assertMatch([{<<"B">>, <<"K1">>, {value, <<"V1">>}},
                    {<<"B">>, <<"K2">>, {value, <<"V2">>}}], ObjLPre),
    ObjLPost = ObjFPost(),
    ?assertMatch([{<<"B">>, <<"K1">>, {value, <<"V1">>}},
                    {<<"B">>, <<"K2">>, {value, <<"V2">>}},
                    {<<"B">>, <<"K3">>, {value, <<"V3">>}}], ObjLPost),
    
    ok = book_destroy(Bookie1).

sqnorder_mutatefold_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath},
                                    {max_journalsize, 1000000},
                                    {cache_size, 500}]),
    ok = book_put(Bookie1, 
                    <<"B">>, <<"K1">>, {value, <<"V1">>}, [], 
                    ?STD_TAG),
    ok = book_put(Bookie1, 
                    <<"B">>, <<"K1">>, {value, <<"V2">>}, [], 
                    ?STD_TAG),
    
    FoldObjectsFun = fun(B, K, V, Acc) -> Acc ++ [{B, K, V}] end,
    {async, ObjFPre} =
        book_objectfold(Bookie1,
                        ?STD_TAG, {FoldObjectsFun, []}, true, sqn_order),
    {async, ObjFPost} =
        book_objectfold(Bookie1,
                        ?STD_TAG, {FoldObjectsFun, []}, false, sqn_order),
    
    ok = book_put(Bookie1, 
                    <<"B">>, <<"K1">>, {value, <<"V3">>}, [], 
                    ?STD_TAG),

    ObjLPre = ObjFPre(),
    ?assertMatch([{<<"B">>, <<"K1">>, {value, <<"V2">>}}], ObjLPre),
    ObjLPost = ObjFPost(),
    ?assertMatch([{<<"B">>, <<"K1">>, {value, <<"V3">>}}], ObjLPost),
    
    ok = book_destroy(Bookie1).

check_notfound_test() ->
    ProbablyFun = fun() -> probably end,
    MissingFun = fun() -> missing end,
    MinFreq = lists:foldl(fun(_I, Freq) ->
                                {false, Freq0} = 
                                    check_notfound(Freq, ProbablyFun),
                                Freq0
                            end,
                            100,
                            lists:seq(1, 5000)), 
                                % 5000 as needs to be a lot as doesn't decrement
                                % when random interval is not hit
    ?assertMatch(?MIN_KEYCHECK_FREQUENCY, MinFreq),
    
    ?assertMatch({true, ?MAX_KEYCHECK_FREQUENCY}, 
                    check_notfound(?MAX_KEYCHECK_FREQUENCY, MissingFun)),
    
    ?assertMatch({false, 0}, check_notfound(0, MissingFun)).

-endif.
