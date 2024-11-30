%%%============================================================================
%%% File paths
%%%============================================================================
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").
%%%============================================================================

%%%============================================================================
%%% Configurable startup defaults
%%%============================================================================
-define(CACHE_SIZE, 2500).
-define(MAX_CACHE_MULTTIPLE, 2).
-define(MIN_CACHE_SIZE, 100).
-define(MIN_PCL_CACHE_SIZE, 400).
-define(MAX_PCL_CACHE_SIZE, 28000). 
    % This is less than actual max - but COIN_SIDECOUNT
-define(ABSOLUTEMAX_JOURNALSIZE, 4000000000).
-define(COMPRESSION_METHOD, lz4).
-define(COMPRESSION_POINT, on_receipt).
-define(COMPRESSION_LEVEL, 1).
-define(LOG_LEVEL, info).
-define(DEFAULT_DBID, 65536).
-define(OPEN_LASTMOD_RANGE, {0, infinity}).
-define(SNAPTIMEOUT_SHORT, 900). % 15 minutes
-define(SNAPTIMEOUT_LONG, 43200). % 12 hours
-define(SST_PAGECACHELEVEL_NOLOOKUP, 1).
-define(SST_PAGECACHELEVEL_LOOKUP, 4).
-define(DEFAULT_STATS_PERC, 10).
-define(DEFAULT_SYNC_STRATEGY, none).
%%%============================================================================

%%%============================================================================
%%% Non-configurable startup defaults
%%%============================================================================
-define(MAX_SSTSLOTS, 256).
-define(MAX_MERGEBELOW, 24).
-define(LOADING_PAUSE, 1000).
-define(LOADING_BATCH, 1000).
-define(CACHE_SIZE_JITTER, 25).
-define(JOURNAL_SIZE_JITTER, 20).
-define(LONG_RUNNING, 1000000).
    % An individual task taking > 1s gets a specific log
-define(MAX_KEYCHECK_FREQUENCY, 100).
-define(MIN_KEYCHECK_FREQUENCY, 1).
-define(MAX_LEVELS, 8).
    %% Should equal the length of the LEVEL_SCALEFACTOR
-define(CACHE_TYPE, skpl).
%%%============================================================================

%%%============================================================================
%%% Tags
%%%============================================================================
-define(RIAK_TAG, o_rkv).
    %% Tag to be used on standard Riak KV objects
-define(STD_TAG, o).
    %% Tag to be used on K/V objects for non-Riak purposes
-define(IDX_TAG, i).
    %% Tag used for secondary index keys
-define(HEAD_TAG, h).
    %% Tag used for head-only objects

-define(INKT_STND, stnd).
    %% Inker key type used for 'normal' objects
-define(INKT_MPUT, mput).
    %% Inker key type used for 'batch' objects
-define(INKT_KEYD, keyd). 
    %% Inker key type used for objects which contain no value, only key changes
    %% This is used currently for objects formed under a 'retain' strategy
    %% on Inker compaction
-define(INKT_TOMB, tomb).
    %% Inker key type used for tombstones
%%%============================================================================


%%%============================================================================
%%% Shared records
%%%============================================================================
-record(level,
                        {level :: integer(),
                        is_basement = false :: boolean(),
                        timestamp :: integer()}).                      

-record(manifest_entry,
                        {start_key :: tuple() | undefined,
                        end_key :: tuple() | undefined,
                        owner :: pid()|list(),
                        filename :: string() | undefined,
                        bloom = none :: leveled_ebloom:bloom() | none}).

-record(cdb_options,
                        {max_size :: pos_integer() | undefined,
                        max_count :: pos_integer() | undefined,
                        file_path :: string() | undefined,
                        waste_path :: string() | undefined,
                        binary_mode = false :: boolean(),
                            % Default set by bookie to be `true`
                            % `false` set here due to legacy of unit tests
                            % using non-binary keys
                        sync_strategy = ?DEFAULT_SYNC_STRATEGY,
                        log_options = leveled_log:get_opts() 
                            :: leveled_log:log_options(),
                        monitor = {no_monitor, 0}
                            :: leveled_monitor:monitor()}).

-record(sst_options,
                        {press_method = ?COMPRESSION_METHOD
                            :: leveled_sst:press_method(),
                        press_level = ?COMPRESSION_LEVEL :: non_neg_integer(),
                        log_options = leveled_log:get_opts() 
                            :: leveled_log:log_options(),
                        max_sstslots = ?MAX_SSTSLOTS :: pos_integer()|infinity,
                        max_mergebelow = ?MAX_MERGEBELOW :: pos_integer()|infinity,
                        pagecache_level = ?SST_PAGECACHELEVEL_NOLOOKUP
                            :: pos_integer(),
                        monitor = {no_monitor, 0}
                            :: leveled_monitor:monitor()}).

-record(inker_options,
                        {cdb_max_size :: integer() | undefined,
                        root_path :: string() | undefined,
                        cdb_options = #cdb_options{} :: #cdb_options{},
                        start_snapshot = false :: boolean(),
                        bookies_pid :: pid() | undefined,
                        source_inker :: pid() | undefined,
                        reload_strategy = [] :: list(),
                        waste_retention_period :: integer() | undefined,
                        compression_method = ?COMPRESSION_METHOD
                            :: lz4|native|none,
                        compress_on_receipt = false :: boolean(),
                        max_run_length,
                        singlefile_compactionperc :: float()|undefined,
                        maxrunlength_compactionperc :: float()|undefined,
                        score_onein = 1 :: pos_integer(),
                        snaptimeout_long :: pos_integer() | undefined,
                        monitor = {no_monitor, 0}
                            :: leveled_monitor:monitor()}).

-record(penciller_options,
                        {root_path :: string() | undefined,
                        sst_options = #sst_options{} :: #sst_options{},
                        max_inmemory_tablesize :: integer() | undefined,
                        start_snapshot = false :: boolean(),
                        snapshot_query,
                        bookies_pid :: pid() | undefined,
                        bookies_mem :: tuple() | undefined,
                        source_penciller :: pid() | undefined,
                        snapshot_longrunning = true :: boolean(),
                        compression_method = ?COMPRESSION_METHOD
                            :: lz4|native|none,
                        levelzero_cointoss = false :: boolean(),
                        snaptimeout_short :: pos_integer() | undefined,
                        snaptimeout_long :: pos_integer() | undefined,
                        monitor = {no_monitor, 0}
                            :: leveled_monitor:monitor()}).

-record(iclerk_options,
                        {inker :: pid() | undefined,
                         max_run_length :: integer() | undefined,
                         cdb_options = #cdb_options{} :: #cdb_options{},
                         waste_retention_period :: integer() | undefined,
                         compression_method = ?COMPRESSION_METHOD
                            :: lz4|native|none,
                         singlefile_compactionperc :: float()|undefined,
                         maxrunlength_compactionperc :: float()|undefined,
                         score_onein = 1 :: pos_integer(),
                         reload_strategy = [] :: list()}).
%%%============================================================================