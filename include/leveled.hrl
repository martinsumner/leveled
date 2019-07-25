% File paths
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").

%% Tag to be used on standard Riak KV objects
-define(RIAK_TAG, o_rkv).
%% Tag to be used on K/V objects for non-Riak purposes
-define(STD_TAG, o).
%% Tag used for secondary index keys
-define(IDX_TAG, i).
%% Tag used for head-only objects
-define(HEAD_TAG, h).

%% Inker key type used for 'normal' objects
-define(INKT_STND, stnd). 
%% Inker key type used for 'batch' objects
-define(INKT_MPUT, mput).
%% Inker key type used for objects which contain no value, only key changes
%% This is used currently for objects formed under a 'retain' strategy on Inker
%% compaction
-define(INKT_KEYD, keyd). 
%% Inker key type used for tombstones
-define(INKT_TOMB, tomb).

-define(CACHE_TYPE, skpl).


-record(level,
                        {level :: integer(),
                        is_basement = false :: boolean(),
                        timestamp :: integer()}).                      

-record(manifest_entry,
                        {start_key :: tuple() | undefined,
                        end_key :: tuple() | undefined,
                        owner :: pid()|list(),
                        filename :: string() | undefined,
                        bloom :: binary() | none | undefined}).

-record(cdb_options,
                        {max_size :: pos_integer() | undefined,
                        max_count :: pos_integer() | undefined,
                        file_path :: string() | undefined,
                        waste_path :: string() | undefined,
                        binary_mode = false :: boolean(),
                        sync_strategy = sync,
                        log_options = leveled_log:get_opts() 
                            :: leveled_log:log_options()}).

-record(sst_options,
                        {press_method = native
                            :: leveled_sst:press_method(),
                        log_options = leveled_log:get_opts() 
                            :: leveled_log:log_options(),
                        max_sstslots = 256 :: pos_integer(),
                        pagecache_level = 1 :: pos_integer()}).

-record(inker_options,
                        {cdb_max_size :: integer() | undefined,
                        root_path :: string() | undefined,
                        cdb_options = #cdb_options{} :: #cdb_options{},
                        start_snapshot = false :: boolean(),
                        bookies_pid :: pid() | undefined,
                        source_inker :: pid() | undefined,
                        reload_strategy = [] :: list(),
                        waste_retention_period :: integer() | undefined,
                        compression_method = native :: lz4|native,
                        compress_on_receipt = false :: boolean(),
                        max_run_length,
                        singlefile_compactionperc :: float()|undefined,
                        maxrunlength_compactionperc :: float()|undefined,
                        snaptimeout_long :: pos_integer() | undefined}).

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
                        compression_method = native :: lz4|native,
                        levelzero_cointoss = false :: boolean(),
                        snaptimeout_short :: pos_integer() | undefined,
                        snaptimeout_long :: pos_integer() | undefined}).

-record(iclerk_options,
                        {inker :: pid() | undefined,
                         max_run_length :: integer() | undefined,
                         cdb_options = #cdb_options{} :: #cdb_options{},
                         waste_retention_period :: integer() | undefined,
                         compression_method = native :: lz4|native,
                         singlefile_compactionperc :: float()|undefined,
                         maxrunlength_compactionperc :: float()|undefined,
                         reload_strategy = [] :: list()}).
