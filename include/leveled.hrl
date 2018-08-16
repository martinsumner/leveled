
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

-record(sft_options,
                        {wait = true :: boolean(),
                        expire_tombstones = false :: boolean(),
                        penciller :: pid()}).

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
                        {max_size :: integer() | undefined,
                         file_path :: string() | undefined,
                         waste_path :: string() | undefined,
                         binary_mode = false :: boolean(),
                         sync_strategy = sync}).

-record(inker_options,
                        {cdb_max_size :: integer() | undefined,
                        root_path :: string() | undefined,
                        cdb_options :: #cdb_options{} | undefined,
                        start_snapshot = false :: boolean(),
		        %% so a snapshot can monitor the bookie and
			%% terminate when it does
                        bookies_pid :: pid() | undefined,
                        source_inker :: pid() | undefined,
                        reload_strategy = [] :: list(),
                        waste_retention_period :: integer() | undefined,
                        compression_method = native :: lz4|native,
                        compress_on_receipt = false :: boolean(),
                        max_run_length,
                        singlefile_compactionperc :: float()|undefined,
                        maxrunlength_compactionperc :: float()|undefined}).

-record(penciller_options,
                        {root_path :: string() | undefined,
                        max_inmemory_tablesize :: integer() | undefined,
                        start_snapshot = false :: boolean(),
                        snapshot_query,
		        %% so a snapshot can monitor the bookie and
			%% terminate when it does
                        bookies_pid :: pid() | undefined,
                        bookies_mem :: tuple() | undefined,
                        source_penciller :: pid() | undefined,
                        snapshot_longrunning = true :: boolean(),
                        compression_method = native :: lz4|native,
                        levelzero_cointoss = false :: boolean()}).

-record(iclerk_options,
                        {inker :: pid() | undefined,
                         max_run_length :: integer() | undefined,
                         cdb_options = #cdb_options{} :: #cdb_options{},
                         waste_retention_period :: integer() | undefined,
                         compression_method = native :: lz4|native,
                         singlefile_compactionperc :: float()|undefined,
                         maxrunlength_compactionperc :: float()|undefined,
                         reload_strategy = [] :: list()}).

-record(recent_aae, {filter :: whitelist|blacklist,
                        % the buckets list should either be a
                        % - whitelist - specific buckets are included, and
                        % entries are indexed by bucket name
                        % - blacklist - specific buckets are excluded, and
                        % all other entries are indexes using the special
                        % $all bucket
                        
                        buckets :: list(),
                        % whitelist or blacklist of buckets to support recent
                        % AAE
                        
                        limit_minutes :: integer(),
                        % how long to retain entries the temporary index for
                        % It will actually be retained for limit + unit minutes
                        % 60 minutes seems sensible
                        
                        unit_minutes :: integer(),
                        % What the minimum unit size will be for a query
                        % e.g. the minimum time duration to be used in range
                        % queries of the aae index
                        % 5 minutes seems sensible
                        
                        tree_size = small :: atom()
                        % Just defaulted to small for now
                        }).

-record(r_content, {
          metadata,
          value :: term()
         }).

-record(r_object, {
          bucket,
          key,
          contents :: [#r_content{}],
          vclock,
          updatemetadata=dict:store(clean, true, dict:new()),
          updatevalue :: term()}).
