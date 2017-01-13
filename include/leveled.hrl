
%% Tag to be used on standard Riak KV objects
-define(RIAK_TAG, o_rkv).
%% Tag to be used on K/V objects for non-Riak purposes
-define(STD_TAG, o).
%% Tag used for secondary index keys
-define(IDX_TAG, i).

%% Inker key type used for 'normal' objects
-define(INKT_STND, stnd). 
%% Inker key type used for objects which contain no value, only key changes
%% This is used currently for objects formed under a 'retain' strategy on Inker
%% compaction, but could be used for special set-type objects
-define(INKT_KEYD, keyd). 
%% Inker key type used for tombstones
-define(INKT_TOMB, tomb).

-record(sft_options,
                        {wait = true :: boolean(),
                        expire_tombstones = false :: boolean(),
                        penciller :: pid()}).

-record(level,
                        {level :: integer(),
                        is_basement = false :: boolean(),
                        timestamp :: integer()}).                      

-record(manifest_entry,
                        {start_key :: tuple(),
                        end_key :: tuple(),
                        owner :: pid(),
                        filename :: string()}).

-record(cdb_options,
                        {max_size :: integer(),
                        file_path :: string(),
                        waste_path :: string(),
                        binary_mode = false :: boolean(),
                        sync_strategy = sync}).

-record(inker_options,
                        {cdb_max_size :: integer(),
                        root_path :: string(),
                        cdb_options :: #cdb_options{},
                        start_snapshot = false :: boolean(),
                        source_inker :: pid(),
                        reload_strategy = [] :: list(),
                        waste_retention_period :: integer(),
                        max_run_length}).

-record(penciller_options,
                        {root_path :: string(),
                        max_inmemory_tablesize :: integer(),
                        start_snapshot = false :: boolean(),
                        source_penciller :: pid(),
                        levelzero_cointoss = false :: boolean}).

-record(iclerk_options,
                        {inker :: pid(),
                        max_run_length :: integer(),
                        cdb_options = #cdb_options{} :: #cdb_options{},
                        waste_retention_period :: integer(),
                        reload_strategy = [] :: list()}).

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
 