
-record(sft_options,
                        {wait = true :: boolean(),
                        expire_tombstones = false :: boolean()}).

-record(penciller_work,
                        {next_sqn :: integer(),
                        clerk :: pid(),
                        src_level :: integer(),
                        manifest :: list(),
                        start_time :: tuple(),
                        ledger_filepath :: string(),
                        manifest_file :: string(),
                        new_manifest :: list(),
                        unreferenced_files :: list()}).

-record(manifest_entry,
                        {start_key :: tuple(),
                        end_key :: tuple(),
                        owner :: pid(),
                        filename :: string()}).

-record(cdb_options,
                        {max_size :: integer(),
                        file_path :: string(),
                        binary_mode = false :: boolean()}).

-record(inker_options,
                        {cdb_max_size :: integer(),
                        root_path :: string(),
                        cdb_options :: #cdb_options{},
                        start_snapshot = false :: boolean(),
                        source_inker :: pid()}).

-record(penciller_options,
                        {root_path :: string(),
                        max_inmemory_tablesize :: integer(),
                        start_snapshot = false :: boolean(),
                        source_penciller :: pid()}).

-record(bookie_options,
                       {root_path :: string(),
                        cache_size :: integer(),
                        max_journalsize :: integer(),
                        metadata_extractor :: function(),
                        indexspec_converter :: function(),
                        snapshot_bookie :: pid()}).

-record(iclerk_options,
                        {inker :: pid(),
                        max_run_length :: integer(),
                        cdb_options :: #cdb_options{}}).                                               

%% Temp location for records related to riak

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
          