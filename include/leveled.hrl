
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
                        {max_size :: integer()}).

-record(inker_options,
                        {cdb_max_size :: integer(),
                        root_path :: string(),
                        cdb_options :: #cdb_options{}}).

-record(penciller_options,
                        {root_path :: string(),
                        max_inmemory_tablesize :: integer()}).

-record(bookie_options,
                       {root_path :: string(),
                        cache_size :: integer(),
                        metadata_extractor :: function(),
                        indexspec_converter :: function()}).

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
          