
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