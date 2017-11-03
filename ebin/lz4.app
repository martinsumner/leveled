{application, 'lz4', [
	{description, "New project"},
	{vsn, "0.1.0"},
	{modules, ['leveled_app','leveled_bookie','leveled_cdb','leveled_codec','leveled_iclerk','leveled_imanifest','leveled_inker','leveled_log','leveled_pclerk','leveled_penciller','leveled_pmanifest','leveled_pmem','leveled_rand','leveled_runner','leveled_sst','leveled_sup','leveled_tictac','leveled_tinybloom','leveled_tree','lz4_nif','lz4f']},
	{registered, []},
	{applications, [kernel,stdlib]},
	{env, []}
]}.