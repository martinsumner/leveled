# Starting Leveled

There are a number of options that can be passed in when starting Leveled, this is an explainer of these options and what they do.  The options are passed in a list of `{option_name, Option}` tuples on startup.

## Head Only

Starting with `{head_only, true}` (defaults to false), will start Leveled in a special mode.  In this mode Leveled works a lot more like Leveldb, in that the Journal is just a buffer of recent writes to be used to recover objects on startup.  The actual object value is now stored in the LSM tree itself rather than in the Journal.

Objects need to be put into the Leveled store using the `book_mput/2` or `book_mput/3` when running in head_only mode.

This mode was specifically added to support Leveled's use as a dedicated aae_store in the `kv_index_tictactree` library.  It may be extensible for other uses where objects are small.

There is no current support for running leveled so that it supports both `head` objects which are stored entirely in the Ledger, along side other objects stored as normal split between the Journal and the Ledger.  Setting `head_only` fundamentally changes the way the store works.

## Log Level

The log level can be set to `debug`, `info`, `warn`, `error`, `critical`.  The `info` log level will generate a significant amount of logs, and in testing this volume of logs has not currently been shown to be detrimental to performance.  The log level has been set to be 'noisy' in this way to suit environments which make use of log indexers which can consume large volumes of logs, and allow operators freedom to build queries and dashboards from those indexes.

There is no stats facility within leveled, the stats are only available from the logs.  In the future, a stats facility may be added to provide access to this information without having to run at `info` log levels.  [Forced Logs](#Forced Logs) may be used to add stats or other info logs selectively.

## Forced logs

The `forced_logs` option will force a particular log reference to be logged regardless of the log level that has been set.  This can be used to log at a higher level than `info`, whilst allowing for specific logs to still be logged out, such as logs providing sample performance statistics.

## User-Defined Tags

There are 2 primary object tags - ?STD_TAG (o) which is the default, and ?RIAK_TAG (o_rkv).  Objects PUT into the store with different tags may have different behaviours in leveled.

The differences between tags are encapsulated within the `leveled_head` module.  The primary difference of interest is the alternative handling within the function `extract_metadata/3`.  Significant efficiency can be gained in leveled (as opposed to other LSM-stores) through using book_head requests when book_get would otherwise be necessary.  If 80% of the requests are interested in less than 20% of the information within an object, then having that 20% in the object metadata and switching fetch requests to the book_head API, will improve efficiency.  Also folds over heads are much more efficient that folds over objects, so significant improvements can be also be made within folds by having the right information within the metadata.

To make use of this efficiency, metadata needs to be extracted on PUT, and made into leveled object metadata.  For the ?RIAK_TAG this work is within the `leveled_head` module.  If an application wants to control this behaviour for its application, then a tag can be created, and the `leveled_head` module updated.  However, it is also possible to have more dynamic definitions for handling of application-defined tags, by passing in alternative versions of one or more of the functions `extract_metadata/3`, `build_head/1` and `key_to_canonicalbinary/1` on start-up.  These functions will be applied to user-defined tags (but will not override the behaviour for pre-defined tags).

The startup option `override_functions` can be used to manage this override.  [This test](../test/end_to_end/appdefined_SUITE.erl) provides a simple example of using override_functions.

This option is currently experimental.  Issues such as versioning, and handling a failure to consistently start a store with the same override_functions, should be handled by the application.

## Max Journal Size

The maximum size of an individual Journal file can be set using `{max_journalsize, integer()}`, which sets the size in bytes.  The default value is 1,000,000,000 (~1GB). The maximum size, which cannot be exceed is `2^32`.  It is not expected that the Journal Size should normally set to lower than 100 MB, it should be sized to hold many thousands of objects at least.

If there are smaller objects, then lookups within a Journal may get faster if each individual journal file is smaller. Generally there should be o(100K) objects per journal, to control the maximum size of the hash table within each file.  Signs that the journal size is too high may include:

- excessive CPU use and related performance impacts during rolling of CDB files, see log `CDB07`;
- excessive load caused during journal compaction despite tuning down `max_run_length`.

If the store is used to hold bigger objects, the `max_journalsize` may be scaled up accordingly.  Having fewer Journal files (by using larger objects), will reduce the lookup time to find the right Journal during GET requests, but in most circumstances the impact of this improvement is marginal.  The primary impact of fewer Journal files is the decision-making time of Journal compaction (the time to calculate if a compaction should be undertaken, then what should be compacted) will increase.  The timing for making compaction calculations can be monitored through log `IC003`.

## Ledger Cache Size

The option `{cache_size, integer()}` is the number of ledger objects that should be cached by the Bookie actor, before being pushed to the Ledger. Note these are ledger objects (so do not normally contain the actual object value, but does include index changes as separate objects).  The default value is 2500.

## Penciller Cache Size

The option `{max_pencillercachesize, integer()}` sets the approximate number of options that should be kept in the penciller memory before it flushes that memory to disk.  Note, when this limit is reached, the persist may be delayed by a some random jittering to prevent coordination between multiple stores in the same cluster.

The default number of objects is 28,000.  A small number may be required if there is a particular shortage of memory.  Note that this is just Ledger objects (so the actual values are not stored at in memory as part of this cache).

## File Write Sync Strategy

The sync strategy can be set as `{sync_strategy, sync|riak_sync|none}`.  This controls whether each write requires that write to be flushed to disk before the write is acknowledged.  If `none` is set flushing to disk is left in the hands of the operating system.  `riak_sync` is a deprecated option (it is related to the lack of sync flag in OTP 16, and will prompt the flush after the write, rather than as part of the write operation).

The default is `sync`.  Note, that without solid state drives and/or Flash-Backed Write Caches, this option will have a significant impact on performance.

## Waste Retention Period

The waste retention period can be used to keep old journal files that have already been compacted for that period.  This might be useful if there is a desire to backup a machine to be restorable to a particular point in time (by clearing the ledger, and reverting the inker manifest).

The retention period can be set using `{waste_retention_period, integer()}` where the value is the period in seconds.  If left as `undefined` all files will be garbage collected on compaction, and no waste will be retained.

## Reload Strategy

The purpose of the reload strategy is to define the behaviour at compaction of the Journal on finding a replaced record, in order to manage the behaviour when reloading the Ledger from the Journal.

By default nothing is compacted from the Journal if the SQN of the Journal entry is greater than the largest sequence number which has been persisted in the Ledger.  So when an object is compacted in the Journal (as it has been replaced), it should not need to be replayed from the Journal into the Ledger in the future - as it, and all its related key changes, have already been persisted to the Ledger.

However, what if the Ledger had been erased?  This could happen due to some corruption, or perhaps because only the Journal is to be backed up.  As the object has been replaced, the value is not required - however KeyChanges may be required (such as indexes which are built incrementally across a series of object changes).  So to revert the indexes to their previous state the Key Changes would need to be retained in this case, so the indexes in the Ledger would be correctly rebuilt.

The are three potential strategies:

 - `skip` - don't worry about this scenario, require the Ledger to be backed up;
 - `retain` - discard the object itself on compaction but keep the key changes;
 - `recalc` - recalculate the indexes on reload by comparing the information on the object with the current state of the Ledger (as would be required by the PUT process when comparing IndexSpecs at PUT time).

There is no code for `recalc` at present it is simply a logical possibility.  So to set a reload strategy there should be an entry like `{reload_strategy, [{TagName, skip|retain}]}`.  By default tags are pre-set to `retain`.  If there is no need to handle a corrupted Ledger, then all tags could be set to `skip`.


## Compression Method

Compression method can be set to `native` or `lz4` (i.e. `{compression_method, native|lz4}`).  Native compression will use the compress option in Erlangs native `term_to_binary/2` function, whereas lz4 compression will use a NIF'd Lz4 library.

This is the compression used both when writing an object to the jourrnal, and a block of keys to the ledger.  There is a throughput advantage of around 2 - 5 % associated with using `lz4` compression.

## Compression Point

Compression point can be set using `{compression_point, on_receipt|on_compact}`.  This refers only to compression in the Journal, key blocks are always compressed in the ledger.  The option is whether to accept additional PUT latency by compressing as objects are received, or defer the compressing of objects in the Journal until they are re-written as part of a compaction (which may never happen).

## Root Path

The root path is the name of the folder in which the database has been (or should be) persisted.

## Journal Compaction

The compaction of the Journal, is the process through which the space of replaced (or deleted) objects can be reclaimed from the journal.  This is controlled through the following parameters:

The `compaction_runs_perday` indicates for the leveled store how many times eahc day it will attempt to run a compaction (it is normal for this to be ~= the numbe rof hours per day that compcation is permitted).

The `compaction_low_hour` and `compaction_high_hour` are the hours of the day which support the compaction window - set to 0 and 23 respectively if compaction is required to be a continuous process.

The `max_run_length` controls how many files can be compacted in a single compaction run.  The scoring of files and runs is controlled through `maxrunlength_compactionpercentage` and `singlefile_compactionpercentage`.


## Snapshot Timeouts

There are two snapshot timeouts that can be configured:

- `snapshot_timeout_short`
- `snapshot_timeout_long`

These set the period in seconds before a snapshot which has not shutdown, is declared to have been released - so that any file deletions which are awaiting the snapshot's completion can go ahead.

This covers only silently failing snapshots.  Snapshots that shutdown neatly will be released from locking deleted files when they shutdown.  The 'short' timeout is used for snapshots which support index queries and bucket listing.  The 'long' timeout is used for all other folds (e.g. key lists, head folds and object folds).
