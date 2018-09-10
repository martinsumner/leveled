# Starting Leveled

There are a number of options that can be passed in when starting Leveled, this is an explainer of these options and what they do.  The options are passed in a list of `{option_name, Option}` tuples on startup.

## Head Only

Starting with `{head_only, true}` (defaults to false), will start Leveled in a special mode.  In this mode Leveled works a lot more like Leveldb, in that the Journal is just a buffer of recent writes to be used to recover objects on startup.  The actual object value is now stored in the LSM tree itself rather than in the Journal.

Objects need to be put into the Leveled store using the `book_mput/2` or `book_mput/3` when running in head_only mode.

This mode was specifically added to support Leveled's use as a dedicated aae_store in the `kv_index_tictactree` library.  It may be extensible for other uses where objects are small.

There is no current support for running leveled so that it supports both `head` objects which are stored entirely in the Ledger, along side other objects stored as normal split between the Journal and the Ledger.  Setting `head_only` fundamentally changes the way the store works.

## Max Journal Size

The maximum size of an individual Journal file can be set using `{max_journalsize, netger()}`, which sets the size in bytes.  The default value is 1,000,000,000 (~1GB), and the maximum size cannot exceed `2^32`.

If there are smaller objects then lookups within a Journal may get faster if each individual journal file is smaller.

An object is converted before storing in the Journal. The conversion
may involve compression, the duplication of index changes prompted by
the object's storage, and the object's key. The max journal size
should always be bigger than the biggest object you wish to store,
accounting for conversion.

Attempting to store a bigger object will crash the store. Ensure there
is ample room - generally it is anticipated that `max_journalsize`
should be greater than 100 MB, and maximum object size should be less
than 10MB.

If you wish to store bigger objects, scale up the `max_journalsize`
accordingly.

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

However, what if the Ledger had been erased?  This could happen due to some corruption, or perhaps because only the Journal is to be backed up.  As the object has been replaced, the value is not required - however KeyChanges ay be required (such as indexes which are built incrementally across a series of object changes).  So to revert the indexes to their previous state the Key Changes would need to be retained in this case, so the indexes in the Ledger would be correctly rebuilt.

The are three potential strategies:

`skip` - don't worry about this scenario, require the Ledger to be backed up;
`retain` - discard the object itself on compaction but keep the key changes;
`recalc` - recalculate the indexes on reload by comparing the information on the object with the current state of the Ledger (as would be required by the PUT process when comparing IndexSpecs at PUT time).

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
