## Further Features

The store supports all the required Riak backend capabilities.  A number of further features are either partially included, in progress or under consideration:

- Support for HEAD operations, and changes to GET/PUT Riak paths to use HEAD requests where GET requests would be unnecessary.

- Support for "Tags" in keys to allow for different treatment of different key types (i.e. changes in merge actions, metadata retention, caching for lookups).

- A fast "list bucket" capability, borrowing from the HanoiDB implementation, to allow for safe bucket listing in production.

- A bucket size query, which requires traversal only of the Ledger and counts the keys and sums he total on-disk size of the objects within the bucket.

- Support for a specific Riak tombstone tag where reaping of tombstones can be deferred (by many days) i.e. so that a 'keep' deletion strategy can be followed that will eventually garbage collect.


## Outstanding work

There is some work required before LevelEd could be considered production ready:

- A strategy for supervision an restart of processes, in particular for clerks.

- Further functional testing within the context of Riak.

- Introduction of property-based testing.

- Riak modifications to support the optimised Key/Clock scanning for hashtree rebuilds.

- Amend compaction scheduling to ensure that all vnodes do not try to concurrently compact during a single window.

- Improved handling of corrupted files.

- A way of identifying the partition in each log to ease the difficulty of tracing activity when multiple stores are run in parallel.