# Anti-Entropy

Leveled is primarily designed to be a backend for Riak, and Riak has a number of anti-entropy mechanisms for comparing database state within and across clusters.  As part of the ongoing community work to build improvements into a new pure open-source release of Riak, some features have been added directly to Leveled to explore some potential enhancements to anti-entropy.  These features are concerned with:

- Allowing for the database state within in a Leveled store or stores to be compared with an other store or stores which should share a portion of that state;

- Allowing for quicker checking that recent changes to one store or stores have also been received by another store or stores that should be receiving the same changes.

The aim is to use these as new backend capabilities, combined with new coverage FSM query behaviour, to allow for new Riak anti-entropy mechanisms with the following features:

- Comparison can be made between clusters with different ring-sizes - comparison is not coupled to partitioning.

- Comparison can use a consistent approach to compare state within and between clusters.

- Comparison does not rely on duplication of database state to a separate anti-entropy database, with further anti-entropy required to manage state variance between the actual stores and anti-entropy stores.

- Comparison of state can be abstracted from Riak specific implementation so that mechanisms to compare between Riak clusters can be re-used to compare between a Riak cluster and another database store.  Coordination with another data store (e.g. Solr) can be controlled by the Riak user not just the Riak developer.

- Comparison can be controlled at a bucket level, so that buckets can be configured to be either specifically whitelisted into the anti-entropy scope, or blacklisted from it - with the option to support different schedules for anti-entropy operations for different buckets when whitelisting is used.

- Through the use of key types allow for flexibility to calculate anti-entropy mechanisms in a way specific to the type of object being stored (e.g. support alternative mechanisms for some CRDT types).

## Merkle Trees

Riak has historically used [Merkle trees](https://en.wikipedia.org/wiki/Merkle_tree) as a way to communicate state efficiently between actors.  Merkle trees have been designed to be cryptographically secure so that they don't leak details of the individual transactions themselves.  This strength is useful in many Merkle Tree use cases, and is part derived from the use of concatenation when calculating branch hashes from leaf hashes:

> A hash tree is a tree of hashes in which the leaves are hashes of data blocks in, for instance, a file or set of files. Nodes further up in the tree are the hashes of their respective children. For example, in the picture hash 0 is the result of hashing the concatenation of hash 0-0 and hash 0-1. That is, hash 0 = hash( hash 0-0 + hash 0-1 ) where + denotes concatenation.

A side effect of the concatenation decision is that trees cannot be calculated incrementally, when elements are not ordered by segment.  To calculate the hash of an individual leaf (or segment), the hashes of all the elements under that leaf must be accumulated first.  In the case of the leaf segments in Riak, the leaf segments are made up of a hash of the concatenation of {Key, Hash} pairs under that leaf:

``hash([{K1, H1}, {K2, H2} .. {Kn, Hn}])``

This requires all of the keys and hashes need to be pulled into memory to build the hashtree - unless the tree is being built segment by segment.  The Riak hashtree data store is therefore ordered by segment so that it can be incrementally built.  The segments which have had key changes are tracked, and at exchange time all "dirty segments" are re-scanned in the store segment by segment, so that the hashtree can be rebuilt.

## Tic-Tac Trees

Anti-entropy in leveled is supported using the leveled_tictac module.  This module uses a less secure form of merkle trees that don't prevent information from leaking out, or make the tree tamper-proof, but allow for the trees to be built incrementally, and trees built incrementally to be merged.  These Merkle trees we're calling Tic-Tac Trees after the [Tic-Tac language](https://en.wikipedia.org/wiki/Tic-tac) to fit in with Bookmaker-based naming conventions of leveled.  The Tic-Tac language has been historically used on racecourses to communicate the state of the market between participants; although the more widespread use of mobile communications means that the use of Tic-Tac is petering out, and rather like Basho employees, there are now only three Tic-Tac practitioners left.

The change from secure Merkle trees is simply to XOR and not hashing/concatenation for combining hashes, combined with using trees of fixed sizes, so that tree merging can also be managed through XOR operations.  So a segment leaf is calculated from:

``hash(K1, H1) XOR hash(K2, H2) XOR ... hash(Kn, Hn)``

The Keys and hashes can now be combined in any order with any grouping.  The use of XOR instead of concatenation is [discouraged in secure Merkle Trees](https://security.stackexchange.com/questions/89847/can-xor-be-used-in-a-merkle-tree-instead-of-concatenation) but is not novel in its use within [trees focused on anti-entropy](http://distributeddatastore.blogspot.co.uk/2013/07/cassandra-using-merkle-trees-to-detect.html).

This enables two things:

- The tree can be built incrementally when scanning across a store not in segment order (i.e. scanning across a store in key order) without needing to hold an state in memory beyond the fixed size of the tree.

- Two trees from stores with non-overlapping key ranges can be merged to reflect the combined state of that store i.e. the trees for each store can be built independently and in parallel and the subsequently merged without needing to build an interim view of the combined state.

It is assumed that the trees will only be transferred securely between trusted actors already permitted to view, store and transfer the real data: so the loss of cryptographic strength of the tree is irrelevant to the overall security of the system.

## Recent and Whole

### Current Riak AAE

Anti-entropy in Riak is a dual-track process:

- there is a need to efficiently and rapidly provide an update on store state that represents recent additions;

- there is a need to ensure that the anti-entropy view of state represents the state of the whole database.

Within the current Riak AAE implementation, recent changes are supported by having a separate anti-entropy store organised by segments so that the Merkle tree can be updated incrementally to reflect recent changes.  The Merkle tree produced following these changes should then represent the whole state of the database.  

However as the view of the whole state is maintained in a different store to that holding the actual data: there is an entropy problem between the actual store and the AAE store e.g. data could be lost from the real store, and go undetected as it is not lost from the AAE store.  So periodically the AAE store is rebuilt by scanning the whole of the real store.  This rebuild can be an expensive process, and the cost is commonly controlled through performing this task infrequently.  Prior to the end of Basho there were changes pending in develop to better throttle and schedule these updates - through the riak_kv_sweeper.

The AAE store also needs to be partially scanned on a regular basis to update the current view of the Merkle tree.  If a vnode has 100M keys, and there has been 1000 updates since the last merkle tree was updated - then there will need to be o(1000) seeks across subsets of the store returning o(100K) keys in total.  As the store grows, the AAE store can grow to a non-trivial size, and have an impact on the page-cache and disk busyness.

The AAE store is re-usable for checking consistency between databases, but with the following limitations:

- the two stores need to be partitioned equally, constraining replication to other database technologies, and preventing replication from being used as an approach to re-partitioning (ring re-sizing).

- the aae store is not split by bucket, and so supporting replication configured per bucket is challenging.  

### Proposed Leveled AAE

There are three primary costs with scanning over the whole database:

- the impact on the page cache as all keys and values have to be read from disk, including not-recently used values;

- the overall I/O load (primarily disk-related) of scanning the full database from disk;

- the overall I/O load (primarily network-related) of streaming results from the fold.

The third cost can be addressed by the fold output being an incrementally updatable tree of a fixed size; i.e. if the fold builds a Tic-Tac tree and doesn't stream results (like list keys), and guarantees a fixed size output both from a single partition and following merging across multiple partitions.  Within Leveled the first two costs are reduced by design due to the separation of Keys and Metadata from the object value, reducing significantly the workload associated with such a scan - especially where values are large.

The [testing of traditional Riak AAE](https://github.com/martinsumner/leveled/blob/master/docs/VOLUME.md#leveled-aae-rebuild-with-journal-check) already undertaken has shown that scanning the database is not necessarily such a big issue in Leveled.  So it does seem potentially feasible to scan the store on a regular basis.  The testing of Leveldb with the riak_kv_sweeper feature shows that with the improved throttling more regular scanning is also possible here: testing with riak_kv_sweeper managed to achieve 10 x the number of sweeps, with only a 9% drop in throughput.

A hypothesis is proposed that regular scanning of the full store to produce a Tic-Tac tree is certainly feasible in Leveled, but also potentially tolerable in other back-ends.  However, <b>frequent</b> scanning may still be impractical.  It is therefore suggested that there should be an alternative form of anti-entropy that can be run in addition to scanning, that is lower cost and can be run be frequently in support of scanning to produce Tic-Tac trees.  This supporting anti-entropy should focus on the job of verifying that <b>recent</b> changes have been received.  So there would be two anti-entropy mechanisms, one which can be run frequently (minutes) to check for the receipt of recent changes, and one that can be run regularly but infrequently (hours/days) to check that full database state is consistent.

To provide a check on recent changes it is proposed to add a temporary index within the store, with an entry for each change that is built from a rounded last modified date and the hash of the value, so that the index can be scanned to form a Tic-Tac tree of recent changes.  This assumes that each object has a Last Modified Date that is consistent (for that version) across all points where that particular version is stored, to use as the field name for the index.  The term of the index is based on the segment ID (for the tree) and the hash of the value. This allows for a scan to build a tree of changes for a given range of modified dates, as well as a scan for keys and hashes to be returned for a given segment ID and date range.

Within the Leveled the index can be made temporary by giving the entry a time-to-live, independent of any object time to live.  So once the change is beyond the timescale in which the operator wishes to check for recent changes, it will naturally be removed from the database (through deletion on the next compaction event that hits the entry in the Ledger).

Hence overall this should give:

- A low cost mechanism for checking for the distribution of recent changes.

- A mechanism for infrequently comparing overall state that is naturally consistent with the actual store state, without compromising operational stability of the store.

- No additional long-term overhead (i.e. duplicate key store for anti-entropy).


## Leveled Implementation

### Full Database Anti-Entropy

There are two parts to the full database anti-entropy mechanism:  the Tic-Tac trees implemented in the leveled_tictac modules; and the queries required to build the trees available through the book_returnfolder function.  There are two types of queries supported -

```
{tictactree_obj,
            {Tag, Bucket, StartKey, EndKey, CheckPresence},
            TreeSize,
            PartitionFilter}
```

```
{tictactree_idx,
            {Bucket, IdxField, StartValue, EndValue},
            TreeSize,
            PartitionFilter}
```

The tictactree_obj folder produces a Tic-Tac tree form a fold across the objects (or more precisely the heads of the objects in the Ledger)m using the constraints Tag, Bucket, StartKey and EndKey.  CheckPresence can be used to require the folder to confirm if the value is present in the Journal before including it in the tree - this will slow down the fold significantly, but protect from corruption in the Journal not represented in the Ledger.  The partition filter cna be used where the store holds data from multiple partitions, and only data form a subset of partitions should be included, with the partition filter being a function on the Bucket and Key to make that decision.

The tictactree_idx folder produces a tic-Tac tree from a range of an index, and so can be used as with tictactree_obj but for checking that an index is consistent between coverage offsets or between databases.

These two folds are tested in the tictac_SUITE test suite in the ``many_put_compare`` and ``index_compare`` tests.

### Near Real-Time Anti-Entropy

The near real-time anti-entropy process can be run in two modes: blacklisting and whitelisting.  In blacklisting mode, specific buckets can be excluded from anti-entropy management, and all buckets not excluded are managed in a single "$all" bucket.  Anti-entropy queries will need to always be requested against the "$all" bucket.  In whitelisting mode, only specific buckets are included in anti-entropy management.  Anti-entropy queries will need to be requested separately for each whitelisted bucket, and may be scheduled differently for each bucket.

The index entry is then of the form:

- Tag: ?IDX_TAG

- Bucket: Bucket

- Field: Last Modified Date (rounded down to a configured unit in minutes)

- Term: Segment ++ "." ++ Hash

- Key : Key

In blacklist mode the Bucket will be $all, and the Key will actually be a {Bucket, Key} pair.

The index entry is given a TTL of a configurable amount (e.g. 1 hour) - and no index entry may be added if the change is already considered to be too far in the past.  The index entry is added to the Ledger in the same transaction as the other changes, and will be re-calculated and re-added out of the Journal under restart conditions where the change has not reached a persisted state in the Ledger prior to the close.

The near real-time entropy index currently has four ct tests:

- recent_aae_noaae (confirming loading a store with real-time aae disabled has no impact);

- recent_aae_allaae (confirming that a single store loaded with data can be compared with the a store where the same data is spread across three leveled instances - with all buckets covered by anti-entropy);

- recent_aae_bucketaae (confirming that a single store loaded with data can be compared with the a store where the same data is spread across three leveled instances - with a single buckets covered by anti-entropy);

- recent_aae_expiry (confirming that aae index will expire).

### Clock Drift

The proposed near-real-time anti-entropy mechanism depends on a timestamp, so ultimately some false positives and false negatives are unavoidable - especially if clock drift is large.  The assumption is though:

- that this method is never a single dependency for ensuring consistency, it is supported by other mechanisms to further validate, that would detect false negatives.

- that recovery from false positives will be safely implemented, so that a falsely identified discrepancy is validated before a change is made (e.g. repair through read-repair).

Even with this mitigation, the volume of false positives and negatives needs to be controlled, in particular where clock drift is small (i.e. measured in seconds), and hence likely.  If the object has a last modified date set in one place, as with Riak, there is no issue with different actors seeing a different last modified date for the same change.  However, as the index object should expire the risk exists that the store will set an inappropriate expiry time, or even not index the object as it considers the object to be a modification too far in the past.  The Near Real-Time AAE process has the concept of unit minutes, which represents the level of granularity all times will be rounded to.  All expiry times are set with a tolerance equal to the unit minutes, to avoid false positives or negatives when clock drift is small.

## Alternative Approaches

The approach considered for Leveled has been to modify the Merkle trees used to ease their implementation, as well as specifically separating out anti-entropy for recent changes as a different problem to long-term anti-entropy of global state.

There is [emergent research ongoing](http://haslab.uminho.pt/tome/files/global_logical_clocks.pdf) to try and leverage the use of dotted version vectors at a node level to improve the efficiency of managing key-level consistency, reduce the risks associated with deletes (without permanent tombstones), but also provide an inherently integrated approach to active anti-entropy.

The Global Logical Clock approach does assume that durability is not mutable:

> Nodes have access to durable storage; nodes can crash but
eventually will recover with the content of the durable storage as at the time of
the crash.

It is strongly preferred that our anti-entropy approach can deal with the loss of data that had been persisted to disk (e.g. perhaps through administrative error or disk failure), not just the loss of updates not received.  This doesn't mean that such an approach is invalid as:

- the near real-time approach element of anti-entropy *is* only focused on the loss of updates not received;

- it may be possible to periodically rebuild the state of bitmapped version vectors based on the data found on disk (similarly to the current hashtree rebuild process in Riak AAE).

Some further consideration has been given to using a version of this Global Logical Clock approach to managing near-real-time anti-entropy only.  More understanding of the approach is required to progress though, in particular:

- How to manage comparisons between clusters with different partitioning algorithms (e.g different ring-sizes);

- How to discover key IDs from missing dots where the controlling node for the update has recently failed.

This likely represent gaps in current understanding, rather than flaws in the approach.  The evolution of this research will be tracked with interest.
