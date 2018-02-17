# Background

Further helpful details on the background to this work can be found in the previous [Anti-Entropy](ANTI_ENTROPY.md) write-up.

The aim is to provide a better answer to the active anti-entropy in Riak.  Specifically, it would be preferable to resolve the following issues:

- Rebuild times.  Both the cost of rebuilds but also the cost in the failure of AAE-dependent processes during rebuilds.  For example, due to the [rate-limiting of rebuilds](https://github.com/basho/riak_kv/blob/2.1.7/src/riak_kv_index_hashtree.erl#L98-L101), rebuilding a single vnode can take o(10) hours.  during this rebuild time, these partitions are not subject to internal AAE, and Multi-Data Centre AAE [may be blocked altogether](https://github.com/basho/riak_repl/issues/772).

- Version inconsistencies.  The process of trying to make the transition from one version of AAE to another smooth, is potentially [too disruptive](https://github.com/basho/riak_kv/issues/1659), and leaves a long legacy in [future versions](https://github.com/basho/riak_kv/issues/1656).

- Cost of AAE.  Every AAE exchange requires in effect a [range scan](https://github.com/basho/riak_core/blob/2.1.9/src/hashtree.erl#L65-L72) in the key-store for every key updated since the last exchange for that partition.  This contributes to a 10% performance overhead associated with running AAE.

- Support for native AAE support within backends.  The Leveled backend can support optimisations for by-segment scanning over its native key-store, potentially rendering the need to keep (and periodically rebuild) a dedicated key-store for AAE unnecessary.  It would be beneficial to have an improved AAE that can exploit this advantage, without preventing the anti-entropy solution form being used on backends that would require a dedicated anti-entropy store.

# Overview Of Needs

The high level changes proposed are:

- Have an AAE solution per vnode where the key-store is both optional (and so can be avoided where native support renders it unnecessary), and has swappable backends (including a pure Erlang alternative to Leveldb).

- Keep the actual AAE Merkle Trees cached using TicTac trees to support updates to the tree without scanning.

- Use per-partition TicTac trees so that the Merkle trees can be merged across vnodes, to make AAE backed synchronisation possible between clusters of different ring sizes.

- Allow rebuilds to take place in parallel to maintaining the old store and cache of the Merkle tree - so exchanges can continue through the rebuild process.

- Formalise the use of dotted version vector as the basis for the object hash to reduce the cost of object binary changes and copying.  Also allow for intelligent comparisons between clusters by exchanging keys & clocks, not just keys & hashes.

- Have the new AAE solution work in parallel to the legacy solution, so that migration is controlled through administration/configuration, and the legacy solution can be permanently forgotten by the cluster.

- Externalise the AAE functions, so that the same functions can be used for synchronisation with different database platforms, without requiring internal changes to Riak.

# AAE design

## Actors, States and Messages

The primary actors

- AAEController (1 per vnode) - gen_fsm

- KeyStore (1 per Controller) - gen_server

- TreeCache (n per Controller) - gen_fsm

- DiskLog (temporary - 1 per Controller) - gen_server

### AAEController

The AAEController will have 3 states: `starting`, `replacing-store`, `replacing-tree` and `steady`.  In all states except `starting` an exchange will be possible.

The AAEController can receive data updates from the vnode in four forms:

- {IndexN, Bucket, Key, CurrentClock, unidentified} for PUTs marshalled via the blind_put (for LWW buckets without 2i support in the backend e.g. LWW -> Bitcask), or when a rehash request has been made for a single object;

- {IndexN, Bucket, Key, CurrentClock, PreviousClock} for standard object updates (PreviousClock will be none for fresh objects);

- {IndexN, Bucket, Key, none, PreviousClock} for actual backend deletes (e.g. post tombstone).

The AAE Controller will handle the casting or calling of these messages by casting a message to the appropriate TreeCache to prompt an update, and then adding the update to a queue to be batch written to the KeyStore.  There is an additional penalty for changes where the PreviousClock is unidentified in that they will require a range scan of the KeyStore to generate the TreeCache update message.

The AAE controller may also receive requests to retrieve the branch or leaf hashes for a given partition TreeCache, as well as trigger rebuilds or rehashes.

### KeyStore

The KeyStore needs to support four operations:

- A batch PUT of objects

- An object fold bounded by a range

- An is_empty check

- A GET of a single object

On startup the AAEController must be informed by the vnode the is_empty status of the actual vnode key store, and this should match the is_empty status of the AAE key store.  If there is a mismatch then the KeyStore must be rebuilt before the AAEController can exit the `starting` state.

As vnode changes are made, these changes should be reflected in batches in the KeyStore.  The Key for the entry in the KeyStore should be a tuple of `{IndexN, SegmentID, Bucket, Key}` where SegmentID is the hash of the Bucket and Key used to map the identifier to a location in the merkle tree.  The Value of the object should be a tuple of `{VectorClock, Hash}`.

Activity in the KeyStore should be optimised for the vast majority of traffic being PUTs. Queries are only used for the KeyStore when:

- Folding over all objects by IndexN and SegmentID to return Keys/Clocks for a given segment;

- Folding over all objects to recalculate an AAE tree for each IndexN;

- Fetching of a specific object by IndexN, SegmentID, Bucket and Key to recalculate a specific hash in the AAE tree when the update to the AAEController has a PreviousClock of `unidentified`.  

When a KeyStore needs to be rebuilt, a new KeyStore is started, but the old KeyStore should continue to receive updates, and be used to fulfil requests for Keys and Clocks and to read `unidentified` Clocks.  Only once the new store is complete, should the old store be destroyed.

A manifest file should be kept to indicate which is the current active store to be used on a restart.

If the vnode backend has native support for the queries required by the AAE KeyStore, then the KeyStore can be run in native mode - ignoring the batch puts, and re-directing the queries to the actual vnode backend.  In native mode `unidentified` previous clocks cannot be supported (and should not be needed).

### TreeCache

There is a TreeCache process for each IndexN managed by the AAEController.  The TreeCache receives changes in the form {SegmentID, HashChange}.  The HashChange is calculated by performing an XOR operation on the hash of the current clock, and the hash of the previous clock.  The SegmentID is calculated from the hash of the <<Bucket, Key>> binary.

The TreeCache process should respond to each update by changing the tree to reflect that update.  

The TreeCache can be in a `starting` state, for instance when a new cache is being built by the AAEController in the `replacing-tree` state.  In the starting state the TreeCache should not be forwarded requests for AAE tree information.


### DiskLog

When both replacing a store and replacing a tree, batches of updates need to be cached until the store or tree is ready to receive them.  For example, rebuilding the store will start a new KeyStore backend and take a snapshot of the real vnode backend to fold and populate the store.  However, the store being rebuilt cannot receive new updates during this rebuild process (without requiring all the updates from the fold to require a read before the PUT) - so the batches of new updates need to be cached in a log, to be applied only once the fold is complete.

## Startup and Shutdown

On shutdown any incomplete batches should be passed to the KeyStore and the KeyStore shutdown.  All functioning TreeCaches should be shutdown, and on shutdown should write a CRC-checked file containing the serialised tree.  At the point the shutdown is requested, the TreeCache may be at a more advanced state than the KeyStore, and if sync_on_write is not enabled in the vnode backend the KeyStore could be in advance of the backend.  To try and protect against situations on startup where the TreeCache reflects a more advanced state than the actual vnode - the TreeCache should not be persisted until the vnode backend and the AAE KeyStore have both successfully closed.

On startup, if shutdown was completed normally, the TreeCaches should be restored from disk, as well as the KeyStore.  Any partially rebuilt KeyStore should be destroyed.

On recovering a TreeCache from disk, the TreeCache process should delete the TreeCache from disk before receiving any update.

If the shutdown was unclean, and there is a KeyStore, but no persisted TreeCache, then before completing startup the AAEController should enforce a fold over the KeyStore to rebuild the TreeCaches.

If the KeyStore has missing updates due to an abrupt shutdown, this will cause (potentially false) repairs of the keys, and the repair will also trigger a rehash.  the rehash should prompt a correction in the AAE KeyStore (through an `unidentified`) previous clock to bring the TreeCache and KeyStore back into line.

## Rebuilds and Rehashes

If an AAE KeyStore is used in non-native mode, periodically the Keystore should be rebuilt, should there be entropy from disk in the actual KeyStore.  This is achieved using the `replacing-store` state in the AAEController.

When replacing a store, the previous version of the store will be kept up to date and used throughout the rebuild process, in order to prevent the blocking of exchanges.  The only exception to this is when a rebuild has been prompted by a conflict of `is_emtpy` properties on startup - in which case the vnode startup process should be paused to allow for the rebuild to complete.

To avoid the need to do reads before writes when updating the AAE KeyStore from the vnode backend fold (so as not to replace a new update with an older snapshot value from the backend) new updates must be parked in a DiskLog process whilst the fold completes.  Once the fold is complete, the rebuild of store can be finished by catching up on updates from the DiskLog.

At this stage the old Keystore can be deleted, and the new KeyStore be used.  At this stage though, the TreeCache does not necessarily reflect the state of the new KeyStore - the `replacing-tree` state is used to resolve this.  When replacing the tree, new empty TreeCaches are started and maintained in parallel to the existing TreeCaches (which continue to be used in exchanges).  A fold of the KeyStore is now commenced, whilst new updates are cached in a DiskLog.  Once the fold is complete, the new updates are applied and the TreeCache can be migrated from the old cache to the new cache.

  
