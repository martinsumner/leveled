# Anti-Entropy

Leveled is primarily designed to be a backend for Riak.  Riak has a number of anti-entropy mechanisms for comparing database state within and across clusters, as part of exploring the potential for improvements in these anti-entropy mechanisms - some features have been added directly to Leveled.  The purpose of these features is to:

- Allow for the database state within in a Leveled store or stores to be compared with an other store or stores which should have the same data;

- Allow for quicker checking that recent changes to one store or stores have also been received by another store or stores that should be receiving the same changes.

The aim is to use these backend capabilities to allow for a Riak anti-entropy mechanism with the following features:

- Comparison can be made between clusters with different ring-sizes - comparison is not coupled to partitioning.

- Comparison can use a consistent approach to compare state within and between clusters.

- Comparison does not rely on duplication of database state to a separate store, with further anti-entropy required to manage state variance between the actual and anti-entropy stores.

- Comparison of state can be abstracted from Riak specific implementation so that mechanisms to compare between Riak clusters can be re-used to compare between a Riak cluster and another database store.  Coordination with another data store (e.g. Solr) can be controlled by the Riak user not the Riak developer.

## Merkle Trees

Riak has historically used [Merkle trees](https://en.wikipedia.org/wiki/Merkle_tree) as a way to communicate state efficiently between actors.  Merkle trees have been designed to be cryptographically secure so that they don't leak details of the individual transactions themselves.  This strength is useful in many Merkle Tree use cases, and is part derived from the use of concatenation when calculating branch hashes from leaf hashes:

> A hash tree is a tree of hashes in which the leaves are hashes of data blocks in, for instance, a file or set of files. Nodes further up in the tree are the hashes of their respective children. For example, in the picture hash 0 is the result of hashing the concatenation of hash 0-0 and hash 0-1. That is, hash 0 = hash( hash 0-0 + hash 0-1 ) where + denotes concatenation.

A side effect of the concatenation decision is that trees cannot be calculated incrementally, when elements are not ordered by segment.  To calculate the hash of an individual leaf (or segment), the hashes of all the elements under that leaf must be accumulated first.  In the case of the leaf segments in Riak, the leaf segments are made up of a hash of the concatenation of {Key, Hash} pairs under that leaf:

``hash([{K1, H1}, {K2, H2} .. {Kn, Hn}])``

This requires all of the keys and hashes need to be pulled into memory to build the hashtree - unless the tree is being built segment by segment.  The Riak hashtree data store is therefore ordered by segment so that it can be incrementally built.  The segments which have had key changes are tracked, and at exchange time all "dirty segments" are re-scanned in the store segment by segment, so that the hashtree can be rebuilt.

## Tic-Tac Trees

Anti-entropy in leveled is supported using the leveled_tictac module.  This module uses a less secure form of merkle trees that don't prevent information from leaking out, or make the tree tamper-proof, but allow for the trees to be built incrementally, and trees built incrementally to be merged.  These trees we're calling Tic-Tac Trees after the [Tic-Tac language](https://en.wikipedia.org/wiki/Tic-tac) which has been historically used on racecourses to communicate the state of the market between participants; although the more widespread use of mobile communications means that the use of Tic-Tac is petering out, and rather like Basho employees, there are now only three Tic-Tac practitioners left.

The change from Merkle trees to Tic-Tac trees is simply to no longer use a cryptographically strong hashing algorithm, and now combine hashes through XORing rather than concatenation.  So a segment leaf is calculated from:

``hash(K1, H1) XOR hash(K2, H2) XOR ... hash(Kn, Hn)``

The Keys and hashes can now be combined in any order with any grouping.

This enables two things:

- The tree can be built incrementally when scanning across a store not in segment order (i.e. scanning across a store in key order) without needing to hold an state in memory beyond the size of the tree.

- Two trees from stores with non-overlapping key ranges can be merged to reflect the combined state of that store i.e. the trees for each store can be built independently and in parallel and the subsequently merged without needing to build an interim view of the combined state.

It is assumed that the trees will only be transferred securely between trusted actors already permitted to view, store and transfer the real data: so the loss of cryptographic strength of the tree is irrelevant to the overall security of the system.

## Recent and Whole

Anti-entropy in Riak is a dual-track process:

- there is a need to efficiently and rapidly provide an update on store state that represents recent additions;

- there is a need to ensure that the anti-entropy view of state represents the state of the whole database.

Within the current Riak AAE implementation, recent changes are supported by having a separate anti-entropy store organised by segments so that the Merkle tree can be updated incrementally to reflect recent changes.  The Merkle tree produced following these changes should then represent the whole state of the database.  

However as the view of the whole state is maintained in a different store to that holding the actual data: there is an entropy problem between the actual store and the AAE store e.g. data could be lost from the real store, and go undetected as it is not lost from the AAE store.  So periodically the AAE store is rebuilt by scanning the whole of the real store.  This rebuild can be an expensive process, and the cost is commonly controlled through performing this task infrequently, with changes pending in develop to try and manage the scheduling and throttling of this process.



.... to be completed


Splitting the problem into two parts

full database state
recent changes

as opposed to

full database state
rebuilt full database state
