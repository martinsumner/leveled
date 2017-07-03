# Anti-Entropy

Leveled is primarily designed to be a backend for Riak.  Riak has a number of anti-entropy mechanisms for comparing database state within and across clusters, as part of exploring the potential for improvements in these anti-entropy mechanisms - some features have been added directly to Leveled.  The purpose of these features is to:

- Allow for the database state within in a Leveled store or stores to be compared with an other store or stores which should have the same data;
- Allow for quicker checking that recent changes to one store or stores have also been received by another store or stores that should be receiving the same changes.

The aim is to use these backend capabilities to allow for a Riak anti-entropy mechanism with the following features:

- Comparison can be made between clusters with different ring-sizes - comparison is not coupled to partitioning.
- Comparison can use a consistent approach to compare state within and between clusters.
- Comparison does not rely on duplication of database state to a separate store, with further anti-entropy required to manage state variance between the actual and anti-entropy stores.
- Comparison of state can be abstracted from Riak specific implementation so that mechanisms to compare between riak clusters can be re-used to compare between a Riak cluster and another database store.  Coordination with another data store (e.g. Solr) can be controlled by the Riak user not the Riak developer.

## Merkle Trees

Riak has historically used [Merkle trees](https://en.wikipedia.org/wiki/Merkle_tree) as a way to communicate state efficiently between actors.  Merkle trees have been designed to be cryptographically secure so that they don't leak details of the individual transactions themselves.  This strength is useful in many Merkle Tree use cases, and is part derived from the use of concatenation when calculating branch hashes from leaf hashes:

> A hash tree is a tree of hashes in which the leaves are hashes of data blocks in, for instance, a file or set of files. Nodes further up in the tree are the hashes of their respective children. For example, in the picture hash 0 is the result of hashing the concatenation of hash 0-0 and hash 0-1. That is, hash 0 = hash( hash 0-0 + hash 0-1 ) where + denotes concatenation.

A side effect of the concatenation decision is that trees cannot be calculated incrementally, when elements are not ordered by segment.  To calculate the hash of an individual leaf (or segment), the hashes of all the elements under that leaf must be accumulated first.  

## Tic-Tac Trees

Anti-entropy in leveled is supported using the leveled_tictac module.  This module uses a less secure form of merkle trees that don't prevent information from leaking out, but allow for the trees to be built incrementally, and trees built incrementally to be merged.  These trees we're calling Tic-Tac Trees after the [Tic-Tac language](https://en.wikipedia.org/wiki/Tic-tac) which has been historically used on racecourses to communicate the state of the market between participants; although the more widespread use of mobile communications means that the use of Tic-Tac is petering out, and rather like Basho employees, there are now only three Tic-Tac practitioners left.

The change from Merkle trees to Tic-Tac trees is simply to no longer use a cryptographically strong hashing algorithm, and now combine hashes through XORing rather than concatenation - to enable merging and incremental builds.

## Divide and Conquer

.... to be completed


Splitting the problem into two parts

full database state
recent changes

as opposed to

full database state
rebuilt full database state
