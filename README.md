# LeveledDB

## Overview

LeveledDB is an experimental Key/Value store based on the Log-Structured Merge Tree concept, written in Erlang. It is not currently suitable for production systems, but is intended to provide a proof of concept of the potential benefits of different design trade-offs in LSM Trees.

The specific goals of this implementation are:

- Be simple and straight-forward to understand and extend
- Support objects which have keys, secondary indexes, a value and potentially metadata which provides key summary information about the value
- Support a HEAD request which has a lower cost than a GET request, so that requests requiring access only to metadata can gain efficiency by saving the full cost of returning the entire value

The system context for the store at conception is as a Riak backend store with a complete set of backend capabilities, but one intended to be use with relatively frequent iterators, and values of non-trivial size (e.g. > 4KB).

## Implementation 

The store is written in Erlang using the actor model, the primary actors being:

- A Bookie
- An Inker
- A Penciller
- Worker Clerks
- File Clerks

### The Bookie

The Bookie provides the public interface of the store, liaising with the Inker and the Penciller to resolve requests to put new objects, and fetch those objects.  The Bookie keeps a copy of key changes and metadata associated with recent modifications, but otherwise has no direct access to state within the store.  The Bookie can provide clones of the Penciller and the Inker to support queries which scan across more than one object in the store.

### The Inker

The Inker is responsible for keeping the Journal of all changes which have been made to the store, with new writes being append to the end of the latest journal file.  The Journal is an ordered log of activity by sequence number (in reverse).  

Changes to the store should be acknowledged if and only if they have been persisted to the Journal.  The Inker can efficiently find value in the store by looking up the journal file using the sequence number of change in a Manifest it maintains mapping sequence number ranges to Journal files.

The Inker can also scan the Journal from a particular sequence number, for example to recover another actor's lost state following a shutdown.

### The Penciller

The Penciller is responsible for maintaining a Ledger of Keys, Index entries and Metadata that represent a near-real-time view of the contents of the store.  The Ledger is a merge tree ordered into Levels of exponentially increasing size, with each level being ordered across files and within files by Key.  Get requests are handled by checking each level in turn - from the top (Level 0), to the basement (up to Level 8).

Changes ripple down the levels in batches and require frequent rewriting of files, in particular at higher levels.

The Penciller keeps an in-memory view of new changes that have yet to be persisted in the Ledger, and at startup can request the Inker to replay any missing changes by scanning the Journal.

### Worker Clerks

Both the Inker and the Penciller must complete compaction work to either garbage collect replaced or deleted objects form the Journal (in the case of the Inker) or to merge files down the tree to free-up capacity for new writes at the top of the Ledger (in the case of the Penciller).  

Both the Penciller and the Inker make use of a clerk for completing this work.  The Clerk will add all new files necessary to represent the new view of that part of the store, and then update the Inker/Penciller with the new manifest that represents that view.  Once the update has been acknowledged, any removed files can be marked as delete_pending, and they will poll the Inker (if a Journal file) or Penciller (if a Ledger file) for it to confirm that no users of the system still depend on the old snapshot of the store to be maintained.

### File Clerks

Every file within the store has is owned by its own dedicated process (modelled as a finite state machine).  Files are never created or accessed by the Inker or the Penciller - interactions with the files are managed through messages sent to the File Clerk processes which own the files.

The Files themselves are ignorant to their context within the store - a file in the Ledger does not know what level of the Tree it resides in.  The state of the store is represented by the Manifest which maintains a picture of the store, and contains the process IDs of the file clerks which represent the files.

Cloning of the store does not require any file-system level activity - a clone simply needs to know the manifest so that it can independently make requests of the File Clerk processes.

The Journal files use a constant database format almost exactly replicating the CDB format originally designed by DJ Bernstein.  The Ledger files use a bespoke format with is based on Google's SST format, with the primary difference being that the bloom filters used to protect against unnecessary lookups are based on the Riak Segment IDs of the key, and use single-hash rice-encoded sets rather using the traditional bloom filter size-optimisation model of extending the number of hashes used to reduce the false-positive rate.

## Trade-Offs

Further information of specific design trade-off decisions is provided:

- Not memory mapping
- Memory management
- The Penciller memory
- The use of Bloom filters
- Stalling or pausing
