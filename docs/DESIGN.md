## Design 

The store is written in Erlang using the actor model, the primary actors being:

- A Bookie

- An Inker

- A Penciller

- Worker Clerks

- File Clerks

### The Bookie

The Bookie provides the public interface of the store, liaising with the Inker and the Penciller to resolve requests to put new objects, and fetch those objects.  The Bookie keeps a copy of key changes and object metadata associated with recent modifications, but otherwise has no direct access to state within the store.  The Bookie can replicate the Penciller and the Inker to provide clones of the store.  These clones can be used for querying across the store at a given snapshot.

![](pics/leveled_intro.png "Introduction")

### The Inker

The Inker is responsible for keeping the Journal of all changes which have been made to the store, with new writes being append to the end of the latest journal file.  The Journal is an ordered log of activity by sequence number.  

Changes to the store should be acknowledged if and only if they have been persisted to the Journal.  The Inker can find a value in the Journal through a manifest which provides a map between sequence numbers and Journal files.  The Inker can only efficiently find a value in the store if the sequence number is known.

The Inker can also scan the Journal from a particular sequence number, for example to recover in-memory state within the store following a shutdown.

![](pics/inker.png "Inker")

### The Penciller

The Penciller is responsible for maintaining a Ledger of Keys, Index entries and Metadata (including the sequence number) that represent a near-real-time view of the contents of the store.  The Ledger is a merge tree ordered into Levels of exponentially increasing size, with each level being ordered across files and within files by Key.  Get requests are handled by checking each level in turn - from the top (Level 0), to the basement (up to Level 8).  The first match for a given key is the returned answer.

Changes ripple down the levels in batches and require frequent rewriting of files, in particular at higher levels.  As the Ledger does not contain the full object values, this write amplification associated with the flow down the levels is limited to the size of the key and metadata.

The Penciller keeps an in-memory view of new changes that have yet to be persisted in the Ledger, and at startup can request the Inker to replay any missing changes by scanning the Journal.

![](pics/penciller.png "Penciller")

### Worker Clerks

Both the Inker and the Penciller must undertake compaction work.  The Inker must garbage collect replaced or deleted objects form the Journal.  The Penciller must merge files down the tree to free-up capacity for new writes at the top of the Ledger.  

Both the Penciller and the Inker each make use of their own dedicated clerk for completing this work.  The Clerk will add all new files necessary to represent the new view of that part of the store, and then update the Inker/Penciller with the new manifest that represents that view.  Once the update has been acknowledged, any removed files can be marked as delete_pending, and they will poll the Inker (if a Journal file) or Penciller (if a Ledger file) for it to confirm that no clones of the system still depend on the old snapshot of the store to be maintained.

### File Clerks

Every file within the store has is owned by its own dedicated process (modelled as a finite state machine).  Files are never created or accessed by the Inker or the Penciller, interactions with the files are managed through messages sent to the File Clerk processes which own the files.

The File Clerks themselves are ignorant to their context within the store.  For example a file in the Ledger does not know what level of the Tree it resides in.  The state of the store is represented by the Manifest which maintains a picture of the store, and contains the process IDs of the file clerks which represent the files.

Cloning of the store does not require any file-system level activity - a clone simply needs to know the manifest so that it can independently make requests of the File Clerk processes, and register itself with the Inker/Penciller so that those files are not deleted whilst the clone is active.

The Journal files use a constant database format almost exactly replicating the CDB format originally designed by DJ Bernstein.  The Ledger files use a bespoke format with is based on Google's SST format.

File clerks spend a short initial portion of their life in a writable state.  Once they have left a writing state, they will for the remainder of their life-cycle, be in an immutable read-only state.

## Clones

Both the Penciller and the Inker can be cloned, to provide a snapshot of the database at a point in time for long-running process that can be run concurrently to other database actions.  Clones are used for Journal compaction, but also for scanning queries in the penciller (for example to support 2i queries or hashtree rebuilds in Riak).

The snapshot process is simple.  The necessary loop-state is requested from the real worker, in particular the manifest and any immutable in-memory caches, and a new gen_server work is started with the loop state.  The clone registers itself as a snapshot with the real worker, with a timeout that will allow the snapshot to expire if the clone silently terminates.  The clone will then perform its work, making requests to the file workers referred to in the manifest.  Once the work is complete the clone should remove itself from the snapshot register in the real worker before closing.

The snapshot register is used by the real worker when file workers are placed in the delete_pending state after they have been replaced in the current manifest.  Checking the list of registered snapshots allows the Penciller or Inker to inform the File Clerk if they remove themselves permanently - as no remaining clones may expect them to be present (except those who have timed out).

Managing the system in this way requires that ETS tables are used sparingly for holding in-memory state, and immutable and hence shareable objects are used instead.  The exception to the rule is the small in-memory state of recent additions kept by the Bookie - which must be exported to a list on every snapshot request.

## Paths

The PUT path for new objects and object changes depends on the Bookie interacting with the Inker to ensure that the change has been persisted with the Journal, the Ledger is updated in batches after the PUT has been completed.

The HEAD path needs the Bookie to look in his cache of recent Ledger changes, and if the change is not present consult with the Penciller.

The GET path follows the HEAD path, but once the sequence number has been determined through the response from the Ledger the object itself is fetched from the journal via the Inker.

All other queries (folds over indexes, keys and objects) are managed by cloning either the Penciller, or the Penciller and the Inker.

