%% -------- Overview ---------
%%
%% The eleveleddb is based on the LSM-tree similar to leveldb, except that:
%% - Keys, Metadata and Values are not persisted together - the Keys and
%% Metadata are kept in a tree-based ledger, whereas the values are stored
%% only in a sequential Journal.
%% - Different file formats are used for Journal (based on constant
%% database), and the ledger (sft, based on sst)
%% - It is not intended to be general purpose, but be specifically suited for
%% use as a Riak backend in specific circumstances (relatively large values,
%% and frequent use of iterators)
%% - The Journal is an extended nursery log in leveldb terms.  It is keyed
%% on the sequence number of the write
%% - The ledger is a LSM tree, where the key is the actaul object key, and
%% the value is the metadata of the object including the sequence number
%%
%%
%% -------- The actors ---------
%% 
%% The store is fronted by a Bookie, who takes support from different actors:
%% - An Inker who persists new data into the jornal, and returns items from
%% the journal based on sequence number
%% - A Penciller who periodically redraws the ledger
%% - One or more Clerks, who may be used by either the inker or the penciller
%% to fulfill background tasks
%%
%% Both the Inker and the Penciller maintain a manifest of the files which
%% represent the current state of the Journal and the Ledger repsectively.
%% For the Inker the manifest maps ranges of sequence numbers to cdb files.
%% For the Penciller the manifest maps key ranges to files at each level of
%% the Ledger.
%%
%% -------- PUT --------
%%
%% A PUT request consists of
%% - A primary Key
%% - Metadata associated with the primary key (2i, vector clock, object size)
%% - A value
%% - A set of secondary key changes which should be made as part of the commit
%%
%% The Bookie takes the place request and passes it first to the Inker to add
%% the request to the ledger.
%%
%% The inker will pass the request to the current (append only) CDB journal
%% fileto persist the change.  The call should return either 'ok' or 'roll'.
%% 'roll' indicates that the CDB file has insufficient capacity for
%% this write.

%% In resonse to a 'roll', the inker should:
%% - start a new active journal file with an open_write_request, and then;
%% - call to PUT the object in this file;
%% - reply to the bookie, but then in the background
%% - close the previously active journal file (writing the hashtree), and move
%% it to the historic journal
%%
%% Once the object has been persisted to the Journal, the Key and Metadata can
%% be added to the ledger.  Initially this will be added to the Bookie's
%% in-memory view of recent changes only.
%%
%% The Bookie's memory consists of up to two in-memory ets tables
%% - the 'cmem' (current in-memory table) which is always subject to potential
%% change;
%% - the 'imem' (the immutable in-memory table) which is awaiting persistence
%% to the disk-based lsm-tree by the Penciller.
%%
%% The key and metadata should be written to the cmem store if it has
%% sufficient capacity, but this potentially should include the secondary key
%% changes which have been made as part of the transaction.
%%
%% If there is insufficient space in the cmem, the cmem should be converted
%% into the imem, and a new cmem be created.  This requires the previous imem
%% to have been cleared from state due to compaction into the persisted Ledger
%% by the Penciller - otherwise the PUT is blocked.  On creation of an imem,
%% the compaction process for that imem by the Penciller should be triggered.
%%
%% This completes the non-deferrable work associated with a PUT
%%
%% -------- Snapshots (Key & Metadata Only) --------
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% must first produce a tree representing the results of the request which are
%% present in its in-memory view of the ledger.  The Bookie then requests
%% a copy of the current Ledger manifest from the Penciller, and the Penciller
%5 should interest of the iterator at the manifest sequence number at the time
%% of the request.
%%
%% Iterators should de-register themselves from the Penciller on completion.
%% Iterators should be automatically release after a timeout period.  A file
%% can only be deleted from the Ledger if it is no longer in the manifest, and
%% there are no registered iterators from before the point the file was
%% removed from the manifest.
%%
%% Snapshots may be non-recent, if recency is unimportant.  Non-recent
%% snapshots do no require the Bookie to return the results of the in-memory
%% table, the Penciller alone cna be asked.
%%
%% -------- Special Ops --------
%%
%% e.g. Get all for SegmentID/Partition
%%


-module(leveled_bookie).

