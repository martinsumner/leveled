## Further Features

The store supports all the required Riak backend capabilities.  A number of further features are either partially included, in progress or under consideration:

- Support for HEAD operations, and changes to GET/PUT Riak paths to use HEAD requests where GET requests would be unnecessary.

- Support for "Tags" in keys to allow for different treatment of different key types (i.e. changes in merge actions, metadata retention, caching for lookups).

- Support for Sub-Keys in objects.  There may be the potential to use this along with key types to support alternative approaches to CRDT sets or local rather than global secondary indexes.

- A fast "list bucket" capability, borrowing from the HanoiDB implementation, to allow for safe bucket listing in production.

- A bucket size query, which requires traversal only of the Ledger and counts the keys and sums the total on-disk size of the objects within the bucket.

- A new SW count to operate in parallel to DW, where SW is the number of nodes required to have flushed to disk (not just written to buffer); with SW being configurable to 0 (no vnodes will need to sync this write), 1 (the PUT coordinator will sync, but no other vnodes) or all (all vnodes will be required to sync this write before providing a DW ack).  This will allow the expensive disk sync operation to be constrained to the writes for which it is most needed.

- Support for a specific Riak tombstone tag where reaping of tombstones can be deferred (by many days) i.e. so that a 'sort-of-keep' deletion strategy can be followed that will eventually garbage collect without the need to hold pending full deletion state in memory.

- The potential to support Map/Reduce functions on object metadata not values, so that cache-pollution and disk i/o overheads of full object Map/Reduce can be eliminated by smarter object construction strategies that promote information designed for queries from values into metadata.


## Outstanding work

There is some work required before LevelEd could be considered production ready:

- A strategy for the supervision and restart of processes, in particular for clerks.

- Further functional testing within the context of Riak.

- Introduction of property-based testing.

- Improved handling of corrupted files.

- A way of identifying the partition in each log to ease the difficulty of tracing activity when multiple stores are run in parallel.


## Riak Features Implemented

The following Riak features have been implemented.  Note that these features are <b>not adequately tested</b> at present.  Sufficient work has been done to support the realism of volume test metrics, with mainly manual tests and some simple eunit tests.

### Leveled Backend

Branch: [mas-leveleddb](https://github.com/martinsumner/riak_kv/tree/mas-leveleddb)

Branched-From: [Basho/develop](https://github.com/basho/riak_kv)

Description:

The leveled backend has been implemented with some basic manual functional tests.  The backend has the following capabilities:

- async_fold - can return a Folder() function in response to 2i queries;
- indexes - support for secondary indexes (currently being returned in the wrong order);
- head - supports a HEAD as well as a GET request;
- hash_query - can return keys/hashes when rebuilding hashtrees, so implementation of hashtree can call this rather than folding objects and hashing the objects returning from the fold.

All standard features should (in theory) be supportable (e.g. TTL, M/R, term_regex etc) but aren't subject to end-to_end testing at present.

There is a uses_r_object capability that leveled should in theory be able to support (making it unnecessary to convert to/from binary form).  However, given that only the yessir backend currently supports this, the feature hasn't been implemented in case there are potential issues lurking within Riak when using this capability.

### Fast List-Buckets

Normally Basho advise not to list buckets in production - as on other backends it is a hugely expensive operation.  Accidentally running list buckets can lead to significant resource pressure, and also rapid consumption of available disk space.

The LevelEd backend borrows from the hanoidb implementation of list buckets, which supports listing buckets with a simple low-cost async fold.  So there should be no issue listing buckets in production with these backends.  

Note - the technique would work in leveldb and memory backends as well (and perhaps bitcask), it just isn't implemented there.  Listing buckets should not really be an issue.

### GET_FSM -> Using HEAD

Branch: [mas-leveled-getfsm](https://github.com/martinsumner/riak_kv/tree/mas-leveled-getfsm)

Branched-From: [mas-leveleddb](https://github.com/martinsumner/riak_kv/tree/mas-leveleddb)

Description:

In standard Riak the Riak node that receives a GET request starts a riak_kv_get_fsm to handle that request.  This FSM goes through the following primary states:

- prepare (get preflists etc)
- validate (make sure there is sufficient vnode availability to handle the request)
- execute (send the GET request to n vnodes)
- waiting_vnode_r (loop waiting for the response from r vnodes, every time a response is received check if enough vnodes have responded, and then either finalise a response or wait for more)
- waiting_read_repair (read repair if a responding vnode is out of date)

The alternative FSM in this branch makes the following changes
- sends HEAD not GET requests at the execute stage
- when 'enough' is received in waiting_vnode_r, elects a vnode or vnodes from which to fetch the body
- recall execute asking for just the necessary GET requests
- in waiting_vnode_r the second time update HEAD responses to GET responses, and recalculate the winner (including body) when all vnodes which have been sent a GET request have responded

So rather than doing three Key/Metadata/Body backend lookups for every request, normally (assuming no siblings) Riak now requires three Key/Metadata and one Key/Metadata/Body lookup.  For larger bodies, this then avoids the additional disk activity and network load associated with fetching and transmitting the body three times for every request.  As another positive side effect the database is in-theory much less likely to have [TCP Incast](http://www.snookles.com/slf-blog/2012/01/05/tcp-incast-what-is-it/) issues, hence reducing the cost of network infrastructure, and reducing the risk of running Riak in shared infrastructure environments.

The feature will not at present work safely with legacy vclocks. This branch generally relies on vector clock comparison only for equality checking, and removes some of the relatively expensive whole body equality tests (either as a result of set:from_list/1 or riak_object:equal/2), which are believed to be a legacy of issues with pre-dvv clocks.

In tests, the benefit of this may not be that significant - as the primary resource saved is disk/network, and so if these are not the resources under pressure, the gain may not be significant.  In tests bound by CPU not disks, only a 10% improvement has so far been measured with this feature.

#### 'n HEADs 1 GET' or '1 GET n-1 HEADs'

A potential alternative to perfoming n HEADs and then 1 GET, would be for the FSM to make 1 GET request and in parallel make n-1 HEAD requests.  This would, in an under-utilised cluster, require less resource and is likely to return a response with lower average latency.  The 1 GET request vnode could be selected in a similar style to the PUT FSM put coordinator by starting the FSM on a node in the preflist and using the local vnode as the GET vnode, and the remote vnodes would be chosen as the HEAD request nodes for clock comparison.  

The primary reason why this approach has not been chosen, and the n HEADs followed by 1 GET mode has been preferred, is to do with the potential for variable vnode mailbox lengths.  

When a cluster is under heavy pressure, especially when a cluster has been expanded so that there are a low number of vnodes per node, vnode mailbox sizes can vary and some vnodes may go into overload status (as the mailbox is bounded by the overload size).  At the overload response stage the client must back-off, and the cluster must run at the pace of the slowest vnode.  Ideally, if a vnode is slow, it should be given less work.  This is a positive advantage of the n HEAD requests followed by 1 GET, the first responder is the one elected to perform the GET, and the slower responders miss out on that workload.  This means that naturally slower vnodes (such as those with longer mailbox queues), are given less work by avoiding the expensive GET requests, and the overload scenario is likely to be avoided.  The approach is designed to work better when one or more vnodes are running slower - perhaps due to the presence of a background process or a hardware failure.

The issue with performing 1 GET and n-1 HEAD requests, what if the slow vnode (say one with a mailbox 2,000 long) is selected for the GET request (given the GET vnode has to be selected without a test HEAD message to calibrate which vnode to use), the request may last the duration of this response.  Once the n-1 HEAD requests have completed, how long should the FSM wait for the response to the GET request, especially as the GET request may be delayed naturally due to the value being large rather than due to the vnode being slow or down?  If the FSM times out aggresively, then larger object requests are more likely to be made more than once - the most expensive requests don't gain the benefit of the optimisation.  If the timeout is loose, then there will be many delays caused by one slow vnode, even when that vnode is not in the overload state.  With the n HEAD 1 GET approach the FSM has evidence that the vnode chosen for the GET is active and fast, as it is the first responder, and so the FSM can wait until the FSM timeout (at risk of failing a request when the failure occurrs between the HEAD and the GET request).  The 1 GET and n-1 HEAD requests doesn't avoid the slow vnode problem, and required difficult reasoning about timeouts if the chosen GET node does not respond quickly. 

One potential optimisation for leveled where some testing has been performed, was caching positive responses to HEAD requests for a short period in the SST files.  There are no currency issues with this cache as it is at an immutable file level.  This means that the second GET request can take advantage of that caching at a SST level, and should be slightly faster.  However, in volume tests this did not appear to make a noticeable difference - perhaps due the relative cost of all the failed checks to the recent request cache.

### PUT -> Using HEAD

Branch: [mas-leveled-putfsm](https://github.com/martinsumner/riak_kv/tree/mas-leveled-putfsm)

Branched-From: [mas-leveled-getfsm](https://github.com/martinsumner/riak_kv/tree/mas-leveled-getfsm)

Description:

The standard PUT process for Riak requires the PUT to be forwarded to a coordinating vnode first.  The coordinating PUT  process requires the object to be fetched from the local vnode only, and a new updated Object created.  The remaining n-1 vnodes are then sent the update object as a PUT, and once w/dw/pw nodes have responded the PUT is acknowledged.

The other n-1 vnodes must also do a local GET before the vnode PUT (so as not to erase a more up-to-date value that may not have been present at the coordinating vnode).

This branch changes the behaviour slightly at the non-coordinating vnodes.  These vnodes will now try a HEAD request before the local PUT (not a GET request), and if the HEAD request contains a vclock which is <strong>dominated</strong> by the updated PUT, it will not attempt to fetch the whole object for the syntactic merge.

This should save two object fetches (where n=3) in most circumstances.

Note, although the branch name refers to the put fsm - the actual fsm is unchanged by this, all of the changes are within vnode_put

### AAE Rebuild Acceleration

Branch: [mas-leveled-scanner-i649](https://github.com/martinsumner/riak_kv/tree/mas-leveled-scanner-i649)

Branched-From: [mas-leveled-putfsm](https://github.com/martinsumner/riak_kv/tree/mas-leveled-putfsm)

Description:

The riak_kv_sweeper which is part of the post-2.2 develop branch controls folds over objects so that multiple functions can be applied to a single fold.  The only aspect of the Riak system that uses this feature at present is AAE hashtree rebuilds.

This branch modifies the kv_sweeper so that if the capability exists, and unless a sweeper has explicitly stated a requirement not to allow this feature, the sweeper can defer the fetching of the objects.  This means that the sweeper will fold over the "heads" of the objects returning a specially crafter Riak Object which contains a reference to the body rather than the actual body - so that the object body can be fetched if and only if access to the object contents is requested via the riak_object module.

### Journal compaction

Branch: [mas-leveled-autocompact](https://github.com/martinsumner/riak_kv/tree/mas-leveled-autocompact)

Branched-From: [mas-leveled-scanner-i649](https://github.com/martinsumner/riak_kv/tree/mas-leveled-scanner-i649)

Description:

Allows for the hours of day in which compaction of the Journal compaction will be run to be configurable.  Also configurable, is the number of times (approximately) each vnode should run journal compaction each day.

The number of times this will need to be run will depend on the distribution of updates - most specifically what proportion of PUTs are changes as opposed to new data.

Cuttlefish config is still broken, so changes to config should be made through the riak_kv_leveled.hrl include file.
