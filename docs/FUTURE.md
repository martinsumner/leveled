## Further Features

The store supports all the required Riak backend capabilities.  A number of further features are either partially included, in progress or under consideration:

- Support for HEAD operations, and changes to GET/PUT Riak paths to use HEAD requests where GET requests would be unnecessary.

- Support for "Tags" in keys to allow for different treatment of different key types (i.e. changes in merge actions, metadata retention, caching for lookups).

- Support for Sub-Keys in objects.  There may be the potential to use this along with key types to support alternative approaches to CRDT sets or local rather than global secondary indexes.

- A fast "list bucket" capability, borrowing from the HanoiDB implementation, to allow for safe bucket listing in production.

- A bucket size query, which requires traversal only of the Ledger and counts the keys and sums the total on-disk size of the objects within the bucket.

- Support for a specific Riak tombstone tag where reaping of tombstones can be deferred (by many days) i.e. so that a 'sort-of-keep' deletion strategy can be followed that will eventually garbage collect without the need to hold pending full deletion state in memory.

- The potential to support Map/Reduce functions on object metadata not values, so that cache-pollution and disk i/o overheads of full object Map/Reduce can be eliminated by smarter object construction strategies that promote information designed for queries from values into metadata.


## Outstanding work

There is some work required before LevelEd could be considered production ready:

- A strategy for the supervision and restart of processes, in particular for clerks.

- Further functional testing within the context of Riak.

- Introduction of property-based testing.

- Riak modifications to support the optimised Key/Clock scanning for hashtree rebuilds.

- Amend compaction scheduling to ensure that all vnodes do not try to concurrently compact during a single window.

- Improved handling of corrupted files.

- A way of identifying the partition in each log to ease the difficulty of tracing activity when multiple stores are run in parallel.


## Riak Features Implemented

The following Riak features have been implemented

### LevelEd Backend

Branch: [mas-leveleddb](https://github.com/martinsumner/riak_kv/tree/mas-leveleddb)

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

Description:

In standard Riak the Riak node that receives a GET request starts a riak_kv_get_fsm to handle that request.  This FSM goes through the following primary states:

- prepare (get preflists etc)
- valiidate (make sure there is sufficient vnode availability to handle the request)
- execute (send the GET request to n vnodes)
- waiting_vnode_r (loop waiting for the response from r vnodes, every time a response is received check if enough vnodes have responded, and then either finalise a response or wait for more)
- waiting_read_repair (read repair if a responding vnode is out of date)

The alternative FSM in this branch makes the following changes
- sends HEAD not GET requests at the execute stage
- when 'enough' is received in waiting_vnode_r, elects a vnode or vnodes from which to fetch the body
- recall execute asking for just the necessary GET requests
- in waiting_vnode_r the second time update HEAD responses to GET responses, and recalculate the winner (including body) when all vnodes which have been sent a GET request have responded

So rather than doing three Key/Metadata/Body backend lookups for every request, normally (assuming no siblings) Riak now requires three Key/Metadata and one Key/Metadata/Body lookup.  For larger bodies, this then avoids the additional disk activity and network load associated with fetching and transmitting the body three times for every request.  There are some other side effects:

- It is more likely to detect out-of-date objects in slow nodes (as the n-r responses may still be received and added to the result set when waiting for the GET response in the second loop).
- The database is in-theory much less likely to have [TCP Incast](http://www.snookles.com/slf-blog/2012/01/05/tcp-incast-what-is-it/) issues, hence reducing the cost of network infrastructure, and reducing the risk of running Riak in shared infrastructure environments.

The feature will not at present work safely with legacy vclocks. This branch generally relies on vector clock comparison only for equality checking, and removes some of the relatively expensive whole body equality tests (either as a result of set:from_list/1 or riak_object:equal/2), which are believed to be a legacy of issues with pre-dvv clocks.

In tests, the benefit of this may not be that significant - as the primary resource saved is disk/network, and so if these are not the resources under pressure, the gain may not be significant.  In tests bound by CPU not disks, only a 10% improvement has so far been measured with this feature.
