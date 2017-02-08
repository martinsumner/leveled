# Why? Why? Why? Why? Why?

## Why not just use RocksDB? 

Well that wouldn't have been as interesting.

All LSM-trees which evolve off the back of leveldb are trying to improve leveldb in a particular context.  I'm considering larger values, with need for iterators and object time-to-lives, optimisations by supporting HEAD requests and also the problem of running multiple isolated nodes in parallel on a single server.  

I suspect that [context of what I'm trying to achieve](https://github.com/basho/riak_kv/issues/1033) matters as much as much as other factors.  I [could be wrong](https://github.com/basho/riak/issues/756).

## Why not use memory-mapped files?

It is hard not to be impressed with performance results from LMDB, is memory-mapping magic?  

I tried hard to fully understand memory-mapping before I started, and ultimately wasn't clear that I fully understood it.  However, the following things made me consider that memory-mapping may not have a fundamental difference in this context:

- I didn't understand how a swap to user space can be avoided if the persisted information is compressed, and wanted the full value of compression in the page cache.

- Most memory map performance results tended to have Keys and Values of convenient sizes to memory pages and/or CPU cache lines, but in leveled I'm specifically looking objects of unpredictable and inconvenient size (normally > 4KB).

- Objects are ultimately processed as Erlang terms of some sort, again it is not obvious how that could be serialised and de-serialised without losing the advantage of memory-mapping.

- The one process per file design of leveled makes database cloning easy but eliminates some advantages of memory mapping.

## Why Erlang? 

Honestly, writing stuff that consistently behaves predictably and safely is hard, achieving this in C or C++ would be beyond my skills.  However, further to the [introduction](INTRO.md), CPU is not a constraint I'm interested in as this is a plentiful cheap resource.  Neither is low median latency a design consideration.  Writing this in another language may result in faster code, but would that be faster in a way that would be material in this context?

How to approach taking a snapshot of a database for queries, how to run background compaction/merge work in parallel to real activity - these are non-trivial challenges and the programming model of Erlang/OTP makes this a lot, lot easier.  The end intention is to include this in Riak KV, and hence reducing serialisation steps, human context-switching and managing reductions correctly means that there may be some distinct advantages in using Erlang.

Theory of constraints guides me to consider optimisation only of the bottleneck, and the bottleneck appears to be access to disk.  A [futile fortnight](https://github.com/martinsumner/eleveleddb/tree/mas-nifile/src) trying to use a NIF for file read/write performance improvements convinced me that Erlang does this natively with reasonable efficiency.

## Why not cache objects in memory intelligently?

The eleveldb smart caching of objects in memory is impressive, in particular in how it auto-adjusts the amount of memory used to fit with the number of vnodes currently running on the physical server.

From the start I decided that fadvise would be my friend, in part as:

- It was easier to depend on the operating system to understand and handle LRU management.

- I'm expecting various persisted elements (not just values, but also key ranges) to benefit from compression, so want to maximise the volume of data holdable in memory (by caching the compressed view in the page cache).

- I expect to reduce the page-cache polluting events that can occur in Riak by reducing object scanning.

Ultimately though, sophisticated memory management is hard, and beyond my capability in the timescale available.  

The design may make some caching strategies relatively easy to implement in the future though.  Each file process has its own LoopData, and to that LoopData independent caches can be added.  This is currently used for caching bloom filters and hash index tables, but could be used in a more flexible way.

## Why make this backwards compatible with OTP16?

Yes why, why do I have to do this?

## Why name things this way?

The names used in the Actor model are loosely correlated with names used for on-course bookmakers (e.g. Bookie, Clerk, Penciller).  

![](pics/ascot_bookies.jpg "Bookies")

There is no strong reason for drawing this specific link, other than the generic sense that this group represents a tight-nit group of workers passing messages from a front-man (the bookie) to keep a local view of state (a book) for a queue of clients, and where normally these groups are working in a loosely-coupled orchestration with a number of other bookmakers to form a betting market that is converging towards an eventually consistent price.

![](pics/betting_market.jpg "Betting Market")

There were some broad parallels between bookmakers in a market and vnodes in a Riak database, and using the actor names just stuck, even though the analogy is imperfect.  Somehow having a visual model of these mysterious guys in top hats working away, helped me imagine the store in action.

The name LevelEd owes some part to the influence of my eldest son Ed, and some part to the Yorkshire phrasing of the word Head.