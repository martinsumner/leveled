# Volume Testing

## Parallel Node Testing

Initial volume tests have been [based on standard basho_bench eleveldb test](../test/volume/single_node/examples) to run multiple stores in parallel on the same node and and subjecting them to concurrent pressure.

This showed a [relative positive performance for leveled](VOLUME_PRERIAK.md) for both population and load. This also showed that although the leveled throughput was relatively stable, it was still subject to fluctuations related to CPU constraints - especially as compaction of the ledger was a CPU intensive activity.  Prior to moving on to full Riak testing, a number of changes where then made to leveled to reduce the CPU load during these merge events.

## Initial Riak Cluster Tests

Testing in a Riak cluster, has been based on like-for-like comparisons between leveldb and leveled - except that leveled was run within a Riak [modified](FUTURE.md) to use HEAD not GET requests when HEAD requests are sufficient.  

The initial testing was based on simple gets and updates, with 5 gets for every update.  

### Basic Configuration - Initial Tests

The configuration consistent across all tests is:

- A 5 node cluster
- Using i2.2xlarge EC2 or d2.2xlarge nodes with mirrored/RAID10 drives (for data partition only)
- deadline scheduler, transparent huge pages disabled, ext4 partition
- A 64 partition ring-size
- AAE set to passive
- A pareto distribution of requests with a keyspace of 200M keys
- 5 GETs for each UPDATE

### Mid-Size Object, SSDs, Sync-On-Write

This test has the following specific characteristics

- An 8KB value size (based on crypto:rand_bytes/1 - so cannot be effectively compressed)
- 60 concurrent basho_bench workers running at 'max'
- i2.2xlarge instances
- allow_mult=false, lww=false
- <b>sync_on_write = on</b>

Comparison charts for this test:

Riak + leveled             |  Riak + eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_one/output/summary_leveled_5n_60t_i2_sync.png "LevelEd")  |  ![](../test/volume/cluster_one/output/summary_leveldb_5n_60t_i2_sync.png "LevelDB")

### Mid-Size Object, SSDs, No Sync-On-Write

This test has the following specific characteristics

- An 8KB value size (based on crypto:rand_bytes/1 - so cannot be effectively compressed)
- 100 concurrent basho_bench workers running at 'max'
- i2.2xlarge instances
- allow_mult=false, lww=false
- <b>sync_on_write = off</b>

Comparison charts for this test:

Riak + leveled             |  Riak + eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_two/output/summary_leveled_5n_100t_i2_nosync.png "LevelEd")  |  ![](../test/volume/cluster_two/output/summary_leveldb_5n_100t_i2_nosync.png "LevelDB")

### Mid-Size Object, HDDs, No Sync-On-Write

This test has the following specific characteristics

- An 8KB value size (based on crypto:rand_bytes/1 - so cannot be effectively compressed)
- 50 concurrent basho_bench workers running at 'max'
- <b>d2.2xlarge instances</b>
- allow_mult=false, lww=false
- sync_on_write = off

Comparison charts for this test:

Riak + leveled           |  Riak + eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_three/output/summary_leveled_5n_50t_d2_nosync.png "LevelEd")  |  ![](../test/volume/cluster_three/output/summary_leveldb_5n_50t_d2_nosync.png "LevelDB")

Note that there is a clear inflexion point when throughput starts to drop sharply at about the hour mark into the test.  
This is the stage when the volume of data has begun to exceed the volume supportable in cache, and so disk activity begins to be required for GET operations with increasing frequency.

### Half-Size Object, SSDs, No Sync-On-Write

This test has the following specific characteristics

- A <b>4KB value size</b> (based on crypto:rand_bytes/1 - so cannot be effectively compressed)
- 100 concurrent basho_bench workers running at 'max'
- i2.2xlarge instances
- allow_mult=false, lww=false
- sync_on_write = off

Comparison charts for this test:

Riak + leveled           |  Riak + eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_four/output/summary_leveled_5n_100t_i2_4KB_nosync.png "LevelEd")  |  ![](../test/volume/cluster_four/output/summary_leveldb_5n_100t_i2_4KB_nosync.png "LevelDB")


### Double-Size Object, SSDs, No Sync-On-Write

This test has the following specific characteristics

- A <b>16KB value size</b> (based on crypto:rand_bytes/1 - so cannot be effectively compressed)
- 60 concurrent basho_bench workers running at 'max'
- i2.2xlarge instances
- allow_mult=false, lww=false
- sync_on_write = off

Comparison charts for this test:

Riak + leveled           |  Riak + eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_five/output/summary_leveled_5n_60t_i2_16KB_nosync.png "LevelEd")  |  ![](../test/volume/cluster_five/output/summary_leveldb_5n_60t_i2_16KB_nosync.png "LevelDB")


### Lies, damned lies etc

The first thing to note about the test is the impact of the pareto distribution and the start from an empty store, on what is actually being tested.  At the start of the test there is a 0% chance of a GET request actually finding an object.  Normally, it will be 3 hours into the test before a GET request will have a 50% chance of finding an object.

![](../test/volume/cluster_two/output/NotPresentPerc.png "Percentage of GET requests being found at different leveled levels")

Both leveled and leveldb are optimised for finding non-presence through the use of bloom filters, so the comparison is not unduly influenced by this.  However, the workload at the end of the test is both more realistic (in that objects are found), and harder if the previous throughput had been greater (in that more objects are found).  

So it is better to focus on the results at the tail of the tests, as at the tail the results are a more genuine reflection of behaviour against the advertised test parameters.


Test Description                  | Hardware     | Duration |Avg TPS    | TPS Delta (Overall)  | TPS Delta (Last Hour)
:---------------------------------|:-------------|:--------:|----------:|-----------------:|-------------------:
8KB value, 60 workers, sync       | 5 x i2.2x    | 4 hr     | 12,679.91 | <b>+ 70.81%</b>  | <b>+ 63.99%</b>
8KB value, 100 workers, no_sync   | 5 x i2.2x    | 6 hr     | 14,100.19 | <b>+ 16.15%</b>  | <b>+ 35.92%</b>
8KB value, 50 workers, no_sync    | 5 x d2.2x    | 4 hr     | 10,400.29 | <b>+  8.37%</b>  | <b>+ 23.51%</b>
4KB value, 100 workers, no_sync   | 5 x i2.2x    | 6 hr     | 14,993.95 | - 10.44%  | - 4.48%
16KB value, 60 workers, no_sync   | 5 x i2.2x    | 6 hr     | 11,167.44 | <b>+ 80.48%</b>  | <b>+ 113.55%</b>
8KB value, 80workers, no_sync, 2i queries | 5 x i2.2x | 6 hr | 9,855.96 | <b>+ 4.48%</b> | <b>+ 22.36%</b>

Leveled, like bitcask, will defer compaction work until a designated compaction window, and these tests were run outside of that compaction window.  So although the throughput of leveldb is lower, it has no deferred work at the end of the test.  Future testing work is scheduled to examine leveled throughput during a compaction window.  

As a general rule, looking at the resource utilisation during the tests, the following conclusions can be drawn:

- When unconstrained by disk I/O limits, leveldb can achieve a greater throughput rate than leveled.
- During these tests leveldb is frequently constrained by disk I/O limits, and the frequency with which it is constrained increases the longer the test is run for.
- leveled is almost always constrained by CPU, or by the limits imposed by response latency and the number of concurrent workers.
- Write amplification is the primary delta in disk contention between leveldb and leveled - as leveldb is amplifying the writing of values not just keys it is creating a significantly larger 'background noise' of disk activity, and that noise is sufficiently variable that it invokes response time volatility even when r and w values are less than n.
- leveled has substantially lower tail latency, especially on PUTs.
- leveled throughput would be increased by adding concurrent workers, and increasing the available CPU.
- leveldb throughput would be increased by having improved disk i/o.


## Riak Cluster Test - Phase 2 - AAE Rebuild

These tests have been completed using the following static characteristics which are designed to be a balanced comparison between leveled and leveldb:

- 8KB value,
- 100 workers,
- no sync on write,
- 5 x i2.2x nodes,
- 6 hour duration.

This is the test used in Phase 1.  Note that since Phase 1 was completed a number of performance improvements have been made in leveled, so that the starting gap between Riak/leveled and Riak/leveldb has widened.

The tests have been run using the new riak_kv_sweeper facility within develop.  This feature is an alternative approach to controlling and scheduling rebuilds, allowing for other work to be scheduled into the same fold.  As the test is focused on hashtree rebuilds, the test was run with:

- active anti-entropy enabled,
- a 3 hour rebuild timer.

The 3-hour rebuild timer is not a recommended configuration, it is an artificial scenario to support testing in a short time window.

In the current Riak develop branch all sweeps use the Mod:fold_objects/4 function in the backend behaviour.  In the testing of Riak/leveled this was changed to allow use of the new Mod:fold_heads/4 function available in the leveled backend (which can be used if the backend supports the fold_heads capability).

In clusters which have fully migrated to Riak 2.2, the hashtrees are built from a hash of the vector clock, not the object - handling the issue of consistent hashing without canonicalisation of riak objects.  This means that hashtrees can be rebuilt without knowledge of the object itself.  However, the purpose of rebuilding the hashtree is to ensure that the hashtree represents the data that is still present in the store, as opposed to the assumed state of the store based on the history of changes.  Rebuilding hashtrees is part of the defence against accidental deletion (e.g. through user error), and data corruption within the store where no read-repair is other wise triggered.  So although leveled can make hashtree rebuilds faster by only folding over the heads, this only answers part of the problem.  A rebuild based on heads only proves deletion/corruption has not occurred in the Ledger, but doesn't rule out the possibility that deletion/corruption has occurred in the Journal.

The fold_heads/4 implementation in leveled partially answers the challenge of Journal deletion or corruption by checking for presence in the Journal as part of the fold.  Presence checking means that the object sequence number is in the Journal manifest, and the hash of the Key & sequence number is the lookup tree for that Journal.  It is expected that corruption of blocks within journal files will be handled as part of the compaction work to be tested in Phase 3.

### Leveled AAE rebuild with journal check

The comparison between leveled and leveldb shows a marked difference in throughput volatility and tail latency (especially with updates).

Riak + leveled           |  Riak + leveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_aae/output/summary_leveled_5n_100t_i2_nosync_inkcheckaae.png "LevelEd")  |  ![](../test/volume/cluster_aae/output/summary_leveldb_5n_100t_i2_nosync_sweeperaae.png "LevelDB")

The differences between the two tests are:

- <strong>47.7%</strong> more requests handled with leveled across the whole test window
- <strong>54.5%</strong> more requests handled with leveled in the last hour of the test
- <b>7</b> more hashtree rebuilds completed in the leveled test

As with other tests the leveled test had a <strong>slower mean GET time</strong> (<b>6.0ms</b> compared to <b>3.7ms</b>) reflecting the extra cycle caused by deferring the GET request until quorum has been reached through HEAD requests.  The leveled test by contrast though had a substantially <strong>more predictable and lower mean UPDATE time</strong> (<b>14.7ms</b> compared to <b>52.4ms</b>).

Throughput in the leveldb test is reduced from a comparative test without active anti-entropy by a significant amount (more than <b>11%</b>), whereas the throughput reduction from enabling/disabling anti-entropy is marginal in leveled comparisons.

### Comparison without journal check

 One surprising result from the AAE comparison is that although the leveled test shows less variation in throughput because of the rebuilds, the actual rebuilds take a roughly equivalent time in both the leveled and leveldb tests:

 Hour          |  Riak + leveled rebuild times |  Riak + leveldb rebuild times
 :-----------:|:-------------------------:|:-------------------------:
 1 | 519.51 | 518.30
 2 | 793.24 | 719.97
 3 | 1,130.36 | 1,111.25
 4 | 1,428.16 | 1,459.03
 5 | 1,677.24 | 1,668.50
 6 | 1,818.63 | 1,859.14

Note that throughput was 50% higher in the Riak + leveled test, so although the times are comparable the database sizes are larger in this test.

To explore this further, the same test was also run but with the leveled backend only folding heads across the Ledger (and not doing any check for presence in the Journal).  In the ledger-only checking version 110 rebuilds were completed in the test as opposed to 80 rebuilds completed without the Journal check.  The time for the checks are halved if the Journal check is removed.

Although the rebuild times are reduced by removing the Journal check, the throughput change for database consumers is negligible (and well within the margin of error between test runs).  So removing the Journal check makes rebuilds faster, but doesn't improve overall database performance for users.

### Comparison with "legacy" rebuild mechanism in leveldb

The sweeper mechanism is a new facility in the riak_kv develop branch, and has a different system of throttles to the direct fold mechanism previously used.  Without sweeper enabled, by default only one concurrent rebuild is allowed per cluster, whereas sweeper restricts to one concurrent rebuild per node.  Sweeper has an additional throttle based on the volume of data passed to control the pace of each sweep.

If the same test is run with a leveldb backend but with the pre-sweeper fold mechanism, then total throughput across the is improved by <b>8.9%</b>.  However, this throughput reduction comes at the cost of a <b>90%</b> reduction in the number of rebuilds completed within the test.

## Riak Cluster Test - Phase 3 - Compaction

to be completed ..

Testing during a journal compaction window

## Riak Cluster Test - Phase 4 - 2i

Testing with secondary indexes provides some new challenges:

- Including index entries on updates will accelerate the flow of merges in the merge tree, as each update into the merge tree is now multiple changes;
- The 2i query needs an efficient snapshot to be taken of the store, and multiple such snapshots to exist in parallel;
- The coverage nature of the query means that the query operation is now as slow as the slowest of the vnodes (out of 22 nodes with a ring size of 64), rather than being as slow as the second slowest of three vnodes.  Coverage queries are reactive to local resource pressure, whereas standard GET flows can work around such pressure.

The secondary index test was built on a test which sent

- 100 GET requests;
- 10 update requests (i.e. GET then PUT) for 8KB fixed sized objects;
- 10 update requests (i.e. GET then PUT) for 8KB fixed sized objects with four index entries each (leading to 4 index changes for a new request, 8 index changes for an update request);
- 1 2i range query on the "date of birth" index with return_terms enabled;
- 1 2i range query on the "postcode index" with return_terms enabled.

The query load is relatively light compared to GET/PUT load in-line with Basho recommendations (decline from 350 queries per second to 120 queries per second through the test).  The queries
return o(1000) results maximum towards the tail of the test and o(1) results at the start of the test.

Further details on the implementation of the secondary indexes for volume tests can be found in the [driver file](https://github.com/martinsumner/basho_bench/blob/mas-nhsload/src/basho_bench_driver_riakc_pb.erl) for the test.

Riak + leveled           |  Riak + leveldb
:-------------------------:|:-------------------------:
![](../test/volume/cluster_2i/output/summary_leveled_5n_80t_i2_nosync_2i.png "LevelEd")  |  ![](../test/volume/cluster_2i/output/summary_leveldb_5n_80t_i2_nosync_2i.png "LevelDB")

The results are similar as to previous tests.  Although the test is on infrastructure with optimised disk throughput (and with no flushing to disk on write from Riak to minimise direct pressure from Riak), when running the tests with leveldb disk busyness rapidly becomes a constraining factor - and the reaction to that is volatility in throughput.  Riak combined with leveldb is capable in short bursts of greater throughput than Riak + leveled, however when throttled within the cluster by a node or nodes with busy disks, the reaction is extreme.

The average throughputs hour by hour are:

Hour      |Riak + leveled             |  Riak + leveldb           | Delta
:--------|-------------------------:|-------------------------:|--------
1 | 14,465.64 | 16,235.83 | - 10.90%
2 | 10,600.16 | 11,813.14 | - 10.27%
3 |  8,605.89 |  7,895.84 | <b>+ 18.63%</b>
4 |  9,366,19 |  7,225.25 | <b>+ 19.11%</b>
5 |  8,180.88 |  6,826.37 | <b>+ 19.84%</b>
6 |  7,916.19 |  6,469.73 | <b>+ 22.36%</b>

The point of inflection when leveled-backed Riak begins to out-perform leveledb-backed Riak is related to the total size of the database going beyond the size of the in-memory cache.  The need to go to disk for reads as well as writes amplifies the disk busyness problem associated with write amplification.

As before, increasing the available CPU will increase the potential throughput of a leveled-backed cluster.  Increasing the disk IOPS rate will increase the potential throughput of a leveldb-backed cluster.  Increasing object sizes, will extend the throughput advantage of leveled and decreasing sizes will improve the performance of leveldb.  Increasing the size of the database relative to cache may increase the throughput advantage of leveled - but this may not hold true for all work profiles (for example those frequently accessing recently updated objects).

In the 2i query test there is the same significant reduction in tail latency when comparing leveled with leveldb.  For the 2i queries there is also a significant reduction in mean latency - a <b> 57.96%</b> reduction over the course of the test.  However this reduction is not a sign that leveled can resolve 2i queries faster than leveldb - it is related to the problem of tail latency.  The minimum response time for a 2i query in leveldb drifts from 4ms to 14ms as the test progresses - whereas the minimum response time for Riak and leveled fluctuates from 5ms through to 20ms at the end of the test.  Outside of resource pressure leveldb will give a faster response - but resource pressure is much more likely with leveldb.


## Riak Cluster Test - Phase 5 - Bitcask compare

For larger objects, it is interesting to compare performance with between Bitcask and leveled.  At the start of a pure push test it is not possible for leveled to operate at the same rate as bitcask - as bitcask is finding/updating pointer s through a NIF'd memory operation in C (whereas leveled needs to has two levels of indirect pointers which are being lazily persisted to disk).

However, as the test progresses, there should be an advantage in that leveled is not needing to read each object from disk N times on both PUT and GET - so as the scope of the database creeps beyond that covered by the page cache, and as the frequency of disk reads starts to create an I/O bottlneck then leveled throughput will become more competitive.

Here is a side-by-side on a standard Phase 1 test on i2, without sync, and with 60 threads and 16KB objects.

Riak + leveled           |  Riak + bitcask
:-------------------------:|:-------------------------:
![](../test/volume/cluster_five/output/summary_leveled_5n_60t_i2_16KB_nosync.png "LevelEd")  |  ![](../test/volume/cluster_five/output/summary_bitcask_5n_60t_i2_16KB_nosync.png "LevelDB")

In the first hour of the test, bitcask throughput is <b>39.13%</b> greater than leveled.  Over the whole test, the bitcask-backed cluster achieves <b>16.48%</b> more throughput than leveled, but in the last hour this advantage is just <b>0.34%</b>.

The results for bitcask look a bit weird and lumpy though, so perhaps there's something else going on here that's contributing to the gap closing.
