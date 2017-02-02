# Volume Testing

## Parallel Node Testing

Initial volume tests have been [based on standard basho_bench eleveldb test](../test/volume/single_node/examples) to run multiple stores in parallel on the same node and and subjecting them to concurrent pressure. 

This showed a relative positive performance for leveled for both population and load:

Populate leveled           |  Populate eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/single_node/output/leveled_pop.png "LevelEd - Populate")  |  ![](../test/volume/single_node/output/leveldb_pop.png "LevelDB - Populate")

Load leveled             |  Load eleveldb
:-------------------------:|:-------------------------:
![](../test/volume/single_node/output/leveled_load.png "LevelEd - Populate")  |  ![](../test/volume/single_node/output/leveldb_load.png "LevelDB - Populate")

This test was a positive comparison for LevelEd, but also showed that although the LevelEd throughput was relatively stable it was still subject to fluctuations related to CPU constraints.  Prior to moving on to full Riak testing, a number of changes where then made to LevelEd to reduce the CPU load in particular during merge events.

The eleveldb results may not be a fair representation of performance in that:

- Within Riak it was to be expected that eleveldb would start with an appropriate default configuration that might better utilise available memory.
- The test node use had a single desktop (SSD-based) drive, and the the 'low' points of eleveldb performance were associated with disk constraints.  Running on more genuine servers with high-performance disks may give better performance.

## Riak Cluster Test - #1

The First test on a Riak Cluster has been based on the following configuration:

- A 5 node cluster
- Using i2.2x large nodes with mirrored drives (for data partition only)
- noop scheduler, transparent huge pages disabled, ext4 partition
- A 64 vnode ring-size
- 45 concurrent basho_bench threads (basho_bench run on separate disks)
- AAE set to passive
- sync writes enabled (on both backends)
- An object size of 8KB
- A pareto distribution of requests with a keyspace of 50M keys
- 5 GETs for each update
- 4 hour test run

This test showed a <b>73.9%</b> improvement in throughput when using LevelEd, but more importantly a huge improvement in variance in tail latency.  Through the course of the test the average of the maximum response times (in each 10s period) were

leveled GET mean(max)           | eleveldb GET mean(max)
:-------------------------:|:-------------------------:
21.7ms | 410.2ms

leveled PUT mean(max)           | eleveldb PUT mean(max)
:-------------------------:|:-------------------------:
101.5ms | 2,301.6ms

Tail latency under load is around in leveled is less than 5% of the comparable value in eleveldb

leveled Results           |  eleveldb Results
:-------------------------:|:-------------------------:
![](../test/volume/cluster_one/output/summary_leveled_5n_45t.png "LevelEd")  |  ![](../test/volume/cluster_one/output/summary_leveldb_5n_45t.png "LevelDB")


## Riak Cluster Test - #2

