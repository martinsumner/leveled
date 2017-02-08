# LevelEd - A Log-Structured Merge Tree

The following section is a brief overview of some of the motivating factors behind developing LevelEd.

## A Paper To Love

The concept of a Log Structured Merge Tree is described within the 1996 paper ["The Log Structured Merge Tree"](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.44.2782&rep=rep1&type=pdf) by Patrick O'Neil et al.  The paper is not specific on precisely how an LSM-Tree should be implemented, proposing a series of potential options.  The paper's focus is on framing the justification for design decisions in the context of hardware economics.  

The hardware economics at the time of the paper were:

- COST<sub>d</sub> = cost of 1MByte of disk storage = $1

- COST<sub>m</sub> = cost of 1MByte of memory storage = $100

- COST<sub>P</sub> = cost of 1 page/second IO rate random pages = $25

- COST<sub>pi</sub> = cost of 1 page/second IO rate multi-page block = $2.50

Consideration on design trade-offs for LevelEd should likewise start with a viewpoint on the modern cost of hardware - in the context where we expect the store to be run (i.e. as multiple stores co-existing on a server, sharing workload across multiple servers).

### Modern Hardware Costs

Based on the experience of running Riak at scale in production for the NHS, what has been noticeable is that although on the servers used around 50% of the cost goes on disk-related expenditure, in almost all scenarios where the system is pushed to limits the limit first hit is in disk throughput.  

The purchase costs of disk though, do not accurately reflect the running costs of disks - because disks fail, and fail often.  Also the hardware choices made by the NHS for the Spine programme, do not necessarily reflect the generic industry choices and costs.

To get an up-to-date and objective measure of what the overall costs are, the Amazon price list can assist if we assume their data centres are managed with a high-degree of efficiency due to their scale.  Assumptions on the costs of individual components can be made by examining differences between specific instance prices.

As at 26th January 2017, the [on-demand instance pricing](https://aws.amazon.com/ec2/pricing/on-demand/) for servers is:

- c4.4xlarge - 16 CPU, 30 GB RAM, EBS only - $0.796

- r4.xlarge  - 4 CPU, 30 GB RAM, EBS only - $0.266

- r4.4xlarge - 16 CPU, 122 GB RAM, EBS only - $1.064

- i2.4xlarge - 16 CPU, 122 GB RAM, 4 X 800 GB SSD - $3.41

- d2.4xlarge - 16 CPU, 122 GB RAM, 12 X 2000 GB HDD - $2.76

By comparing these prices we can surmise that the relative costs are:

- 1 CPU - $0.044 per hour

- 1GB RAM - $0.0029 per hour

- 1GB SDD - $0.0015 per hour (assumes mirroring)

- 1GB HDD - $0.00015 per hour (assumes mirroring)

If a natural ratio in a database server is 1 CPU: 10GB RAM: 200GB disk - this would give a proportional cost of the disk of 80% for SSD and 25% for HDD.  

Compared to the figures at the time of the LSM-Tree paper, the actual delta in the per-byte cost of memory and the per-byte costs of disk space has closed significantly, even when using the lowest cost disk options.  This may reflect changes in the pace of technology advancement, or just the fact that maintenance cost associated with different failure rates is now more correctly priced.  

The availability of SDDs is not a silver bullet to disk i/o problems when cost is considered, as although they eliminate the additional costs of random page access through the removal of the disk head movement overhead (of about 6.5ms per shift), this benefit is at an order of magnitude difference in cost compared to spinning disks, and at a cost greater than half the price of DRAM.  SSDs have not taken the problem of managing the overheads of disk persistence away, they've simply added another dimension to the economic profiling problem.

In physical on-premise server environments there is also commonly the cost of disk controllers.  Disk controllers bend the economics of persistence through the presence of flash-backed write caches.  However, disk controllers also fail - within the NHS environment disk controller failures are the second most common device failure after individual disks.  Failures of disk controllers are also expensive to resolve, not being hot-pluggable like disks, and carrying greater risk of node data-loss due to either bad luck or bad process during the change.  

It is noticeable that EC2 does not have disk controllers and given their failure rate and cost of recovery, this appears to be a sensible trade-off.  However, software-only RAID has drawbacks, include the time to setup RAID (24 hours on a d2.2xlarge node), recover from a disk failure and the time to run [scheduled checks](https://www.thomas-krenn.com/en/wiki/Mdadm_checkarray).

Making cost-driven decisions about storage design remains as relevant now as it was two decades ago when the LSM-Tree paper was published, especially as we can now directly see those costs reflected in hourly resource charges.

### eleveldb Evolution

The evolution of leveledb in Riak, from the original Google-provided store to the significantly refactored eleveldb provided by Basho reflects the alternative hardware economics that the stores were designed for.  

The original leveledb considered in part the hardware economics of the phone where there are clear constraints around CPU usage - due to both form-factor and battery life, and where disk space may be at a greater premium than disk IOPS.  Some of the evolution of eleveldb is down to the Riak-specific problem of needing to run multiple stores on a single server, where even load distribution may lead to a synchronisation of activity.  Much of the evolution is also about how to make better use of the continuous availability of CPU resource, in the face of the relative scarcity of disk resource.  Changes such as overlapping files at level 1, hot threads, compression improvements etc all move eleveldb in the direction of being easier on disk at the cost of CPU; and the hardware economics of servers would indicate this is a wise choice

### Planning for LevelEd

The primary design differentiation between LevelEd and LevelDB is the separation of the key store (known as the Ledger in LevelEd) and the value store (known as the journal).  The Journal is like a continuous extension of the nursery log within LevelDB, only with a gradual evolution into [CDB files](https://en.wikipedia.org/wiki/Cdb_(software)) so that file offset pointers are not required to exist permanently in memory.  The Ledger is a merge tree structure, with values substituted with metadata and a sequence number - where the sequence number can be used to find the value in the Journal.

This is not an original idea, the LSM-Tree paper specifically talked about the trade-offs of placing identifiers rather than values in the merge tree:

> To begin with, it should be clear that the LSM-tree entries could themselves contain records rather than RIDs pointing to records elsewhere on disk. This means that the records themselves can be clustered by their keyvalue. The cost for this is larger entries and a concomitant acceleration of the rate of insert R in bytes per second and therefore of cursor movement and total I/O rate H. 

The reasoning behind the use of this structure is an attempt to differentiate more clearly between a (small) hot database space (the Ledger) and a (much larger) cold database space (the non-current part of the Journal) so that through use of the page cache, or faster disk, the hot part of the database can be optimised for rapid access.

In parallel to this work, there has also been work published on [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) which explores precisely this trade-off.

There is an additional optimisation that is directly relevant to Riak.  Riak always fetches both Key and Value as a GET operation within a cluster, but theoretically in many cases, where the object is not a sibling, this is not necessary.  For example when GETting an object to perform a PUT, only the vector clock and index specs are actually necessary if the object is not a CRDT.  Also when performing a Riak GET operation the value is fetched three times, even if there is no conflict between the values.

So the hypothesis that separating Keys and Values may be optimal for LSM-Trees in general is potentially extendable for Riak, where there exists the potential in the majority of read requests to replace the GET of a value with a lower cost HEAD request for just Key and Metadata.   

## Being Operator Friendly

The LSM-Tree paper focuses on hardware trade-offs in database design.  LevelEd is focused on the job of being a backend to a Riak database, and the Riak database is opinionated on the trade-off between developer and operator productivity.  Running a Riak database imposes constraints and demands on developers - there are things the developer needs to think hard about: living without transactions, considering the resolution of siblings, manual modelling for query optimisation.  

However, in return for this pain there is great reward, a reward which is gifted to the operators of the service.  Riak clusters are reliable and predictable, and operational processes are slim and straight forward - preparation for managing a Riak cluster in production needn't go much beyond rehearsing magical cure-alls of the the node stop/start and node join/leave processes.  At the NHS, where we have more than 50 Riak nodes in 24 by 365 business critical operations, it is not untypical to go more than 28-days without anyone logging on to a database node.  This is a relief for those of us who have previously lived in the world with databases with endless configuration parameters to test or blame for issues, where you always seem to be the unlucky one who suffer the outages "never seen in any other customer", where the databases come with ever more complicated infrastructure dependencies and and where DBAs need to be constantly at-hand to analyse reports, kill rogue queries and re-run the query optimiser as an answer to the latest 'blip' in performance.

Developments on Riak of the past few years, in particular the introduction of CRDTs, have made some limited progress in easing the developer headaches.  No real progress has been made though in making things more operator friendly, and although operator sleep patterns are the primary beneficiary of a Riak installation, that does not mean to say that things cannot be improved.

### Planning For LevelEd

The primary operator improvements sought are:

- Increased visibility of database contents.  Riak lacks efficient answers to simple questions about the bucket names which have been defined and the size and space consumed by different buckets.

- Reduced variation.  Riak has unscheduled events, in particular active anti-entropy hashtree rebuilds, that will temporarily impact the performance of the cluster both during the event (due to resource pressure of the actual rebuild) and immediately after (e.g. due to page cache pollution).  The intention is to convert object-scanning events, into events which require only Key/Metadata scanning.

- More predictable capacity management.  Production systems constrained by disk throughput are hard to monitor for capacity, especially as there is no readily available measure of disk utilisation (note the often missed warning in the iostat man page - 'Device saturation occurs when this value is close to 100% for devices serving requests serially. But for devices serving requests in parallel, such as RAID arrays and modern SSDs, this number does not reflect their performance limits').  Monitoring disk-bound *nix-based systems requires the monitoring of volatile late-indicators of issues (e.g. await times), and this can be exacerbated by volatile demands on disk (e.g. due to compaction), and the ongoing risk of either individual disk failure or the overhead of individual disk recovery.

- Code context switching.  Looking at code can be helpful, having the majority of the code in Erlang and then a portion of the code in C++, makes understanding and monitoring behaviour within the database that bit harder.

- Backup space issues.  Managing the space consumed by backups, and the time required to produce backups, is traditionally handled through the use of delta-based backup systems such as rsync.  However, such systems are far less effective if the system to be backed up has write amplification by design.

