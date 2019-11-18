# Leveled - An Erlang Key-Value Store

## Introduction

[![Build Status](https://travis-ci.com/martinsumner/leveled.svg?branch=master)](https://travis-ci.com/martinsumner/leveled)

Leveled is a simple Key-Value store based on the concept of Log-Structured Merge Trees, with the following characteristics:

- Optimised for workloads with <b>larger values</b> (e.g. > 4KB).

- Explicitly supports <b>HEAD requests</b> in addition to GET requests:
  - Splits the storage of value between keys/metadata and body (assuming some definition of metadata is provided);
  - Allows for the application to define what constitutes object metadata and what constitutes the body (value-part) of the object - and assign tags to objects to manage multiple object-types with different extraction rules;
  - Stores keys/metadata in a merge tree and the full object in a journal of [CDB files](https://en.wikipedia.org/wiki/Cdb_(software))
  - allowing for HEAD requests which have lower overheads than GET requests; and
  - queries which traverse keys/metadatas to be supported with fewer side effects on the page cache than folds over keys/objects.

- Support for tagging of <b>object types</b> and the implementation of alternative store behaviour based on type.
  - Allows for changes to extract specific information as metadata to be returned from HEAD requests;
  - Potentially usable for objects with special retention or merge properties.

- Support for low-cost clones without locking to provide for <b>scanning queries</b> (e.g. secondary indexes).
  - Low cost specifically where there is a need to scan across keys and metadata (not values).

- Written in <b>Erlang</b> as a message passing system between Actors.

The store has been developed with a <b>focus on being a potential backend to a Riak KV</b> database, rather than as a generic store.  It is intended to be a fully-featured backend - including support for secondary indexes, multiple fold types and auto-expiry of objects.

An optimised version of Riak KV has been produced in parallel which will exploit the availability of HEAD requests (to access object metadata including version vectors), where a full GET is not required.  This, along with reduced write amplification when compared to leveldb, is expected to offer significant improvement in the volume and predictability of throughput for workloads with larger (> 4KB) object sizes, as well as reduced tail latency.

There may be more general uses of Leveled, with the following caveats:
  - Leveled should be extended to define new tags that specify what metadata is to be extracted for the inserted objects (or to override the behaviour for the ?STD_TAG).  Without this, there will be limited scope to take advantage of the relative efficiency of HEAD and FOLD_HEAD requests.
  - If objects are small, the [`head_only` mode](docs/STARTUP_OPTIONS.md#head-only) may be used, which will cease separation of object body from header and use the Key/Metadata store as the only long-term persisted store.  In this mode all of the object is treated as Metadata, and the behaviour is closer to that of the leveldb LSM-tree, although with higher median latency.

## More Details

For more details on the store:

- An [introduction](docs/INTRO.md) to Leveled covers some context to the factors motivating design trade-offs in the store.

- The [design overview](docs/DESIGN.md) explains the actor model used and the basic flow of requests through the store.

- [Future work](docs/FUTURE.md) covers new features being implemented at present, and improvements necessary to make the system production ready.

- There is also a ["Why"](docs/WHY.md) section looking at lower level design choices and the rationale that supports them.

## Is this interesting?

Making a positive contribution to this space is hard - given the superior brainpower and experience of those that have contributed to the KV store problem space in general, and the Riak backend space in particular.

The target at inception was to do something interesting, to re-think certain key assumptions and trade-offs, and prove through working software the potential for improvements to be realised.

[Initial volume tests](docs/VOLUME.md) indicate that it is at least interesting.  With improvements in throughput for multiple configurations, with this improvement becoming more marked as the test progresses (and the base data volume becomes more realistic).  

The delta in the table below  is the comparison in Riak throughput between the identical test run with a leveled backend in comparison to leveldb.  The realism of the tests increase as the test progresses - so focus is given to the throughput delta in the last hour of the test.

Test Description                  | Hardware     | Duration |Avg TPS    | TPS Delta (Overall)  | TPS Delta (Last Hour)
:---------------------------------|:-------------|:--------:|----------:|-----------------:|-------------------:
8KB value, 60 workers, sync       | 5 x i2.2x    | 4 hr     | 12,679.91 | <b>+ 70.81%</b>  | <b>+ 63.99%</b>
8KB value, 100 workers, no_sync   | 5 x i2.2x    | 6 hr     | 14,100.19 | <b>+ 16.15%</b>  | <b>+ 35.92%</b>
8KB value, 50 workers, no_sync    | 5 x d2.2x    | 4 hr     | 10,400.29 | <b>+  8.37%</b>  | <b>+ 23.51%</b>
4KB value, 100 workers, no_sync   | 5 x i2.2x    | 6 hr     | 14,993.95 | - 10.44%  | - 4.48%
16KB value, 60 workers, no_sync   | 5 x i2.2x    | 6 hr     | 11,167.44 | <b>+ 80.48%</b>  | <b>+ 113.55%</b>
8KB value, 80 workers, no_sync, 2i queries | 5 x i2.2x | 6 hr | 9,855.96 | <b>+ 4.48%</b> | <b>+ 22.36%</b>

Tests generally show a 5:1 improvement in tail latency for leveled.

All tests have in common:

- Target Key volume - 200M with pareto distribution of load
- 5 GETs per 1 update
- RAID 10 (software) drives
- allow_mult=false, lww=false
- modified riak optimised for leveled used in leveled tests

The throughput in leveled is generally CPU-bound, whereas in comparative tests for leveledb the throughput was disk bound.  This potentially makes capacity planning simpler, and opens up the possibility of scaling out to equivalent throughput at much lower cost (as CPU is relatively low cost when compared to disk space at high I/O) - [offering better alignment between resource constraints and the cost of resource](docs/INTRO.md).

More information can be found in the [volume testing section](docs/VOLUME.md).

As a general rule though, the most interesting thing is the potential to enable [new features](docs/FUTURE.md).  The tagging of different object types, with an ability to set different rules for both compaction and metadata creation by tag, is a potential enabler for further change.   Further, having a separate key/metadata store which can be scanned without breaking the page cache or working against mitigation for write amplifications, is also potentially an enabler to offer features to both the developer and the operator.

## Feedback

Please create an issue if you have any suggestions.  You can ping me <b>@masleeds</b> if you wish

## Running Leveled

Unit and current tests in leveled should run with rebar3.  Leveled has been tested in OTP18, but it can be started with OTP16 to support Riak (although tests will not work as expected).  

A new database can be started by running

```
{ok, Bookie} = leveled_bookie:book_start(StartupOptions)   
```

This will start a new Bookie.  It will start and look for existing data files, under the RootPath, and start empty if none exist.  Further information on startup options can be found here [here](docs/STARTUP_OPTIONS.md).

The book_start method should respond once startup is complete.  The [leveled_bookie module](src/leveled_bookie.erl) includes the full API for external use of the store.

Running in Riak requires Riak 2.9 or beyond, which is available from January 2019.

### Contributing

In order to contribute to leveled, fork the repository, make a branch
for your changes, and open a pull request. The acceptance criteria for
updating leveled is that it passes rebar3 dialyzer, xref, eunit, and
ct with 100% coverage.

To have rebar3 execute the full set of tests, run:

    `rebar3 as test do cover --reset, eunit --cover, ct --cover, cover --verbose`
