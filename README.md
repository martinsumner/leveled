# Leveled - An Erlang Key-Value Store

## Introduction

[![Build Status](https://github.com/martinsumner/leveled/actions/workflows/erlang.yml/badge.svg?branch=develop-3.1)](https://github.com/martinsumner/leveled/actions)

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

## Feedback

Please create an issue if you have any suggestions.  You can ping me <b>@masleeds</b> if you wish

## Running Leveled

Unit and current tests in leveled should run with rebar3.  

A new database can be started by running

```
{ok, Bookie} = leveled_bookie:book_start(StartupOptions)   
```

This will start a new Bookie.  It will start and look for existing data files, under the RootPath, and start empty if none exist.  Further information on startup options can be found here [here](docs/STARTUP_OPTIONS.md).

The book_start method should respond once startup is complete.  The [leveled_bookie module](src/leveled_bookie.erl) includes the full API for external use of the store.

Running in Riak requires Riak 2.9 or beyond, which is available from January 2019.

There are two main branches under active development:

[`develop-3.4`  - default](https://github.com/martinsumner/leveled/tree/develop-3.4): Target for the Riak 3.4 release with support for OTP 24 and OTP 26;

[`develop-3.1`](https://github.com/martinsumner/leveled/tree/develop-3.1): Target for the Riak 3.2 release with support for OTP 22 and OTP 24.

There are two legacy branches, used in older versions of Riak:

[`develop-3.0`](https://github.com/martinsumner/leveled/tree/develop-3.0): Used in the Riak 3.0 release with support for OTP 20 and OTP 22;

[`develop-2.9`](https://github.com/martinsumner/leveled/tree/develop-2.9): Used in the Riak 2.9 release with support for OTP R16 through to OTP 20.

### Contributing

In order to contribute to leveled, fork the repository, make a branch for your changes, and open a pull request. The acceptance criteria for updating leveled is that it passes rebar3 dialyzer, xref, eunit, and ct with 100% coverage.

To have rebar3 execute the full set of tests, run:

```./rebar3 do xref, dialyzer, cover --reset, eunit --cover, ct --cover, cover --verbose```

For those with a Quickcheck license, property-based tests can also be run using:

```./rebar3 as eqc do eunit --module=leveled_simpleeqc, eunit --module=leveled_statemeqc```
