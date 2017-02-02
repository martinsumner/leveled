# LevelEd - An Erlang Key-Value store

## Introduction

LevelEd is a work-in-progress prototype of a simple Key-Value store based on the concept of Log-Structured Merge Trees, with the following characteristics:

- Optimised for workloads with larger values (e.g. > 4KB).
- Supporting of values split between metadata and body, and allowing for HEAD requests which have lower overheads than GET requests.
- Support for tagging of object types and the implementation of alternative store behaviour based on type.
- Support for low-cost clones without locking to provide for scanning queries, specifically where there is a need to scan across keys and metadata (not values).
- Written in Erlang as a message passing system between Actors.

The store has been developed with a focus on being a potential backend to a Riak KV database, rather than as a generic store.  The primary aim of developing (yet another) Riak backend is to examine the potential to reduce the broader costs providing sustained throughput in Riak i.e. to provide equivalent throughput on cheaper hardware.  It is also anticipated in having a fully-featured pure Erlang backend may assist in evolving new features through the Riak ecosystem  which require end-to-end changes.

The store is not expected to be 'faster' than leveldb (the primary fully-featured Riak backend available today), but it is likely in some cases to offer improvements in throughput.

## More Details

For more details on the store:

- An [introduction](docs/INTRO.md) to LevelEd covers some context to the factors motivating design trade-offs in the store.

- The [design overview](docs/DESIGN.md) explains the actor model used and the basic flow of requests through the store.

- [Future work](docs/FUTURE.md) covers new features being implemented at present, and improvements necessary to make the system production ready.

- There is also a ["Why"](WHYWHYWHYWHYWHY.md) section looking at lower level design choices and the rationale that supports them.

## Is this interesting?

At the initiation of the project I accepted that making a positive contribution to this space is hard - given the superior brainpower and experience of those that have contributed to the KV store problem space in general, and the Riak backend space in particular.

The target at inception was to do something interesting, something that articulates through working software the potential for improvement to exist by re-thinking certain key assumptions and trade-offs.

[Initial volume tests](docs/VOLUME.md) indicate that it is at least interesting, with substantial improvements in both throughput (73%) and tail latency (1:20) when compared to eleveldb - in scenarios expected to be optimised for leveled.  Note, to be clear, this is statement falls well short of making a general claim that it represents a 'better' Riak backend than leveldb.

More information can be found in the [volume testing section](docs/VOLUME.md).