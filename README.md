## Onyx

### What is it?

- a cloud scale, fault tolerant, distributed computation system
- written in Clojure, for Clojure
- batch and stream processing hybrid
- exposes an information model for the description and construction of distributed workflows
- enabled by hardware advances in the last 10 years
- Competes against Storm, Cascading, Map/Reduce, Dryad, Apache Sqoop, Twitter Crane, etc

### What would I use this for?

- Realtime event stream processing
- Continuous computation 
- Extract, transform, load
- Data transformation à la map-reduce
- Data cleaning
- Data ingestion and storage medium transfer

### Installation

Available on Clojars:

```
[com.mdrogalis/onyx "0.3.0"]
```

### Quick Start Guide

Feeling impatient? Hit the ground running ASAP with the [onyx-starter repo](https://github.com/MichaelDrogalis/onyx-starter) and [walkthrough](https://gist.github.com/MichaelDrogalis/bc620a7617396704125b).

### User Guide 0.3.0
- [What does Onyx offer?](doc/user-guide/what-does-it-offer.md)
- [Concepts](doc/user-guide/concepts.md)
- [Environment](doc/user-guide/environment.md)
- [Hardware](doc/user-guide/hardware.md)
- [APIs](doc/user-guide/apis.md)
- [Constraints](doc/user-guide/constraints.md)
- [Architecture](doc/user-guide/architecture.md)
- [Coordinator and Peer Configuration](doc/user-guide/coord-peer-config.md)
- [Information Model](doc/user-guide/information-model.md)
- [Plugins](doc/user-guide/plugins.md)
- [HornetQ Internal Plugin](doc/user-guide/hornetq-plugin.md)
- [Job and Peer Execution Scheduling](doc/user-guide/scheduling.md)
- [Reliability Guarantees](doc/user-guide/reliability-guarantees.md)
- [Coordinator High Availability](doc/user-guide/coordinator-ha.md)
- [Logging](doc/user-guide/logging.md)
- [Performance Tuning](doc/user-guide/performance-tuning.md)
- [Examples](doc/user-guide/examples.md)
- [Frequently Asked Questions](doc/user-guide/faq.md)

### Official plugin listing

- [`onyx-hornetq`](doc/user-guide/hornetq-plugin.md)
- [`onyx-datomic`](https://github.com/MichaelDrogalis/onyx-datomic)
- [`onyx-sql`](https://github.com/MichaelDrogalis/onyx-sql)
- [`onyx-core-async`](https://github.com/MichaelDrogalis/onyx-core-async)

### Need help?

Check out the [Onyx Google Group](https://groups.google.com/forum/#!forum/onyx-user).

### Running the tests

A simple `lein midje` will run the full suite, which takes about 15 minutes on my quad-core MacBook Pro. Expect a long, 5-10 minute pause during the coordinator tests as it's being completely slammed with requests and checked for correctness. The pause is waiting for the Coordinator to catch up and close out cleanly.

### Contributing

Contributions are welcome. Please fork the repository and send a pull request to the master branch.

#### Branching

Onyx uses a similiar branching strategy to Clojure itself. Onyx uses semantic versioning, and each minor version gets its own branch. All work is done on master or feature branches and dropped into a major.minor.x branch when it's time to cut a new release. Pull requests into the master branch are welcome.

#### Commit rights

Anyone who has a patch accepted may request commit rights. Please do so inside the pull request post-merge.

#### Contributor list

- [Michael Drogalis](https://github.com/MichaelDrogalis)
- [Owen Jones](https://github.com/owengalenjones)

#### Project Maturity

Onyx is **not** a battle-tested framework compared to Storm or Cascading. Please keep this in mind when considering taking it to production. That being said, the test suite uses simulation testing to create exceptionally bad data center network partitions, and checks against a number of correctness criteria afterwards. Community effort will help a create deal on this front.

#### Performance Benchmarks

At the time of writing this, I do not have any performance benchmarks to publish. Creating a correct, useful benchmark is extremely difficult. I'm working on it - hang tight.

### Author

This project is authored by [Michael Drogalis](https://twitter.com/MichaelDrogalis), an independent software consultant. Get in touch (mjd3089.at.rit.edu) to work together.

### License

Copyright © 2014 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
