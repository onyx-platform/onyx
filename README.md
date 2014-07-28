## Onyx

### What is it?

- a cloud scale, fault tolerant, distributed computation system
- written in Clojure, for Clojure
- batch and stream processing hybrid
- exposes an information model for the description and construction of distributed workflows
- enabled by hardware advances in the last 10 years
- Competes against Storm, Cascading, Map/Reduce, Dryad, Apache Sqoop, Twitter Crane, etc

### Installation

Available on Clojars:

```
[com.mdrogalis/onyx "0.3.0"]
```

### Quick Start Guide

Feeling impatient? Hit the ground running ASAP with the [onyx-starter repo](https://github.com/MichaelDrogalis/onyx-starter).

### Full length documentation 0.3.0

- [What does Onyx offer?](doc/user-guide/what-does-it-offer.md)
- [Concepts](doc/user-guide/concepts.md)
- [Environment](doc/user-guide/environment.md)
- [APIs](doc/user-guide/apis.md)
- [Constraints](doc/user-guide/constraints.md)
- [Architecture](doc/user-guide/architecture.md)
- [Coordinator and Peer Configuration](doc/user-guide/coord-peer-config.md)
- [Information Model](doc/user-guide/information-model.md)
- [Plugins](doc/user-guide/plugins.md)
- [HornetQ Internal Plugin](doc/user-guide/hornetq-plugin.md)
- [Job and Peer Execution Scheduling](doc/user-guide/scheduling.md)
- [Reliability Gauruntees](doc/user-guide/relability-guaruntees.md)
- [Coordinator High Availability](doc/user-guide/coordinator-ha.md)
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

### Contributing

Contributions are welcome. Please fork the repository and send a pull request to the master branch.

#### Branching

Onyx uses a similiar branching strategy to Clojure itself. Onyx uses semantic versioning, and each minor version gets its own branch. All work is done on master or feature branches and dropped into a major.minor.x branch when it's time to cut a new release. Pull requests into the master branch are welcome.

#### Commit rights

Anyone who has a patch accepted may request commit rights. Please do so inside the pull request post-merge.

#### Contributor list

- [Michael Drogalis](https://github.com/MichaelDrogalis)

### Author

This project is authored by [Michael Drogalis](https://twitter.com/MichaelDrogalis), an independent software consultant. Get in touch (mjd3089.at.rit.edu) to work together.

### License

Copyright © 2014 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
