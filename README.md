## Onyx

### What is it?

- a cloud scale, fault tolerant, distributed computation system
- written in Clojure
- batch and stream processing hybrid
- exposes an *information model* for the description and construction of distributed workflows
- enabled by hardware advances in the last 10 years with respect to solid state drives and increased network speeds
- Competes against Storm, Cascading, Cascalog, Map/Reduce, Dryad, Apache Sqoop, Twitter Crane, and Netflix Forklift

### I built this because I wanted...
- an information model, rather than an API, for describing distributed workflows
- temporal decoupling of workflow descriptions from workflow execution
- an elimination of macros from the API that is offered
- plain Clojure functions as building blocks for application logic
- a very easy way to test distributed workflows locally, without buckets of mocking code
- a decoupled technique for configuring workflows
- transactional, exactly-once semantics for moving data between nodes in a cluster
- transparent code reuse between streaming and batching workflows
- friendlier interfaces to plug into IO for data sources
- aspect orientation without a headache
- to get away from AOT complilation and avoid dependency hell
- heterogenous jar execution for performing rolling releases

### Installation

Available on Clojars:

```
[com.mdrogalis/onyx "0.3.0"]
```

### Get Me Started ASAP!

### Full length documentation

- [What is it?](docs/user-guide/what-is-it.md)
- [What does it offer?](docs/user-guide/what-does-it-offer.md)
- [Concepts](docs/user-guide/concepts.md)
- [Environment](docs/user-guide/environment.md)
- [APIs](docs/user-guide/apis.md)
- [Constraints](docs/user-guide/constraints.md)
- [Architecture](docs/user-guide/architecture.md)
- [Coordinator High Availability](docs/user-guide/coordinator-ha.md)
- [Information Model](docs/user-guide/information-model.md)
- [Plugins](docs/user-guide/plugins.md)
- [HornetQ Internal Plugin](docs/user-guide/hornetq-plugin.md)
- [Job and Peer Execution Scheduling](docs/user-guide/scheduling.md)
- [Reliability Gauruntees](docs/user-guide/relability-guaruntees.md)
- [Performance Tuning](docs/user-guide/performance-tuning.md)
- [Examples](docs/user-guide/examples.md)
- [Frequently Asked Questions](docs/user-guide/faq.md)

### Official plugin listing

- [`onyx-hornetq`](docs/user-guide/hornetq-plugin.md)
- [`onyx-datomic`](https://github.com/MichaelDrogalis/onyx-datomic)
- [`onyx-sql`](https://github.com/MichaelDrogalis/onyx-sql)
- [`onyx-core-async`](https://github.com/MichaelDrogalis/onyx-core-async)

### Need help?

#### Google Groups

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