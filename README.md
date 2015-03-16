# ![Logo](http://i.imgur.com/zdlOSZD.png?1) Onyx

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/MichaelDrogalis/onyx?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### What is it?

- a masterless, cloud scale, fault tolerant, distributed computation system
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
[com.mdrogalis/onyx "0.5.3"]
```

### Quick Start Guide

Feeling impatient? Hit the ground running ASAP with the [onyx-starter repo](https://github.com/MichaelDrogalis/onyx-starter) and [walkthrough](https://gist.github.com/MichaelDrogalis/bc620a7617396704125b).

### User Guide 0.5.3
- [What does Onyx offer?](doc/user-guide/what-does-it-offer.md)
- [Concepts](doc/user-guide/concepts.md)
- [Environment](doc/user-guide/environment.md)
- [Hardware](doc/user-guide/hardware.md)
- [APIs](doc/user-guide/apis.md)
- [Constraints](doc/user-guide/constraints.md)
- [Internal Design](doc/user-guide/internal-design.md)
- [Peer Configuration](doc/user-guide/peer-config.md)
- [Information Model](doc/user-guide/information-model.md)
- [Functions](doc/user-guide/functions.md)
- [Flow Conditions](doc/user-guide/flow-conditions.md)
- [Scheduling Jobs and Tasks](doc/user-guide/scheduling.md)
- [Event Subscription](doc/user-guide/subscription.md)
- [Error Handling](doc/user-guide/error-handling.md)
- [Plugins](doc/user-guide/plugins.md)
- [HornetQ Internal Plugin](doc/user-guide/hornetq-plugin.md)
- [Reliability Guarantees](doc/user-guide/reliability-guarantees.md)
- [Logging](doc/user-guide/logging.md)
- [Performance Tuning](doc/user-guide/performance-tuning.md)
- [Examples](doc/user-guide/examples.md)
- [Frequently Asked Questions](doc/user-guide/faq.md)

### API Docs 0.5.3

Code level API documentation [can be found here](http://michaeldrogalis.github.io/onyx/).

### Official plugin listing

Official plugins are vetted by Michael Drogalis. Ensure in your project that plugin versions directly correspond to the same Onyx version (e.g. `onyx-core-async` version `0.5.3` goes with `onyx` version `0.5.3`). Fixes to plugins can be applied using a 4th versioning identifier (e.g. `0.5.3.1`).

- [`onyx-hornetq`](doc/user-guide/hornetq-plugin.md)
- [`onyx-datomic`](https://github.com/MichaelDrogalis/onyx-datomic)
- [`onyx-sql`](https://github.com/MichaelDrogalis/onyx-sql)
- [`onyx-kafka`](https://github.com/MichaelDrogalis/onyx-kafka)
- [`onyx-core-async`](https://github.com/MichaelDrogalis/onyx-core-async)

Generate plugin templates through Leiningen with [`onyx-plugin`](https://github.com/MichaelDrogalis/onyx-plugin).

### Offical Dashboard

You can run a dashboard to monitor Onyx cluster activity, found [here](https://github.com/lbradstreet/onyx-dashboard).

### lib-onyx

[lib-onyx](https://github.com/MichaelDrogalis/lib-onyx) is a library created to support extra functionality in Onyx. It provides pluggable functionality such as in-memory streaming joins, automatic message retry, and interval-based actions.

### Release Notes

You can find [the lastest major release notes here](doc/release-notes/0.5.0.md).

### Need help?

Check out the [Onyx Google Group](https://groups.google.com/forum/#!forum/onyx-user).

### Want the logo?

Feel free to use it anywhere. You can find [a few different versions here](https://github.com/MichaelDrogalis/onyx/tree/0.5.x/resources/logo).

### Running the tests

A simple `lein midje` will run the full suite, which takes about 15-20 minutes on my quad-core MacBook Pro.

### Contributing

Contributions are welcome. Please fork the repository and send a pull request to the master branch.

#### Branching

Onyx uses a similiar branching strategy to Clojure itself. Onyx uses semantic versioning, and each minor version gets its own branch. All work is done on master or feature branches and dropped into a major.minor.x branch when it's time to cut a new release. Pull requests into the master branch are welcome.

#### Commit rights

Anyone who has a patch accepted may request commit rights. Please do so inside the pull request post-merge.

#### Contributor list

- [Michael Drogalis](https://github.com/MichaelDrogalis)
- [Owen Jones](https://github.com/owengalenjones)
- [Bruce Durling](https://github.com/otfrom)
- [Malcolm Sparks](https://github.com/malcolmsparks)
- [Lucas Bradstreet](https://github.com/lbradstreet)
- [Bryce Blanton](https://github.com/bblanton)
- [David Rupp](https://github.com/davidrupp)
- [sbennett33](https://github.com/sbennett33)
- [Tyler van Hensbergen](https://github.com/tvanhens)

#### Performance Benchmarks

At the time of writing this, I do not have any performance benchmarks to publish. Creating a correct, useful benchmark is extremely difficult. I'm working on it - hang tight.

### Author

This project is authored by [Michael Drogalis](https://twitter.com/MichaelDrogalis). You can get me directly at (mjd3089.at.rit.edu) if needed.

### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.

### Profiler

![YourKit](https://raw.githubusercontent.com/MichaelDrogalis/onyx/master/resources/logo/yourkit.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.
