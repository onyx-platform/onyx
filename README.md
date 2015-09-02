# ![Logo](http://i.imgur.com/zdlOSZD.png?1) Onyx

[![Join the chat at https://gitter.im/onyx-platform/onyx](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/onyx-platform/onyx?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### What is it?

- a masterless, cloud scale, fault tolerant, high performance distributed computation system
- batch and stream hybrid processing model
- exposes an information model for the description and construction of distributed workflows
- Competes against Storm, Cascading, Cascalog, Spark, Map/Reduce, Sqoop, etc
- written in pure Clojure

### What would I use this for?

- Realtime event stream processing
- Continuous computation 
- Extract, transform, load
- Data transformation à la map-reduce
- Data ingestion and storage medium transfer
- Data cleaning

### Installation

Available on Clojars:

```
[org.onyxplatform/onyx "0.7.2"]
```

### Build Status

Component | `0.7.x`| `master`
----------|--------|--------
onyx core | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx/tree/0.7.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx/tree/0.7.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx/tree/master)
onyx-sql  | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-sql/tree/0.7.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-sql/tree/0.7.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-sql/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-sql/tree/master)
onyx-datomic  | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/0.7.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/0.7.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/master)
onyx-kafka| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/0.7.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/0.7.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/master)
onyx-durable-queue| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.7.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.7.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/master)
onyx-seq| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.7.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-seq/tree/0.7.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-seq/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-seq/tree/master)

### Quick Start Guide

Feeling impatient? Hit the ground running ASAP with the [onyx-starter repo](https://github.com/onyx-platform/onyx-starter) and [walkthrough](https://github.com/onyx-platform/onyx-starter/blob/0.7.x/WALKTHROUGH.md). You can also boot into preloaded a Leiningen [application template](https://github.com/onyx-platform/onyx-template).

### User Guide 0.7.2

- [User Guide HTML](http://onyx-platform.gitbooks.io/onyx/content/)
- [User Guide PDF](https://www.gitbook.com/download/pdf/book/onyx-platform/onyx)
- [User Guide Website](http://onyx-platform.gitbooks.io/onyx)

### API Docs 0.7.2

Code level API documentation [can be found here](http://www.onyxplatform.org/api/0.7.2).

### Official plugin listing

Official plugins are vetted by Michael Drogalis. Ensure in your project that plugin versions directly correspond to the same Onyx version (e.g. `onyx-core-async` version `0.7.2` goes with `onyx` version `0.7.2`). Fixes to plugins can be applied using a 4th versioning identifier (e.g. `0.7.2.1`).

- [`onyx-core-async`](doc/user-guide/core-async-plugin.md)
- [`onyx-datomic`](https://github.com/onyx-platform/onyx-datomic)
- [`onyx-sql`](https://github.com/onyx-platform/onyx-sql)
- [`onyx-kafka`](https://github.com/onyx-platform/onyx-kafka)
- [`onyx-s3`](https://github.com/onyx-platform/onyx-s3)
- [`onyx-durable-queue`](https://github.com/onyx-platform/onyx-durable-queue)

Generate plugin templates through Leiningen with [`onyx-plugin`](https://github.com/onyx-platform/onyx-plugin).

### Offical Dashboard and Metrics

You can run a dashboard to monitor Onyx cluster activity, found [here](https://github.com/lbradstreet/onyx-dashboard). Further, you can collect metrics and send them to the dashboard, or anywhere, by using the [onyx-metrics plugin](https://github.com/onyx-platform/onyx-metrics).

### Release Notes

You can find [the latest major release notes here](doc/release-notes/0.7.0.md).

### Need help?

Check out the [Onyx Google Group](https://groups.google.com/forum/#!forum/onyx-user).

### Want the logo?

Feel free to use it anywhere. You can find [a few different versions here](https://github.com/onyx-platform/onyx/tree/0.7.x/resources/logo).

### Running the tests

A simple `lein midje` will run the full suite.

### Contributing

Contributions are welcome. Please fork the repository and send a pull request to the master branch.

#### Branching

Onyx uses a similiar branching strategy to Clojure itself. Onyx uses semantic versioning, and each minor version gets its own branch. All work is done on develop or feature branches and dropped into a major.minor.x branch when it's time to cut a new release. Pull requests into the develop branch are welcome.

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
- [David Leatherman](https://github.com/leathekd)
- [Daniel Compton](https://github.com/danielcompton)
- [Jeff Rose](https://github.com/rosejn)
- [Ole Krüger](https://github.com/dignati)

#### Acknowledgements

Some code has been incorporated from the following projects:

- [Riemann] (https://github.com/aphyr/riemann)
- [zookeeper-clj] (https://github.com/liebke/zookeeper-clj)

### Author

The lead for this project is [Michael Drogalis](https://twitter.com/MichaelDrogalis), the original creator. You can get me directly at (mjd3089.at.rit.edu) if needed.

### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.

### Profiler

![YourKit](https://raw.githubusercontent.com/onyx-platform/onyx/master/resources/logo/yourkit.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.
