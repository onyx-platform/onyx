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
- CQRS
- Continuous computation 
- Extract, transform, load
- Data transformation à la map-reduce
- Data ingestion and storage medium transfer
- Data cleaning

### Installation

Available on Clojars:

```
[org.onyxplatform/onyx "0.11.1"]
```

### Plugins and Libraries

#### Unsupported plugins

Some plugins are currently unsupported in onyx 0.10.x. These are:

- [`onyx-durable-queue`](https://github.com/onyx-platform/onyx-durable-queue)
- [`onyx-elasticsearch`](https://github.com/onyx-platform/onyx-elasticsearch)
- [`onyx-http`](https://github.com/onyx-platform/onyx-http)
- [`onyx-kafka-0.8`](https://github.com/onyx-platform/onyx-kafka-0.8)

#### Plugin Use

To use the supported plugins, please use version coordinates such as
`[org.onyxplatform/onyx-amazon-sqs "0.11.1.0"]`, and read
the READMEs on the 0.10.x branches linked above.

### Build Status

Component | `release`| `unstable` | `compatibility`
----------|--------|----------|----------------
[onyx core](https://github.com/onyx-platform/onyx)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx/tree/master) | `-`
[onyx-kafka](https://github.com/onyx-platform/onyx-kafka)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka/tree/compatibility)
[onyx-kafka-0.8](https://github.com/onyx-platform/onyx-kafka-0.8)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka-0.8/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka-0.8/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka-0.8/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka-0.8/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-kafka-0.8/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-kafka-0.8/tree/compatibility)
[onyx-datomic](https://github.com/onyx-platform/onyx-datomic)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-datomic/tree/compatibility)
[onyx-redis](https://github.com/onyx-platform/onyx-redis)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-redis/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-redis/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-redis/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-redis/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-redis/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-redis/tree/compatibility)
[onyx-sql](https://github.com/onyx-platform/onyx-sql)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-sql/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-sql/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-sql/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-sql/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-sql/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-sql/tree/compatibility)
[onyx-bookkeeper](https://github.com/onyx-platform/onyx-bookkeeper)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-bookkeeper/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-bookkeeper/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-bookkeeper/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-bookkeeper/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-bookkeeper/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-bookkeeper/tree/compatibility)
[onyx-durable-queue](https://github.com/onyx-platform/onyx-durable-queue)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/compatibility)
[onyx-elasticsearch](https://github.com/onyx-platform/onyx-elasticsearch)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/compatibility)
[onyx-amazon-sqs](https://github.com/onyx-platform/onyx-amazon-sqs)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/compatibility)
[onyx-amazon-s3](https://github.com/onyx-platform/onyx-amazon-s3)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/compatibility)
[onyx-amazon-kinesis](https://github.com/onyx-platform/onyx-amazon-kinesis)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-kinesis/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-kinesis/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-kinesis/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-kinesis/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-kinesis/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-kinesis/tree/compatibility)
[onyx-http](https://github.com/onyx-platform/onyx-http)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-http/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-http/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-http/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-http/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-http/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-http/tree/compatibility)
[learn-onyx](https://github.com/onyx-platform/learn-onyx)| [![Circle CI](https://circleci.com/gh/onyx-platform/learn-onyx/tree/answers.svg?style=svg)](https://circleci.com/gh/onyx-platform/learn-onyx/tree/answers) | `-` | [![Circle CI](https://circleci.com/gh/onyx-platform/learn-onyx/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/learn-onyx/tree/compatibility)
[onyx-examples](https://github.com/onyx-platform/onyx-examples)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-examples/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-examples/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-examples/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-examples/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-examples/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-examples/tree/compatibility)
[onyx-dashboard](https://github.com/onyx-platform/onyx-dashboard)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/compatibility)
[onyx-metrics](https://github.com/onyx-platform/onyx-metrics)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/compatibility)
[onyx-peer-http-query](https://github.com/onyx-platform/onyx-peer-http-query)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/master)

- `release`: stable, released content
- `unstable`: unreleased content
- `compatibility`: edge, unstable, unreleased content depending on core `master`

### Companies Running Onyx in Production

<img src="doc/images/cognician.png" height="30%" width="30%">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/indaba.png" height="40%" width="40%">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/yapster.png" height="15%" width="15%">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/modnakasta.png">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/breeze-125.png">

### Quick Start Guide

Feeling impatient? Hit the ground running ASAP with the [onyx-starter repo](https://github.com/onyx-platform/onyx-starter) and [walkthrough](https://github.com/onyx-platform/onyx-starter/blob/master/WALKTHROUGH.md). You can also boot into preloaded a Leiningen [application template](https://github.com/onyx-platform/onyx-template).

### User Guide 0.11.1

- [User Guide Table of Contents](http://www.onyxplatform.org/docs)
- [API docs](http://www.onyxplatform.org/docs/api/latest)
- [Cheat Sheet](http://www.onyxplatform.org/docs/cheat-sheet/latest)

### Developer's Guide 0.11.1

- [Branch Policy](doc/developers-guide/branch-policy.md)
- [Release Checklist](doc/developers-guide/release-checklist.md)
- [Deployment Process](doc/developers-guide/deployment-process.md)

### API Docs 0.11.1

Code level API documentation [can be found here](http://www.onyxplatform.org/docs/api/0.11.1).

### Official plugin listing

Official plugins are vetted by Michael Drogalis. Ensure in your project that plugin versions directly correspond to the same Onyx version (e.g. `onyx-kafka` version `0.11.1.0-SNAPSHOT` goes with `onyx` version `0.11.1`). Fixes to plugins can be applied using a 4th versioning identifier (e.g. `0.11.1.1-SNAPSHOT`).

- [`onyx-core-async`](doc/user-guide/core-async-plugin.adoc)
- [`onyx-kafka`](https://github.com/onyx-platform/onyx-kafka)
- [`onyx-kafka-0.8`](https://github.com/onyx-platform/onyx-kafka-0.8)
- [`onyx-datomic`](https://github.com/onyx-platform/onyx-datomic)
- [`onyx-redis`](https://github.com/onyx-platform/onyx-redis)
- [`onyx-sql`](https://github.com/onyx-platform/onyx-sql)
- [`onyx-bookkeeper`](https://github.com/onyx-platform/onyx-bookkeeper)
- [`onyx-seq`](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/seq.clj)
- [`onyx-durable-queue`](https://github.com/onyx-platform/onyx-durable-queue)
- [`onyx-elasticsearch`](https://github.com/onyx-platform/onyx-elasticsearch)
- [`onyx-http`](https://github.com/onyx-platform/onyx-http)
- [`onyx-amazon-sqs`](https://github.com/onyx-platform/onyx-amazon-sqs)
- [`onyx-amazon-s3`](https://github.com/onyx-platform/onyx-amazon-s3)

Generate plugin templates through Leiningen with [`onyx-plugin`](https://github.com/onyx-platform/onyx-plugin).

### 3rd Party plugin listing

Unofficial plugins have not been vetted.
- [`onyx-rethink`](https://github.com/cddr/onyx-rethink)

### Offical Dashboard and Metrics

You can run a dashboard to monitor Onyx cluster activity, found [here](https://github.com/lbradstreet/onyx-dashboard). Further, you can collect metrics and send them to the dashboard, or anywhere, by using the [onyx-metrics plugin](https://github.com/onyx-platform/onyx-metrics).

### Need help?

Check out the [Onyx Google Group](https://groups.google.com/forum/#!forum/onyx-user).

### Want the logo?

Feel free to use it anywhere. You can find [a few different versions here](https://github.com/onyx-platform/onyx/tree/0.10.x/doc/images/logo).

### Running the tests

A simple `lein test` will run the full suite for Onyx core.

#### Contributor list

- [Michael Drogalis](https://github.com/MichaelDrogalis)
- [Lucas Bradstreet](https://github.com/lbradstreet)
- [Owen Jones](https://github.com/owengalenjones)
- [Bruce Durling](https://github.com/otfrom)
- [Malcolm Sparks](https://github.com/malcolmsparks)
- [Bryce Blanton](https://github.com/bblanton)
- [David Rupp](https://github.com/davidrupp)
- [sbennett33](https://github.com/sbennett33)
- [Tyler van Hensbergen](https://github.com/tvanhens)
- [David Leatherman](https://github.com/leathekd)
- [Daniel Compton](https://github.com/danielcompton)
- [Jeff Rose](https://github.com/rosejn)
- [Ole Krüger](https://github.com/dignati)
- [Juho Teperi](https://github.com/Deraen)
- [Nicolas Ha](https://github.com/nha)
- [Andrew Meredith](https://github.com/kendru)
- [Bridget Hillyer](https://github.com/bridgethillyer)
- [Ivan Mushketyk](https://github.com/mushketyk)
- [Jochen Rau](https://github.com/jocrau)
- [Tienson Qin](https://github.com/tiensonqin)
- [Roman Volosovskyi](https://github.com/rasom)
- [Vijay Kiran](https://github.com/vijaykiran)
- [Paul Kehrer](https://github.com/reaperhulk)
- [Scott Bennett](https://github.com/sbennett33)
- [Nathan Todd.stone](https://github.com/nathants)
- [Mariusz Jachimowicz](https://github.com/mariusz-jachimowicz-83)
- [Jason Bell](https://github.com/jasebell)


#### Acknowledgements

Some code has been incorporated from the following projects:

- [Riemann](https://github.com/aphyr/riemann)
- [zookeeper-clj](https://github.com/liebke/zookeeper-clj)

### License

Copyright © 2017 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
