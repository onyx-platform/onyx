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
[org.onyxplatform/onyx "0.10.0-alpha7"]
```

## Onyx 0.10.0 (Asynchronous Barrier Snapshotting)

Onyx 0.10.0 is a revamp of Onyx using a new fault tolerance mechanism. This
should be treated as pre-alpha software, however we do recommend developing
against it if you are not going to production in the short term. We will be
quickly iterating on it from here, to release something production worthy.

### What is it?

The Asynchronous Barrier Snapshotting (ABS) based release of Onyx moves away
from fault tolerance and state mechanisms that track and acking individual
segments, to inserting and tracking barriers that flow through the Directed
Acyclic Graph (DAG). 

### Motivation
ABS improves performance by reducing acking overhead, and allows for exactly
once aggregations which do not require message de-duplication. In ABS,
consistent state snapshots can be made by tracking and aligning the barriers,
and snapshotting state at appropriate points of barrier alignment.

### Concepts

Onyx 0.10.0 uses the Asynchronous Barrier Snapshotting method described in
[Lightweight Asynchronous Snapshots for Distributed Dataflows, Carbone et
al.](http://arxiv.org/abs/1506.08603), and [Distributed Snapshots: Determining Global
States of Distributed Systems, Chandy, Lamport](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf) to ensure fault tolerance and exactly once processing of data (not exactly once
side effects!).

Every job is assigned a coordinator peer, that notifies input peers of when
they should inject a barrier into their datastream (generally every n seconds).
These barriers are tracked and aligned throughout the job, with the tasks
performing snapshots of their state every time a barrier is aligned from all of
its input channels.

Concepts:
- Barrier: a message injected into the data stream, containing the epoch id of the barrier
- Epoch: the id of the barrier, re-starting from 0 whenever the cluster has performed a reallocation.
- Coordinator - a process that injects a barrier into the data stream on a schedule.
- Barrier Alignment: occurs when barriers with a particular id have been received from all input channels on a peer.
- Snapshot: Peer state that can be stored whenever a barrier alignment has occurred.
- Channel: Network messaging channel. Channel may become blocked waiting for channel alignment.

### ABS Execution Example

**Step 1:**
Coordinator peer emits barrier with epoch 3 after the coordinator period passes.
![Coordinator emits epoch 3](https://raw.githubusercontent.com/onyx-platform/onyx/0.10.x/doc/user-guide/abs/barrier-example-1/step1.png)

**Step 2:**
:input1 peer synchronizes on epoch 3, snapshots state to durable storage, and re-emits the barrier.
![:input1 synchronizes and emits barrier](https://raw.githubusercontent.com/onyx-platform/onyx/0.10.x/doc/user-guide/abs/barrier-example-1/step2.png)

**Step 3:**
:input2 peer synchronizes on epoch 3, snapshots state to durable storage, and re-emits the barrier. :agg1 reads barrier with epoch 3 from :input1, blocks the channel.
![:input2 synchronizes and emits barrier](https://raw.githubusercontent.com/onyx-platform/onyx/0.10.x/doc/user-guide/abs/barrier-example-1/step3.png)

**Step 4:**
:agg1 synchronizes on epoch 3, snapshots state to durable storage, and re-emits the barrier to :output.
![Coordinator emits epoch 3](https://raw.githubusercontent.com/onyx-platform/onyx/0.10.x/doc/user-guide/abs/barrier-example-1/step4.png)

## 0.10.0 Status

### New and Improved

#### Plugins

Easier to use plugin interfaces handle more of the work around checkpointing.
Plugin authors previously needed to checkpointing code that wrote to ZooKeeper. This
is now handled by simply implementing the checkpoint protocol function.  

- [Input plugin interface](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/protocols/input.clj)
- [Output plugin interface](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/protocols/output.clj)
- [Plugin start/stop interface](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/protocols/plugin.clj)

#### Monitoring

A monitoring config should now be supplied via `:onyx.monitoring/config` in the peer-config map.

#### Resume Points

[Resume points](doc/user-guide/resume-points.adoc) are a new feature that make
it simple to resume state from jobs in new jobs. This allows for simplified
deployment / upgrades of long running streaming jobs, simpler refactoring of
jobs (e.g. split one job into two jobs, but keep the state for each respective
part), and more.

#### Windows and Aggregations

Onyx Windows now perform exactly once data processing (not side effects!),
without needing deduplication and an [`:onyx/uniqueness-key`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#catalog-entry/:onyx/uniqueness-key) 
to be set. 

#### Performance

Performance will be much better than 0.10.x in the future. Performance is
currently limited by slow serialization and lack of batch messaging, however
this will improve greatly before release.

#### S3 / HDFS checkpointing

ABS requires performant checkpointing to durable storage. The current
checkpointing implementation checkpoints to ZooKeeper, which is much slower
than the alternatives (S3/HDFS/etc), and has a 1MB maximum node size. The final
0.10.0 release will include checkpointing via alternative storage mechanisms.

### Deprecations 

#### Job data

- [:acker/exempt-tasks](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:job/:acker/exempt-tasks)
- [:acker/exempt-input-tasks?](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:job/:acker/exempt-input-tasks?)
- [:acker/percentage](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:job/:acker/percentage)
- [:acker/exempt-output-tasks?](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:job/:acker/exempt-output-tasks?)

#### Catalog Entry 

Most jobs will need to remove the `:onyx/max-pending` option from their input
tasks to become compatible with 0.10.x. Some jobs will also need to remove the
`:onyx/pending-timeout`. Aggregation jobs will likely need to remove both
`:onyx/uniqueness-key` and `:onyx/deduplicate?` from their aggregation tasks.

- [:onyx/max-pending](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/max-pending)
- [:onyx/pending-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/pending-timeout)
- [:onyx/input-retry-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/input-retry-timeout)
- [:onyx/uniqueness-key](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/uniqueness-key)
- [:onyx/deduplicate?](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/deduplicate?)

#### Peer Config
- [:onyx.messaging/ack-daemon-clear-interval](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/ack-daemon-clear-interval)
- [:onyx.messaging/ack-daemon-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/ack-daemon-timeout)
- [:onyx.messaging.aeron/offer-idle-strategy](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/offer-idle-strategy)
- [:onyx.messaging.aeron/write-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/write-buffer-size)
- [:onyx.messaging.aeron/publication-creation-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/publication-creation-timeout)
- [:onyx.messaging.aeron/subscriber-count](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/subscriber-count)
- [:onyx.messaging/allow-short-circuit?](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/allow-short-circuit?)
- [:onyx.messaging/peer-link-idle-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/peer-link-idle-timeout)
- [:onyx.messaging/peer-link-gc-interval](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/peer-link-gc-interval)
- [:onyx.messaging/retry-ch-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/retry-ch-buffer-size)
- [:onyx.messaging/release-ch-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/release-ch-buffer-size)
- [:onyx.messaging/completion-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/completion-buffer-size)
- [:onyx.messaging/compress-fn](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/compress-fn)
- [:onyx.messaging/decompress-fn](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/decompress-fn)
- [:onyx.messaging/inbound-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/inbound-buffer-size)
- [:onyx.peer/backpressure-low-water-pct](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.peer/backpressure-low-water-pct)
- [:onyx.peer/backpressure-high-water-pct](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.peer/backpressure-high-water-pct)

### TODO

- Implement ability for triggers to emit segments to downstream tasks [#639](https://github.com/onyx-platform/onyx/issues/639)
- Re-implement flow condition retries [#714](https://github.com/onyx-platform/onyx/issues/714)
- Some barrier behaviours should be definable on a per job basis, not just a peer-config basis [#619](https://github.com/onyx-platform/onyx/issues/691)
- Iterative computation - we do not currently provide the ability to feed segments back up the DAG (i.e. you cannot currently turn a DAG into a cyclic graph).
- At least once data processing. Currently all barriers must be properly aligned from all task sources, which allows for exactly once processing at the cost of occasional latency. We do not yet support a mode for at least once data processing, which would improve latency at the cost of exact data processing.

### Plugins and Libraries

#### Supported

Supported Plugins:

##### `onyx-seq` 
onyx-seq is now included with onyx core. See
[onyx.plugin.seq](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/seq.clj)
and
[onyx.tasks.seq](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/tasks/seq.clj).

- [`onyx-core-async`](doc/user-guide/core-async-plugin.adoc)

`:done` messages are no longer supported on core async input channels. Simply close the channel instead of putting on a done.

In addition, `:done` messages are no longer written to output channels. Insead
use `onyx.api/await-job-completion` or `onyx.test-helper/feedback-exception!`
to wait for the job to end, and then drain the output channels until no more
messages can be read.

Note, the core async plugin now requires a buffer to temporarily hold unacked segments

```clojure
(def in-chan (atom nil))
(def in-buffer (atom {}))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

;; to add to your task lifecycles
(conj lifecyles {:lifecycle/task :in
                 :lifecycle/calls ::in-calls})
```

##### [`onyx-kafka`](https://github.com/onyx-platform/onyx-kafka/0.10.x)
onyx-kafka should work without issues.
##### [`onyx-datomic`](https://github.com/onyx-platform/onyx-datomic/0.10.x)
onyx-datomic should work without issues.
##### [`onyx-amazon-sqs`](https://github.com/onyx-platform/onyx-amazon-sqs/0.10.x)
onyx-amazon-sqs should work without issues.
##### [`onyx-amazon-s3`](https://github.com/onyx-platform/onyx-amazon-s3/0.10.x)
onyx-amazon-s3 should work without issues.
##### [`onyx-bookkeeper`](https://github.com/onyx-platform/onyx-bookkeeper-s3/0.10.x)
onyx-bookkeeper should work without issues.

##### [`onyx-metrics`](https://github.com/onyx-platform/onyx-metrics/0.10.x)

onyx-metrics currently only supports JMX metrics reporting. There are some breaking changes with respect to use. See [README] (https://github.com/onyx-platform/onyx-metrics/0.10.x/README.md)

##### [`onyx-peer-http-query`](https://github.com/onyx-platform/onyx-peer-http-query/0.10.x)

onyx-peer-http-query now supports an onyx-metrics endpoint at "/metrics" which is compatible with [prometheus](https://prometheus.io/). Simply enable onyx-metrics and your metrics will be reported to JMX.

##### [`onyx-dashboard`](https://github.com/onyx-platform/onyx-dashboard/0.10.x)

An 0.10.0 compatible onyx-dashboard release is [available](https://891-29719943-gh.circle-artifacts.com/0/tmp/circle-artifacts.LBDzpIX/onyx-dashboard.jar)

#### Plugin Use

To use the supported plugins, please use version coordinates such as
`[org.onyxplatform/onyx-amazon-sqs "0.10.0.0-alpha2"]`, and read
the READMEs on the 0.10.x branches linked above.

#### Currently Unsupported

- [`onyx-redis`](https://github.com/onyx-platform/onyx-redis)
- [`onyx-sql`](https://github.com/onyx-platform/onyx-sql)
- [`onyx-durable-queue`](https://github.com/onyx-platform/onyx-durable-queue)
- [`onyx-elasticsearch`](https://github.com/onyx-platform/onyx-elasticsearch)
- [`onyx-http`](https://github.com/onyx-platform/onyx-http)
- [`onyx-kafka-0.8`](https://github.com/onyx-platform/onyx-kafka-0.8)

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
[onyx-seq](https://github.com/onyx-platform/onyx-seq)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-seq/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-seq/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-seq/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-seq/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-seq/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-seq/tree/compatibility)
[onyx-durable-queue](https://github.com/onyx-platform/onyx-durable-queue)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-durable-queue/tree/compatibility)
[onyx-elasticsearch](https://github.com/onyx-platform/onyx-elasticsearch)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-elasticsearch/tree/compatibility)
[onyx-amazon-sqs](https://github.com/onyx-platform/onyx-amazon-sqs)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-sqs/tree/compatibility)
[onyx-amazon-s3](https://github.com/onyx-platform/onyx-amazon-s3)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-amazon-s3/tree/compatibility)
[onyx-http](https://github.com/onyx-platform/onyx-http)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-http/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-http/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-http/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-http/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-http/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-http/tree/compatibility)
[learn-onyx](https://github.com/onyx-platform/learn-onyx)| [![Circle CI](https://circleci.com/gh/onyx-platform/learn-onyx/tree/answers.svg?style=svg)](https://circleci.com/gh/onyx-platform/learn-onyx/tree/answers) | `-` | [![Circle CI](https://circleci.com/gh/onyx-platform/learn-onyx/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/learn-onyx/tree/compatibility)
[onyx-examples](https://github.com/onyx-platform/onyx-examples)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-examples/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-examples/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-examples/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-examples/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-examples/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-examples/tree/compatibility)
[onyx-dashboard](https://github.com/onyx-platform/onyx-dashboard)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-dashboard/tree/compatibility)
[onyx-metrics](https://github.com/onyx-platform/onyx-metrics)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-metrics/tree/compatibility)
[onyx-peer-http-query](https://github.com/onyx-platform/onyx-peer-http-query)| [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/0.10.x.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/0.10.x) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/master.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/master) | [![Circle CI](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/compatibility.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-peer-http-query/tree/compatibility)

- `release`: stable, released content
- `unstable`: unreleased content
- `compatibility`: edge, unstable, unreleased content depending on core `master`

### Companies Running Onyx in Production

<img src="doc/images/cognician.png" height"30%" width="30%">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/indaba.png" height"40%" width="40%">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/yapster.png" height="15%" width="15%">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/modnakasta.png">
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<img src="doc/images/breeze-125.png">

### Quick Start Guide

Feeling impatient? Hit the ground running ASAP with the [onyx-starter repo](https://github.com/onyx-platform/onyx-starter) and [walkthrough](https://github.com/onyx-platform/onyx-starter/blob/0.10.x/WALKTHROUGH.md). You can also boot into preloaded a Leiningen [application template](https://github.com/onyx-platform/onyx-template).

### User Guide 0.10.0-alpha7

- [User Guide Table of Contents](http://www.onyxplatform.org/docs)
- [API docs](http://www.onyxplatform.org/docs/api/latest)
- [Cheat Sheet](http://www.onyxplatform.org/docs/cheat-sheet/latest)

### Developer's Guide 0.10.0-alpha7

- [Branch Policy](doc/developers-guide/branch-policy.md)
- [Release Checklist](doc/developers-guide/release-checklist.md)
- [Deployment Process](doc/developers-guide/deployment-process.md)

### API Docs 0.10.0-alpha7

Code level API documentation [can be found here](http://www.onyxplatform.org/docs/api/0.10.0-alpha7).

### Official plugin listing

Official plugins are vetted by Michael Drogalis. Ensure in your project that plugin versions directly correspond to the same Onyx version (e.g. `onyx-kafka` version `0.10.0-alpha7.0-SNAPSHOT` goes with `onyx` version `0.10.0-alpha7`). Fixes to plugins can be applied using a 4th versioning identifier (e.g. `0.10.0-alpha7.1-SNAPSHOT`).

- [`onyx-core-async`](doc/user-guide/core-async-plugin.adoc)
- [`onyx-kafka`](https://github.com/onyx-platform/onyx-kafka)
- [`onyx-kafka-0.8`](https://github.com/onyx-platform/onyx-kafka-0.8)
- [`onyx-datomic`](https://github.com/onyx-platform/onyx-datomic)
- [`onyx-redis`](https://github.com/onyx-platform/onyx-redis)
- [`onyx-sql`](https://github.com/onyx-platform/onyx-sql)
- [`onyx-bookkeeper`](https://github.com/onyx-platform/onyx-bookkeeper)
- [`onyx-seq`](https://github.com/onyx-platform/onyx-seq)
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

- [Riemann] (https://github.com/aphyr/riemann)
- [zookeeper-clj] (https://github.com/liebke/zookeeper-clj)

### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.

### Profiler

![YourKit](https://raw.githubusercontent.com/onyx-platform/onyx/master/doc/images/logo/yourkit.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.
