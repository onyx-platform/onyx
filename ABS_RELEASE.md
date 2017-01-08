
# Onyx 0.10.0 (Asynchronous Barrier Snapshotting)

## What is it?

The Asynchronous Barrier Snapshotting (ABS) based release of Onyx moves away
from fault tolerance and state mechanisms that track and acking individual
segments, to inserting and tracking barriers that flow through the Directed
Acyclic Graph (DAG). 

## Motivation
ABS improves performance by reducing acking overhead, and allows for exactly
once aggregations which do not require message de-duplication. In ABS,
consistent state snapshots can be made by tracking and aligning the barriers,
and snapshotting state at appropriate points of barrier alignment.

## Concepts

Onyx 0.10.0 uses the Asynchronous Barrier Snapshotting method described in
[Lightweight Asynchronous Snapshots for Distributed Dataflows, Carbone et
al.](http://arxiv.org/abs/1506.08603) to ensure fault tolerance and exactly once
processing of data (not exactly once side effects!).

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

## ABS Execution Example

**Step 1:**
Coordinator peer emits barrier with epoch 3 after the coordinator period passes.
![Coordinator emits epoch 3](https://raw.githubusercontent.com/onyx-platform/onyx/abs-engine/doc/user-guide/abs/barrier-example-1/step1.png)

**Step 2:**
:input1 peer synchronizes on epoch 3, snapshots state to durable storage, and re-emits the barrier.
![:input1 synchronizes and emits barrier](https://raw.githubusercontent.com/onyx-platform/onyx/abs-engine/doc/user-guide/abs/barrier-example-1/step2.png)

**Step 3:**
:input2 peer synchronizes on epoch 3, snapshots state to durable storage, and re-emits the barrier. :agg1 reads barrier with epoch 3 from :input1, blocks the channel.
![:input2 synchronizes and emits barrier](https://raw.githubusercontent.com/onyx-platform/onyx/abs-engine/doc/user-guide/abs/barrier-example-1/step3.png)

**Step 4:**
:agg1 synchronizes on epoch 3, snapshots state to durable storage, and re-emits the barrier to :output.
![Coordinator emits epoch 3](https://raw.githubusercontent.com/onyx-platform/onyx/abs-engine/doc/user-guide/abs/barrier-example-1/step4.png)

## 0.10.0 Status

### New and Improved

#### Plugins

Easier to use plugin interfaces handle more of the work around checkpointing.
Plugin authors previously needed to checkpointing code that wrote to ZooKeeper. This
is now handled by simply implementing the checkpoint protocol function.  

- [Input plugin interface](https://github.com/onyx-platform/onyx/blob/abs-engine/src/onyx/plugin/protocols/input.clj)
- [Output plugin interface](https://github.com/onyx-platform/onyx/blob/abs-engine/src/onyx/plugin/protocols/output.clj)
- [Plugin start/stop interface](https://github.com/onyx-platform/onyx/blob/abs-engine/src/onyx/plugin/protocols/plugin.clj)


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

Performance will be much better than 0.9.x in the future. Performance is
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
- [:onyx/uniqueness-key](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/uniqueness-key)
- [:onyx/deduplicate?](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/deduplicate?)
- [:onyx/input-retry-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/input-retry-timeout)
- [:onyx/max-pending](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/max-pending)
- [:onyx/pending-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:catalog-entry/:onyx/pending-timeout)

#### Peer Config
- [:onyx.messaging/retry-ch-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/retry-ch-buffer-size)
- [:onyx.messaging/ack-daemon-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/ack-daemon-timeout)
- [:onyx.messaging/allow-short-circuit?](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/allow-short-circuit?)
- [:onyx.messaging.aeron/offer-idle-strategy](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/offer-idle-strategy)
- [:onyx.peer/backpressure-low-water-pct](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.peer/backpressure-low-water-pct)
- [:onyx.peer/backpressure-high-water-pct](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.peer/backpressure-high-water-pct)
- [:onyx.messaging/compress-fn](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/compress-fn)
- [:onyx.messaging/peer-link-idle-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/peer-link-idle-timeout)
- [:onyx.messaging/release-ch-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/release-ch-buffer-size)
- [:onyx.messaging.aeron/publication-creation-timeout](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/publication-creation-timeout)
- [:onyx.messaging.aeron/subscriber-count](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/subscriber-count)
- [:onyx.messaging/peer-link-gc-interval](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/peer-link-gc-interval)
- [:onyx.messaging/ack-daemon-clear-interval](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/ack-daemon-clear-interval)
- [:onyx.messaging/completion-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/completion-buffer-size)
- [:onyx.messaging/decompress-fn](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/decompress-fn)
- [:onyx.messaging/inbound-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging/inbound-buffer-size)
- [:onyx.messaging.aeron/write-buffer-size](http://www.onyxplatform.org/docs/cheat-sheet/0.10.0/#:peer-config/:onyx.messaging.aeron/write-buffer-size)

### TODO

- Implement ability for triggers to emit segments to downstream tasks [#639](https://github.com/onyx-platform/onyx/issues/639)
- Re-implement flow condition retries [#714](https://github.com/onyx-platform/onyx/issues/714)
- Fast implementation of checkpointing in S3 [#662](https://github.com/onyx-platform/onyx/issues/662<Paste>) and HDFS [#715](https://github.com/onyx-platform/onyx/issues/715).
- Function plugins cannot currently be used as output tasks [#716](https://github.com/onyx-platform/onyx/issues/716)
- Improve serialization with [Simple Binary Encoding](https://github.com/real-logic/simple-binary-encoding). Current serialization of network messages is slow.
- Some barrier behaviours should be definable on a per job basis, not just a peer-config basis [#619](https://github.com/onyx-platform/onyx/issues/691)
- Improve idle strategies for peers that are not processing work.
- Iterative computation - we do not currently provide the ability to feed segments back up the DAG (i.e. you cannot currently turn a DAG into a cyclic graph).

### Plugins and Libraries

#### Supported

Supported Plugins:
- `onyx-seq` - Now included in onyx under [onyx.plugin.seq](https://github.com/onyx-platform/onyx/blob/abs-engine/src/onyx/plugin/seq.clj) and [onyx.tasks.seq](https://github.com/onyx-platform/onyx/blob/abs-engine/src/onyx/tasks/seq.clj).
- [`onyx-core-async`](doc/user-guide/core-async-plugin.adoc)
- [`onyx-kafka`](https://github.com/onyx-platform/onyx-kafka)
- [`onyx-datomic`](https://github.com/onyx-platform/onyx-datomic)
- [`onyx-amazon-sqs`](https://github.com/onyx-platform/onyx-amazon-sqs)
- [`onyx-amazon-s3`](https://github.com/onyx-platform/onyx-amazon-s3)

To use the supported plugins, please use version coordinates such as
`[org.onyxplatform/onyx-amazon-sqs "0.10.0-technical-preview2"]`.

Supported tools and libraries:

- [`onyx-dashboard`] (https://github.com/onyx-platform/onyx-dashboard)
- [`onyx-peer-http-query`] (https://github.com/onyx-platform/onyx-peer-http-query)

#### Currently Unsupported

Currently unsupported tools and libraries:

- [`onyx-metrics`] (https://github.com/onyx-platform/onyx-metrics)

Currently Unsupported Plugins:

- [`onyx-redis`](https://github.com/onyx-platform/onyx-redis)
- [`onyx-sql`](https://github.com/onyx-platform/onyx-sql)
- [`onyx-bookkeeper`](https://github.com/onyx-platform/onyx-bookkeeper)
- [`onyx-durable-queue`](https://github.com/onyx-platform/onyx-durable-queue)
- [`onyx-elasticsearch`](https://github.com/onyx-platform/onyx-elasticsearch)
- [`onyx-http`](https://github.com/onyx-platform/onyx-http)
- [`onyx-kafka-0.8`](https://github.com/onyx-platform/onyx-kafka-0.8)
