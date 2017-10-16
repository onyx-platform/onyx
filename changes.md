## 0.11.1
* Event and processing time [watermark support](http://www.onyxplatform.org/docs/user-guide/0.11.x/#watermarks), which provides a much more rigorous way to apply watermark triggers to you windowed state, allowing input sources to be offset in time, while still correctly triggering.
* Enabled input and output plugin `checkpointed!` call which is called after a barrier epoch is fully checkpointed.
* Large performance improvements to task windows.

## 0.11.0
* **BREAKING CHANGE** Checkpoint schema format has changed, which will invalidate existing checkpoints. Please rebuild these from durable storage. Future schema checkpoints will be migrateable.
* **BREAKING CHANGE** `:onyx.refinements/discarding` and `:onyx.refinements/accumulating` have been removed. To retain current behaviour, use [`:trigger/post-evictor :all`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#trigger-entry/:trigger/post-evictor)
* **BREAKING CHANGE** [`:aggregation/create-state-update`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#state-aggregation/:aggregation/create-state-update) fn signature has changed from (window, state, segment) to (window, segment).
  achieve discarding behaviour, and delete the refinement to achieve accumulating behaviour as the accumulating refinement did nothing.
* [`:trigger/post-evictor`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#trigger-entry/:trigger/post-evictor) has been added, which improves window cleanup for flushed window extents.
* [`:window/storage-strategy`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#window-entry/:window/storage-strategy) feature has been added to allow full window entries to be stored and materialized at any time. Defaults to previous behaviour, which is `:incremental`.
* [`:trigger/state-context`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#trigger-entry/:trigger/state-context) feature has been added to allow users to use triggers without any trigger state. Defaults to `:trigger-state` which is the previous behaviour.
* Experimental LMDB state backend has been added.
* Improved performance.

## 0.10.0 changes

Please see the [cheat sheet](http://www.onyxplatform.org/docs/cheat-sheet/latest/#/search/0.10.0) for a full list of added and removed features in 0.10.0.

#### Plugins

Easier to use plugin interfaces handle more of the work around checkpointing.
Plugin authors previously needed to checkpointing code that wrote to ZooKeeper. This
is now handled by simply implementing the checkpoint protocol function.  

- [Input plugin interface](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/protocols/input.clj)
- [Output plugin interface](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/protocols/output.clj)
- [Plugin start/stop interface](https://github.com/onyx-platform/onyx/blob/0.10.x/src/onyx/plugin/protocols/plugin.clj)

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

Triggers can now emit aggregates to downstream tasks [`:trigger/emit`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#trigger-entry/:trigger/emit).

**Breaking Change** Triggers must now include a [`trigger/id`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#trigger-entry/:trigger/id) that is unique over the windows that it applies to.

#### Changes to the core async plugin

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

#### S3 checkpointing

ABS requires performant checkpointing to durable storage. The default
checkpointing implementation checkpoints to ZooKeeper, which is much slower
than the alternatives (S3/HDFS/etc), and has a 1MB maximum node size. Please
configure s3 checkpointing via the [`peer-config`](http://www.onyxplatform.org/docs/cheat-sheet/latest/#/peer-config).

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

#### 0.10.0
- Final release of 0.10.0.
- **BREAKING CHANGE** resume points will not allow restoration of window state from prior to 0.10.0 final.

#### 0.10.0-rc2
- Plugin interface updated to supply remaining batch-timeout to input interface. poll! now supplies the remaining ms for the batch.

#### 0.10.0-rc1
- Added a new plugin, [onyx-amazon-kinesis](https://github.com/onyx-platform/onyx-amazon-kinesis).
- Improved peer liveness detection.
- Added new metrics: peer-group.scheduler-lag, peer-group.peers-shutting-down.

#### 0.10.0-beta17
- Fix bug in short circuiting.

#### 0.10.0-beta16
- Re-release.

#### 0.10.0-beta15

- **BREAKING CHANGE** Fixed issue where namespaced tasks with same name would collide. This will break the ability to use old resume points for namespaced tasks.
- Big performance improvements, re-enable local to local short circuiting.
- Fix bug in `:trigger/emit`.
- Upgrade aeron back to 1.2.5.
- Report `:job-name` from job `:metadata` in metrics tags.
- Support routing of exceptions from flow conditions predicates.

#### 0.10.0-beta14
- Back out Aeron dependency upgraded in beta13 as it broke media driver health checks. This will be upgraded after resolving the health check issue.

#### 0.10.0-beta13
- Fix error with punctuation triggers which prevented checkpointing to S3.

#### 0.10.0-beta12
- `:window/id` and `:trigger/id` can now be uuids as well as keywords.
- Add peer group heartbeat metric, reported to "peer-group.since-heartbeat" in JMX.

#### 0.10.0-beta11
- Fix an issue where a replica invariant assertion would be tripped though the replica state was correct.
- Report JMX metrics to org.onyxplatform namespace.

#### 0.10.0-beta9
- Support dynamic write batch sizes which will not overflow Aeron buffers. Under a default term buffer configuration, Aeron allows messages of up to 2MB. Onyx's batching mechanism means that a batch can be at maximum 2MB, which means that an individual segment had to be well under 2MB. Onyx will now split segments into multiple batches once the buffer has been filled.

#### 0.10.0-beta8
- Revert change in heartbeating logic that was causing heartbeat channels to become stuck.

#### 0.10.0-beta8

- Improve flow conditions validation errors.
- Adjust metrics to include slot-id in metrics tags.
- Add metric "offset" so that input plugins can report their medium's offset (e.g. kafka topic offset for a given slot-id / partition).
- Increase default subscriber / publisher timeout to 20000ms.
- Decrease heartbeat period from 5000ms to 1000ms.
- Fix bug in status updates that reduces latency of barrier status reporting.
- Fix bug in session windows which would cause it to lag one message behind.
- Add gen-class to aeron media driver to allow for main entry point when AOTd

#### 0.10.0-beta7

- Fix timer trigger fires
- Fix documentation for trigger/emit.
- Fixes boundary expansion for Session windows.

## 0.9.16

- Bug fix: Fixed exception type check for the CLJS reader.

## 0.9.15
- Feature: new `:lifecycle/after-apply-fn` lifecycle has been added. This lifecycle is called after `:onyx/fn` is applied, but before the batch is written.
- Reduced number of dependencies.
- Increase default publication creation timeout to 5000ms
- Dependency change: Upgraded Aeron to 1.0.4
- [onyx-peer-http-query](https://github.com/onyx-platform/onyx-peer-http-query) added important health checks for [Aeron media driver status](https://github.com/onyx-platform/onyx-peer-http-query/blob/master/CHANGES.MD#09150)
- Increase default `:onyx.messaging/inbound-buffer-size` to 100000.
- Reduced default core.async thread pool size to 16.
- Reduced use of `clojure.core.async/alts!!` to improve performance and reduce thread sharing.

## 0.9.14
- Bug fix: Fix issue where subscriber would time out and would not be re-created [onyx#681](https://github.com/onyx-platform/onyx/issues/681).
- Increase default core.async thread pool size to 32 to decrease blocking issues under certain conditions. This can be overridden via the java property "clojure.core.async.pool-size"

## 0.9.13
- Bug fix: Fix cross talk between jobs where the jobs contained tasks with the same name.

## 0.9.12
- Bug fix: Change hashing algorithm for repeatable job IDs. The previous implementation was not consistent across JVMs.
- Bug fix: batch-fn did not respect lifecycle/handle-exception behaviour.
- Bug fix: core.async plugin did not use channel size lifecycle parameter.

## 0.9.11
- **Breaking change** The onyx log is now versioned by Onyx version, and will throw an exception if you do not supply a new `:onyx/tenancy-id` when upgrading or downgrading. This was best practice, and is now being enforced to prevent errors.
- `:onyx.core/scheduler-event` is now added to the event map before `:lifecycle/after-task-stop` is called. Peers can thus now wdetermine whether they are being shutdown because the job is completed or killed.
- Feature: `:onyx/batch-fn?` allows `:onyx/fn` to be supplied with a whole batch of segments. This can be a useful optimisation, especially when batching async requests. [More details](http://www.onyxplatform.org/docs/cheat-sheet/latest/#catalog-entry/:onyx/bulk-QMARK). Some useful libraries to help with batch resolution include [claro](https://github.com/xsc/claro) and [urania](https://github.com/funcool/urania).
- Deprecated: `:onyx/bulk?`. Use `:onyx/batch-fn?` by returning the segments that were passed in instead.
- Fixed [627](https://github.com/onyx-platform/onyx/issues/627) where serialized exception could not be deserialized again by dashboard 
- Dependency change: Upgraded to `[com.taoensso/timbre "4.7.4"]`, and `[com.taoensso/nippy "2.12.2"]`.

## 0.9.10
- Bug fix: Fixes #640 [Triggers firing for all window extents] (https://github.com/onyx-platform/onyx/issues/640)
- Improved documentation, via adoc, readable at www.onyxplatform.org
- Adds ability to hook in a replica query server. An implementation will shortly be found [here](https://github.com/onyx-platform/onyx-peer-http-query)
- onyx.peer.operation/kw->fn was moved to onyx.static.util

## 0.9.9

- Bug fix: Fix ex-info arity error in BookKeeper Component.

## 0.9.8

- Improvement: onyx.api/submit-job errors are now printed to stderr
- Improvement: BookKeeper cookie is now cleaned up, which will help with repl reload issues. [#612](https://github.com/onyx-platform/onyx/issues/612)
- Bug fix: Fix flow conditions where flow/from and flow/to is :all. [#617](https://github.com/onyx-platform/onyx/pull/617)

## 0.9.7

- Bug fix: Fixed suppressed exceptions on `with-test-env` start up sequence.
- Bug fix: Fixed schema merging for task bundles.
- Bug fix: Suppress Aeron MediaDriver NoSuchFileException when Aeron cleans up the directory before we do.
- Bug fix: Fixed ZooKeeper thread leak bug. [#600](https://github.com/onyx-platform/onyx/issues/600)
- Bug fix: Fixed logging statement echoing the number of processed segments.
- Bug fix: Only allow peer to write one exhausted input log entry per task. [#493](https://github.com/onyx-platform/onyx/issues/493)
- Bug fix: Fixed bug in percentage scheduler that failed to use all available peers due to a rounding error.
- Documentation fix: Fixed lifecycles exception description.
- Documentation fix: Fixed trigger predicate signature description. [#586](https://github.com/onyx-platform/onyx/issues/586)
- Documentation: Added more content to the FAQ section.
- Dependency change: Upgraded `org.btrplace/scheduler-api` to `0.46`.
- Dependency change: Upgraded `org.btrplace/scheduler-choco` to `0.46`.
- Dependency change: Excluded ClojureScript from clj-fuzzy dependencies.
- Design change: Implemented peer-group ZooKeeper connection sharing. This reduces the number of ZooKeeper connections required per machine, and improves scheduler performance.
- Enhancement: Tasks now time out when being stopped, allowing a peer to be rescheduled in cases where the task is stuck. This is configured via the peer-config `:onyx.peer/stop-task-timeout-ms`.
- Enhancement: A message is now emitted to the logs when a job is submitted, but does not start because of the scheduler gave it no peers.

## 0.9.6

- Enhancement: remove stray printlns.

## 0.9.5

- New feature: Advanced static analysis. Semantic errors are detected upon job submission, and significantly better error messages are written to standard out, rather than throwing an exception.
- Enhancement: updated Onyx Schema's to restrict namespace level keys on all job data structures, rather than only the catalog.

## 0.9.4

- Bug fix: exhaust-input events should not change replica if job is finished

## 0.9.3

- Bug fix: Loosened Event schema bad release in 0.9.2

## 0.9.2

- Bug fix: Loosened Event schema when checked for stateful tasks. [#568](https://github.com/onyx-platform/onyx/issues/568)

## 0.9.1

- New aggregation: added `onyx.windowing.aggregation/collect-by-key` aggregation.
- Bug fix: Exceptions were being swallowed by Curator's logging configuration. [#563](https://github.com/onyx-platform/onyx/issues/563)
- Bug fix: Some exceptions that kill jobs were failing to be serialized to ZooKeeper by Nippy. [#564](https://github.com/onyx-platform/onyx/issues/564)

## 0.9.0

- **API breaking change**: `onyx/id` in peer-config and env-config was renamed to `:onyx/tenancy-id`
- **API breaking change**: `:aggregation/fn` was renamed to [:aggregation/create-state-update](http://www.onyxplatform.org/docs/cheat-sheet/latest/#state-aggregation/:aggregation/create-state-update).
- **API breaking change**: changed the signatures of: [:aggregation/init](http://www.onyxplatform.org/docs/cheat-sheet/latest/#state-aggregation/:aggregation/init), [:aggregation/create-state-update](http://www.onyxplatform.org/docs/cheat-sheet/latest/#state-aggregation/:aggregation/create-state-update), [:aggregation/apply-state-update](http://www.onyxplatform.org/docs/cheat-sheet/latest/#state-aggregation/:aggregation/apply-state-update), and [:aggregation/super-aggregation-fn](http://www.onyxplatform.org/docs/cheat-sheet/latest/#state-aggregation/:aggregation/super-aggregation-fn). All now take the window as the first argument.
- **API breaking change**: internal messaging functions no longer take the event map as an argument. Note, this will break any plugins that manually manage the ack count, such as plugins using async callbacks.
- **API breaking change**: triggers and refinements functions are now resolved to vars via namespace lookup e.g.
  Trigger example: `:trigger/on :segment` -> `:trigger/on :onyx.triggers/segment`,
  Refinement example: `:trigger/refinement :accumulating` -> `:trigger/refinement :onyx.refinements/accumulating`,
- **Breaking change in onyx-metrics**: :metrics/workflow-name was deprecated in favor of metadata map. See onyx-metrics [changes](https://github.com/onyx-platform/onyx-metrics/blob/master/changes.md#090)
- Bug fix: fixed exceptions losing their main `.getCause` message when thrown inside a lifecycle
- New feature: Jobs now support metadata by including a metadata map with job submission data e.g. `{:workflow ... :catalog ... :metadata {:name "myjobname"}}`. This makes it easier to correlate information about jobs, query the cluster for jobs, etc.
- Design change: Implemented trigger refinements in terms of state updates. In order to implement a refinement, one must now implement a refinement calls map, analagous to the aggregation calls map. This takes the form `{:refinement/create-state-update (fn [trigger state state-event]) :refinement/apply-state-update (fn [trigger state entry])}`
- Enhancement: Lifecycles can now catch flow condition exceptions through `:lifeycycle/handle-exception` from the `:lifecycle/execute-flow-conditions` and `:lifecycle/apply-fn` phases.
- Enhancement: loosened the peer configuration schema needed for log subscription
- Dependency change: Upgraded `org.clojure/clojure` to `1.8.0`

## 0.8.11

- Bug fix: Fix list of disallowed joining peers. Previous patch did not account for the single-peer, single machine cluster state.

## 0.8.10
- Bug fix: fix peer join issue which occurs when cluster is in a stuck state, and dead peers need to be evicted.

## 0.8.9

- **Breaking change**: Removed `:onyx/restart-pred-fn` feature, which has been deprecated in favor of the new `:lifecycle/handle-exception` feature.
- New feature: Task Constraints are a new feature that allow peers to tag themselves indicating user-defined capabilities. Onyx's scheduler will only allocate peers with tags that match a task to be executed. This composes with all existing scheduler features.
- Bug fix: Fixed a bug where a job would continue running even if it didn't have enough peers to cover all the tasks.

## 0.8.8

- Fixed a problem with a file in Onyx's test suite that was causing problems for the release process. No functional changes in this release.

## 0.8.7
- **Breaking change**: No longer AOT compile onyx.interop. This will be done in [onyx-java](https://github.com/onyx-platform/onyx-java) for use by other languages.
- Bug fix: Fix bug where shutdown resulted in NPE when trying to delete a non-existent Aeron directory when not using the embedded driver.
- Bug fix: Fix issues in handle-exception lifecycle where it wouldn't handle exceptions in read-batch/write-batch/assign-windows [#505](https://github.com/onyx-platform/onyx/issues/491)
- Enhancement: trigger notification :task-complete has been renamed to :task-lifecycle-stopped, as it occurs whenever a task lifecycle is stopped, not necessarily when the task has been completed
- Enhancement: reduce unnecessary peer reallocation in Onyx's scheduler [#503](https://github.com/onyx-platform/onyx/issues/503)

## 0.8.6
- Dependency change: Revert back to Clojure 1.7.0, as 1.8.0 was causing issues with Onyx users on 1.7.0

## 0.8.5
- MAJOR bug fix: fixed bug causing slot-ids to be misallocated, which will affect recovery for state/windowed tasks [#504](https://github.com/onyx-platform/onyx/issues/504)
- MAJOR bug fix: fixed bug causing peers to stop checkpointing to state/windowed log [#390](https://github.com/onyx-platform/onyx/issues/390).
- MAJOR bug fix: fixed a number of peer join bugs found by onyx-jepsen [#453](https://github.com/onyx-platform/onyx/issues/453), [#462](https://github.com/onyx-platform/onyx/issues/462), [#437](https://github.com/onyx-platform/onyx/issues/437).
- Bug fix: Fixed bug introduced by a breaking change in Aeron 0.2.2 where we would retry a send to a closed publication.
- Enhancement: Improved performance, especially for windowed tasks [#500](https://github.com/onyx-platform/onyx/issues/500)
- Enhancement: Switch embedded aeron media driver to use SHARED mode by default, which is more robust on small peers.
- Enhancement: Embedded media driver now cleans up its directory to resolve version incompatibilities encountered by users on upgrading.
- Dependency change: Upgraded to Clojure 1.8.0
- Dependency change: Upgraded to Aeron 0.9

## 0.8.4

- **Breaking change**: Changed the signature of trigger sync function.
- New feature: Added support for handling lifecycle exceptions with `:lifecycle/handle-exception`
- New feature: Added the Colocation Task scheduler
- Bug fix: Fixed a bug where `:flow/from` -> `:all` didn't match compilation step. [#464](https://github.com/onyx-platform/onyx/issues/464)
- Bug fix: Fixed an issue where peers didn't restart properly on failure under certain conditions
- Enhancement: Switched to 3rd party scheduling library - BtrPlace.
- Enhancement: Added parameters for BookKeeper disk threshold error and warnings
- Dependency change: Upgraded `uk.co.real-logic/aeron-all` to `0.2.3`

## 0.8.3
- **Breaking change**: Removed `:onyx.messaging.aeron/inter-service-timeout-ns` peer config setting. Client liveness timeout is now completely set via java property: `aeron.client.liveness.timeout`
- New feature: Serialize the exception that kills a job to ZooKeeper
- New monitoring metrics: `zookeeper-write-exception` and `zookeeper-read-exception`
- New peer configuration parameter: `onyx.zookeeper/prepare-failure-detection-interval`
- Bug fix: Fixed method dispatch bug for punctuation triggers
- Bug fix: Fixed `:trigger/pred` not being in the validation schema
- Bug fix: Fixed `:trigger/watermark-percentage` not being in the validation schema
- Bug fix: Fixed issue where peers would not reconnect after losing connection to ZooKeeper.
- Bug fix: Fixed race condition where ephemeral node would not release in time, deadlocking peers on start up.
- Enhancement: Added extra schema validation to the `onyx.api` public functions
- Enhancement: Added `:added` keys to the information model
- Dependency change: Upgraded `org.apache.bookkeeper/bookkeeper-server` to `4.3.2`
- Dependency change: Upgraded `uk.co.real-logic/aeron-all` to `0.2.2`

## 0.8.2
- Changed job specification returned by onyx.api/submit-job. task-ids are now keyed by task name.
- Turned on nippy compression for ZooKeeper writes and messaging writes
- Fix badly named 0.8.1 release version caused by release scripts.

## 0.8.1
- Changed job specification returned by onyx.api/submit-job. task-ids are now keyed by task name
- Turned on nippy compression for ZooKeeper writes and messaging writes
- `onyx.helper-env` has been removed, which is superseded by `onyx.test-helper`'s functions and components

## 0.8.0
- **Breaking change** `:onyx.messaging/peer-port-range` and `:onyx.messaging/peer-ports` are deprecated in favour of a single `:onyx.messaging/peer-port` port. The Aeron layer multiplexed all communication over a single port so multiple port selection is longer required.
- **Important change**: default task batch timeout was reduced from 1000ms to 50ms.
- New major feature: Windowing and Triggers.
- New feature: Fixed windows
- New feature: Sliding windows
- New feature: Session windows
- New feature: Global windows
- New feature: Timer triggers
- New feature: Segment-based triggers
- New feature: Watermark triggers
- New feature: Percentile Watermark triggers
- New feature: Punctuation triggers
- New feature: Accumulating refinement mode
- New feature: Discarding refinement mode
- New feature: BookKeeper & embedded RocksDB automatic "exactly once" filtering.
- New feature: friendlier error messages. Added custom schema error handlers to print out documentation and required types in a more customized format.
- New flux policy: Recover
- New lifecycle: after-read-batch
- New metric: peer-send-bytes
- Bug fix: fixed an issue where the percentage job scheduler would misallocate.
- Bug fix: fixed `:data` key being removed from exception messages in the logs.

## 0.7.14

- Bug fix: Fixed a case where a peer would complete multiple jobs in response to a sealing event, rather than the only completing the one job that it was supposed to.

## 0.7.13

- No functional changes in this release. Fixing build issue.

## 0.7.12

- No functional changes in this release. Fixing build issue.

## 0.7.11

- Fixes transitive AOT compilation problem of interop namespace. [#339](https://github.com/onyx-platform/onyx/issues/339).

## 0.7.10

- No functional changes in this release. Fixing build issue.

## 0.7.9

- No functional changes in this release. Fixing build issue.

## 0.7.8

- No functional changes in this release. Fixing build issue.

## 0.7.7

- Improve fault tolerance aeron connection reaping (GC)

## 0.7.6

- Fixed performance regression caused by reflection in Aeron messaging layer.

## 0.7.5

- No functional changes in this release. We had a build problem that wasn't worth fixing across all 0.7.4 releases. Fixed build and trying again under alias 0.7.5

## 0.7.4

- Operations: Onyx now requires Java 8.
- **API breaking change**: update signature of `onyx.api/await-job-completion` to take an opts map.
- **API breaking change**: removed Netty and core.async messaging implementations.
- API: New catalog entry option `:onyx/n-peers` to automatically expand to make `:onyx/min-peers` and `:onyx/max-peers` peers the same value. [#282](https://github.com/onyx-platform/onyx/issues/282)
- API: Allow functions in leaf position of a workflow. [#198](https://github.com/onyx-platform/onyx/issues/198)
- Bug fix: flow-conditions retry default action should emit segments [#262](https://github.com/onyx-platform/onyx/issues/262)
- Bug fix: cleaned up publications on write failure

## 0.7.3

- Bug fix: Kill-job no longer throws a malformed exception with bad parameters.
- Bug fix: Fixed arity in garbage collection to seek the next origin.
- Documentation: Fixed Aeron docstring for port allocation
- Added schema type-checking to all replica updates in the log.
- Allow Aeron to use short virtual peer ID when hashing. [#250](https://github.com/onyx-platform/onyx/issues/250)
- Exposed all schemas used for internal validation through `onyx.schema` namespace, meant for use in 3rd party tooling.
- Allow plugins to write task-metadata to the replica, which will be cleaned up when jobs are completed or killed [#287](https://github.com/onyx-platform/onyx/pull/287)

## 0.7.2

- Fixed issue where cluster jammed when all peers left and joined [#273](https://github.com/onyx-platform/onyx/issues/273)
- Allow `:all` in task lifecycle names to match all tasks. [#209](https://github.com/onyx-platform/onyx/issues/209)

## 0.7.1
- :onyx.core/params is initialised as a vector
- Greatly improved lifecyle and keyword validation
- await-job-completion additionally returns when a job is killed. Return code now denotes whether the job completed successfully (true) or was killed (false).
- Throw exceptions from tasks, even if Nippy can serialize them.
- Fix typo'ed monitoring calls.

## 0.7.0

- API: :onyx/ident has been renamed :onyx/plugin, and now takes a keyword path to a fn that instantiates the plugin e.g. :onyx.plugin.core-async/input. (**Breaking change**)
- API: plugins are now implemented by the Pipeline and PipelineInput protocols. (**Breaking change**)
- API: output plugins may now find leaf segments in (:leaves (:tree (:onyx.core/results event))) instead of (:leaves (:onyx.core/results event)) (**Breaking change**)
- API: New lifecycle functions "after-ack-message" and "after-retry-message" are now available.
- API: `:onyx/group-by-key` can now group can now take a vector of keywords, or just a keyword.
- API: `:onyx/restart-pred-fn` catalog entry points to a boolean function that permits a task to hot restart on an exception.
- API: Peer configuration can now be instructed to short-circuit and skip network I/O if downstream peers are on the same machine.
- New feature: Jobs will now enter an automatic backpressure mode when internal peer buffers fill up past a high water mark, and will be turned off after reaching a low water mark. See [Backpressure](doc/user-guide/backpressure.md) for more details.
- New documentation: Use GitBook for documentation. [#119](https://github.com/onyx-platform/onyx/issues/119)
- New feature: Onyx Monitoring. Onyx emits a vast amount of metrics about its internal health.
- New feature: Aeron messaging transport layer. Requires Java 8. Use the `:aeron` key for `:onyx.messaging/impl`.
- New feature: Messaging short circuiting. When virtual peers are co-located on the same peer, the network and serialization will be bypassed completely. Note, this feature is currently only available when using Aeron messaging.
- New feature: Connection multiplexing. Onyx's scalability has been improved by [multiplexing connections](doc/user-guide/messaging.md#subscription-connection-multiplexing). Note, this feature is currently only available when using Aeron messaging.
- New feature: Java integration via catalog entry `:onyx/language` set to `:java`.
- Bug fix: Several log / replica edge cases were fixed.
- Bug fix: Peers not picking up new job after current job was killed.
- Bug fix: Bulk functions are no longer invoked when the batch is empty. [#260](https://github.com/onyx-platform/onyx/issues/260)

## 0.6.0

- Dropped feature: support for `:sequential` tasks
- Dropped feature: support for `onyx.task-scheduler/greedy`
- Dropped feature: support for HornetQ messaging
- Dropped feature: internal HornetQ messaging plugin
- Dropped feature: multimethod lifecycles
- New messaging transport: Netty TCP
- New messaging transport: Aeron
- New messaging transport: core.async
- New feature: Percentage-based, elastically scalable acknowledgement configuration
- New feature: Input, output, and specific task name exemption from acting as an acker node
- New feature: Functions can take an entire batch of segments as their input with catalog key `:onyx/bulk?` true
- New feature: Flow conditions handle exceptions as predicates
- New feature: Flow conditions may post-transform exception values into new segments
- New feature: Flow conditions support a new `:action` key with `:retry` to reprocess a segment from its root value
- New feature: Custom compression and decompression functions through Peer configuration
- New feature: Data driven lifecycles
- New feature: Internal core.async plugin
- New feature: Flux policies
- New feature: `:onyx/min-peers` can be set on grouping tasks to create a lower bound on the number of peers required to start a task
- New documentation: Event context map information model
- New documentation: Default configuration & timeout values
- API: Added metadata to all public API functions indicating which Onyx version they were added in
- API: `onyx.api/start-peers!` API renamed to `onyx.api/start-peers`
- API: `onyx.api/shutdown-peers` is now idempotent
- API: Return type of public API function submit-job has changed. It now returns a map containing job-id, and task-ids keys.
- API: Renamed "Round Robin" schedulers to "Balanced"
- API: The last task in a workflow no longer needs to be an `output` task
- API: Plugin lifecycle extensions now dispatch off of identity, rather than type and name.
- API: Peers now launch inside of a "peer group" to share network resources.

## 0.5.3

- New feature: Flow Conditions. Flow Conditions let you manage predicates that route segments across tasks in your workflow.
- Fixed extraneous ZooKeeper error messages.

## 0.5.2

- Development environment doesn't required that the job scheduler be known ahead of time. It's now discovered dynamically.

## 0.5.1

- Adds ctime to log entries. Useful for things like the dashboard and other log subscribers.

## 0.5.0

- Design change: the Coordinator has been abolished. Onyx is now a fully masterless system. The supporting environment now only requires Zookeeper, HornetQ, and a shared Onyx ID across cluster members.
- New feature: Realtime event subscription service. All coordination events can be observed in a push-based API backed with core.async.
- New feature: Job schedulers are now available to control how many peers get assigned to particular jobs. Supports `:onyx.job-scheduler/greedy`, `:onyx.job-scheduler/round-robin`, and `:onyx.job-scheduler/percentage`.
- New feature: Task schedulers are now available to control how many peers get assigned to particular tasks within a single job. Supports `:onyx.task-scheduler/greedy`, `:onyx.task-scheduler/round-robin`, and `:onyx.task-scheduler/percentage`.
- New feature: `:onyx/max-peers` may optionally be specified on any catalog entry to create an upper bound on the number of peers executing a particular task. Only applicable under a Round Robin Task Scheduler.
- New feature: `:onyx/params` may be specified on any catalog entry. It takes a vector of keywords. These keywords are resolved to other keys in the same catalog entry, and the corresponding values are passing as arguments to the function that implements that catalog entry.
- New feature: `:onyx.peer/join-failure-back-off` option in the peer config specifies a cooldown period to wait to try and join the cluster after previously aborting
- New feature: `:onyx.peer/inbox-capacity` option in the peer config specifies the max number of inbound messages a peer will buffer in memory.
- New feature: `:onyx.peer/outbox-capacity` option in the peer config specifies the max number of outbound messages a peer will buffer in memory.
- New feature: `kill-job` API function.
- New feature: `subscribe-to-log` API function.
- New feature: `shutdown-peer` API function.
- New feature: `shutdown-env` API function. Shuts down a development environment (ZK/HQ in memory).
- New feature: `gc` API function. Garbage collects all peer replicas and deletes old log entries from ZooKeeper.
- Enhancement: peers now automatically kill their currently running job if it throws an exception other than a Zookeeper or HornetQ connection failure. The latter cases still cause the peer to automatically reboot.

## 0.4.1

- Fixes aggregate ignoring `:onyx/batch-timeout`. [#33](https://github.com/onyx-platform/onyx/issues/33)
- Adds log rotation to default Onyx logging configuration. [#35](https://github.com/onyx-platform/onyx/issues/35)
- Peer options available in pipeline event map under key `:onyx.core/peer-opts`

## 0.4.0

- Grouper and Aggregate functions removed, replaced by catalog-level grouping and implicit aggregation. [#20](https://github.com/onyx-platform/onyx/issues/20)
- Support for directed, acylic graphs as workflows. [#26](https://github.com/onyx-platform/onyx/issues/26)
- Fix for peer live lock on task completion. [#23](https://github.com/onyx-platform/onyx/issues/23)
- Fixed bug where job submission silently fails due to malformed workflow [#24](https://github.com/onyx-platform/onyx/issues/24)
- `submit-job` throws exceptions on a malformed catalog submission. [#3](https://github.com/onyx-platform/onyx/issues/3)
- Fix HornetQ ipv6 multicast socket bind issue when running on hosts with ipv6 interfaces.
- Adds `:onyx/batch-timeout` option to all catalog entries. [#29](https://github.com/onyx-platform/onyx/issues/29)

## 0.3.3

- Fixes a scenario where a virtual peer can deadlock on task completion. [#18](https://github.com/onyx-platform/onyx/issues/18)

## 0.3.2

- Made peer shutdown function synchronous.

## 0.3.1

- Performance improvement by eliminating superfluous decompression.

## 0.3.0
- Coordinator can be made highly available via stand-by coordinators
- HornetQ connection via UDP multicast for clustering
- HornetQ connection via JGroups for clustering
- HornetQ embedded mode for development with an HQ cluster
- HornetQ VM mode for development with an in-JVM HQ instance
- ZooKeeper in-memory mode for for development without an external ZooKeeper running
- Concurrent tasks executed by a single v-peer no longer implies sequential message processing

## 0.2.0

- Rename internal API extensions to use "node" instead of "place".
- Throw an explicit error on function resolution failure.
- Add state audits into test suite.
- Remove Datomic log component, replace with ZooKeeper.
- Return job ID on submission.
- Expose API function for blocking on job completion.
- Allow overriding of peer lifecycle methods, merge results back together.
- Add aggregate operation.
- Add grouping operation.
- Change lifecycle API functions.
- Log all Onyx output to file system.
