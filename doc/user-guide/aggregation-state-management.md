---
layout: user_guide_page
title: Aggregation and State
categories: [user-guide-page]
---

## Aggregation / State Management

This section discusses state management and fault tolerance used in windowing/streaming joins.

### Summary

Onyx provides the ability to perform updates to a state machine for segments
which are calculated over [windows] (doc/user-guide/windowing.md). For example,
a grouping task may accumulate incoming values for a number of keys over
windows of 5 minutes. This feature is commonly used for aggregations, such as
summing values, though it can be used to build more complex state machines.

#### State Example

```clojure

;; Task definition
{:onyx/name :sum-all-ages
 :onyx/fn :clojure.core/identity
 :onyx/type :function
 :onyx/group-by-key :name
 :onyx/flux-policy :recover
 :onyx/min-peers 2
 :onyx/batch-size 20}

;; Window definition
{:window/id :sum-all-ages-window
 :window/task :sum-all-ages
 :window/type :global
 :window/aggregation [:your-sum-ns/sum :age]
 :window/window-key :event-time
 :window/range [1 :hour]
 :window/doc "Adds the :age key in all segments in 1 hour fixed windows"}
```

As segments are processed, an internal state within the calculated window is
updated. In this case we are trying to sum the ages of the incoming segments.

Window aggregations are defined by a map containing the following keys:

| Key                                 | Optional? | Description                                                                                                |
|------------------------------------ | --------- | -----------------------------------------------------------------------------------------------------------|
| `:aggregation/init`                 | true      | Fn (window) to initialise the state.                                                                       |
| `:aggregation/fn`                   | false     | Fn (state, window, segment) to generate a serializable state machine update.                               |
| `:aggregation/apply-state-update`   | false     | Fn (state, entry) to apply state machine update entry to a state.                                          |
| `:aggregation/super-aggregation-fn` | true      | Fn (state-1, state-2, window) to combine two states in the case of two windows being merged.               |


In the `:window/aggregation` map in the `:sum-all-ages` window referenced above.

```clojure
(ns your-sum-ns)

(defn sum-init-fn [window]
  0)

(defn sum-application-fn [state [changelog-type value]]
  (case changelog-type
    :set-value value))

(defn sum-aggregation-fn [state window segment]
  ; k is :age
  (let [k (second (:window/aggregation window))]
    [:set-value (+ state (get segment k))]))

;; sum aggregation referenced in window definition.
(def sum
  {:aggregation/init sum-init-fn
   :aggregation/fn sum-aggregation-fn
   :aggregation/apply-state-update sum-application-fn})
    
```

Let's try processing some example segments using this aggregation:

```clojure
[{:name "John" :age 49}
 {:name "Madeline" :age 55}
 {:name "Geoffrey" :age 14}]
```

Results in the following events:

| Action           | Result                       |
|------------------|------------------------------|
| Initial state    | `0`                          |
| Incoming segment | `{:name "John" :age 49}`     |
| Changelog entry  | `[:set-value 49]`            |
| Applied to state | `49`                         |
| Incoming segment | `{:name "Madeline" :age 55}` |
| Changelog entry  | `[:set-value 104]`           |
| Applied to state | `104`                        |
| Incoming segment | `{:name "Geoffrey" :age 14}` |
| Changelog entry  | `[:set-value 128]`           |
| Applied to state | `128`                        |

This state can be emitted via triggers or another mechanism. By describing
changelog updates as a vector with a log command, such as `:set-value`
aggregation function can emit multiple types of state transition if necessary.

### Fault Tolerance

To allow for full recovery after peer crashes, the window state must be replicated
somewhere. As state updates occur, Onyx publishes the stream of changelog
updates to a replicated log.

After the changelog entry is written to the replicated log, the segment is
acked, ensuring that a segment is only cleared from the input source after the
update to window states it caused has been fully written to the log. When a
peer crash occurs, a new peer will be assigned to the task, and this peer will
play back all of the changelog entries, and apply them to the state, starting
with the initial state. As the changelog updates are read back in the same
order that they were written, the full state will be recovered. Partial updates
ensure that only minimal update data is written for each segment processed,
while remaining correct on peer failure.

### Exactly Once Aggregation Updates

Exactly once aggregation updates are supported via Onyx's filtering feature. When a
task's catalog has `:onyx/uniqueness-key` set, this key is looked up in the
segment and used as an ID key to determine whether the segment has been seen
before. If it has previously been processed, and state updates have been
persisted, then the segment is not re-processed. This key is persisted to the
state log transactionally with the window changelog updates, so that previously seen keys
can be recovered in case of a peer failure.

**See the section "Exactly Once Side-Effects" for discussion of why
side-effects are impossible to achieve Exactly Once**.
            
#### Considerations

In order to reduce memory consumption, uniqueness-key values are persisted to a
local database, currently implemented with RocksDB. This database uses a bloom
filter, and a memory cache, allowing Onyx to avoid hitting disk
for most filter key checks.

In order to prevent unbounded increase in the size of the filter's disk
consumption, uniqueness-key values are bucketed based on recency, and the
oldest bucket is expired as the newest is filled.

Several configuration parameters are available for the rocksdb based local
filter. The most relevant of these for general configuration is
`:onyx.rocksdb.filter/num-ids-per-bucket`, and `:onyx.rocksdb.num-buckets`,
which are the size and the number of buckets referenced above.

| Parameter                                    | Description                                                                                                                                                                         | Default             |
|--------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------|
| `:onyx.rocksdb.filter/peer-block-cache-size` | RocksDB block cache size in bytes. Larger caches reduce the chance that the peer will need to check for the prescence of a uniqueness key on disk. Defaults to 100MB.               | 104857600           |
| `:onyx.rocksdb.filter/bloom-filter-bits`     | Number of bloom filter bits to use per uniqueness key value                                                                                                                         | 10                  |
| `:onyx.rocksdb.filter/num-ids-per-bucket`    | Number of uniqueness key values that can exist in a RocksDB filter bucket.                                                                                                          | 10000000            |
| `:onyx.rocksdb.filter/num-buckets`           | Number of rotating filter buckets to use. Buckets are rotated every `:onyx.rocksdb.filter/num-ids-per-bucket`, with the oldest bucket being discarded if num-buckets already exist. | 10                  |
| `:onyx.rocksdb.filter/block-size`            | RocksDB block size. May worth being tuned depending on the size of your uniqueness-key values.                                                                                      | 4096                |
| `:onyx.rocksdb.filter/compression`           | Whether to use compression in rocksdb filter. It is recommended that `:none` is used unless your uniqueness keys are large and compressible.                                        | `:none`             |
| `:onyx.rocksdb.filter/base-dir`              | Temporary directory to persist uniqueness filtering data.                                                                                                                           | /tmp/rocksdb_filter |

#### Exactly Once Side-Effects

Exactly once *side-effects* resulting from a segment being processed may occur,
as exactly once side-effects are impossible to achieve. Onyx guarantees that a
window state updates resulting from a segment are perfomed exactly once,
however any side-effects that occur as a result of the segment being processed
cannot be guaranteed to only occur once.

### BookKeeper Implementation

State update changelog entries are persisted to BookKeeper, a replicated log
server. An embedded BookKeeper server is included with Onyx. The embedded
server is currently the recommended approach to running BookKeeper along side
Onyx. This will be re-evaluated in the beta release of Onyx 0.8.0.

BookKeeper ensures that changelog entries are replicated to multiple nodes,
allowing for the recovery of windowing states upon the crash of a windowed task
task.

By default the the Onyx BookKeeper replication is striped to 3 BookKeeper
instances (the quorum), and written to 3 instances (the ensemble).

#### Running the embedded BookKeeper server

The embedded BookKeeper server can be started via the `onyx.api/start-env` api
call, with an env-config where `:onyx.bookkeeper/server?` is `true`.

When running on a single node, you may wish to use BookKeeper without starting
the multiple instances of BookKeeper required to meet the ensemble and quorum
requirements. In this case you may start a local quorum (3) of BookKeeper
servers by setting `:onyx.bookkeeper/local-quorum?` to `true`.

##### Embedded BookKeeper Configuration Parameters

| Parameter                             | Description                                                                                                                                                    | Default                 |
|-------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------|
| `:onyx.bookkeeper/server?`            | Bool to denote whether to startup a BookKeeper instance on this node, for use in persisting Onyx state information.                                            | false                   |
| `:onyx.bookkeeper/base-ledger-dir`    | Directory to store BookKeeper's ledger in. It is recommended that this is altered to somewhere fast, preferably on a different disk to the BookKeeper journal  | /tmp/bookkeeper_ledger  |
| `:onyx.bookkeeper/port`               | Port to startup this node's BookKeeper instance on.                                                                                                            | 3196                    |
| `:onyx.bookkeeper/local-quorum-ports` | Ports to use for the local BookKeeper quorum.                                                                                                                  | [3196 3197 3198]        |
| `:onyx.bookkeeper/base-journal-dir`   | Directory to store BookKeeper's journal in. It is recommended that this is altered to somewhere fast, preferably on a different disk to the BookKeeper ledger. | /tmp/bookkeeper_journal |
| `:onyx.bookkeeper/local-quorum?`      | Bool to denote whether to startup a full quorum of BookKeeper instances on this node. **Important: for TEST purposes only.**                                   | false                   |


#### State Log Compaction

It is recommended that the state changelog is periodically compacted. When
compaction occurs, the current state is written to a new ledger and all
previous ledgers are swapped for the new compacted state ledger.

Compaction can currently only be performed within a task lifecycle for the
windowed task. Be careful to choose the condition (see `YOUR-CONDITION` in the
example below, as compacting too often is likely expensive. Compacting once
every X segments is reasonable good choice of condition.

```clojure
(def compaction-lifecycle
    {:lifecycle/before-batch 
     (fn [event lifecycle]
      (when YOUR-CONDITION
        (state-extensions/compact-log (:onyx.core/state-log event) event @(:onyx.core/window-state event)))
      {})})
```

#### BookKeeper Implementation Configuration

The BookKeeper state log implementation can be configured via the peer-config.
Of particular note, is `:onyx.bookkeeper/ledger-password` which generally be
changed to a more secure default.


| Parameter                                     | Description                                                                                                                                                                                                                                                                            | Default                 |
|---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------|
| `:onyx.bookkeeper/read-batch-size`            | Number of bookkeeper ledger entries to read at a time when recovering state. Effective batch read of state entries is write-batch-size * read-batch-size.                                                                                                                              | 50                      |
| `:onyx.bookkeeper/ledger-id-written-back-off` | Number of milliseconds to back off (sleep) after writing BookKeeper ledger id to the replica.                                                                                                                                                                                          | 50                      |
| `:onyx.bookkeeper/ledger-password`            | Password to use for Onyx state persisted to BookKeeper ledgers. Highly recommended this is changed on cluster wide basis.                                                                                                                                                              | INSECUREDEFAULTPASSWORD |
| `:onyx.bookkeeper/client-throttle`            | Tunable write throttle for BookKeeper ledgers.                                                                                                                                                                                                                                         | 30000                   |
| `:onyx.bookkeeper/write-buffer-size`          | Size of the buffer to which BookKeeper ledger writes are buffered via.                                                                                                                                                                                                                 | 10000                   |
| `:onyx.bookkeeper/client-timeout`             | BookKeeper client timeout.                                                                                                                                                                                                                                                             | 60000                   |
| `:onyx.bookkeeper/write-batch-size`           | Number of state persistence writes to batch into a single BookKeeper ledger entry.                                                                                                                                                                                                     | 20                      |
| `:onyx.bookkeeper/ledger-quorum-size`         | The number of BookKeeper instances over which entries will be written to. For example, if you have an ledger-ensemble-size of 3, and a ledger-quorum-size of 2, the first write will be written to server1 and server2, the second write will be written to server2, and server3, etc. | 3                       |
| `:onyx.bookkeeper/ledger-ensemble-size`       | The number of BookKeeper instances over which entries will be striped. For example, if you have an ledger-ensemble-size of 3, and a ledger-quorum-size of 2, the first write will be written to server1 and server2, the second write will be written to server2, and server3, etc.    | 3                       |
| `:onyx.bookkeeper/write-batch-timeout`        | Maximum amount of time to wait while batching BookKeeper writes, before writing the batch to BookKeeper. In case of a full batch read, timeout will not be hit.                                                                                                                        | 50                      |
