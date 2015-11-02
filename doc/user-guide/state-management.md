## State Management

This section discusses state management and fault tolerance used in windowing/streaming joins.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Summary](#summary)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Summary

Onyx provides the ability to perform stateful updates for segments which are
calculated over [windows] (doc/user-guide/windowing.md). For example, a
grouping task may accumulate incoming values for a number of keys over windows
of 5 minutes.

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
{:window/id :sum-all-ages
 :window/task :identity
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
| Applied to state | `104`                        |

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

### Exactly Once Data Processing

Exactly once data processing is supported via Onyx's filtering feature. When a
windowing task's catalog has `:onyx/uniqueness-key` set, this key is looked up
in the segment and used as an ID for whether the segment has been seen before.
If it has been seen, then the segment is not processed. This key is persisted to
the state log, along with the window changelog updates, so that the key can be
recovered in case of a peer failure.
            
#### Considerations

In order to reduce memory consumption, uniqueness-key values are persisted to a
local database, currently implemented with RocksDB. This database uses a bloom
filter, and a memory cache, allowing Onyx to avoid hitting disk
for most filter key checks.

In order to prevent unbounded increase in the size of the filter's disk
consumption, uniqueness-key values are bucketed based on recency, and the
oldest bucket is expired as the newest is filled.

Several configuration parameters are available for the rocksdb based local filter. The most relevant of these for general configuration is `:onyx.rocksdb.filter/num-ids-per-bucket`, and `:onyx.rocksdb.num-buckets`, which are the size and the number of buckets referenced above.

| Parameter                                    | Description                                                                                                                                                                         | Optional? |
|--------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------|
| `:onyx.rocksdb.filter/peer-block-cache-size` | RocksDB block cache size in bytes. Larger caches reduce the chance that the peer will need to check for the prescence of a uniqueness key on disk. Defaults to 100MB.               | true      |
| `:onyx.rocksdb.filter/num-buckets`           | Number of rotating filter buckets to use. Buckets are rotated every `:onyx.rocksdb.filter/num-ids-per-bucket`, with the oldest bucket being discarded if num-buckets already exist. | true      |
| `:onyx.rocksdb.filter/bloom-filter-bits`     | Number of bloom filter bits to use per uniqueness key value                                                                                                                         | true      |
| `:onyx.rocksdb.filter/num-ids-per-bucket`    | Number of uniqueness key values that can exist in a RocksDB filter bucket.                                                                                                          | true      |
| `:onyx.rocksdb.filter/block-size`            | RocksDB block size. May worth being tuned depending on the size of your uniqueness-key values.                                                                                      | true      |
| `:onyx.rocksdb.filter/compression`           | "Whether to use compression in rocksdb filter. It is recommended that `:none` is used unless your uniqueness keys are large and compressible.                                       | true      |
| `:onyx.rocksdb.filter/base-dir`              | Temporary directory to persist uniqueness filtering data.                                                                                                                           | true      |

#### Exactly Once Side-Effects

Exactly once *side-effects* resulting from a segment being processed may occur,
as exactly once side-effects are impossible to achieve. Onyx guarantees that a
window state updates resulting from a segment are perfomed exactly once,
however any side-effects that occur as a result of the segment being processed
cannot be guaranteed to only occur once.

### BookKeeper Implementation

Local bookkeeper implementation available via start-env.
Writes are batched and compressed by default.

### Log Compaction


### BookKeeper Configuration


TODO: FILL ME IN
