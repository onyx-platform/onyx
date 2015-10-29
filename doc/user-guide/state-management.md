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

;; Sample segments:
[{:name "John" :age 49}
 {:name "Madeline" :age 55}
 {:name "Geoffrey" :age 14}]
```

As segments arrive, a state is updated to account for the age of the incoming
segment, and the given name. For example, given an aggregation function:

```
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

Resulting in the following events:


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

This state can be emitted via triggers or another mechanism.  Use of a log
command type like `:set-value` ensures that your aggregation function can emit
multiple types of state transition if necessary.

### Exactly Once Processing

Should exactly once discussion be in another doc?

### Fault Tolerance

To allow for full recovery after peer crashes, this state must be replicated
somewhere. As state updates occur, Onyx publishes the stream of changelog updates
to a replicated log.

After the changelog entry is written to the replicated log, the segment is
acked, ensuring that a segment is only cleared from the input source after the
update it caused has been fully written to the log.  When a peer crash occurs,
a new peer will be assigned to the task, and this peer will play back all of
the changelog entries, and apply them to the state, starting with the initial
state. As the changelog updates are read back in the same order that they were
written, the full state will be recovered. Partial updates ensure that only
minimal update data is written for each segment processed, while remaining
correct on peer failure.

### BookKeeper Implementation

TODO: FILL ME IN

### BookKeeper Configuration

### Log Compaction

TODO: FILL ME IN
