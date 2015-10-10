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
grouping task may accumulate incoming values for a number of keys over a global
window.

#### State Example

```clojure

;; Task definition
{:onyx/name :sum-ages
 :onyx/fn :clojure.core/identity
 :onyx/type :function
 :onyx/group-by-key :name
 :onyx/flux-policy :recover
 :onyx/min-peers 2
 :onyx/batch-size 20}

;; Window definition
{:window/id :sum-ages
 :window/task :identity
 :window/type :global
 :window/aggregation [:onyx.windowing.aggregation/sum :age]
 :window/window-key :event-time
 :window/range [1 :hour]
 :window/doc "Adds the :age key in all segments in 1 hour fixed windows"}

;; Sample segments:
[{:name "John" :age 49}
 {:name "Madeline" :age 55}
 {:name "John" :age 14}]
```

As segments arrive, a state hash-map is updated to account for the age of the incoming
segment, and the given name. For example:

Initial state: `{"John" 51}`. 
Incoming segment: `{:name "John" :age 49}` 
New state: `{"John" 51}`
Incoming segment: `{:name "Madeline" :age 55}`
Updated state: `{"John" 51 "Madeline" 55}`

This state can be emitted via triggers or another mechanism. 

### Fault Tolerance

To allow for full recovery after peer crashes, this state must be replicated
somewhere. As state updates occur, Onyx publishes a stream of changelog updates
to a replicated log. Using the example from above:

Initial state: `{"John" 51}`. 
Incoming segment: `{:name "Madeline" :age 55}`
Changelog entry: `[:set-key "Madeline" 55]`
Updated state: `{"John" 51 "Madeline" 55}`

After the changelog entry is written to the replicated log, the segment is
acked, ensuring that a segment is only cleared from the input source after the
update it caused has been fully written to the log.  When a peer crash occurs,
a new peer will be assigned to the task, and this peer will play back all of
the changelog entries, starting with the initial state. As the changelog
updates are read back in the same order that they were written, the full state
will be recovered. Partial updates ensure that only minimal data is written for
each segment processed.

### Exactly Once Processing

Should exactly once discussion be in another doc?

### BookKeeper Implementation

TODO: FILL ME IN

### Log Compaction

TODO: FILL ME IN
