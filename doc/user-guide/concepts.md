## Concepts

We'll take a quick overview of some terms you'll see in the rest of this user guide.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Terminology](#terminology)
  - [Task](#task)
  - [Workflow](#workflow)
  - [Catalog](#catalog)
  - [Flow Conditions](#flow-conditions)
  - [Segment](#segment)
  - [Function](#function)
  - [Plugin](#plugin)
  - [Sentinel](#sentinel)
  - [Peer](#peer)
  - [Virtual Peer](#virtual-peer)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Terminology

#### Segment

A segment is the unit of data in Onyx, and it's represented by a Clojure map. Segments represent the data flowing through the cluster. Segments are the only shape of data that Onyx allows you to emit between functions.

#### Task

A task is the smallest unit of work in Onyx. It represents an activity of either input, processing, or output.

#### Workflow

A workflow is the structural specification of an Onyx program. Its purpose is to articulate the paths that data flows through the cluster at runtime. It is specified via a directed, acyclic graph.

The workflow representation is a Clojure vector of vectors. Each inner vector contains exactly two elements, which are keywords. The keywords represent nodes in the graph, and the vector represents a directed edge from the first node to the second.

```clojure
;;;    in
;;;    |
;;; increment
;;;    |
;;;  output
[[:in :increment] [:increment :out]]
```

```clojure
;;;            input
;;;             /\
;;; processing-1 processing-2
;;;     |             |
;;;  output-1      output-2

[[:input :processing-1]
 [:input :processing-2]
 [:processing-1 :output-1]
 [:processing-2 :output-2]]
```

```clojure
;;;            input
;;;             /\
;;; processing-1 processing-2
;;;         \      /
;;;          output

[[:input :processing-1]
 [:input :processing-2]
 [:processing-1 :output]
 [:processing-2 :output]]
```

#### Catalog

All inputs, outputs, and functions in a workflow must be described via a catalog. A catalog is a vector of maps, strikingly similar to Datomic’s schema. Configuration and docstrings are described in the catalog.

Example:

```clojure
[{:onyx/name :in
  :onyx/ident :core.async/read-from-chan
  :onyx/type :input
  :onyx/medium :core.async
  :onyx/batch-size batch-size
  :onyx/max-peers 1
  :onyx/doc "Reads segments from a core.async channel"}

 {:onyx/name :inc
  :onyx/fn :onyx.peer.min-peers-test/my-inc
  :onyx/type :function
  :onyx/batch-size batch-size}

 {:onyx/name :out
  :onyx/ident :core.async/write-to-chan
  :onyx/type :output
  :onyx/medium :core.async
  :onyx/batch-size batch-size
  :onyx/max-peers 1
  :onyx/doc "Writes segments to a core.async channel"}]
```

#### Flow Conditions

In contrast to a workflow, flow conditions specify on a segment-by-segment basis which direction data should flow determined by predicate functions. This is helpful for conditionally processing a segment based off of its content.

Example:

```clojure
[{:flow/from :input-stream
  :flow/to [:process-adults]
  :flow/predicate :my.ns/adult?
  :flow/doc "Emits segment if this segment is an adult."}
```

#### Function

A function is a construct that receives segments and emits segments for further processing. It literally is a Clojure function.

#### Lifecycle

A lifecycle is a construct that describes the lifetime of a task. There is an entire chapter devoted to lifecycles, but to be brief, a lifecycle allows you to hook in and execute arbitrary code at critical points during a task. A lifecycle carries a context map that you can merge results back into for use later.

#### Plugin

A plugin is a means for hooking into data sources to extract data as input and produce data as output. Onyx comes with a few plugins, but you can craft your own, too.

#### Sentinel

A sentinel is a value that can be pushed into Onyx to signal the end of a stream of data. This effectively lets Onyx switch between streaming and batching mode. The sentinel in Onyx is represented by the Clojure keyword `:done`.

#### Peer

A Peer is a node in the cluster responsible for processing data. A single "peer" refers to a physical machine, though we often use the terms peer and virtual peer interchangably when the difference doesn't matter.

#### Virtual Peer

A Virtual Peer refers to a single peer process running on a single physical machine. A single Virtual Peer executes at most one task at a time.

#### Job

A job is the collection of a workflow, catalog, flow conditions, lifecycles, and execution parameters. A job is most coarse unit of work, and every task is associated with exactly one job - hence a peer can only be working at least most one job at any given time.
