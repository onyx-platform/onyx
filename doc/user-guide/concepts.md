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

#### Task

A task is the smallest unit of work in Onyx. It represents an activity of either input, processing, or output.

#### Workflow

A workflow is the structural specification of an Onyx program. Its purpose is to articulate the paths that data flows through the cluster at runtime. It can either by specified via a tree, or a directed, acyclic graph.

In the case of a tree, the workflow is a Clojure map representing a multi-rooted tree of tasks. The outermost keys of the map must name sources of input, and the innermost values of the map must name sources of output. Everything in-between must name a function. Elements of a workflow must be Clojure keywords.

In the case of a directed acyclic graph, the workflow is a Clojure vector of vectors. Each inner vector contains exactly two elements, which are keywords. The keywords represent nodes in the graph, and the vector represents a directed edge from the first node to the second.

Tree Examples:

```clojure
;;;    in
;;;    |
;;; increment
;;;    |
;;;  output
{:in {:increment :out}}
```

```clojure
;;;            input
;;;             /\
;;; processing-1 processing-2
;;;     |             |
;;;  output-1      output-2
{:input {:processing-1 :output-1
         :processing-2 :output-2}}
```

```clojure
;;;                       workflow root
;;;                    /                 \
;;;            input-1                    input-2
;;;             /\                          /\
;;; processing-1 processing-2   processing-2 processing-1
;;;     |             |              |            |
;;;  output-1      output-2      output-1      output-2
{:input-1 {:processing-1 :output-1
           :processing-2 :output-2}
 :input-2 {:processing-2 :output-1
           :processing-1 :output-2}
```

DAG Examples:

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
 :onyx/ident :hornetq/read-segments
 :onyx/type :input
 :onyx/medium :hornetq
 :hornetq/queue-name in-queue
 :hornetq/host hornetq-host
 :hornetq/port hornetq-port
 :onyx/batch-size batch-size
 :onyx/doc "A HornetQ input stream"}

{:onyx/name :inc
 :onyx/fn :my.namespace/my-function-name
 :onyx/type :function
 :onyx/batch-size batch-size
 :onyx/doc "A function to increment integers"}

{:onyx/name :out
 :onyx/ident :hornetq/write-segments
 :onyx/type :output
 :onyx/medium :hornetq
 :hornetq/queue-name out-queue
 :hornetq/host hornetq-host
 :hornetq/port hornetq-port
 :onyx/batch-size batch-size
 :onyx/doc "A HornetQ output stream"}]
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

#### Segment

A segment is the unit of data in Onyx, and it's represented by a Clojure map. Segments represent the data flowing through the cluster.

#### Function

A function is a construct that receives segments and emits segments for further processing. It literally translates down to a Clojure function.

#### Plugin

A plugin is a means for hooking into data sources to extract data as input and produce data as output. Onyx comes with a few plugins, but you can craft your own, too.

#### Sentinel

A sentinel is a value that can be pushed into Onyx to signal the end of a stream of data. This effectively lets Onyx switch between streaming and batching mode. The sentinel in Onyx is represented by the Clojure keyword `:done`.

#### Peer

A Peer is a node in the cluster responsible for processing data. A "peer" generally refers to a physical machine.

#### Virtual Peer

A Virtual Peer refers to a single peer process running on a single physical machine. The Coordinator often does not need to know which virtual peers belong to which physical machines, so parallelism can be increased by treating all virtual peers equally. A single Virtual Peer executes at most one task at a time.

