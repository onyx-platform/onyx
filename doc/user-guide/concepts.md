## Concepts

### Terminology

#### Task

A task is the smallest unit of work in Onyx. It represents an activity of either input, processing, or output.

#### Workflow

A workflow is a Clojure map representing a multi-rooted, acyclic tree of tasks. The outermost keys of the map must name sources of input, and the innermost values of the map must name sources of output. Everything inbetween must name a function. Elements of a workflow must be Clojure keywords.

Examples:

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

#### Catalog

All inputs, outputs, and functions in a workflow must be described via a catalog. A catalog is a vector of maps, strikingly similar to Datomic’s schema. Configuration and docstrings are described in the catalog.

Example:

```clojure
[{:onyx/name :in
 :onyx/ident :hornetq/read-segments
 :onyx/type :input
 :onyx/medium :hornetq
 :onyx/consumption :concurrent
 :hornetq/queue-name in-queue
 :hornetq/host hornetq-host
 :hornetq/port hornetq-port
 :onyx/batch-size batch-size
 :onyx/doc "A HornetQ input stream"}

{:onyx/name :inc
 :onyx/fn :onyx.peer.multi-peer-mem-test/my-inc
 :onyx/type :transformer
 :onyx/consumption :concurrent
 :onyx/batch-size batch-size
 :onyx/doc "A function to increment integers"}

{:onyx/name :out
 :onyx/ident :hornetq/write-segments
 :onyx/type :output
 :onyx/medium :hornetq
 :onyx/consumption :concurrent
 :hornetq/queue-name out-queue
 :hornetq/host hornetq-host
 :hornetq/port hornetq-port
 :onyx/batch-size batch-size
 :onyx/doc "A HornetQ output stream"}]
```

#### Segment

A segment is the smallest unit of data in Onyx. Segments are required to be Clojure maps. They represent the data flowing through the cluster.

#### Transformer

A transformer is a function that receives segments and emits segments for further processing.

#### Grouper

A grouper is a function that takes a segment as an argument and emits a value. All segments with the same emitted value are routed to the same node in the cluster on the next task.

#### Aggregator

An aggregator is a function that receives segments with the same grouping value. Aggregators must immediately follow groupers. If a node performing aggregation is partitioned from the cluster during execution, the remaining, unprocessed segments are routed to another node in the cluster.

#### Plugin

A plugin is a means for hooking into data sources to extract data as input and produce data as output.

#### Sentinel

A sentinel is a value that can be pushed into Onyx to signal the end of a stream of data. This effectively lets Onyx switch between streaming and batching mode. The sentinel in Onyx is represented by the Clojure keyword `:done`.

#### Coordinator

The Coordinator is single node in the cluster responsible for doing distributed coordinator. This node can be made highly available through traditional heart beat techniques as Datomic's transactor does.

#### Peer

A Peer is a node in the cluster responsible for processing data. A "peer" generally refers to a physical machine.

#### Virtual Peer

A Virtual Peer refers to a single peer process running on a single physical machine. The Coordinator often does not need to know which virtual peers belong to which physical machines, so parallelism can be increased by treating all virtual peers equally.