## Onyx

(Docs under construction)

### What is it?

- a cloud scale, fault tolerant, distributed computation system
- written in Clojure
- batch and stream processing hybrid
- exposes an *information model* for the description and construction of distributed workflows
- enabled by hardware advances in the last 10 years with respect to solid state drives and increased network speeds
- Competes against Storm, Cascading, Cascalog, Map/Reduce, Dryad, Apache Sqoop, Twitter Crane, and Netflix Forklift

### I built this because I wanted...
- an information model, rather than an API, for describing distributed workflows
- temporal decoupling of workflow descriptions from workflow execution
- an elimination of macros from the API that is offered
- plain Clojure functions as building blocks for application logic
- a very easy way to test distributed workflows locally, without buckets of mocking code
- a decoupled technique for configuring workflows
- transactional, exactly-once semantics for moving data between nodes in a cluster
- transparent code reuse between streaming and batching workflows
- friendlier interfaces to plug into IO for data sources
- aspect orientation without a headache
- to get away from AOT complilation and avoid dependency hell
- heterogenous jar execution for performing rolling releases

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

### Constraints
- The only way for data to get into or out of Onyx is via HornetQ. Plugins, however, can provided the illusion that this isn't true.
- Only maps may enter Onyx and be emitted internally as segments. The only exception to this rule is the sentinel value.
- Unlike Hadoop and Storm, the Onyx application jar is not transfered to the nodes on the cluster via a command line utility. Configuration management, such as Chef and Puppet, have come far enough to allow for an improved deployment story.

### Dependencies
- Java 7+
- Clojure 1.6.0
- HornetQ 2.4.0
- ZooKeeper 3.4.5+

### Environment set up

In order to host a coordinator or peer on a node, Java 7+ must be installed.
Additionally, a ZooKeeper and HornetQ connection need to be available to all the nodes in the cluster.
See below for the set up each of these services.

#### Chef Recipe

See `chef-onyx`.

#### Manual set up

##### HornetQ 2.4.0-Final

- [Download HornetQ 2.4.0-Final here](http://hornetq.jboss.org/downloads). Grab the .tar.gz.
- Untar the downloaded file.
- Make the following adjustments to `config/stand-alone/non-clustered/hornetq-configuration.xml` `security-settings`. We need to enable permission to create and destroy durable queues:

```xml
<security-settings>
    <security-setting match="#">
        <permission type="createNonDurableQueue" roles="guest"/>
        <permission type="deleteNonDurableQueue" roles="guest"/>
        <permission type="createDurableQueue" roles="guest"/>
        <permission type="deleteDurableQueue" roles="guest"/>
        <permission type="consume" roles="guest"/>
        <permission type="send" roles="guest"/>
    </security-setting>
</security-settings>
```

- Make the following adjustment to `config/stand-alone/non-clustered/hornetq-configuration.xml`. We need to make HornetQ page messages to disk by default:

```xml
<address-settings>
    <address-setting match="#">
        .... more settings here ....
        <address-full-policy>PAGE</address-full-policy>
    </address-setting>
</address-settings>
```

##### ZooKeeper

There's a pretty good [installation guide for ZooKeeper here](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html). No special configuration is needed.

### Integer Streaming Example

This example program streams integers onto a HornetQ queue and increments each as it comes through, placing the results segments on an outgoing HornetQ queue:

```clojure
(ns onyx.peer.multi-peer-mem-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

;;; Write 15,000 segments onto the queue for ingestion
(def n-messages 15000)

;;; Preconfigure a batch size for performance tuning.
(def batch-size 1320)

;;; Configuration
(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

;;; Small utility function to write segments onto a HornetQ queue.
;;; Notice it forms a map with key :n. Segments must be maps.
(hq-util/write! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) 1000)

;;; The transformation function - takes a single segment, which is a map,
;;; as an argument, and returns a segment.
(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

;;; The catalog definition
(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size
    :onyx/doc "The queue for ingestion"}

   {:onyx/name :inc
    ;;; Keyword referring to fully qualified fn definition.
    :onyx/fn :onyx.peer.multi-peer-mem-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "The incrementing function."}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size
    :onyx/doc "The outgoing queue of resulting segments"}])

;;; The workflow, showing how data goes from
;;; <input source> => <transformation> => <output source>
(def workflow {:in {:inc :out}})

;;; Identifier for the coordinator. Used for coordinator fault tolerance
(def id (str (java.util.UUID/randomUUID)))

;;; Configure the coordinator
(def coord-opts {:hornetq-host hornetq-host
                 :hornetq-port hornetq-port
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 5000})

;;; Make a connect to the coordinator, using in-memory mode
(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

;;; Configure the peers to run on this node
(def peer-opts {:hornetq-host hornetq-host
                :hornetq-port hornetq-port
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

;;; Start 3 virtual peers to do some processing
;;; This is sort of like a "parallelism factor" of 3 that can be tuned
;;; for each node
(def v-peers (onyx.api/start-peers conn 3 peer-opts))

;;; Submit the job
(onyx.api/submit-job conn {:catalog catalog :workflow workflow})
```

### Datomic Batch Processing Example

This program shows off the Datomic plugin and how Onyx seemlessly processes a data set in a batch style without any major changes. Here, we lift all the datoms out of a Datomic database and retain only those that represent a person's name of 5 characters or less. The results are placed on a HornetQ queue for consumption.

```clojure
(ns onyx.plugin.input-test
  (:require [midje.sweet :refer :all]
            [datomic.api :as d]
            [onyx.plugin.datomic]  ;;; Import the plugin
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.api]))

;;; The database that we'll be ingesting
(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

;;; The schema for that database
(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def conn (d/connect db-uri))

@(d/transact conn schema)

;;; Insert some data to play with
(def people
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Derek"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Kristen"}])

@(d/transact conn people)

(def db (d/db conn))

;;; Grab the t value - the plugin lets you configure
;;; which t-value to read off of
(def t (d/next-t db))

;;; Define a batching size for performance tuning
(def batch-size 1000)

;;; Configuration
(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def out-queue (str (java.util.UUID/randomUUID)))

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:hornetq-host hornetq-host
                 :hornetq-port hornetq-port
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 5000})

(def peer-opts {:hornetq-host hornetq-host
                :hornetq-port hornetq-port
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

;;; The query we'll be executing across the data set.
;;; Very easy to repl test.
(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

;;; The function that executes the query on a single segment.
;;; The Datomic plugin batches n datoms into a single segment as a
;;; performance optimization.
(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

;;; A workflow that reads the database datoms index, partitions
;;; it into equal chunks, loads each chunk in parallel, runs the
;;; query across the data set, and persists the results to HornetQ.
(def workflow {:partition-datoms {:load-datoms {:query :persist}}})

;;; The catalog. :partition-datoms and :load-datoms are copied from
;;; the plugin's README - similiar to how Chef recipe configurations are copied.
(def catalog
  [{:onyx/name :partition-datoms
    :onyx/ident :datomic/partition-datoms
    :onyx/type :input
    :onyx/medium :datomic
    :onyx/consumption :sequential
    :onyx/bootstrap? true
    :datomic/uri db-uri
    :datomic/t t
    :onyx/batch-size batch-size
    :datomic/partition :com.mdrogalis/people
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms"}

   {:onyx/name :load-datoms
    :onyx/ident :datomic/load-datoms
    :onyx/fn :onyx.plugin.datomic/load-datoms
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :datomic/uri db-uri
    :datomic/t t
    :onyx/doc "Reads and enqueues a range of the :eavt datom index"}

   {:onyx/name :query
    :onyx/fn :onyx.plugin.input-test/my-test-query
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Queries for names of 5 characters or fewer"}

   {:onyx/name :persist
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size
    :onyx/doc "Output source for intermediate query results"}])

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-utils/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

;;; Check the results. Success!
(fact (into #{} (mapcat #(apply concat %) (map :names results)))
      => #{"Mike" "Benti" "Derek"})
```

### Multiple Inputs & Outputs Example

### Aggregation Example

### Coordinator Connection Strings

#### In-Memory Mode

#### Distributed Mode

### Architecture

![Architecture](http://i.imgur.com/zRbA47X.png)

#### Coordinator

Onyx uses a single node to do distributed coordination across the cluster. This node is accepts new compute peers, watches for faults in peers, accepts jobs, and balances workloads. It’s fault tolerant by writing all data to ZooKeeper. This node can be made highly available by using traditional heartxbeat techniques. It plays a similar role to Storm’s Nimbus.

#### ZooKeeper Cluster

While the Coordinator decides how to balance workflows, ZooKeeper carries out the hard work of doing the actual distributed communication. For each Virtual Peer, a node set is designated inside of ZooKeeper. The Coordinator interacts with the Virtual Peer through this node set by a mutual exchange of reading, writing, and touching znodes. This has the effect of being able to both push and pull data from the perspective of both the Coordinator and the Virtual Peer.

#### Onyx Virtual Peer Cluster

A cluster of Virtual Peers performs the heavy lifting in Onyx. Similar to DynamoDB’s notion of a virtual node in a consistent hash ring, Onyx allows physical machines to host as many virtual peers as the compute machine would like. The relationship between virtual peers and physical nodes is transparent to the Coordinator.

Each virtual peer is allowed to be active on at most one task in an Onyx workflow. Concurrency at the virtual peer level is achieved by extensive pipelining, similar to the way Datomic’s transactor is (supposedly) built. Parallelism at the physical node level is achieved by starting more virtual peers. Onyx efficiently uses all the nodes in the box, and its thread­safe API means that as the physical node gets beefier, more work can be accomplished in a shorter period of time.

#### HornetQ cluster

A cluster of HornetQ nodes carries out the responsibility of moving data between virtual peers. When a job is submitted to the coordinator, a tree­walk is performed on the workflow. Queues are constructed out of the edges of the tree, and this information is persisted to the log. This enables every peer to receive a task and also know where to consume data from and where to produce it to.

Fault tolerance is achieved using HornetQ transacted sessions. As with most systems of this nature, transformation functions should be idempotent as data segments will be replayed if they were consumed from a queue and the transaction was not committed.

### Information Model

#### Workflow

- a single Clojure map which is EDN serializable/deserializable
- all elements in the map are keywords
- all elements in the map must correspond to an `:onyx/name` entry in the catalog
- the outer-most keys of the map must have catalog entries of `:onyx/type` that map to `:input`
- only innermost values of the map may have catalog entries of `:onyx/type` that map to `:output`
- elements in the map with `:onyx/type` mapping to `:aggregator` can only directly follow elements with `:onyx/type` mapping to `:grouper`

#### Catalog

- a single Clojure vector which is EDN serializable/deserializable
- all elements in the vector must be Clojure maps

##### All maps in the vector must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/name`       | `keyword`  | `any`
|`:onyx/type`       | `keyword`  | `:input`, `:output`, `:transformer`, `:grouper`, `:aggregator`
|`:onyx/consumption`| `keyword`  | `:sequential`, `:concurrent`
|`:onyx/batch-size` | `integer`  | `>= 0`

##### All maps may optionally have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/ident`      | `keyword`  | `any`

##### Maps with `:onyx/type` set to `:input` or `:output` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/medium`     | `keyword`  | `any`

##### Maps with `:onyx/type` set to `:transformer`, `:grouper`, or `:aggregator` must have these keys

| key name          | type       | choices
|-------------------|------------|----------
|`:onyx/fn`         | `keyword`  | `any`

#### ZooKeeper

ZooKeeper itself has an information model which Onyx uses to propagate messages and information to clients. It generally uses a flat structure with UUIDs to cross-reference data nodes. See the diagram below:

![ZooKeeper](http://i.imgur.com/mQ7I9Le.png)

### User-facing API

### Task Lifecycle API

Each time a virtual peer receives a task from the coordinator to execute, a lifecycle of functions are called. Onyx creates a map of useful data for the functions at the start of the lifecycle and proceeds to pass the map through to each function. 

Onyx provides hooks for user-level modification of this map both before the task begins executing, after each batch is completed, and after the task is completed. These are the `inject-lifecycle-resources`, `close-temporal-resources`, and `close-lifecycle-resources`, respectively. Each of these last three functions allows dispatch based on the name, identity, type, and type/medium combination of a task. Map merge prescendence happens in this exact order.

The virtual peer process is extensively pipelined, providing asynchrony between each lifecycle function. Hence, each virtual peer allocates at least 11 threads. See the Clojure docs for a description of each function.

- `inject-lifecycle-resources`

- `read-batch`

- `decompress-batch`

- `requeue-sentinel`

- `ack-batch`

- `apply-fn`

- `compress-batch`

- `write-batch`

- `close-temporal-resources`

- `close-lifecycle-resources`
