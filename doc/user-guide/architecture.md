## Architecture

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Architecture](#architecture)
  - [Onyx Coordinator](#onyx-coordinator)
  - [ZooKeeper Cluster](#zookeeper-cluster)
  - [Onyx Virtual Peer Cluster](#onyx-virtual-peer-cluster)
  - [HornetQ cluster](#hornetq-cluster)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

![Architecture](http://i.imgur.com/ZfNfLb7.png)

### Onyx Coordinator

Onyx uses a single node to do distributed coordination across the cluster. This node is accepts new compute peers, watches for faults in peers, accepts jobs, and balances workloads. It’s fault tolerant by writing all data to ZooKeeper. This node can be made highly available by using traditional heartbeat techniques. It plays a similar role to Storm’s Nimbus.

### ZooKeeper Cluster

While the Coordinator decides how to balance workflows, ZooKeeper carries out the hard work of doing the actual distributed communication. For each Virtual Peer, a node set is designated inside of ZooKeeper. The Coordinator interacts with the Virtual Peer through this node set by a mutual exchange of reading, writing, and touching znodes. This has the effect of being able to both push and pull data from the perspective of both the Coordinator and the Virtual Peer.

### Onyx Virtual Peer Cluster

A cluster of Virtual Peers performs the heavy lifting in Onyx. Similar to DynamoDB’s notion of a virtual node in a consistent hash ring, Onyx allows physical machines to host as many virtual peers as the compute machine would like. The relationship between virtual peers and physical nodes is transparent to the Coordinator.

Each virtual peer is allowed to be active on at most one task in an Onyx workflow. Concurrency at the virtual peer level is achieved by extensive pipelining, similar to the way Datomic’s transactor is (supposedly) built. Parallelism at the physical node level is achieved by starting more virtual peers. Onyx efficiently uses all the nodes in the box, and its thread­safe API means that as the physical node gets beefier, more work can be accomplished in a shorter period of time.

### HornetQ cluster

A cluster of HornetQ nodes carries out the responsibility of moving data between virtual peers. When a job is submitted to the coordinator, a tree­walk is performed on the workflow. Queues are constructed out of the edges of the tree, and this information is persisted to the log. This enables every peer to receive a task and also know where to consume data from and where to produce it to.

Fault tolerance is achieved using HornetQ transacted sessions. As with most systems of this nature, transformation functions should be idempotent as data segments will be replayed if they were consumed from a queue and the transaction was not committed.
