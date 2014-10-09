## Internal Design

This chapter outlines how Onyx works on the inside to meet the required properties of a distributed data processing system. This is not a proof nor an iron-clad specification for other implementations of Onyx. I will do my best to be transparent about how everything is working under the hood - good and bad.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Internal Design](#internal-design)
  - [High Level Components](#high-level-components)
    - [Coordinator](#coordinator)
    - [Peer](#peer)
    - [Virtual Peer](#virtual-peer)
    - [ZooKeeper](#zookeeper)
    - [HornetQ](#hornetq)
  - [Cross Entity Communication](#cross-entity-communication)
    - [ZNodes](#znodes)
  - [Virtual Peer States](#virtual-peer-states)
    - [Description](#description)
    - [State Transitions](#state-transitions)
    - [Transition Circumstances](#transition-circumstances)
  - [Coordinator Event Handling](#coordinator-event-handling)
    - [Serial Execution](#serial-execution)
    - [Fault Tolerant Logging](#fault-tolerant-logging)
    - [Timeouts](#timeouts)
  - [Coordinator/Virtual Peer Interaction](#coordinatorvirtual-peer-interaction)
  - [Segment Transportation](#segment-transportation)
    - [HornetQ Single Server](#hornetq-single-server)
    - [HornetQ Cluster](#hornetq-cluster)
  - [Virtual Peer Task Execution](#virtual-peer-task-execution)
    - [Phases of Execution](#phases-of-execution)
    - [Pipelining](#pipelining)
    - [Local State](#local-state)
  - [Sentinel Values in a Distributed Setting](#sentinel-values-in-a-distributed-setting)
    - [Sentinel-in-the-Middle](#sentinel-in-the-middle)
    - [Sentinel Reduction](#sentinel-reduction)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### High Level Components

#### Coordinator

The Coordinator is single node in the cluster responsible for doing distributed coordination. It receives jobs from Onyx clients, assigns work to peers, and handles peer failure.

#### Peer

A Peer is a node in the cluster responsible for processing data. It is similar to Storm's Worker node. A peer generally refers to a physical machine, though in the documentation, "peer" and "virtual peer" are often used interchangeably.

#### Virtual Peer

A Virtual Peer refers to a single peer process running on a single physical machine. Each virtual peer spawns about 15 threads to support itself since it is a pipelined process. The Coordinator sees all virtual peers as equal, whether they are on the same physical peer or not. Virtual peers *never* communicate with each other - they only communicate with the Coordinator.

#### ZooKeeper

Apache ZooKeeper is used as storage and communication layer. ZooKeeper takes care of things like CAS, consensus, leader election, and sequential file creation. ZooKeeper watches are at the heart of how Onyx virtual peers communicate with the Coordinator.

#### HornetQ

HornetQ is employed for shuttling segments between virtual peers for processing. HornetQ is a queueing platform from the JBoss stack. HornetQ queues can cluster for scalability.

### Cross Entity Communication

ZooKeeper is used to facilitate communication between the Coordinator and each virtual peer. Onyx expects to use the `/onyx` path in ZooKeeper without interference. The structure of this directory looks like the following tree:

- `/onyx`
  - `/<deployment UUID>`
    - `/peer`
      - `/<UUID>`
    - `/status`
      - `/<UUID>`
    - `/task`
      - `/<UUID>`
    - `/ack`
      - `/<UUID>`
    - `/catalog`
      - `/<UUID>`
    - `/job-log`
      - `/offer-<sequential id>`
    - `/job`
      - `/<UUID>`
    - `/payload`
      - `/<UUID>`
    - `/pulse`
      - `/<UUID>`
    - `/completion`
      - `/<UUID>`
    - `/cooldown`
      - `/<UUID>`
    - `/workflow`
      - `/<UUID>`
    - `/election`
      - `/proposal-<sequential id>`
    - `/plan`
      - `/<UUID>`
    - `/seal`
      - `/<UUID>`
    - `/exhaust`
      - `/<UUID>`
    - `/coordinator`
      - `/revoke-log`
        - `/log-entry-<sequential id>`
      - `/born-log`
        - `/log-entry-<sequential id>`
      - `/seal-log`
        - `/log-entry-<sequential id>`
      - `/ack-log`
        - `/log-entry-<sequential id>`
      - `/death-log`
        - `/log-entry-<sequential id>`
      - `/evict-log`
        - `/log-entry-<sequential id>`
      - `/offer-log`
        - `/log-entry-<sequential id>`
      - `/shutdown-log`
        - `/log-entry-<sequential id>`
      - `/exhaust-log`
        - `/log-entry-<sequential id>`
      - `/planning-log`
        - `/log-entry-<sequential id>`
      - `/complete-log`
        - `/log-entry-<sequential id>`
    - `/peer-state`
      - `/<UUID>`
    - `/shutdown`
      - `/<UUID>`


### Virtual Peer States

#### Description


#### State Transitions


#### Transition Circumstances


### Coordinator Event Handling

#### Serial Execution

#### Fault Tolerant Logging

#### Timeouts

### Coordinator/Virtual Peer Interaction

### Segment Transportation

#### HornetQ Single Server

#### HornetQ Cluster

<Symetric clustering, session per thread, message offloading, load balancing>

### Virtual Peer Task Execution

#### Phases of Execution

#### Pipelining

#### Local State

### Sentinel Values in a Distributed Setting

#### Sentinel-in-the-Middle

#### Sentinel Reduction


