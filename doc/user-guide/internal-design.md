## Internal Design

This chapter outlines how Onyx works on the inside to meet the required properties of a distributed data processing system.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Internal Design](#internal-design)
  - [High Level Components](#high-level-components)
    - [Coordinator](#coordinator)
    - [Peeer](#peeer)
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

#### Peeer

#### Virtual Peer

#### ZooKeeper

#### HornetQ

### Cross Entity Communication

#### ZNodes


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


