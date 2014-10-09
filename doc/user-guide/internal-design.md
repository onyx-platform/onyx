## Internal Design

This chapter outlines how Onyx works on the inside to meet the required properties of a distributed data processing system.

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


