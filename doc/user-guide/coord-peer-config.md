<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Coordination and Peer Configuration](#coordination-and-peer-configuration)
  - [Coordinator & Peer](#coordinator-&-peer)
    - [Base Configuration](#base-configuration)
    - [`:hornetq/mode`](#hornetqmode)
    - [UDP Configuration](#udp-configuration)
    - [JGroups Configuration](#jgroups-configuration)
  - [Coordinator Only](#coordinator-only)
      - [`:onyx.coordinator/revoke-delay`](#onyxcoordinatorrevoke-delay)
      - [`:onyx.coordinator/port`](#onyxcoordinatorport)
      - [`:hornetq/server`](#hornetqserver)
      - [`:hornetq.server/type`](#hornetqservertype)
      - [`:zookeeper/server`](#zookeeperserver)
    - [Embedded Configuration](#embedded-configuration)
      - [`:hornetq.embedded/config hq-servers`](#hornetqembeddedconfig-hq-servers)
  - [Peer Only](#peer-only)
    - [Base Configuration](#base-configuration-1)
      - [`:onyx.peer/retry-start-interval`](#onyxpeerretry-start-interval)
      - [`:onyx.peer/sequential-back-off`](#onyxpeersequential-back-off)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Coordination and Peer Configuration

The chapter describes the options available to configure both the Coordinator and Virtual Peers.

### Coordinator & Peer

#### Base Configuration

| key name                      | type       |
|-------------------------------|------------|
|`:onyx/id`                     |  `any`     |
|`:hornetq/mode`                |  `keyword` |
|`:zookeeper/address`           |  `string`  |

#### `:hornetq/mode`

The mechanism by which to connect to one or more HornetQ servers. One of `:vm, :udp, :jgroups, :standalone`.

#### UDP Configuration

| key name                       | type       |
|--------------------------------|------------|
|`:hornetq.udp/cluster-name`     |  `string`  |
|`:hornetq.udp/group-address`    |  `string`  |
|`:hornetq.udp/group-port`       |  `int`     |
|`:hornetq.udp/refresh-timeout`  |  `int`     |
|`:hornetq.udp/discovery-timeout`|  `int`     |

#### JGroups Configuration

| key name                                      | type      |
|-----------------------------------------------|-----------|
|`:hornetq.udp/cluster-name`                    |  `string` |
|`:hornetq.jgroups/file jgroups-file`           |  `string` |
|`:hornetq.jgroups/channel-name jgroups-channel`|  `string` |
|`:hornetq.udp/refresh-timeout`                 |  `int`    |
|`:hornetq.udp/discovery-timeout`               |  `int`    |

### Coordinator Only

| key name          | type       | choices    | optional?                         |
|-------------------|------------|------------|-----------------------------------|
|`:onyx.coordinator/revoke-delay`|  `int`     |                                   |
|`:onyx.coordinator/port`        |  `int`     | Optional for in-memory coordinator|
|`:hornetq/server?`              |  `boolean` | Yes                               |
|`:hornetq.server/type`          |  `keyword` |                                   |
|`:zookeeper/server?`            |  `boolean` | Yes                               |
|`:zookeeper.server/port`        |  `int`     | Optional for in-memory coordinator|

##### `:onyx.coordinator/revoke-delay`

Number of ms to wait for a peer to acknowledge an assigned task before revoking it.

##### `:onyx.coordinator/port`

The port to run the Coordinator web server on.

##### `:hornetq/server`

True to spin up a HornetQ server inside the Coordinator for convenience.

##### `:hornetq.server/type`

The type of server to spin up inside the Coordinator on the developers behalf. One of `:embedded, :vm`.

##### `:zookeeper/server`

True to spin up a ZooKeeper server inside the Coordinator for convenience.

#### Embedded Configuration

| key name                             | type      |
|--------------------------------------|-----------|
|`:hornetq.embedded/config hq-servers` |  `seq`    |

##### `:hornetq.embedded/config hq-servers`

A sequence of strings, each representing a HornetQ configuration file on the classpath.

### Peer Only

#### Base Configuration

| key name                        | type       | default|
|---------------------------------|------------|--------|
|`:onyx.peer/retry-start-interval`| `int`      | 2000   |
|`:onyx.peer/sequential-back-off` | `int`      | 2000   |

##### `:onyx.peer/retry-start-interval`

Number of ms to wait before trying to reboot a virtual peer after failure.

##### `:onyx.peer/sequential-back-off`

Number of ms to wait before retrying to execute a sequential task in the presence of other queue consumers.

