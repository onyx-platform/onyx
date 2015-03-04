## Peer Configuration

The chapter describes the options available to configure the Virtual Peers.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Coordinator & Peer](#coordinator-&-peer)
  - [Base Configuration](#base-configuration)
  - [`:hornetq/mode`](#hornetqmode)
  - [VM](#vm)
  - [Standalone](#standalone)
  - [UDP Configuration](#udp-configuration)
  - [JGroups Configuration](#jgroups-configuration)
- [Environment Only](#environment-only)
    - [`:hornetq/server?`](#hornetqserver)
    - [`:hornetq.server/type`](#hornetqservertype)
    - [`:zookeeper/server?`](#zookeeperserver)
  - [Embedded Configuration](#embedded-configuration)
    - [`:hornetq.embedded/config hq-servers`](#hornetqembeddedconfig-hq-servers)
- [Peer Only](#peer-only)
  - [Base Configuration](#base-configuration-1)
    - [`:onyx.peer/inbox-capacity`](#onyxpeerinbox-capacity)
    - [`:onyx.peer/outbox-capacity`](#onyxpeeroutbox-capacity)
    - [`:onyx.peer/retry-start-interval`](#onyxpeerretry-start-interval)
    - [`:onyx.peer/sequential-back-off`](#onyxpeersequential-back-off)
    - [`:onyx.peer/drained-back-off`](#onyxpeerdrained-back-off)
    - [`onyx.peer/fn-params`](#onyxpeerfn-params)
    - [`onyx.peer/join-failure-back-off`](#onyxpeerjoin-failure-back-off)
- [Peer Full Example](#peer-full-example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Coordinator & Peer

#### Base Configuration

| key name                      | type       |
|-------------------------------|------------|
|`:onyx/id`                     |  `any`     |
|`:hornetq/mode`                |  `keyword` |
|`:zookeeper/address`           |  `string`  |

#### `:hornetq/mode`

The mechanism by which to connect to one or more HornetQ servers. One of `:vm, :udp, :jgroups, :standalone`.

#### VM

None

#### Standalone

| key name                       | type       |
|--------------------------------|------------|
|`:hornetq.standalone/host`      |  `string`  |
|`:hornetq.standalone/port`      |  `int`     |

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

### Environment Only

| key name               | type       | optional?  |
|------------------------|------------|------------|
|`:hornetq/server?`      |  `boolean` | Yes        |
|`:hornetq.server/type`  |  `keyword` | No         |
|`:zookeeper/server?`    |  `boolean` | Yes        |
|`:zookeeper.server/port`|  `int`     | Yes        |

##### `:hornetq/server?`

True to spin up a HornetQ server inside the Coordinator for convenience.

##### `:hornetq.server/type`

The type of server to spin up inside the Coordinator on the developers behalf. One of `:embedded, :vm`.

##### `:zookeeper/server?`

True to spin up a ZooKeeper server inside the Coordinator for convenience.

#### Embedded Configuration

| key name                             | type      |
|--------------------------------------|-----------|
|`:hornetq.embedded/config hq-servers` |  `seq`    |

##### `:hornetq.embedded/config hq-servers`

A sequence of strings, each representing a HornetQ configuration file on the classpath.

### Peer Only

#### Base Configuration

| key name                         | type       | default|
|----------------------------------|------------|--------|
|`:onyx.peer/inbox-capacity`       | `int`      | `1000` |
|`:onyx.peer/outbox-capacity`      | `int`      | `1000` |
|`:onyx.peer/join-failure-back-off`| `int`      | `250`  |
|`:onyx.peer/retry-start-interval` | `int`      | `2000` |
|`:onyx.peer/sequential-back-off`  | `int`      | `2000` |
|`:onyx.peer/drained-back-off`     | `int`      | `400`  |
|`:onyx.peer/fn-params`            | `map`      | `{}`   |

##### `:onyx.peer/inbox-capacity`

Maximum number of messages to try to prefetch and store in the inbox, since reading from the log happens asynchronously.

##### `:onyx.peer/outbox-capacity`

Maximum number of messages to buffer in the outbox for writing, since writing to the log happens asynchronously.

##### `:onyx.peer/retry-start-interval`

Number of ms to wait before trying to reboot a virtual peer after failure.

##### `:onyx.peer/sequential-back-off`

Number of ms to wait before retrying to execute a sequential task in the presence of other queue consumers.

##### `:onyx.peer/drained-back-off`

Number of ms to wait before executing peer pipeline run if all ingress queues have been exhausted.

##### `onyx.peer/fn-params`

A map of keywords to vectors. Keywords represent task names, vectors represent the first parameters to apply
to the function represented by the task. For example, `{:add [42]}` for task `:add` will call the function
underlying `:add` with `(f 42 <segment>)`.

##### `onyx.peer/join-failure-back-off`

Number of ms to wait before trying to rejoin the cluster after a previous join attempt has aborted.

### Peer Full Example

```clojure
(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name "onyx-cluster"
   :hornetq.udp/group-address "231.7.7.7"
   :hornetq.udp/group-port 9876
   :hornetq.udp/refresh-timeout 5000
   :hornetq.udp/discovery-timeout 5000
   :zookeeper/address "127.0.0.1:2181"
   :onyx/id "df146eb8-fd6e-4903-847e-9e748ca08021"
   :onyx.peer/retry-start-interval 2000
   :onyx.peer/sequential-back-off 2000
   :onyx.peer/fn-params {:add [42]}})
```
