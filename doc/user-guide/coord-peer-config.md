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

| key name          | type       | choices
|-------------------|------------|----------
|`onyx/id`
|`:onyx.peer/retry-start-interval`
|`:onyx.peer/sequential-back-off`

