## Coordination and Peer Configuration

The chapter describes the options available to configure both the Coordinator and Virtual Peers.

### Coordinator

#### Base Configuration

| key name                      | type       | optional?
|-------------------------------|------------|-----------------------------------|
|`onyx/id`                      |  `any`     |                                   |
|`onyx.coordinator/revoke-delay`|  `int`     |                                   |
|`onyx.coordinator/port`        |  `int`     | Optional for in-memory coordinator|
|`:hornetq/mode`                |  `keyword` |                                   |
|`:hornetq/server?`             |  `boolean` | Yes                               |
|`:hornetq.server/type`         |  `keyword` |                                   |
|`:zookeeper/address`           |  `string`  |                                   |
|`:zookeeper/server?`           |  `boolean` | Yes                               |
|`zookeeper.server/port`        |  `int`     | Optional for in-memory coordinator|

##### `onyx.coordinator/revoke-delay`

Number of ms to wait for a peer to acknowledge an assigned task before revoking it.

##### `onyx.coordinator/port`

The port to run the Coordinator web server on.

##### `:hornetq/server`

True to spin up a HornetQ server inside the Coordinator for convenience.

##### `:hornetq/mode`

The mechanism by which to connect to one or more HornetQ servers. One of `:vm, :udp, :jgroups, :standalone`.

##### `:hornetq.server/type`

The type of server to spin up inside the Coordinator on the developers behalf. One of `:embedded, :vm`.

##### `:zookeeper/server`

True to spin up a ZooKeeper server inside the Coordinator for convenience.

#### UDP Configuration

#### JGroups Configuration

#### Embedded Configuration


### Peer

#### Base Configuration

| key name          | type       | choices
|-------------------|------------|----------
|`onyx/id`
|`:onyx.peer/retry-start-interval`
|`:onyx.peer/sequential-back-off`

