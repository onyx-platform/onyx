## Peer Configuration

The chapter describes the all options available to configure the virtual peers and development environment.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Base Configuration

| key name                      | type       |
|-------------------------------|------------|
|`:onyx/id`                     |  `any`     |
|`:zookeeper/address`           |  `string`  |


### Environment Only

| key name               | type       | optional?  |
|------------------------|------------|------------|
|`:zookeeper/server?`    |  `boolean` | Yes        |
|`:zookeeper.server/port`|  `int`     | Yes        |


### Peer Only

#### Base Configuration

| key name                               | type       | default                            |
|----------------------------------------|------------|------------------------------------|
|`:onyx.peer/inbox-capacity`             | `int`      | `1000`                             |
|`:onyx.peer/outbox-capacity`            | `int`      | `1000`                             |
|`:onyx.peer/retry-start-interval`       | `int`      | `2000`                             |
|`:onyx.peer/join-failure-back-off`      | `int`      | `250`                              |
|`:onyx.peer/drained-back-off`           | `int`      | `400`                              |
|`:onyx.peer/peer-not-ready-back-off`    | `int`      | `2000`                             |
|`:onyx.peer/job-not-ready-back-off`     | `int`      | `500`                              |
|`:onyx.peer/fn-params`                  | `map`      | `{}`                               |
|`:onyx.peer/zookeeper-timeout`          | `int`      | `6000`                             |
|`:onyx.messaging/inbound-buffer-size`   | `int`      | `20000`                            |
|`:onyx.messaging/completion-buffer-size`| `int`      | `1000`                             |
|`:onyx.messaging/release-ch-buffer-size`| `int`      | `10000`                            |
|`:onyx.messaging/retry-ch-buffer-size`  | `int`      | `10000`                            |
|`:onyx.messaging/ack-daemon-timeout`    | `int`      | `60000`                            |
|`:onyx.messaging/ack-daemon-clear-interval`| `int`      | `15000`                         |
|`:onyx.messaging/decompress-fn`         | `function` | `onyx.compression.nippy/decompress`|
|`:onyx.messaging/compress-fn`           | `function` | `onyx.compression.nippy/compress`  |
|`:onyx.messaging/impl`                  | `keyword`  | `:netty`, `:core.async`            |
|`:onyx.messaging/bind-addr`             | `string`   | `nil`                              |
|`:onyx.messaging/peer-port-range`       | `vector`   | `[]`                               |
|`:onyx.messaging/peer-ports`            | `vector`   | `[]`                               |

##### `:onyx.peer/inbox-capacity`

Maximum number of messages to try to prefetch and store in the inbox, since reading from the log happens asynchronously.

##### `:onyx.peer/outbox-capacity`

Maximum number of messages to buffer in the outbox for writing, since writing to the log happens asynchronously.

##### `:onyx.peer/retry-start-interval`

Number of ms to wait before trying to reboot a virtual peer after failure.

##### `:onyx.peer/drained-back-off`

Number of ms to wait before trying to complete the job if all input tasks have been exhausted. Completing the job may not succeed if the cluster configuration is being shifted around.

##### `:onyx:onyx.peer/peer-not-ready-back-off`

Number of ms to back off and wait before retrying the call to `start-task?` lifecycle hook if it returns false.

##### `:onyx:onyx.peer/job-not-ready-back-off`

Number of ms to back off and wait before trying to discover configuration needed to start the subscription after discovery failure.

##### `onyx.peer/join-failure-back-off`

Number of ms to wait before trying to rejoin the cluster after a previous join attempt has aborted.

##### `onyx.peer/fn-params`

A map of keywords to vectors. Keywords represent task names, vectors represent the first parameters to apply
to the function represented by the task. For example, `{:add [42]}` for task `:add` will call the function
underlying `:add` with `(f 42 <segment>)`.

##### `:onyx.peer/zookeeper-timeout`

Number of ms to timeout from the ZooKeeper client connection on disconnection.

##### `onyx.messaging/inbound-buffer-size`

Number of messages to buffer in the core.async channel for received segments.

##### `onyx.messaging/completion-buffer-size`

Number of messages to buffer in the core.async channel for completing messages on an input task.

##### `:onyx.messaging/release-ch-buffer-size`

Number of messages to buffer in the core.async channel for released completed messages.

##### `:onyx.messaging/retry-ch-buffer-size`

Number of messages to buffer in the core.async channel for retrying timed-out messages.

#### `:onyx.messaging/ack-daemon-timeout`

Number of milliseconds that an ack value can go without being updates on a daemon before it is elligible to time out.

#### `:onyx.messaging/ack-daemon-clear-interval`

Number of milliseconds to wait for process to periodically clear out ack-vals that have timed out in the daemon.

##### `onyx.messaging/decompress-fn`

The Clojure function to use for messaging decompression. Receives one argument - a byte array. Must return
the decompressed value of the byte array.

##### `onyx.messaging/compress-fn`

The Clojure function to use for messaging compression. Receives one argument - a sequence of segments. Must return a byte
array representing the segment seq.

##### `:onyx.messaging/impl`

The messaging protocol to use for peer-to-peer communication.

##### `:onyx.messaging/bind-addr`

An IP address to bind the peer to for messaging. Defaults to `nil`, binds to it's external IP to the result of calling `http://checkip.amazonaws.com`.

##### `:onyx.messaging/peer-port-range`

A vector of two integers that denotes the low and high values, inclusive, of ports that peers should use to communicate. Ports are allocated predictable in-order.

##### `onyx.messaging/peer-ports`

A vector of integers denoting ports that may be used for peer communication. This differences from `peer-port-range` in that this names specific ports, not a sequence of ports. Ports are allocated predictable in-order.

### Peer Full Example

```clojure
(def peer-opts
  {:onyx/id "df146eb8-fd6e-4903-847e-9e748ca08021"
   :zookeeper/address "127.0.0.1:2181"
   :onyx.peer/inbox-capacity 2000
   :onyx.peer/outbox-capacity 2000
   :onyx.peer/retry-start-interval 4000
   :onyx.peer/join-failure-back-off 500
   :onyx.peer/drained-back-off 400
   :onyx.peer/peer-not-ready-back-off 5000
   :onyx.peer/job-not-ready-back-off 1000
   :onyx.peer/fn-params {:add [42]}
   :onyx.peer/zookeeper-timeout 10000
   :onyx.messaging/completion-buffer-size 2000
   :onyx.messaging/release-ch-buffer-size 50000
   :onyx.messaging/retry-ch-buffer-size 100000
   :onyx.messaging/ack-daemon-timeout 90000
   :onyx.messaging/ack-daemon-clear-interval 15000
   :onyx.messaging/decompress-fn onyx.compression.nippy/decompress
   :onyx.messaging/compress-fn onyx.compression.nippy/compress
   :onyx.messaging/impl :netty
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/peer-port-range [50000 60000]
   :onyx.messaging/peer-ports [45000 45002 42008]})
```
