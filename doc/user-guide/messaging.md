## Messaging

### Background

The messaging layer takes care of the direct peer to peer transfer of segment
batches, acks, segment completion and segment retries to the relevant virtual
peers. The messaging layer design document can be found at
[design/messaging.md](../design/messaging.md).

### Messaging Implementations

The Onyx messaging implementation is pluggable and alternative implementatins
can be selected via the `:onyx.messaging/impl` [peer-config](peer-config.md).

#### Aeron Messaging

Owing to [Aeron's](https://github.com/real-logic/Aeron) high throughput and low
latency, Aeron is the default Onyx messaging implementation. There are a few
relevant considerations when using the Aeron implementation.

##### Subscription (Connection) Multiplexing

One issue when scaling Onyx to a many node cluster, is that every virtual peer
may require a communications channel to any other virtual peer. As a result, a
naive implementation will require m<sup>2</sup>, where m is the number of
virtual peers, connections to be established for the cluster. By sharing Aeron
subscribers (multiplexing), between virtual peers on a node, this can be
reduced to n<sup>2</sup>, where n is the number of nodes. This reduces the
amount of overhead that is required to maintain connections between peers,
allowing the cluster to better scale.

It is worth noting that Aeron subscribers (receivers) must also generally
perform deserialization.  Therefore, subscribers may become CPU bound by the
amount of deserializaton work that needs to be performed. In order to reduce
this effect, we allow multiple subscribers to be created per node (i.e. peer
group). This can be tuned via `:onyx.messaging.aeron/subscriber-count` in
[peer-config](peer-config.md). As increasing the number of subscribers, may
lead back to an undesirable growth in the number of connections between nodes,
each node will only choose one subscription to communicate through. These are
chosen via a hash of the combined IPs of the nodes, in order to consistently
spread the use of subscribers over the cluster.

Clusters which perfom a large proportion of the time serializing should
consider increasing the subscriber count. As a general guide, `# cores = #
virtual peers + # subscribers`.

##### Connection Short Circuiting

When virtual peers are co-located on the same node, messaging will bypass the
use of Aeron and directly communicate the message without any use of the
network and without any serialization. Therefore, performance benchmarks
performed on a single node can be very misleading.

The [peer-config](peer-config) option, `:onyx.messaging/allow-short-circuit?`
is provided for the purposes of more realistic performance testing on a single node.

##### Port Use

The Aeron messaging implementation will use the first port configured via
`:onyx.messaging/peer-port-range` and `:onyx.messaging/peer-ports`. UDP ports
coinciding with these options must be open.

#### Netty Messaging

The Netty messaging implementation does not currently perform connection
multiplexing. The performance/reliability behaviour of a cluster using netty
with many virtual peers is unknown. Future work will allow connections to be
multiplexed as with Aeron. The Netty implementation does not currently perform
any connection short circuiting.

##### Port Use

The netty messaging implementation assigns a port for each virtual peer from
`:onyx.messaging/peer-port-range` and `:onyx.messaging/peer-ports`. TCP ports
coinciding with these options must be open.


####  Core-Async Messaging

Core-async messaging has been provided for local testing. This messaging layer
communicates via core-async channels, and does not perform any networking or
serialization. Note, with the advent of connection short circuiting in the
Aeron implementation, the performance characterstics are now very similar to
Aeron. As such, this layer may be deprecated in the future.
