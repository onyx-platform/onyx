# The Cluster as a Value

## Context

Onyx 0.4.0's design revolves around the notion of a centralized Coordinator. This Coordinator manages distributed state, scheduled jobs, and handles peer birth and failure. This Coordinator uses an internal log with checkpointing to write and process messages, potentionally failing over to stand-by Coordinators. State is kept inside of ZooKeeper.

### The good things about a centralized Coordinator

- Most znodes only have one watch that triggers on change - the Coordinators
- Znode contention is low. Usually only the Coordinator and one peer are accessing znode
- Coordination decisions happen in a single place, moderately easier to reason about

### The bad things about a centralized Coordinator

- Most bugs have come up inside the Coordinator than any other part of Onyx
- Multiple implementations of Coordinator are supported in parallel (HTTP, memory)
- Some state in ZooKeeper is mutable, and the Coordinator needs an atomic read of particular znodes
- Writes and some reads to ZooKeeper need to be serialized - burden on me to do it right
- Burden on the user to stand up a Coordinator and its failover instances
- Only supporting one scheduling algorithm in 0.4.0
- Task rotation to support peer failure replacement is really hard with the current set up

### Towards a masterless design

An alternate design approach abolishes the Coordinator. This design proposal centers around the following ideas:

- ZooKeeper maintains a totally ordered log-structure of command proposals. This is a history and arbiter.
- All peers consume all commands from the log, starting from the beginning.
- Each peer maintains *local state*. Local state begins with the empty value. Each command execution may modify the local state (via an idempotent function) and generate one or more commands to submit back to the log.
- All peers that have executed `k` logs entries will have *exactly* the same local state.
- Peers contend for certain commands (volunteering to execute a task) by submitting a *proposal* command to the log. The totally ordering of the log acts as an arbiter as to whether the proposal should be accepted or rejected.
- Non-reactive commands (submitting a job, registering a new peer) are done through a client API, and are submitted to the log.

This design proposal raises the following questions and concerns:

- How are peers detected as dead and removed from their executing tasks?
- Contention sky-rockets under scenarios where all peers can submit proposal commands when reacting to a command.
- How do proposals actually work?
- What happens when the log is huge and a new peer joins? It needs to replay a *lot* of commands, and react (submitting proposals) in vain to everything it sees.
- How are situations handled when a peer wins the proposal, but never confirms its success?

#### Joining the cluster

Aside from the log structure, ZooKeeper will maintain one other directory for heartbeats. Each virtual peer registers exactly one ephemeral node in the heartbeats directory. The name of this znode is a UUID.

##### 2-Phase Cluster Join Strategy

When a peer wishes to join the cluster, it must engage in a 2 phase protocol. Two phases are required because the peer that is joining needs to coordinate with another peer to change its ZooKeeper watch.

The technique needs peers to play by the following rules:
  - Every peer must be watched by another peer in ZooKeeper, unless there is exactly one peer in the cluster.
  - When a peer joins the cluster, all peers must form a "ring". This makes failure repair very easy because peer's can transitvely close any gaps in the ring after machine failure.
  - As a peer joining the cluster begins playing the log, it must buffer all reactive messages unless otherwise specified. The buffered messages are flushed after the peer has fully joined the cluster. This is because a peer could volunteer to perform work, but later abort its attempt to join the cluster.
  - A peer picks another peer to watch by selecting a candidate group. This candidate group is sorted by peer ID. The target peer is chosen by taking the message id modulo the number of peers in the sorted candidate list.

The algorithm works as follows ("it" refers to the joining peer):
- it sends a `prepare-join-cluster` command to the log, indicating its peer ID and heartbeat znode
- it plays the log forward until it encounters the `prepare-join-cluster` message that it sent
- if the cluster size (`n`) is `>= 1`:
  - let Q = this peer
  - let A = the set of all peers in the fully joined in the cluster
  - let X = the single peer paired with no one (case only when n = 1)
  - let P = set of all peers prepared to join the cluster
  - let D = set of all nodes in A that are depended on by a node in P
  - let V = sorted vector of `(set-difference (set-union A X) D)` by peer ID
  - if V is empty:
    - it sends an `abort-join-cluster` command to the log
    - when it encounters `abort-join-cluster`, it backs off and tries to join again later
  - let T = nth in V of `message-id mod (count V)`
  - let W = the nodes that depend on T
  - it adds a watch to peer T
  - it sends a `notify-watchers` command to the log, notifying nodes in W, adding itself to P
  - for all nodes in W that encounter `notify-watchers`:
    - it adds a watch to Q
    - it removes its watch from T
    - it sends a `accept-join-cluster` command, removing Q from P, adding P to A
  - when `accept-join-cluster` has been encountered, this node is part of the cluster
  - it flushes its outbox of commands
- if the cluster size is `0`:
  - this node instantly becomes part of the cluster
  - it flushes its outbox of commands

##### Examples

- [Example 1: 3 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-1.md)
- [Example 2: 3 node cluster, 2 peers successfully join](/doc/design/join-examples/example-2.md)
- [Example 3: 2 node cluster, 1 peer successfully joins, 1 aborts](/doc/design/join-examples/example-3.md)
- [Example 4: 1 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-4.md)
- [Example 5: 0 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-5.md)
- [Example 6: 3 node cluster, 1 peer fails to join due to 1 peer dying during 2-phase join](/doc/design/join-examples/example-6.md)
- [Example 7: 3 node cluster, 1 peer dies while joining](/doc/design/join-examples/example-7.md)

##### Relevant Commands

-------------------------------------------------
`prepare-join-cluster`

-------------------------------------------------
`abort-join-cluster`

-------------------------------------------------
`notify-watchers`

-------------------------------------------------
`accept-join-cluster`

- Takes 2 args: pair to move from prepared to pairs, and pair to update in pairs.

-------------------------------------------------
`leave-cluster`

-------------------------------------------------

More questions:
- If a lot of commands have been buffered up, will there be negative affects by appending a huge number of commands to the log when the buffer is flushed?
- What about the very first peer to join the cluster? No one can watch it
- What happens if it buffers a huge number of messages?
- Talk about transitive closures during failure
- What happens if a peer tries to join and dies after prepare?

#### Dead peer removal

Peers will fail, or be shut down purposefully. Onyx needs to:
- detect the downed peer
- inform all peers that this peer is no longer executing its task
- inform all peers that this peer is no longer part of the cluster

##### Peer Failure Detection Strategy

In a cluster of > 1 peer, when a peer dies another peer will have a watch registered on its znode to detect the ephemeral disconnect. When a peer fails (peer F), the peer watching the failed peer (peer W) needs to inform the cluster about the failure, *and* go watch the node that the failed node was watching (peer Z). The joining strategy that has been outlined forces peers to form a ring. A ring structure is advantage because there is no coordination or contention as to who must now watch peer Z for failure. Peer W is responsible for watching Z, because W *was* watching F, and F *was* watching Z. Therefore, W transitively closes the ring, and W watches Z. All replicas can deterministicly compute this answer.

##### Peer Failure Garbage Collection Strategy

It's not always the case that failures can be reported reliably. Consider the following scenario:

- A cluster exists with 3 nodes: A, B, and C
- A watches B, B watches C, and C watches A
- C dies
- B notices that C has died, but before it can send a `leave-cluster` command to the log, B dies.
- A notices that B has died, but before it can send a `leave-cluster` command to the log, A dies.
- Peer D tries to join the cluster, and sees that there are 3 active peers since none of the deaths were reported. D times out, unable to ever join the cluster

This is a pretty nasty edge case. Normally, a failure will case *some* node in the ring to respond and send a command to the log about the death of another peer. This strategy utterly fails in the above sequence. It becomes necessary to take a fallback approach.

We start by observing that this scenario is only a problem when:
- all peers die in this particular fashion (no death reports)
- a new peer arrives

The strategy outlined below will pessimistically garbage collect peers as it joins the cluster:

- Peer Z wants to join the cluster
- Peers A, B, and C are fully joined in the cluster, but actually all dead due to the above scenario
- Peer Z begins by sending a `peer-gc` command to the log
- Peer Z plays the log forward
- Peer Z encounters its `peer-gc` command
- Peer Z takes the set of nodes in the cluster and checks if their heartbeat nodes are still online
- Peer Z sends a `leave-cluster` command to the log for every peer who's heartbeat node is disconnected
- Peer Z continues to play the log, and eventually runs into the case of it being the only peer in the cluster. It is then fully joined

##### Example 1: 4 node cluster, 1 peer crashes

- Nodes A, B, C, and D fully in the cluster
- Local state before (1): `{:pairs {:a :b, :b :c, :c :d, :d :a}}`

- A, B, C, and D play the log
- B dies, A sends `leave-cluster` (1)

- A encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - adds watch to C

- C and D encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - ignores

##### Example 2: 4 node cluster, 2 peers instantaneously crash

- Nodes A, B, C, and D fully in the cluster
- Local state before (1): `{:pairs {:a :b, :b :c, :c :d, :d :a}}`

- A, B, C, and D play the log
- B and C at the exact same time, A sends `leave-cluster` (1)

- A encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - notices C is also dead
  - sends `leave-cluster` (2)

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - ignores

- A encounters (2)
  - Pre: `{:pairs {:a :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :d, :d :a}}`
  - determines A will transitively close the gap and watch D
  - adds watch to D

- D encounters (2)
  - Pre: `{:pairs {:a :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :d, :d :a}}`
  - determines A will transitively close the gap and watch D
  - ignore

##### Example 3: 3 node cluster, 1 joins with garbage collection                                                                                            

- Nodes A, B, and C fully in the cluster
- All peers are dead, none could report peer death
- D sends `peer-gc` (1)
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- D plays the log

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - D notices that A, B, and C are all dead
  - D sends `leave-cluster` (2), `leave-cluster` (3), and `leave-cluster` (4)
  - D sends `prepare-join-cluster` (5)

- D encounters (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - Command for B's death. A transitively fills the hole

- D encounters (3)
  - Pre: `{:pairs {:a :c, :c :a}}`
  - Post: `{:pairs {}}`
  - Command for A's death. C becomes the lone cluster member

- D encounters (4)
  - Pre: `{:pairs {}}`
  - Post: `{:pairs {}}`
  - Command for C's death. The cluster is now empty

- D encounters (5)
  - Pre: `{:pairs {}}`
  - Post: `{:pairs {}}`
  - D is the only node in the cluster. Promotes itself to full status
  - D flushes its outbox

##### Example 4: 3 node cluster, 1 peer lags during GC                                                                                                      

- Nodes A, B, and C fully in the cluster
- D sends `peer-gc` (1)
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- A, B, C, and D play the log
- B dies, A is exceptionally slow to react and does not yet send a command to the log

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - D notices that B is dead
  - D sends `leave-cluster` (2)
  - *A wakes up and sends `leave-cluster` (3) for B*
  - D sends `prepare-join-cluster` (4)

- C encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - ignores, D initiated GC

- C and D encounter (2):
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - determines A will close the gap and watch C

- A encounters (1):
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - ignores, D initiated GC

- A encounters (2):
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - determines A will close the gap and watch C
  - adds watch to C

- A, C, and D encounter (3):
  - Pre: `{:pairs {:a :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - idempontently ignores message

- (4) continues as normal

##### Relevant Commands

-------------------------------------------------
`peer-gc`

-------------------------------------------------

### New functionality

This design enables a few things that I want to add to the API:

- Alternate schedulers
- Dynamic task reassignment
- Percentage task allocation (e.g. 75% of the cluster on task A)
- `kill-job` API

### Formal verification

?

### Open questions
- How am I going to handle `onyx-id`?

