## Messaging

This design document outlines the new approach that Onyx will take in implementing messaging from 0.6.0 forward.
For a refresher of how Onyx messaging works as of 0.5.0, please refer to the Internal Design section of the User Guide.

### The pros of using a message broker

As of 0.5.0, Onyx uses the HornetQ message broker for communication. There are certainly some benefits to this approach:

- Provides a mechanism for built-in message acknowledgement
- Provides a mechanism for built-in message grouping
- Out of the box load balacing strategies for both the server and the client
- Built-in failover and replication, bugs are HornetQ's problem, not mine

### The cons of using a message broker

- While bugs in HornetQ may be JBoss's problem, they're also sort of mine, because they affect my product
- We write to disk way too much as a result of transactionally moving messages between tasks
- Need to scale the message brokers alongside peers - an operational burden on the developer
- Good fault tolerance requires replication, increasing latency by a constant factor per replica
- Setting up HornetQ is an operational concern by itself
- Configuring HornetQ is hard
- Transactional message movement doesn't matter nearly as much as I thought it might

### An alternate solution

One proposed solution is to copy-cat Apache Storm. Storm is very fast, and uses an in-memory algorithm
to implement fault tolerancy using ordinary HTTP. This boils down to the following large pieces:

- Each peer runs a lightweight HTTP server and client
- We move what it means to "acknowledge" and "replay" a segment to an interface that the input medium implements
- We use multiple, independent "acker server routes" across different peers to manage the success and fail of segments

Let's try a visual description. I'll take you through every piece of this below.

![Summary](images/messaging-summary.png)

There are 5 nodes in this workflow. An input, three functions, and two outputs. Every peer runs an acknowledgment daemon, which runs inside a lightweight webserver. As each message flows into Onyx, it's given a unique ID. The ID is mapped to the segment, and it's held inside the "holding pen" on the input for N seconds. In this example, ID `abcdef` is mapped to a specific segment.

The peer executing the input task then takes the segment and assigns it a random bit pattern. Let's say it starts with bit pattern `837878`. It uses the segment's ID, `abcdef`, and hashes it to a particular peer. In this example, segment `abcdef` hashes to the peer executing `fn 1`. The peer executing the input task sends a message to the peer executing `fn 1` that the input's peer's id is `wxyz`, that the segment's id to report is `abcdef`, and that it should XOR the existing value with the bit patterns `837878`, `23944`, and `993758`. `837878` represents acknowledging the completion of the segment on the input task, and the other two values are new random bit patterns that represent two new segments being created. These bit patterns are repeated when the segments are acked again. The acker daemon receives this message. When it's processed, it checks to see if the result value of XOR'ing is 0. If it is, it sends a message to the holding pen to acknowledge that segment `abcdef` is finished, and should be released.

If N seconds pass and no one acknowledges the message in the holding pen, the `replay` interface function is invoked to try it again (it timed out), and the segment is removed from the holding pen. If the holding pen receives a message to acknowledge the segment, the `ack` interface function is invoked, and this segment gets removed from the holding pen.

I'll next outline the work that needs to be completed to achieve this approach.

#### Messaging infrastructure on each peer

Add the necessary code to each peer when it starts up with component to receive messages. For the first interation, that means booting up an HTTP-Kit server. Also ensure that any code needed to send messages to other peers get the chance to boot up.

#### Ack/replay interface

Create an interface for what it means to "ack" and "replay" a segment for a specific input medium. This used to be handled by HornetQ, but now we're pushing it right into the input medium.

#### Ack/replay for batch storage

Define how to "ack" and "replay" segments for input mediums that don't provide it out of the box (e.g. SQL).

#### Ensure full execution path

Replica logic needs to be redefined to disallow any peer taking a job unless at least 1 peer per task can also join it. Otherwise messages would just get sent to no where and time out, pointlessly.

#### Stall task lifecycle execution

Redefine peer logic to not start a peer lifecycle until it receives confirmation that at least one peer per task is ready. Even though the job may have enough peers, make sure all peers actually decide to take the job before starting task execution. Otherwise segments will be sent to nowhere and timeout.

#### Back off policies

Implement back-off policies for peer's failing sending segments to each other. A peer might temporarily be unavailable, or slow to respond. We'll use core.async channels on the receiving and sending side of a peer to buffer message flow.

#### Create an interface for sending/receiving messages

We want this to be pluggable, so make sure this is behind an interface.

#### Ensure that grouping is undisturbed

Redefine peer logic to *never* add new peers to a grouping task after a job has been started - this would throw off the hash-mod'ing algorithm. If a peer fails, it can continue though. Storm does the same thing and makes sure all messages will be sticky to a new peer.

#### Implement a holding pen

Add an atom to any task lifecycle for an input as a "container-pen" for objects that need to be natively acked. (e.g. real HornetQ ack)

#### Implement replay timer

Add a timer-based job to every input task that releases segments in the "container-pen" that time out and need to be replayed.

#### Implement an Acker daemon

Add an "acker" route on *every* peer web server to perform Onyx-specific message acknowledgment. Make sure this gets booted up alongside the peer.

#### Add acking atom

Add an atom to every peer's acking machinery as a "container-pen" for segments that need to be ack'ed via Onyx with bit-patterns.

#### XOR algorithm

Implement XOR algorithm in peer's acking thread as described above.

#### Implement load balancing

Implement a load balancing algorithm for spreading out messages over a range of peer's for downstream tasks.

### Impleenntation plan

- Add a new component to all peers - BufferChannels.
  - Two channels: inbound & outbound
  - Fixed sized buffers on both, make configurable
- Add `send-message` and `receive-message` to `onyx.extensions`.
  - Implement in folder named `messaging`.
  - Add an `http-kit` file to this folder, implement send/receive message
- Make a map of keyword to function that returns a new Component
  - `:http-kit` -> New HttpKit server
  - Use this for when the peers boots up
  - Make sure the result of this gets passed into the peer task lifecycle
  - Use this for receiving messages
  - Do the same, make a Component for sending messages, make sure it gets the receiver as a parameter in case it needs it
- Strip out all pipelining. I want to start from scratch and tune performance from the ground up
- Add an Acker component to every peer
  - Listens for ack messages, contains an atom as described above
  - This should be part of the interface for any messaging implementations
  - Reuse existing booted up messaging components
- Add a holding pen to all input tasks
  - Add an atom to maintain the holding pen
  - Make sure a future is running to periodically clear the atom out
  - Do the same as above wrt to reusing messaging components, needs to come through the same interface
- Implement HornetQ first, straightforward semantics

### Open questions

- How do peers look each other up?
- Pluggable messaging?
- Talk about how this is different from Storm
- Greedy task scheduler needs to go
- How do we ensure that *each* message is getting N seconds before a replay call?