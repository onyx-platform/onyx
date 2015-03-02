## Scratch pad for acking ideas

This is a brain dump on my ideas for tackling fast message acknowledgment.

- Fast messaging relies on prestablishing sockets before messaging takes place
  - Do the handshake just once
  - The HTTP prototype got to skip this constraint, but going fast with websockets and Aeron means that it needs to be addressed

- One option to handle this is to open a socket connection to every other peer in the cluster
  - Really starts to become too many sockets at scale
  - This is why Storm makes you configure the number of ackers to an integer value

- Storm's approach has a few flaws
  - Can't control which node becomes the acker
    - Input nodes are already getting hammered by the acker nodes, would prefer not to double up the load
  - Percentage values are better to elastically scale than integer numbers

- Can ackers be pulled out into their own service?
- Could use the log to mark acker values, but writing to disk is a lot slower than a network call, and will eat up disk space very fast.

### Solution 1 - Configurable Ackers

This solution relies on the user setting and tuning a value that indicates what percentage of peers will act as ackers.

- `submit-job` now takes a few more optional parameters that have defaults
  - `:acker/percentage`, mapped to an integer between `1` and `100`. This indicates the percentage of peers *executing this job* that will act as ackers. The number of peers as ackers are rounded up to the nearest whole number, and must at least be 1. This number will fluctuate at runtime with as peers join and leave the job. Default is `10`.
  - `:acker/exempt-input-tasks`, mapped to a boolean. Any peer executing an `:input` task is not elligible to be an acker. This is useful is the amount of network IO that this peer is doing is already too high to be useful as an acker. Default is `false`.
  - `:acker/exempt-output-tasks`, mapped to a boolean. Any peer executing an `:output` task is not elliginle to be an acker. Default is `false`.
  - `:acker/exempt-tasks`, mapped to a vector of keywords that represent task names. Any peer executing one of these task names is not elligble to be an acker. Default is `[]`.

#### Who becomes an acker?

- The first peers that *can* become ackers *will* become ackers via `volunteer-for-task`.
- If a peer dies, the earliest peer (topologically sorted by tasks) that isn't an acker will become an acker.
- If the number of ackers ever shrinks to zero, stop the job.

#### Acker service

The Acker service is always up for every peer, but it doesn't receive load unless other peers know about it. It should drop messages that it is wrongly ssent, or if it's overloaded. The segment will be replayed from the origin task if a message is dropped.

#### Volunteering for a task

Now needs to ask...

- Are there enough ackers? If yes, proceed as normal
- If no, am I elligable to be an acker?
  - If no, proceed as normal
  - If yes, add myself to the `:ackers` keyed under the job ID.

- When pinning a segment to an acker ID, only pick from `:ackers` in the replica.

#### Starting a job

In addition to checking if the job is covered, ensure that there is at least one acker. Start the job if there's at least one acker, and presume more virtual peers will join the job later if needed.

#### Killing a peer

If a peer goes down, pick the *latest* acker that can be relieved and remove it's key from `:ackers`.

#### Adding a peer

If a new peer gets added, and percentage-wise a new acker needs to be stood up, add its key to `:ackers`.

#### Handshake

Most protocols require an initial handshake. Per task lifecycle, maintain an atom with a map of peer ID to connection. If there's no connection, make one and store it. This lets us lazily acquire connections. We trade off predicatable latency for an easier to understand design. We lose the predicatability, because each time a peer is first contacted we lose some time establishing the connection. I think this is acceptable, and can be deemed a "warm up" period for the cluster, which is a pretty normal thing.

### Solution 2 - Eventual Consistency

Half baked idea. Store up acking bitsets for a period of time and eventually merge them back to the origin input node.

### Solution 3 - Gossip Protocol

Half baked idea. Trade increased number of messages for increased cluster capacity. If a peer can't directly route a message to another peer, find one that can and ask it to send it on your behalf.

