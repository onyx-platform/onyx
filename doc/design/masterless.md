# The Cluster as a Value

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Context](#context)
- [The good things about a centralized Coordinator](#the-good-things-about-a-centralized-coordinator)
- [The bad things about a centralized Coordinator](#the-bad-things-about-a-centralized-coordinator)
- [Towards a masterless design](#towards-a-masterless-design)
  - [The Log](#the-log)
  - [The Inbox and Outbox](#the-inbox-and-outbox)
  - [Applying Log Entries](#applying-log-entries)
  - [Joining the Cluster](#joining-the-cluster)
    - [3-Phase Cluster Join Strategy](#3-phase-cluster-join-strategy)
    - [Examples](#examples)
  - [Dead peer removal](#dead-peer-removal)
    - [Peer Failure Detection Strategy](#peer-failure-detection-strategy)
    - [Peer Failure Garbage Collection Strategy](#peer-failure-garbage-collection-strategy)
    - [Examples](#examples-1)
  - [Allocating Peers to Jobs and Tasks](#allocating-peers-to-jobs-and-tasks)
    - [Job Schedulers](#job-schedulers)
      - [Greedy Job Scheduler](#greedy-job-scheduler)
      - [Round Robin Job Scheduler](#round-robin-job-scheduler)
      - [Round Robin Rebalancing Strategy](#round-robin-rebalancing-strategy)
      - [Percentage Job Scheduler](#percentage-job-scheduler)
      - [Percentage Rebalancing Strategy](#percentage-rebalancing-strategy)
    - [Task Schedulers](#task-schedulers)
      - [Greedy Task Scheduler](#greedy-task-scheduler)
      - [Round Robin Task Scheduler](#round-robin-task-scheduler)
      - [Percentage Task Scheduler](#percentage-task-scheduler)
    - [Partial Coverage Protection](#partial-coverage-protection)
    - [Examples](#examples-2)
- [Command Reference](#command-reference)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Context

Onyx 0.4.0's design revolves around the notion of a centralized Coordinator. This Coordinator manages distributed state, scheduled jobs, and handles peer birth and failure. This Coordinator uses multiple internal logs with checkpointing to write and process messages, potentionally failing over to stand-by Coordinators. State is kept inside of ZooKeeper.

## The good things about a centralized Coordinator

- Most znodes only have one watch that triggers on change - the Coordinator. Very few reactions need to happen.
- Znode contention is low. Usually only the Coordinator and one peer are accessing znode
- Coordination decisions happen in a single place, and can be easily traced down to a single location in the code executing on a single box

## The bad things about a centralized Coordinator

- More bugs have come up inside the Coordinator than any other part of Onyx
- Multiple implementations of Coordinator are supported in parallel (HTTP, memory)
- Some state in ZooKeeper is mutable, and the Coordinator needs an atomic read of particular znodes
- *Most* writes and reads to ZooKeeper need to have (application/semantic level) serializable consistency - burden on me to do it right
- Burden on the user to stand up a Coordinator and its failover instances
- Only supporting one scheduling algorithm in 0.4.0. It's hard to support more scheduling algorithms
- Task rotation to support peer failure replacement is really hard with the current set up

## Towards a masterless design

An alternate design approach abolishes the Coordinator. The rest of this document describes a design proposal centers around turning Onyx into a masterless system.

### The Log

This design centers around a totally ordered sequence of commands using a log structure. The log acts as an immutable history and arbiter. It's maintained through ZooKeeper using a directory of persistent, sequential znodes. Virtual peers act as processes that consume from the log. At the time a peer starts up, it initializes its *local replica* to the "empty state". Log entries represent deterministic, idempontent functions to be applied to the local replica. The peer plays the log from the beginning, applying each log entry to its local replica. The local replica is transformed from one value to another. As a reaction to each replica transformation, the peer may send more commands to the tail of the log. Peers may play the log at any rate. After each peer has played `k` log entries, all peers will have *exactly* the same local replica. Peers store everything in memory - so if a peer fails, it simply reboots from scratch and plays the log forward from the beginning.

### The Inbox and Outbox

Every virtual peer maintains its own inbox and output. Messages received appear in order on the inbox, and messages to-be-sent are placed in order on the outbox.

Messages arrive in the inbox as commands are proposed into the ZooKeeper log. Technically, the inbox need only be size 1 since all log entries are processed strictly in order. As an optimization, the peer can choose to read a few extra commands behind the one it's current processing - hence the inbox will probably be configured with a size greater than one.

The outbox is used to send commands to the log. Certain commands processed by the peer will generate *other* commands. For example, if a peer is idle and it receives a command notifying it about a new job, the peer will *react* by sending a command to the log that it gets allocated for work. Each peer can choose to *pause* or *resume* the sending of its outbox messages. This is useful when the peer is just acquiring membership to the cluster. It will have to play log commands to join the cluster fully, but it cannot volunteer to be allocated for work.

### Applying Log Entries

This section describes how log entries are applied to the peer's local replica. A log entry is a persistent, sequential znode. It's content is a map with keys `:fn` and `:args`. `:fn` is mapped to a keyword that finds this log entries implementation. `:args` is mapped to another map with any data needed to apply the log entry to the replica.

Peers begin with the empty state value, and local state. Local state maintains a mapping of things like the inbox and outbox. Local state does not contain values.

Each virtual peer starts a thread that listens for additions to the log. When it the log gets a new entry, the peer calls `onyx.extensions/apply-log-entry`. This is a function that takes a log entry and the replica, and returns a new replica with the log entry applied to it. This is a value-to-value transformation.

Peers affect change in the world by reacting to log entries. When a log entry is applied, the peer calls `onyx.extensions/replica-diff`, passing it the old and new replicas. The peer produces a value summarization of what changed. This diff is used in subsequent sections to decide how to react and what side-effects to carry out.

Next, the peer calls `onyx.extensions/reactions` on the old/new replicas, the diff, and it's local state. The peer can decide to submit new entries back to the log as a reaction to the log entry it just saw. It might react to "submit-job" with "volunteer-for-task", for instance.

Finally, the peer can carry out side-effects by invoking `onyx.extensions/fire-side-effects!`. This function will do things like talking to ZooKeeper or writing to core.async channels. Isolating side effects means that a subset of the test suite can operate on pure functions alone.

<img src="/doc/design/images/diagram-1.png" height="75%" width="75%">

*Figure 1: A single peer begins with the empty replica (`{}`) and progressively applies log entries to the replica, advancing its state from one immutable value to the next.*

### Joining the Cluster

Aside from the log structure and any strictly data/storage centric znodes, ZooKeeper will maintain one other directory for pulses. Each virtual peer registers exactly one ephemeral node in the pulses directory. The name of this znode is a UUID.

#### 3-Phase Cluster Join Strategy

When a peer wishes to join the cluster, it must engage in a 3 phase protocol. Three phases are required because the peer that is joining needs to coordinate with another peer to change its ZooKeeper watch. I call this process "stitching" a peer into the cluster.

The technique needs peers to play by the following rules:
  - Every peer must be watched by another peer in ZooKeeper, unless there is exactly one peer in the cluster - in which cases there are no watches.
  - When a peer joins the cluster, all peers must form a "ring" in terms of who-watches-who. This makes failure repair very easy because peers can transitvely close any gaps in the ring after machine failure.
  - As a peer joining the cluster begins playing the log, it must buffer all reactive messages unless otherwise specified. The buffered messages are flushed after the peer has fully joined the cluster. This is because a peer could volunteer to perform work, but later abort its attempt to join the cluster, and therefore not be able to carry out any work.
  - A peer picks another peer to watch by determining a candidate list of peers it can stitch into. This candidate list is sorted by peer ID. The target peer is chosen by taking the message id modulo the number of peers in the sorted candidate list. The peer chosen can't be random because all peers will play the message to select a peer to stitch with, and they must all determine the same peer. Hence, the message modulo piece is a sort of "random seed" trick.

The algorithm works as follows:
- let S = the peer to stitch into the cluster
- S sends a `prepare-join-cluster` command to the log, indicating its peer ID
- S plays the log forward
- Eventually, all peers encounter `prepare-join-cluster` message that was sent by it
- if the cluster size (`n`) is `>= 1`:
  - let Q = this peer playing the log entry
  - let A = the set of all peers in the fully joined in the cluster
  - let X = the single peer paired with no one (case only when `n = 1`)
  - let P = set of all peers prepared to join the cluster
  - let D = set of all peers in A that are depended on by a peer in P
  - let V = sorted vector of `(set-difference (set-union A X) D)` by peer ID
  - if V is empty:
    - S sends an `abort-join-cluster` command to the log
    - when S encounters `abort-join-cluster`, it backs off and tries to join again later
  - let T = nth in V of `message-id mod (count V)`
  - let W = the peer that T watches
  - T adds a watch to S
  - T sends a `notify-join-cluster` command to the log, notifying S that it is watched, adding S to P
  - when S encounters `notify-join-cluster`:
    - it adds a watch to W
    - it sends a `accept-join-cluster` command, removing S from P, adding S to A
  - when `accept-join-cluster` has been encountered, this peer is part of the cluster
  - S flushes its outbox of commands
  - T drops it watch from W - it is now redundant, as S is watching Q
- if the cluster size is `0`:
  - S instantly becomes part of the cluster
  - S flushes its outbox of commands

#### Examples

- [Example 1: 3 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-1.md)
- [Example 2: 3 node cluster, 2 peers successfully join](/doc/design/join-examples/example-2.md)
- [Example 3: 2 node cluster, 1 peer successfully joins, 1 aborts](/doc/design/join-examples/example-3.md)
- [Example 4: 1 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-4.md)
- [Example 5: 0 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-5.md)
- [Example 6: 3 node cluster, 1 peer fails to join due to 1 peer dying during 3-phase join](/doc/design/join-examples/example-6.md)
- [Example 7: 3 node cluster, 1 peer dies while joining](/doc/design/join-examples/example-7.md)

### Dead peer removal

Peers will fail, or be shut down purposefully. Onyx needs to:
- detect the downed peer
- inform all peers that this peer is no longer executing its task
- inform all peers that this peer is no longer part of the cluster

#### Peer Failure Detection Strategy

In a cluster of > 1 peer, when a peer dies another peer will have a watch registered on its znode to detect the ephemeral disconnect. When a peer fails (peer F), the peer watching the failed peer (peer W) needs to inform the cluster about the failure, *and* go watch the node that the failed node was watching (peer Z). The joining strategy that has been outlined forces peers to form a ring. A ring structure has an advantage because there is no coordination or contention as to who must now watch peer Z for failure. Peer W is responsible for watching Z, because W *was* watching F, and F *was* watching Z. Therefore, W transitively closes the ring, and W watches Z. All replicas can deterministicly compute this answer without conferring with each other.

#### Peer Failure Garbage Collection Strategy

It's not always the case that failures can be reported reliably. Consider the following scenario:

- A cluster exists with 3 nodes: A, B, and C
- A watches B, B watches C, and C watches A
- C dies
- B notices that C has died, but before it can send a `leave-cluster` command to the log, B dies.
- A notices that B has died, but before it can send a `leave-cluster` command to the log, A dies.
- Peer D tries to join the cluster, and sees that there are 3 active peers since none of the deaths were reported. D times out, unable to ever join the cluster

This is a pretty nasty edge case. Normally, a failure will case *some* node in the ring to respond and send a command to the log about the death of another peer. This strategy utterly fails in the above sequence. It becomes necessary to take a fallback approach.

When peer P sends a `prepare-join-cluster` command, it eventually encounters the message and realizes it will be stitched in by Q. P adds a watch to Q's pulse node. If Q is no longer alive, P will send a `leave-cluster` command for Q. P will encounter this message, remove Q from the cluster, abort its attempted join, and retry. If Q is alive, P cancels its watch on Q later in the algorithm.

#### Examples

- [Example 1: 4 node cluster, 1 peer crashes](/doc/design/leave-examples/example-1.md)
- [Example 2: 4 node cluster, 2 peers instantaneously crash](/doc/design/leave-examples/example-2.md)

### Allocating Peers to Jobs and Tasks

In a masterless design, there is no single entity that assigns to peers. Instead, peers need to contend for tasks to execute as jobs are submitted to Onyx. Conversely, as peers are added to the cluster, the peers must "shift" make distribute the workload across the cluster. Onyx `0.5.0` will ship new job and task allocation policies. End users will be able to change the levels of fairness that each job gets with respect to cluster power. And remember, one virtual peer executes *at most* one task.

#### Job Schedulers

Each running Onyx instance is configured with exactly one job scheduler. The purpose of the job scheduler is to coordinate which jobs peers are allowed to volunteer to execute. There are a few different kinds of schedulers, listed below. To use each, configure the Peer Options map with key `:onyx.peer/job-scheduler` to the value specified below.

##### Greedy Job Scheduler

The Greedy job scheduler allocates *all peers* to each job in the order that it was submitted. For example, suppose you had 100 virtual peers and you submitted two jobs - job A and job B. With a Greedy scheduler, *all* 100 peers would be allocated to job A. When (if) job A completes, all 100 peers will then execute tasks for job B. This *probably* isn't desirable when you're running streaming workflows, since they theoretically never end.

To use this scheduler, set key `:onyx.peer/job-scheduler` to `:onyx.job-scheduler/greedy` in the Peer Options.

**Peer Addition**

In the event that a peer joins the cluster while the Greedy job scheduler is running, that new peer will be allocated to the current job that is being run greedily.

**Peer Removal**

In the event that a peer leaves the cluster while the Greedy job scheduler is running, no peers will be shifted off of the job that is greedily running.

**Job Addition**

If a job is submitted while this scheduler is running, no peers will be allocated to this job. The only exception to this rule is if *no* jobs are currently running. In this case, all peers will be allocated to this job.

**Job Removal**

If a job is completed or otherwise cancelled, *all* of the peers executed that task will move to the job that was submitted after this job.

##### Round Robin Job Scheduler

The Round Robin job scheduler allocates peers in a rotating fashion to jobs that were submitted. For example, suppose that you had 100 virtual peers (virtual peer 1, virtual peer 2, ... virtual peer 100) and you submitted two jobs - job A and job B. With a Round Robin scheduler, job A would be allocated peer 1, job B would be allocated peer 2, job A would be allocated peer 3, and so on. In the end, both jobs will end up with 50 virtual peers allocated to each. Round robin begins allocating by selecting the first job submitted.

To use this scheduler, set key `:onyx.peer/job-scheduler` to `:onyx.job-scheduler/round-robin` in the Peer Options.

**Peer Addition**

In the event that a peer joins the cluster while the Round Robin scheduler is running, that new peer will be allocated to the job *next* in the round robin sequence. The local replica has a reference to the job ID of the previous job that a peer was allocated for.

**Peer Removal**

In the event that a peer leaves the cluster while the Round Robin scheduler is running, the peers across *all* jobs will be rebalanced to evenly distribute the workflow. At most, one peer will change jobs to rebalance the workload.

**Job Addition**

If a job is submitted while this scheduler is running, the entire cluster will be rebalanced. This will result in *at least one* peer changing jobs to rebalance the cluster. For example, if job A has all 100 peers executing its task, and job B is submitted, 50 peers will move from job A to job B.

**Job Removal**

If a job is completed or otherwise cancelled while this scheduler is running, the entire cluster will be rebalanced. This will result in *at least one* peer changing jobs to rebalance the cluster. For example, if job A, B, and C had 20 peers executing each of its tasks (60 peers total), and job C finished, job A would gain 10 peers, and job B would gain 10 peers.

##### Round Robin Rebalancing Strategy

When a job is added or removed in Onyx, the round robin peer allocation needs to change to ensure that all jobs receive about the same amount of resources. When an even number of virtual peers cannot be distributed across the cluster (8 virtual peers, 3 jobs of 2 tasks each), the *fraction* of peers that is left is allocated round-robin, starting with the oldest job, working forward. Consider the following scenario:

- There are 8 virtual peers running
- There are 2 jobs, A and B, each using 4 virtual peers each
- Job C is submitted
- Onyx must rebalance and allocate 3 peers to A, 3 peers to B, and 2 peers to C

The algorithm works as follows:

- let J be the number of jobs executing
- let P be the number of virtual peers in the cluster
- every job will be allocated *at least* `P / J` virtual peers (integer division)
- let R be the `(remainder of (P / J)) / J`
- let N be the numerator of R
- the first N jobs to be submitted will be allocated exactly `(P / J) + 1` peers
- let K be the original number of peers executing this job
- if `(P / J)` or `(P / J) + 1` (depending on the job) is less than K, this job reassigns `K - (P / J)` or `K - (P / J) + 1)` peers to the new job that was submitted
- exactly which peers are released from a job depends on the Task Scheduler for that job

##### Percentage Job Scheduler

The Percentage job scheduler allows jobs to be submitted with a percentage value. The percentage value indicates what percentage of the cluster will be allocated to this job. The use case for this scheduler is for when you have a static number of jobs and a varying number of peers. For example, if you have 2 jobs - A and B, you'd give each of this percentage values - say 70% and 30%, respectively. If you had 100 virtual peers running, 70 would be allocated to A, and 30 to B. If you then added 100 more peers to the cluster, job A would be allocated 140 peers, and job B 30. This dynamically scaling is a big step forward over statically configuring slots, which is normal in ecosystems like Hadoop.

If there aren't enough peers to satisfy the percentage values of all the jobs, this scheduler will allocate with priority to jobs with the highest percentage value. When percentage values are equal, the earliest submitted job will get priority. In the event that jobs are submitted, and the total percentage value exceeds 100%, the earliest submitted jobs that do not exceed 100% will receive peers. Jobs that go beyond it will not receive peers. For example, if you submitted jobs A, B, and C with 70%, 30%, and 20% respectively, jobs A and B would receive peers, and C will not be allocated any peers until either A or B completes.

If the total percentages of all submitted jobs doesn't sum up to 100%, the job with the highest percentage value will receive the extra peers. When percentage values are equal, the earliest submitted job will get priority.

If the algorithm determines that any job should receive a number of peers that is less than 1 (a decimal value), that job receives no peers. This value is floored, and is described in more detail below.

This scheduler does not compose with using `:onyx/max-peers` set on all tasks. The strict upper bound on the number of peers will be respected.

To use this scheduler, set key `:onyx.peer/job-scheduler` to `:onyx.job-scheduler/percentage` in the Peer Options.

**Peer Addition**

In the event that a peer joins the cluster while the Percentage job scheduler is running, the entire cluster will rebalance.

**Peer Removal**

In the event that a peer leaves the cluster while the Percentage job scheduler is running, the entire cluster will rebalance.

**Job Addition**

If a job is submitted while this scheduler is running, the entire cluster will be rebalanced.

**Job Removal**

If a job is completed or otherwise cancelled while this scheduler is running, the entire cluster will be rebalanced.

##### Percentage Rebalancing Strategy

When a job or peer are added or removed, the Percentage job scheduler needs to dynamically adjust which peers are allocated to which jobs. 

The algorithm works as follows:

- let P be the number of virtual peers in the cluster
- let S be the jobs sorted from highest to lowest by percentage, subsorted by submit time, earliest first
- let J be the first n jobs in S who's percentage values do not exceed 100%
- For each job in J, in order:
  - let this job be J'
  - J' will be allocated *at least* (floor (P * J's % value))
- let X be the sum of all allocations for all J' values
- Allocate P - X *more* peers to the first job in J

And now, for the reassignment phase:
- for each job J' in J
- let K be the original number of peers executing this job at current
- if the current number of peers executing this job exceeds the new assigned amount, this job reassigns the difference to another job
- exactly which peers are released from a job depends on the Task Scheduler for that job

#### Task Schedulers

Each Onyx job is configured with exactly one task scheduler. The task scheduler is specified at the time of calling `submit-job`. The purpose of the task scheduler is to control the order in which available peers are allocated to which tasks. There are a few different Task Scheduler implementations, listed below. To use each, call `onyx.api/submit-job`. The second argument of this function is a map. Supply a `:task-scheduler` key and map it to the value specified below.

##### Greedy Task Scheduler

The Greedy Task Scheduler takes a topological sort of the workflow for a specific job. As peers become available, this scheduler always assigns the next peer to the *earliest* task in the sorted workflow that is not complete. For example, if a workflow has a topological sort of tasks A, B, C, and D, this scheduler assigns each peer to task A.

To use, set `:task-scheduler` in `submit-job` to `:onyx.task-scheduler/greedy`.

**Task Completion**

When a task is complete, this scheduler moves all peers executing that task to the next task in the topologically sorted workflow. If there are no more tasks, these peers are elligible to execute tasks for another job. For example, if a workflow has a topological sort of tasks A, B, C, and D, and task A completes, this scheduler assigns all peers *from the completed task* to task B.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler defers to the Job scheduler to rebalance the cluster. If a new peer is added to this task as a result of a peer failing in another job, it is added to the currently executing task that all peers are greedily assigned to.

##### Round Robin Task Scheduler

The Round Robin Scheduler takes a topological sort of the workflow for a specific job. As peers become available, this scheduler assigns tasks to peers in a rotating order. For example, if a workflow has a topological sort of tasks A, B, C, and D, this scheduler assigns each peer to tasks A, B, C, D, A, B, C, D, ... and so on.

To use, set `:task-scheduler` in `submit-job` to `:onyx.task-scheduler/round-robin`.

**Task Completion**

When a task is complete, this scheduler moves all peers executing that task and round robin assigns them, starting with the first incomplete task. the workflow for a specific job. As peers become available, this scheduler assigns tasks to peers in a rotating order. For example, if a workflow has a topological sort of tasks A, B, C, and D, and task A completes, this scheduler assigns each peer *from the completed task* to tasks B, C, D, B, C, D, ... and so on.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler defers to the Job scheduler to rebalance the cluster. If a new peer is added to this task as a result of a peer failing in another job, it is assigned the next task in the round robin sequence.

**Max Peer Parameter**

With the Round Robin Task Scheduler, each entry in the catalog can specify a key of `:onyx/max-peers` with an integer value > 0. When this key is set, Onyx will never assign that task more than that number of peers. Round Robin will simply skip the task for allocation when more peers are available, and continue assigning round robin to other tasks.

##### Percentage Task Scheduler

The Percentage Scheduler takes a set of tasks, all of which must be assigned a percentage value (`:onyx/percentage`) in the corresponding catalog entries. The percentage values *must* add up to 100 or less. Percent values may be integers between 1 and 99, inclusive. This schedule will allocate peers for this job in proportions to the specified tasks. As more or less peers join the cluster, allocations will automatically scale. For example, if a job has tasks A, B, and C with 70%, 20%, and 30% specified as their percentages, and there are 10 peers, task A receives 7 peers, B 2 peers, and C 1 peer.

This scheduler handles corner cases (fractions of peers) in the same way as the Percentage Job Scheduler. See that documentation for a full description.

To use, set `:task-scheduler` in `submit-job` to `:onyx.task-scheduler/percentage`.

**Task Completion**

When a task is complete, this scheduler moves all peers executing and adds them all to the next incomplete task with the largest percentage.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler rebalances all the peers for even distribution.

**Max Peer Parameter**

This task scheduler is *not* composable with `:onyx/max-peers`.

#### Partial Coverage Protection

Onyx is a batch and streaming hybrid data processing framework. Sometimes, you want Onyx to behave quite differently depending on whether your workload is batch or streaming oriented. In this section, we introduce the concept of Partial Coverage Protection.

When your workload is a batch job, it is possible to make progress on a job with 3 tasks using a single virtual peer. Each task *will eventually* complete, and that single peer can move from one task to another and finish the job. When your job is streaming oriented, a single peer is insufficient *to ever* produce results for a job with 3 tasks. Since no tasks *ever* complete, that peer will never move to work on other tasks. This is obviously not desirable.

Partial Coverage Protection is an option that can be enabled at the time of `submit-job`. When Partial Coverage Protection is enabled, Onyx will not assign *any* peers to a job of N tasks unless *at least* N peers *may* volunteer to execute tasks for that job. If a peer fails during the time that the job is executing and the total number of peers executing that job becomes less than N, *all peers* executing for that job will stop and go seek a new job. The scheduler will behave as if this job has been removed. When another peer joins the cluster, and this job now has enough peers to execute its tasks, the scheduler will behave as if this is a new job and assign work to it in either Greedy or Round Robin order (depending on which is enabled).

#### Examples

- [Example 1: 3 node cluster, 1 job, Greedy job scheduler, Greedy task scheduler](/doc/design/allocate-examples/example-1.md)
- [Example 2: 3 node cluster, 1 job, Greedy job scheduler, Round Robin task scheduler](/doc/design/allocate-examples/example-2.md)
- [Example 3: 7 node cluster, 2 jobs, Round Robin job scheduler, Greedy task schedulers](/doc/design/allocate-examples/example-3.md)
- [Example 4: 7 node cluster, Round Robin job scheduler, Greedy task schedulers, 2 job cluster shift](/doc/design/allocate-examples/example-4.md)

## Command Reference

-------------------------------------------------
[`prepare-join-cluster`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/prepare_join_cluster.clj)

- Submitter: peer (P) that wants to join the cluster
- Purpose: determines which peer (Q) that will watch P. If P is the only peer, it instantly fully joins the cluster
- Arguments: P's ID
- Replica update: assoc `{Q P}` to `:prepare` key. If P is the only P, P is immediately added to the `:peers` key, and no further reactions are taken
- Side effects: Q adds a ZooKeeper watch to P's pulse node
- Reactions: Q sends `notify-join-cluster` to the log, with args P and R (R being the peer Q watches currently)

-------------------------------------------------
[`notify-join-cluster`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/notify_join_cluster.clj)

- Submitter: peer Q helping to stitch peer P into the cluster
- Purpose: Add's a watch from P to R, where R is the node watched by Q
- Arguments: P and R's ids
- Replica update: assoc `{Q P}` to `:accept` key, dissoc `{Q P}` from `:prepare` key
- Side effects: P adds a ZooKeeper watch to R's pulse node
- Reactions: P sends `accept-join-cluster` to the log, with args P, Q, and R

-------------------------------------------------
[`accept-join-cluster`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/accept_join_cluster.clj)

- Submitter: peer P wants to join the cluster
- Purpose: confirms that P can safely join, Q can drop its watch from R, since P now watches R, and Q watches P
- Arguments: P, Q, and R's ids
- Replica update: dissoc `{Q P}` from `:accept` key, merge `{Q P}` and `{P R}` into `:pairs` key, conj P onto the `:peers` key
- Side effects: Q drops its ZooKeeper watch from R
- Reactions: peer P flushes its outbox of messages

-------------------------------------------------
[`abort-join-cluster`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/abort_join_cluster.clj)

- Submitter: peer (Q) determines that peer (P) cannot join the cluster (P may = Q)
- Purpose: Aborts P's attempt at joining the cluster, erases attempt from replica
- Arguments: P's id
- Replica update: Remove any `:prepared` or `:accepted` entries where P is a key's value
- Side effects: P optionally backs off for a period
- Reactions: P optionally sends `:prepare-join-cluster` to the log and tries again

-------------------------------------------------
[`leave-cluster`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/leave_cluster.clj)

- Submitter: peer (Q) reporting that peer P is dead
- Purpose: removes P from `:prepared`, `:accepted`, `:pairs`, and/or `:peers`, transitions Q's watch to R (the node P watches) and transitively closes the ring
- Arguments: peer ID of P
- Replica update: assoc `{Q R}` into the `:pairs` key, dissoc `{P R}`
- Side effects: Q adds a ZooKeeper watch to R's pulse node

-------------------------------------------------
[`volunteer-for-task`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/volunteer_for_task.clj)

- Submitter: peer (P) that wants to execute a new task
- Purpose: P is possibly available to execute a new task because a new job was submitted, or the cluster size changed - depending on the job and task schedulers
- Arguments: peer ID of P
- Replica update: Updates `:allocations` with peer ID under the chosen job and task, if any. Switches `:peer-state` for this peer to `:active` if a task is chosen
- Side effects: Stop the current task lifecycle, if one is running. Starts a new task lifecycle for the chosen task
- Reactions: None

-------------------------------------------------
[`seal-task`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/seal_task.clj)

- Submitter: peer (P), who has seen the leader sentinel
- Purpose: P wants to propagate the sentinel to all downstream tasks
- Arguments: P's ID (`:id`), the job ID (`:job`), and the task ID (`:task`)
- Replica update: If this peer is allowed to seal, updates `:sealing-task` with the task ID associated this peers ID.

<<< Needs more code, docs will be incorrect is transcribed now >>>

- Side effects:
- Reactions: None

-------------------------------------------------
[`complete-task`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/complete_task.clj)

- Submitter: peer (P), who has successfully sealed the task
- Purpose: Indicates to the replica that all downstream tasks have received the sentinel, so this task can be marked complete
- Arguments: P's ID (`:id`), the job ID (`:job`), and the task ID (`:task`)
- Replica update: Updates `:completions` to associate job ID to a vector of task ID that have been completed. Removes all peers under `:allocations` for this task. Sets `:peer-state` for all peers executing this task to `:idle`
- Side effects: Stops this task lifecycle
- Reactions: Any peer executing this task reacts with `:volunteer-for-task`

-------------------------------------------------
[`submit-job`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/submit_job.clj)

- Submitter: Client, via public facing API
- Purpose: Send a catalog and workflow to be scheduled for execution by the cluster
- Arguments: The job ID (`:id`), the task scheduler for this job (`:task-scheduler`), a topologically sorted sequence of tasks (`:tasks`), the catalog (`:catalog`), and the saturation level for this job (`:saturation`). Saturation denotes the number of peers this job can use, at most. This is typically Infinity, unless all catalog entries set `:onyx/max-peers` to an integer value. Saturation is then the sum of those numbers, since it creates an upper bound on the total number of peers that can be allocated to this task.
- Replica update: 
- Side effects: None
- Reactions: If the job scheduler dictates that this peer should be reallocated to this job or another job, sends `:volunteer-for-task` to the log

-------------------------------------------------
[`kill-job`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/kill_job.clj)

- Submitter: Client, via public facing API
- Purpose: Stop all peers currently working on this job, and never allow this job's tasks to be scheduled for execution again
- Arguments: The job ID (`:job`)
- Replica update: Adds this job id to `:killed-jobs` vector, removes any peers in `:allocations` for this job's tasks. Switches the `:peer-state` for all peer's executing a task for this job to `:idle`.
- Side effects: If this peer is executing a task for this job, stops the current task lifecycle
- Reactions: If this peer is executing a task for this job, reacts with `:volunteer-for-task`

-------------------------------------------------
[`gc`](https://github.com/MichaelDrogalis/onyx/blob/0.5.x/src/onyx/log/commands/gc.clj)

- Submitter: Client, via public facing API
- Purpose: Compress all peer local replicas and trim old log entries in ZooKeeper.
- Arguments: The caller ID (`:id`)
- Replica update: Clears out all data in all keys about completed and killed jobs - as if they never existed.
- Side effects: Deletes all log entries before this command's entry, creates a compressed replica at a special origin log location, and updates to the pointer to the origin
- Reactions: None

-------------------------------------------------
