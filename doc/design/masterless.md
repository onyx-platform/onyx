# The Cluster as a Value

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Context](#context)
- [The good things about a centralized Coordinator](#the-good-things-about-a-centralized-coordinator)
- [The bad things about a centralized Coordinator](#the-bad-things-about-a-centralized-coordinator)
- [Towards a masterless design](#towards-a-masterless-design)
  - [Joining the cluster](#joining-the-cluster)
    - [2-Phase Cluster Join Strategy](#2-phase-cluster-join-strategy)
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
    - [Task Schedulers](#task-schedulers)
      - [Greedy Task Scheduler](#greedy-task-scheduler)
      - [Round Robin Task Scheduler](#round-robin-task-scheduler)
    - [Partial Coverage Protection](#partial-coverage-protection)
- [Command Reference](#command-reference)
- [New functionality](#new-functionality)
- [Formal verification](#formal-verification)

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

An alternate design approach abolishes the Coordinator. This design proposal centers around the following ideas:

- ZooKeeper maintains a totally ordered log-structure of command proposals. This is a history and arbiter.
- All peers consume all commands from the log at any rate, starting from the beginning.
- Each peer maintains a local replica of the global state. The replica begins with the empty value. Each command execution may modify the local state (via a deterministic, idempotent function) and generate one or more commands to submit back to the log as a reaction.
- All peers that have executed `k` logs entries will have *exactly* the same local replica. No exceptions, *ever*.
- Peers contend for certain commands (say, volunteering to execute a task that can have at most one peer executing it) by submitting a *proposal* command to the log. The totally ordering of the log acts as an arbiter to settle whether the proposal should be accepted or rejected.
- Non-reactive commands (submitting a job, registering a new peer) are done through a client API, and are submitted to the log.

With no single node in the cluster in charge of all others, this raises the following questions and concerns:

- How are peers detected as dead and removed from their executing tasks?
- Contention sky-rockets under scenarios where all peers can submit proposal commands when reacting to a command. How can we mitigate that?
- How do proposals actually work when there is semantic contention?
- What happens when the log is huge and a new peer joins? Since a peer needs to play the log sequentially, from the beginning, it needs to replay a *lot* of commands, and react (submitting proposals) in vain to most everything it sees.
- How are situations handled when a peer wins the proposal, but dies and never confirms its success?

Below is an outline to implementing a fully masterless design in Onyx that mitigates the above concerns.

### Joining the cluster

Aside from the log structure, ZooKeeper will maintain one other directory for pulses. Each virtual peer registers exactly one ephemeral node in the pulses directory. The name of this znode is a UUID.

#### 2-Phase Cluster Join Strategy

When a peer wishes to join the cluster, it must engage in a 2 phase protocol. Two phases are required because the peer that is joining needs to coordinate with another peer to change its ZooKeeper watch.

The technique needs peers to play by the following rules:
  - Every peer must be watched by another peer in ZooKeeper, unless there is exactly one peer in the cluster.
  - When a peer joins the cluster, all peers must form a "ring". This makes failure repair very easy because peer's can transitvely close any gaps in the ring after machine failure.
  - As a peer joining the cluster begins playing the log, it must buffer all reactive messages unless otherwise specified. The buffered messages are flushed after the peer has fully joined the cluster. This is because a peer could volunteer to perform work, but later abort its attempt to join the cluster.
  - A peer picks another peer to watch by selecting a candidate group. This candidate group is sorted by peer ID. The target peer is chosen by taking the message id modulo the number of peers in the sorted candidate list.

The algorithm works as follows ("it" refers to the joining peer):
- it sends a `prepare-join-cluster` command to the log, indicating its peer ID and pulse znode
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

#### Examples

- [Example 1: 3 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-1.md)
- [Example 2: 3 node cluster, 2 peers successfully join](/doc/design/join-examples/example-2.md)
- [Example 3: 2 node cluster, 1 peer successfully joins, 1 aborts](/doc/design/join-examples/example-3.md)
- [Example 4: 1 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-4.md)
- [Example 5: 0 node cluster, 1 peer successfully joins](/doc/design/join-examples/example-5.md)
- [Example 6: 3 node cluster, 1 peer fails to join due to 1 peer dying during 2-phase join](/doc/design/join-examples/example-6.md)
- [Example 7: 3 node cluster, 1 peer dies while joining](/doc/design/join-examples/example-7.md)

More questions:
- If a lot of commands have been buffered up, will there be negative affects by appending a huge number of commands to the log when the buffer is flushed?
- What about a node's memory if the outbox buffers a huge number of messages?

### Dead peer removal

Peers will fail, or be shut down purposefully. Onyx needs to:
- detect the downed peer
- inform all peers that this peer is no longer executing its task
- inform all peers that this peer is no longer part of the cluster

#### Peer Failure Detection Strategy

In a cluster of > 1 peer, when a peer dies another peer will have a watch registered on its znode to detect the ephemeral disconnect. When a peer fails (peer F), the peer watching the failed peer (peer W) needs to inform the cluster about the failure, *and* go watch the node that the failed node was watching (peer Z). The joining strategy that has been outlined forces peers to form a ring. A ring structure is advantage because there is no coordination or contention as to who must now watch peer Z for failure. Peer W is responsible for watching Z, because W *was* watching F, and F *was* watching Z. Therefore, W transitively closes the ring, and W watches Z. All replicas can deterministicly compute this answer.

#### Peer Failure Garbage Collection Strategy

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
- Peer Z takes the set of nodes in the cluster and checks if their pulse nodes are still online
- Peer Z sends a `leave-cluster` command to the log for every peer who's pulse node is disconnected
- Peer Z continues to play the log, and eventually runs into the case of it being the only peer in the cluster. It is then fully joined

#### Examples

- [Example 1: 4 node cluster, 1 peer crashes](/doc/design/leave-examples/example-1.md)
- [Example 2: 4 node cluster, 2 peers instantaneously crash](/doc/design/leave-examples/example-2.md)
- [Example 3: 3 node cluster, 1 joins with garbage collection](/doc/design/leave-examples/example-3.md)
- [Example 4: 3 node cluster, 1 peer lags during GC](/doc/design/leave-examples/example-4.md)

### Allocating Peers to Jobs and Tasks

In a masterless design, there is no single entity that assigns to peers. Instead, peers need to contend for tasks to execute as jobs are submitted to Onyx. Conversely, as peers are added to the cluster, the peers must "shift" make distribute the workload across the cluster. Onyx `0.5.0` will ship new job and task allocation policies. End users will be able to change the levels of fairness that each job gets with respect to cluster power. And remember, one virtual peer executes *at most* one task.

#### Job Schedulers

Each running Onyx instance is configured with exactly one job scheduler. The purpose of the job scheduler is to coordinate which jobs peers are allowed to volunteer to execute. There are currently two kinds of schedulers - Greedy and Round Robin.

##### Greedy Job Scheduler

The Greedy job scheduler allocates *all peers* to each job in the order that it was submitted. For example, suppose you had 100 virtual peers and you submitted two jobs - job A and job B. With a Greedy scheduler, *all* 100 peers would be allocated to job A. When (if) job A completes, all 100 peers will then execute tasks for job B. This *probably* isn't desirable when you're running streaming workflows, since they theoretically never end.

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

**Peer Addition**

In the event that a peer joins the cluster while the Round Robin scheduler is running, that new peer will be allocated to the job *next* in the round robin sequence. The local replica has a reference to the job ID of the previous job that a peer was allocated for.

**Peer Removal**

In the event that a peer leaves the cluster while the Round Robin scheduler is running, the peers across *all* jobs will be rebalanced to evenly distribute the workflow. At most, one peer will change jobs to rebalance the workload.

**Job Addition**

If a job is submitted while this scheduler is running, the entire cluster will be rebalanced. This will result in *at least one* peer changing jobs to rebalance the cluster.

**Job Removal**

If a job is completed or otherwise cancelled while this scheduler is running, the entire cluster will be rebalanced. This will result in *at least one* peer changing jobs to rebalance the cluster.

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

#### Task Schedulers

Each Onyx job is configured with exactly one task scheduler. The task scheduler is specified at the time of calling `submit-job`. The purpose of the task scheduler is to control the order in which available peers are allocated to which tasks. There are currently two kinds of schedulers - Greedy and Round Robin.

##### Greedy Task Scheduler

The Greedy Task Scheduler takes a topological sort of the workflow for a specific job. As peers become available, this scheduler always assigns the next peer to the *earliest* task in the sorted workflow that is not complete. For example, if a workflow has a topological sort of tasks A, B, C, and D, this scheduler assigns each peer to task A.

**Task Completion**

When a task is complete, this scheduler moves all peers executing that task to the next task in the topologically sorted workflow. If there are no more tasks, these peers are elligible to execute tasks for another job. For example, if a workflow has a topological sort of tasks A, B, C, and D, and task A completes, this scheduler assigns all peers *from the completed task* to task B.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler defers to the Job scheduler to rebalance the cluster. If a new peer is added to this task as a result of a peer failing in another job, it is added to the currently executing task that all peers are greedily assigned to.

##### Round Robin Task Scheduler

The Round Robin Scheduler takes a topological sort of the workflow for a specific job. As peers become available, this scheduler assigns tasks to peers in a rotating order. For example, if a workflow has a topological sort of tasks A, B, C, and D, this scheduler assigns each peer to tasks A, B, C, D, A, B, C, D, ... and so on.

**Task Completion**

When a task is complete, this scheduler moves all peers executing that task and round robin assigns them, starting with the first incomplete task. the workflow for a specific job. As peers become available, this scheduler assigns tasks to peers in a rotating order. For example, if a workflow has a topological sort of tasks A, B, C, and D, and task A completes, this scheduler assigns each peer *from the completed task* to tasks B, C, D, B, C, D, ... and so on.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler defers to the Job scheduler to rebalance the cluster. If a new peer is added to this task as a result of a peer failing in another job, it is assigned the next task in the round robin sequence.

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
`prepare-join-cluster`

- Submitter: peer (P) that wants to join the cluster
- Purpose: determines which peer (Q) that P will watch
- Arguments: peer ID, peer pulse node
- Replica update: assoc {P Q} to `:prepare` key
- Side effects: P adds a ZooKeeper watch to Q's pulse node
- Reactions: P sends `notify-watchers` to the log, with arg {Z P} (Z is Q's watcher)

-------------------------------------------------
`abort-join-cluster`

- Submitter: peer (P) determines that peer (Q) cannot join the cluster (P may = Q)
- Purpose: Aborts Q's attempt at joining the cluster, erases attempt from replica
- Arguments: pair of nodes to add a watch before ({T P})
- Replica update: assoc {P Q} to `:accept` key, dissoc {P Q} from `:prepare` key
- Side effects: T adds a ZooKeeper watch to P's pulse node, T removes a ZooKeeper watch from Q's pulse node
- Reactions: T sends `accept-join-cluster` to the log, with args {P Q} and {T P}

-------------------------------------------------
`notify-watchers`

- Submitter: peer (P) that wants to join the cluster
- Purpose: Transitions this peer's watch (T) from one peer to another
- Arguments: pair of nodes to add a watch before ({T P})
- Replica update: assoc {P Q} to `:accept` key, dissoc {P Q} from `:prepare` key
- Side effects: T adds a ZooKeeper watch to P's pulse node, T removes a ZooKeeper watch from Q's pulse node
- Reactions: T sends `accept-join-cluster` to the log, with args {P Q} and {T P}

-------------------------------------------------
`accept-join-cluster`

- Submitter: peer (T) wants to confirm that peer P can join the cluster
- Purpose: confirms that T has a watch on P's pulse node
- Arguments: pair of nodes {P Q} and pair of nodes {T P} to add to fully joined cluster
- Replica update: dissoc {P Q} from `:accept` key, merge {P Q} and {T P} into `:pairs` key
- Side effects: None
- Reactions: peer P flushes its outbox of messages

-------------------------------------------------
`leave-cluster`

- Submitter: peer (P) reporting that peer Q is dead
- Purpose: removes Q from all activity, transitions P's watch to R and transitively closes the ring
- Arguments: peer ID of Q
- Replica update: assoc {P R} into the `:pairs` key, dissoc {Q R}
- Side effects: P adds a ZooKeeper watch to R's pulse node

-------------------------------------------------
`peer-gc`

- Submitter: peer (P) that wants to join the cluster
- Purpose: Generates `leave-cluster` commands for any peers that are dead, but haven't yet been reported
- Arguments: P's ID
- Replica update: none
- Side effects: P reads pulse nodes from ZooKeeper
- Reactions: P sends `leave-cluster` commands to the log, for all peers with missing pulses

-------------------------------------------------

## New functionality

This design enables a few things that I want to add to the API:

- Alternate schedulers
- Dynamic task reassignment
- Percentage task allocation (e.g. 75% of the cluster on task A)
- `kill-job` API

## Formal verification

?

