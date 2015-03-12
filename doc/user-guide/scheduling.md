## Scheduling

Onyx offers fine-grained control of how many peers are allocated to particular jobs and tasks. This section outlines how to use the built-in schedulers.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Allocating Peers to Jobs and Tasks](#allocating-peers-to-jobs-and-tasks)
  - [Job Schedulers](#job-schedulers)
    - [Greedy Job Scheduler](#greedy-job-scheduler)
    - [Round Robin Job Scheduler](#round-robin-job-scheduler)
    - [Round Robin Rebalancing Strategy](#round-robin-rebalancing-strategy)
    - [Percentage Job Scheduler](#percentage-job-scheduler)
    - [Percentage Rebalancing Strategy](#percentage-rebalancing-strategy)
  - [Task Schedulers](#task-schedulers)
    - [Round Robin Task Scheduler](#round-robin-task-scheduler)
    - [Percentage Task Scheduler](#percentage-task-scheduler)
  - [Examples](#examples)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Allocating Peers to Jobs and Tasks

In a masterless design, there is no single entity that assigns tasks to peers. Instead, peers need to contend for tasks to execute as jobs are submitted to Onyx. Conversely, as peers are added to the cluster, the peers must "shift" make distribute the workload across the cluster. Onyx `0.5.0` ships new job and task allocation policies. End users will be able to change the levels of fairness that each job gets with respect to cluster power. And remember, one virtual peer executes *at most* one task.

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

If a job is completed or otherwise canceled, *all* of the peers executed that task will move to the job that was submitted after this job.

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

If a job is completed or otherwise canceled while this scheduler is running, the entire cluster will be rebalanced. This will result in *at least one* peer changing jobs to rebalance the cluster. For example, if job A, B, and C had 20 peers executing each of its tasks (60 peers total), and job C finished, job A would gain 10 peers, and job B would gain 10 peers.

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

If a job is completed or otherwise canceled while this scheduler is running, the entire cluster will be rebalanced.

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

#### Examples

- [Example 1: 3 node cluster, 1 job, Greedy job scheduler, Round Robin task scheduler](/doc/design/allocate-examples/example-1.md)

