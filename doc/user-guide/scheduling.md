---
layout: user_guide_page
title: Scheduling
categories: [user-guide-page]
---

## Scheduling

Onyx offers fine-grained control of how many peers are allocated to particular jobs and tasks. This section outlines how to use the built-in schedulers.

### Allocating Peers to Jobs and Tasks

In a masterless design, there is no single entity that assigns tasks to peers. Instead, peers need to contend for tasks to execute as jobs are submitted to Onyx. Conversely, as peers are added to the cluster, the peers must "shift" to distribute the workload across the cluster. Onyx ships out-of-the-box job and task allocation policies. End users can change the levels of fairness that each job gets with respect to cluster power. And remember, one virtual peer executes *at most* one task.

#### Job Schedulers

Each running Onyx instance is configured with exactly one job scheduler. The purpose of the job scheduler is to coordinate which jobs peers are allowed to volunteer to execute. There are a few different kinds of schedulers, listed below. To use each, configure the Peer Options map with key `:onyx.peer/job-scheduler` to the value specified below.

##### Greedy Job Scheduler

The Greedy job scheduler allocates *all peers* to each job in the order that it was submitted. For example, suppose you had 100 virtual peers and you submitted two jobs - job A and job B. With a Greedy scheduler, *all* 100 peers would be allocated to job A. If job A completes, all 100 peers will then execute tasks for job B. This *probably* isn't desirable when you're running streaming workflows, since they theoretically never end.

To use this scheduler, set key `:onyx.peer/job-scheduler` to `:onyx.job-scheduler/greedy` in the Peer Options.

**Peer Addition**

In the event that a peer joins the cluster while the Greedy job scheduler is running, that new peer will be allocated to the current job that is being run greedily.

**Peer Removal**

In the event that a peer leaves the cluster while the Greedy job scheduler is running, no peers will be shifted off of the job that is greedily running.

**Job Addition**

If a job is submitted while this scheduler is running, no peers will be allocated to this job. The only exception to this rule is if *no* jobs are currently running. In this case, all peers will be allocated to this job.

**Job Removal**

If a job is completed or otherwise canceled, *all* of the peers executed that task will move to the job that was submitted after this job.

##### Balanced Robin Job Scheduler

The Balanced job scheduler allocates peers in a rotating fashion to jobs that were submitted. For example, suppose that you had 100 virtual peers (virtual peer 1, virtual peer 2, ... virtual peer 100) and you submitted two jobs - job A and job B. With a Balanced scheduler, both jobs will end up with 50 virtual peers allocated to each. This scheduler begins allocating by selecting the first job submitted.

To use this scheduler, set key `:onyx.peer/job-scheduler` to `:onyx.job-scheduler/balanced` in the Peer Options.

**Peer Addition**

In the event that a peer joins the cluster while the Balanced scheduler is running, that new peer will be allocated to the job that most evenly balances the cluster according to a the number of jobs divided by the number of peers. If there is a tie, the new peer is added to the earliest submitted job.

**Peer Removal**

In the event that a peer leaves the cluster while the Balanced scheduler is running, the peers across *all* jobs will be rebalanced to evenly distribute the workflow.

**Job Addition**

If a job is submitted while this scheduler is running, the entire cluster will be rebalanced. For example, if job A has all 100 peers executing its task, and job B is submitted, 50 peers will move from job A to job B.

**Job Removal**

If a job is completed or otherwise canceled while this scheduler is running, the entire cluster will be rebalanced. For example, if job A, B, and C had 20 peers executing each of its tasks (60 peers total), and job C finished, job A would gain 10 peers, and job B would gain 10 peers.

##### Percentage Job Scheduler

The Percentage job scheduler allows jobs to be submitted with a percentage value. The percentage value indicates what percentage of the cluster will be allocated to this job. The use case for this scheduler is for when you have a static number of jobs and a varying number of peers. For example, if you have 2 jobs - A and B, you'd give each of this percentage values - say 70% and 30%, respectively. If you had 100 virtual peers running, 70 would be allocated to A, and 30 to B. If you then added 100 more peers to the cluster, job A would be allocated 140 peers, and job B 60. This dynamically scaling is a big step forward over statically configuring slots, which is normal in ecosystems like Hadoop and Storm.

If there aren't enough peers to satisfy the percentage values of all the jobs, this scheduler will allocate with priority to jobs with the highest percentage value. When percentage values are equal, the earliest submitted job will get priority. In the event that jobs are submitted, and the total percentage value exceeds 100%, the earliest submitted jobs that do not exceed 100% will receive peers. Jobs that go beyond it will not receive peers. For example, if you submitted jobs A, B, and C with 70%, 30%, and 20% respectively, jobs A and B would receive peers, and C will not be allocated any peers until either A or B completes.

If the total percentages of all submitted jobs doesn't sum up to 100%, the job with the highest percentage value will receive the extra peers. When percentage values are equal, the earliest submitted job will get priority.

If the algorithm determines that any job should receive a number of peers that is less than the minimum number it needs to execute, that job receives no peers.

To use this scheduler, set key `:onyx.peer/job-scheduler` to `:onyx.job-scheduler/percentage` in the Peer Options.

**Peer Addition**

In the event that a peer joins the cluster while the Percentage job scheduler is running, the entire cluster will rebalance.

**Peer Removal**

In the event that a peer leaves the cluster while the Percentage job scheduler is running, the entire cluster will rebalance.

**Job Addition**

If a job is submitted while this scheduler is running, the entire cluster will be rebalanced.

**Job Removal**

If a job is completed or otherwise canceled while this scheduler is running, the entire cluster will be rebalanced.

#### Task Schedulers

Each Onyx job is configured with exactly one task scheduler. The task scheduler is specified at the time of calling `submit-job`. The purpose of the task scheduler is to control the order in which available peers are allocated to which tasks. There are a few different Task Scheduler implementations, listed below. To use each, call `onyx.api/submit-job`. The second argument of this function is a map. Supply a `:task-scheduler` key and map it to the value specified below.

##### Balanced Task Scheduler

The Balanced Scheduler takes a topological sort of the workflow for a specific job. As peers become available, this scheduler assigns tasks to peers in a rotating order. For example, if a workflow has a topological sort of tasks A, B, C, and D, this scheduler assigns each peer to tasks A, B, C, D, A, B, C, D, ... and so on.

To use, set `:task-scheduler` in `submit-job` to `:onyx.task-scheduler/balanced`.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler defers to the Job scheduler to rebalance the cluster. If a new peer is added to this task as a result of a peer failing in another job, it is assigned the next task in the balanced sequence.

**Max Peer Parameter**

With the Balanced Task Scheduler, each entry in the catalog can specify a key of `:onyx/max-peers` with an integer value > 0. When this key is set, Onyx will never assign that task more than that number of peers. Balanced will simply skip the task for allocation when more peers are available, and continue assigning balanced to other tasks.

##### Percentage Task Scheduler

The Percentage Scheduler takes a set of tasks, all of which must be assigned a percentage value (`:onyx/percentage`) in the corresponding catalog entries. The percentage values *must* add up to 100 or less. Percent values may be integers between 1 and 99, inclusive. This schedule will allocate peers for this job in proportions to the specified tasks. As more or less peers join the cluster, allocations will automatically scale. For example, if a job has tasks A, B, and C with 70%, 20%, and 30% specified as their percentages, and there are 10 peers, task A receives 7 peers, B 2 peers, and C 1 peer.

This scheduler handles corner cases (fractions of peers) in the same way as the Percentage Job Scheduler. See that documentation for a full description.

To use, set `:task-scheduler` in `submit-job` to `:onyx.task-scheduler/percentage`.

**Peer Removal**

If a peer fails, or is otherwise removed from the cluster, the Task scheduler rebalances all the peers for even distribution.

##### Colocation Task Scheduler

The Colocation Schedule takes all of the tasks for a job and, if possible, assigns them to the peers on a single physical machine represented by the same peer group. If a job has 4 tasks and the cluster is one machine with 5 peers, 4 peers will become active. If that machine had 8 peers, all 8 would become active as this schedule operates in peer chunks that are divisible by the task size. If more machines are capable of executing the entire job, they will also be used.

This scheduler is useful for dramatically increasing performance of jobs where the latency is bound by the network of transmitting data across tasks. Using this scheduler with peer short circuiting will ensure that segments are never serialized and never cross the network between tasks (with the exception of grouping tasks). Onyx's usual fault tolerancy mechanisms are still used to ensure that data is processed in the presence of machine failure.

To use, set `:task-scheduler` in `submit-job` to `:onyx.task-scheduler/colcocated`.

To use colocation, but to disable the scheduler's affinity to always send segments to a local peer, set `:onyx.task-scheduler.colocated/only-send-local?` to `false` in the peer config. This is desirable when optimal performance depends on the uniformity of tasks being evenly assigned to machines in your cluster, but strictly local execution is not helpful for performance.

**Peer Addition**

If a peer is added to the cluster and its machine is capable of executing all the tasks for this job, the entire machine will be used - provided that it falls into the pool of peers elligible to execute this job, per the job scheduler's perogative.

**Peer Removal**

If a peer is removed, all the peers associated with this job's tasks for this chunk of peers will stop executing their tasks.

### Tags

It's often the case that a set of machines in your cluster are privileged in some way. Perhaps they are running special hardware, or they live in a specific data center, or they have a license to use a proprietary database. Sometimes, you'll have Onyx jobs that require tasks to run on a predetermined set of machines. Tags are a feature that let peers denote "capabilities". Tasks may declare which tags peers must have in order to be selected to execute them.

#### Peers

To declare a peer as having special capabilities, use a vector of keywords in the Peer Configuration under key `:onyx.peer/tags`. For example, if you wanted to declare that a peer has a license for its JVM to communicate with Datomic, you might add this to your Peer Configuation:

```clojure
{...
 :onyx/tenancy-id "my-cluster"
 :onyx.peer/tags [:datomic]
 ...
}
```

You can specify multiple tags. The default is no tags (`[]`), in which case this peer can execute any tasks that do not require tags.

#### Tasks

Now that we have a means for expressing which peers can do which kinds of things, we'll need a way to express which tasks require which capabilities. We do this in the catalog. Any task can use the key `:onyx/required-tags` with a vector of keywords as a value. Any peer that executes this task is garunteed to have `:onyx/required-tags` as a subset of its `:onyx.peer/tags`.

For example, to declare that task `:read-datoms` must be executed by a peer that can talk to Datomic, you might write:

```clojure
[{:onyx/name :read-datoms
  :onyx/plugin :onyx.plugin.datomic/read-datoms
  :onyx/type :input
  :onyx/medium :datomic
  :onyx/required-tags [:datomic] ;; <- Add this!
  :datomic/uri db-uri
  ...
  :onyx/batch-size batch-size
  :onyx/doc "Reads a sequence of datoms from the d/datoms API"}
 ...
]
```
