## Scheduling

### Job Priority

Onyx uses round-robin to pick which job to search for open tasks next. When a task is found, it's given to a peer for execution. If a job doesn't have any open tasks, it's skipped for the next job in the sequence. Exact order depends on the order that the jobs were received.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Scheduling](#scheduling)
  - [Job Priority](#job-priority)
  - [Task Scheduling and Peer Allocation](#task-scheduling-and-peer-allocation)
    - [Minimum Number of Virtual Peers](#minimum-number-of-virtual-peers)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Task Scheduling and Peer Allocation

Onyx allocates tasks within a job using a balanced, breadth-first approach. That is, given the following workflow:

```clojure
{:a {:b :d
     :c :e}}
```

Onyx will assign peers in roughly the following sequence: A, B, C, D, E. The exact order within a level of the workflow tree depends on how Clojure hashed the underlying key sequence, but it is determistic given the key set. After the entire tree has filled up with peers, Onyx iterates over it again in the same order. If a task is marked `:sequential` and has one peer executing, it's skipped in favor of the next task.

Alternate peer allocate strategies to come in future versions. It's obviously desirable to have a less balanced approach in certain circumstances.

Onyx will eagerly apply as many peers as possible to each job.

#### Minimum Number of Virtual Peers

Notice that since one virtual peer executes exactly one task, streaming jobs require at least as many virtual peers as tasks.

