## APIs

Onyx ships with three distinct APIs to accommodate different needs. A description of each follows.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Core API](#core-api)
    - [`start-env`](#start-env)
    - [`start-peers!`](#start-peers!)
    - [`submit-job`](#submit-job)
    - [`await-job-completion`](#await-job-completion)
    - [`shutdown-peer`](#shutdown-peer)
    - [`shutdown-env`](#shutdown-env)
- [Task Lifecycle API](#task-lifecycle-api)
    - [`start-lifecycle?`](#start-lifecycle)
    - [`inject-lifecycle-resources`](#inject-lifecycle-resources)
    - [`inject-batch-resources`](#inject-batch-resources)
    - [`close-batch-resources`](#close-batch-resources)
    - [`close-lifecycle-resources`](#close-lifecycle-resources)
- [Peer Pipeline API](#peer-pipeline-api)
    - [`read-batch`](#read-batch)
    - [`decompress-batch`](#decompress-batch)
    - [`requeue-sentinel`](#requeue-sentinel)
    - [`ack-batch`](#ack-batch)
    - [`apply-fn`](#apply-fn)
    - [`compress-batch`](#compress-batch)
    - [`write-batch`](#write-batch)
    - [`seal-resource`](#seal-resource)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


### Core API

The [Core API](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/src/onyx/api.clj) is used to start/stop resources, jobs, and monitor job progress. It's accessible through the `onyx.api` namespace.

##### `start-env`

Starts a development environment with in-memory ZooKeeper and HornetQ. Helpful for developing locally without needing to start any other services.

##### `start-peers!`

Starts N virtual peers to execute tasks.

##### `submit-job`

Submits a job to Onyx to be scheduled for execution. Takes a map with keys `:catalog`, `:workflow`, and `:task-scheduler`.

##### `await-job-completion`

Given a job ID, blocks the calling thread until all the tasks for this job have been completed.

##### `shutdown-peer`

Shuts down a single peer, stopping any task that it is presently executing.

##### `shutdown-env`

Shuts down the development environment, stopping in memory HornetQ and ZooKeeper.

### Task Lifecycle API

Each time a virtual peer receives a task to execute, a lifecycle of functions are called. Onyx creates a map of useful data for the functions at the start of the lifecycle and proceeds to pass the map through to each function. The [Task Lifecycle API](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/src/onyx/peer/task_lifecycle_extensions.clj) facilitates this flow.

Onyx provides hooks for user-level modification of this map both before the task begins executing, before each segment batch begins, after each segment batch is completed, and after the task is completed. See below for a description of each. Each of these functions allows dispatch based on the name, identity, type, and type/medium combination of a task. Map merge precedence happens in this exact order, allowing you to override behavior specified by a plugin, or Onyx itself.

##### `start-lifecycle?`

Just before beginning the task, this function is called to check whether the peer is ready to begin. For reasons external
to Onyx, the peer might need to block and wait for another event. This function must return a map with key
`:onyx.core/start-lifecycle?` and a boolean value. If true, the task will begin executing. If false, the peer will back off
for `:onyx.peer/retry-start-interval` before recalling `start-lifecycle?`.

##### `inject-lifecycle-resources`

Adds data once to the start of a peer's task execution. This data can be accessed in every iteration of the pipeline.

##### `inject-batch-resources`

Adds data to each iteration of the pipeline per peer task execution. Called at the start of each pipeline.

##### `close-batch-resources`

Hook for closing out any stateful data injected into the pipeline. Called once at the end of each iteration.

##### `close-lifecycle-resources`

Hook for closing out any stateful data injected into the pipeline. Called once at the end of task execution. Called regardless of the task execution status.

### Peer Pipeline API

The virtual peer process is extensively pipelined, providing asynchrony between each lifecycle function. Hence, each virtual peer allocates at least 11 threads. Each function may be extended for new behavior. The [Peer Pipeline API](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/src/onyx/peer/pipeline_extensions.clj) allows you to latch on.

##### `read-batch`

Reads multiple segments off the previous element in the workflow.

##### `decompress-batch`

Decompresses the batch that was received. Internally, Onyx uses Fressian for compression.

##### `requeue-sentinel`

If applicable for your data source, send the sentinel back to the input source when it's found to not block future iterations of the pipeline.

##### `ack-batch`

Acknowledges the batch of read segments.

##### `apply-fn`

Applies the function for this task to the incoming segments.

##### `compress-batch`

Compresses the batch to send. Internally, Onyx uses Fressian for compression.

##### `write-batch`

Writes the batch with the function applied to the output stream.

##### `seal-resource`

Called by one peer exactly once (subsequent calls occur if the sealing peer fails) when the task is completing. Used internally to propagate the sentinel downstream.

