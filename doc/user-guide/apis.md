## APIs

Onyx ships with three distinct APIs to accomodate different needs. A description of each follows.

### Connection API

The [Connection API](https://github.com/MichaelDrogalis/onyx/blob/0.3.x/src/onyx/api.clj) is used for typical interaction with Onyx. You'd use the Connection API for interaction with the Coordinator to make Onyx do work.

##### `connect`

Connects the calling thread to the Coordinator. Connection returned is used for submitting jobs and starting peers.

##### `start-peers`

Starts N virtual peer pipelines and registers them with the Coordinator for task execution.

##### `register-peer`

Informs the Coordinator of a new peer. Used by other API functions, and probably not something you'd want to use directly.

##### `submit-job`

Submits a job to Onyx to be scheduled for execution. Takes a map with keys `:catalog` and `:workflow`.

##### `await-job-completion`

Given a job ID, blocks the calling thread until the job is complete.

##### `shutdown`

Spins down a connection to a Coordinator.

### Task Lifecycle API

Each time a virtual peer receives a task from the coordinator to execute, a lifecycle of functions are called. Onyx creates a map of useful data for the functions at the start of the lifecycle and proceeds to pass the map through to each function. The [Task Lifecycle API](https://github.com/MichaelDrogalis/onyx/blob/0.3.x/src/onyx/peer/task_lifecycle_extensions.clj) facilitaties this flow.

Onyx provides hooks for user-level modification of this map both before the task begins executing, before each segment batch begins, after each segment batch is completed, and after the task is completed. See below for a description of each. Each of these functions allows dispatch based on the name, identity, type, and type/medium combination of a task. Map merge prescendence happens in this exact order, allowing you to override behavior specified by a plugin, or Onyx itself.

##### `inject-lifecycle-resources`

##### `inject-temporal-resources`

##### `close-temporal-resources`

##### `close-lifecycle-resources`

### Peer Pipeline API

The virtual peer process is extensively pipelined, providing asynchrony between each lifecycle function. Hence, each virtual peer allocates at least 11 threads. Each function may be extended for new behavior. The [Peer Pipeline API](https://github.com/MichaelDrogalis/onyx/blob/0.3.x/src/onyx/peer/pipeline_extensions.clj) allows you to latch on.

##### `inject-lifecycle-resources`

##### `read-batch`

##### `decompress-batch`

##### `requeue-sentinel`

##### `ack-batch`

##### `apply-fn`

##### `compress-batch`

##### `write-batch`

##### `close-temporal-resources`

##### `close-lifecycle-resources`

##### `seal-resource`
