## APIs

Onyx ships with three distinct APIs to accomodate different needs. A description of each follows.

### Connection API

- `connect`

- `start-peers`

- `register-peer`

- `submit-job`

- `await-job-completion`

- `shutdown`

### Task Lifecycle API

Each time a virtual peer receives a task from the coordinator to execute, a lifecycle of functions are called. Onyx creates a map of useful data for the functions at the start of the lifecycle and proceeds to pass the map through to each function. 

Onyx provides hooks for user-level modification of this map both before the task begins executing, before each segment batch begins, after each segment batch is completed, and after the task is completed. See below for a description of each. Each of these functions allows dispatch based on the name, identity, type, and type/medium combination of a task. Map merge prescendence happens in this exact order, allowing you to override behavior specified by a plugin, or Onyx itself.

- `inject-lifecycle-resources`

- `inject-temporal-resources`

- `close-temporal-resources`

- `close-lifecycle-resources`

### Peer Pipeline API

The virtual peer process is extensively pipelined, providing asynchrony between each lifecycle function. Hence, each virtual peer allocates at least 11 threads. Each function may be extended for new behavior. See below for a description of each.

- `inject-lifecycle-resources`

- `read-batch`

- `decompress-batch`

- `requeue-sentinel`

- `ack-batch`

- `apply-fn`

- `compress-batch`

- `write-batch`

- `close-temporal-resources`

- `close-lifecycle-resources`

- `seal-resource`
