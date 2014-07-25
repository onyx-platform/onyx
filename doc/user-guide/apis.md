### User-facing API

### Task Lifecycle API

Each time a virtual peer receives a task from the coordinator to execute, a lifecycle of functions are called. Onyx creates a map of useful data for the functions at the start of the lifecycle and proceeds to pass the map through to each function. 

Onyx provides hooks for user-level modification of this map both before the task begins executing, after each batch is completed, and after the task is completed. These are the `inject-lifecycle-resources`, `close-temporal-resources`, and `close-lifecycle-resources`, respectively. Each of these last three functions allows dispatch based on the name, identity, type, and type/medium combination of a task. Map merge prescendence happens in this exact order.

The virtual peer process is extensively pipelined, providing asynchrony between each lifecycle function. Hence, each virtual peer allocates at least 11 threads. See the Clojure docs for a description of each function.

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
