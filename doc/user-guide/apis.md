---
layout: user_guide_page
title: APIs
categories: [user-guide-page]
---

## APIs

Onyx ships with two distinct APIs to accommodate different needs. A description of each follows.

### Core API

The [Core API](https://github.com/onyx-platform/onyx/blob/0.6.x/src/onyx/api.clj) is used to start/stop resources, jobs, and monitor job progress. It's accessible through the `onyx.api` namespace.

##### `start-env`

Starts a development environment with in-memory ZooKeeper. Helpful for developing locally without needing to start any other services.

##### `start-peer-group`

Starts a resource pool to be shared across a group of peers. You should only start one peer group per physical machine.

##### `start-peers`

Starts N virtual peers to execute tasks. In a production environment, you should start by booting up N virtual peers for N cores on the physical machine. Tune performance from there.

##### `submit-job`

Submits a job to Onyx to be scheduled for execution. Takes a map with keys `:catalog`, `:workflow`, `:flow-conditions`, `:windows`, `:triggers`, `:metadata`, and `:task-scheduler`. Returns a map of `:job-id` and `:task-ids`, which map to a UUID and vector of maps respectively. `:metadata` is a map of values that must serialize to EDN. `:metadata` will be logged with all task output, and is useful for identifying a particular task based on something other than its name or ID.

Additionally, `:metadata` may optionally contain a `:job-id` key. When specified, this key will be used for the job ID instead of a randomly chosen UUID. Repeated submissions of a job with the same :job-id will be treated as an idempotent action. If a job with the same ID has been submitted more than once, the original task IDs associated with the catalog will be returned, and the job will not run again, even if it has been killed or completed. It is undefined behavior to submit two jobs with the same :job-id metadata whose :workflow, :catalog, :flow-conditions, etc are not equal.

##### `await-job-completion`

Given a job ID, blocks the calling thread until all the tasks for this job have been completed.

Example project: [block-on-job-completion](https://github.com/onyx-platform/onyx-examples/tree/0.9.x/block-on-job-completion)

##### `gc`

Invokes the garbage collector. Compresses the replica in Zookeeper, freeing up storage and deleting log history. Frees up memory on the local, in memory replica on all peers.

##### `kill-job`

Stops this job from executing, never allowing it to be run again.

Example project: [kill-job](https://github.com/onyx-platform/onyx-examples/tree/0.9.x/kill-job)

##### `subscribe-to-log`

Sends all events in the log to a core.async channel. Events are received in the order that they appeared in the log. Starts from the beginning of the log, blocking until more entries are available.

##### `shutdown-peer`

Shuts down a single peer, stopping any task that it is presently executing.

##### `shutdown-peer-group`

Shuts down the peer group, releasing any messaging resources it was holding open.

##### `shutdown-env`

Shuts down the development environment and stops in memory ZooKeeper.

### Peer Pipeline API

The [Peer Pipeline API](https://github.com/onyx-platform/onyx/blob/0.6.x/src/onyx/peer/pipeline_extensions.clj) allows you to interact with data storage mediums to read and write data for plugins.

##### `read-batch`

Reads multiple segments off the previous element in the workflow.

##### `write-batch`

Writes the batch with the function applied to the output stream.

##### `seal-resource`

Called by one peer exactly once (subsequent calls occur if the sealing peer fails) when the task is completing. Close out target output resources.

##### `ack-segment`

Acknowledges a segment natively on the input medium, causing the segment to be released from durable storage.

##### `retry-segment`

Processes a segment again from the root of the workflow.

##### `pending?`

Given a segment ID, returns true if this segment is pending completion.

##### `drained?`

Returns true if all messages on the input medium have successfully been processed. Never returns true for an infinite message stream.
