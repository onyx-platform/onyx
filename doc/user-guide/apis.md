## APIs

Onyx ships with two distinct APIs to accommodate different needs. A description of each follows.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Core API](#core-api)
    - [`start-env`](#start-env)
    - [`start-peer-group`](#start-peer-group)
    - [`start-peers`](#start-peers)
    - [`submit-job`](#submit-job)
    - [`await-job-completion`](#await-job-completion)
    - [`gc`](#gc)
    - [`kill-job`](#kill-job)
    - [`subscribe-to-log`](#subscribe-to-log)
    - [`shutdown-peer`](#shutdown-peer)
    - [`shutdown-peer-group`](#shutdown-peer-group)
    - [`shutdown-env`](#shutdown-env)
- [Peer Pipeline API](#peer-pipeline-api)
    - [`read-batch`](#read-batch)
    - [`write-batch`](#write-batch)
    - [`seal-resource`](#seal-resource)
    - [`ack-message`](#ack-message)
    - [`retry-message`](#retry-message)
    - [`pending?`](#pending)
    - [`drained?`](#drained)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Core API

The [Core API](https://github.com/onyx-platform/onyx/blob/0.6.x/src/onyx/api.clj) is used to start/stop resources, jobs, and monitor job progress. It's accessible through the `onyx.api` namespace.

##### `start-env`

Starts a development environment with in-memory ZooKeeper. Helpful for developing locally without needing to start any other services.

##### `start-peer-group`

Starts a resource pool to be shared across a group of peers. You should only start one peer group per physical machine.

##### `start-peers`

Starts N virtual peers to execute tasks. In a production environment, you should start by booting up N virtual peers for N cores on the physical machine. Tune performance from there.

##### `submit-job`

Submits a job to Onyx to be scheduled for execution. Takes a map with keys `:catalog`, `:workflow`, and `:task-scheduler`. Returns a map of `:job-id` and `:task-ids`, which map to a UUID and vector of maps respectively.

##### `await-job-completion`

Given a job ID, blocks the calling thread until all the tasks for this job have been completed.

##### `gc`

Invokes the garbage collector. Compresses the replica in Zookeeper, freeing up storage and deleting log history. Frees up memory on the local, in memory replica on all peers.

##### `kill-job`

Stops this job from executing, never allowing it to be run again.

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

##### `ack-message`

Acknowledges a segment natively on the input medium, causing the segment to be released from durable storage.

##### `retry-message`

Processes a segment again from the root of the workflow.

##### `pending?`

Given a segment ID, returns true if this segment is pending completion.

##### `drained?`

Returns true if all messages on the input medium have successfully been processed. Never returns true for an infinite message stream.
