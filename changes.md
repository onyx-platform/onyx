#### 0.7.3

- Bug fix: Kill-job no longer throws a malformed exception with bad parameters.
- Bug fix: Fixed arity in garbage collection to seek the next origin.
- Documentation: Fixed Aeron docstring for port allocation
- Added schema type-checking to all replica updates in the log.
- Allow Aeron to use short virtual peer ID when hashing. [#250](https://github.com/onyx-platform/onyx/issues/250)
- Exposed all schemas used for internal validation through `onyx.schema` namespace, meant for use in 3rd party tooling.
- Allow plugins to write task-metadata to the replica, which will be cleaned up when jobs are completed or killed [#287](https://github.com/onyx-platform/onyx/pull/287)

#### 0.7.2

- Fixed issue where cluster jammed when all peers left and joined [#273](https://github.com/onyx-platform/onyx/issues/273)
- Allow `:all` in task lifecycle names to match all tasks. [#209](https://github.com/onyx-platform/onyx/issues/209)

#### 0.7.1
- :onyx.core/params is initialised as a vector
- Greatly improved lifecyle and keyword validation
- await-job-completion additionally returns when a job is killed. Return code now denotes whether the job completed successfully (true) or was killed (false).
- Throw exceptions from tasks, even if Nippy can serialize them.
- Fix typo'ed monitoring calls.

#### 0.7.0

- API: :onyx/ident has been renamed :onyx/plugin, and now takes a keyword path to a fn that instantiates the plugin e.g. :onyx.plugin.core-async/input. (**Breaking change**)
- API: plugins are now implemented by the Pipeline and PipelineInput protocols. (**Breaking change**)
- API: output plugins may now find leaf segments in (:leaves (:tree (:onyx.core/results event))) instead of (:leaves (:onyx.core/results event)) (**Breaking change**)
- API: New lifecycle functions "after-ack-message" and "after-retry-message" are now available.
- API: `:onyx/group-by-key` can now group can now take a vector of keywords, or just a keyword.
- API: `:onyx/restart-pred-fn` catalog entry points to a boolean function that permits a task to hot restart on an exception.
- API: Peer configuration can now be instructed to short-circuit and skip network I/O if downstream peers are on the same machine.
- New feature: Jobs will now enter an automatic backpressure mode when internal peer buffers fill up past a high water mark, and will be turned off after reaching a low water mark. See [Backpressure](doc/user-guide/backpressure.md) for more details.
- New documentation: Use GitBook for documentation. [#119](https://github.com/onyx-platform/onyx/issues/119)
- New feature: Onyx Monitoring. Onyx emits a vast amount of metrics about its internal health.
- New feature: Aeron messaging transport layer. Requires Java 8. Use the `:aeron` key for `:onyx.messaging/impl`.
- New feature: Messaging short circuiting. When virtual peers are co-located on the same peer, the network and serialization will be bypassed completely. Note, this feature is currently only available when using Aeron messaging.
- New feature: Connection multiplexing. Onyx's scalability has been improved by [multiplexing connections](doc/user-guide/messaging.md#subscription-connection-multiplexing). Note, this feature is currently only available when using Aeron messaging.
- New feature: Java integration via catalog entry `:onyx/language` set to `:java`.
- Bug fix: Several log / replica edge cases were fixed.
- Bug fix: Peers not picking up new job after current job was killed.
- Bug fix: Bulk functions are no longer invoked when the batch is empty. [#260](https://github.com/onyx-platform/onyx/issues/260)

#### 0.6.0

- Dropped feature: support for `:sequential` tasks
- Dropped feature: support for `onyx.task-scheduler/greedy`
- Dropped feature: support for HornetQ messaging
- Dropped feature: internal HornetQ messaging plugin
- Dropped feature: multimethod lifecycles
- New messaging transport: Netty TCP
- New messaging transport: Aeron
- New messaging transport: core.async
- New feature: Percentage-based, elastically scalable acknowledgement configuration
- New feature: Input, output, and specific task name exemption from acting as an acker node
- New feature: Functions can take an entire batch of segments as their input with catalog key `:onyx/bulk?` true
- New feature: Flow conditions handle exceptions as predicates
- New feature: Flow conditions may post-transform exception values into new segments
- New feature: Flow conditions support a new `:action` key with `:retry` to reprocess a segment from its root value
- New feature: Custom compression and decompression functions through Peer configuration
- New feature: Data driven lifecycles
- New feature: Internal core.async plugin
- New feature: Flux policies
- New feature: `:onyx/min-peers` can be set on grouping tasks to create a lower bound on the number of peers required to start a task
- New documentation: Event context map information model
- New documentation: Default configuration & timeout values
- API: Added metadata to all public API functions indicating which Onyx version they were added in
- API: `onyx.api/start-peers!` API renamed to `onyx.api/start-peers`
- API: `onyx.api/shutdown-peers` is now idempotent
- API: Return type of public API function submit-job has changed. It now returns a map containing job-id, and task-ids keys.
- API: Renamed "Round Robin" schedulers to "Balanced"
- API: The last task in a workflow no longer needs to be an `output` task
- API: Plugin lifecycle extensions now dispatch off of identity, rather than type and name.
- API: Peers now launch inside of a "peer group" to share network resources.

#### 0.5.3

- New feature: Flow Conditions. Flow Conditions let you manage predicates that route segments across tasks in your workflow.
- Fixed extraneous ZooKeeper error messages.

#### 0.5.2

- Development environment doesn't required that the job scheduler be known ahead of time. It's now discovered dynamically.

#### 0.5.1

- Adds ctime to log entries. Useful for things like the dashboard and other log subscribers.

#### 0.5.0

- Design change: the Coordinator has been abolished. Onyx is now a fully masterless system. The supporting environment now only requires Zookeeper, HornetQ, and a shared Onyx ID across cluster members.
- New feature: Realtime event subscription service. All coordination events can be observed in a push-based API backed with core.async.
- New feature: Job schedulers are now available to control how many peers get assigned to particular jobs. Supports `:onyx.job-scheduler/greedy`, `:onyx.job-scheduler/round-robin`, and `:onyx.job-scheduler/percentage`.
- New feature: Task schedulers are now available to control how many peers get assigned to particular tasks within a single job. Supports `:onyx.task-scheduler/greedy`, `:onyx.task-scheduler/round-robin`, and `:onyx.task-scheduler/percentage`.
- New feature: `:onyx/max-peers` may optionally be specified on any catalog entry to create an upper bound on the number of peers executing a particular task. Only applicable under a Round Robin Task Scheduler.
- New feature: `:onyx/params` may be specified on any catalog entry. It takes a vector of keywords. These keywords are resolved to other keys in the same catalog entry, and the corresponding values are passing as arguments to the function that implements that catalog entry.
- New feature: `:onyx.peer/join-failure-back-off` option in the peer config specifies a cooldown period to wait to try and join the cluster after previously aborting
- New feature: `:onyx.peer/inbox-capacity` option in the peer config specifies the max number of inbound messages a peer will buffer in memory.
- New feature: `:onyx.peer/outbox-capacity` option in the peer config specifies the max number of outbound messages a peer will buffer in memory.
- New feature: `kill-job` API function.
- New feature: `subscribe-to-log` API function.
- New feature: `shutdown-peer` API function.
- New feature: `shutdown-env` API function. Shuts down a development environment (ZK/HQ in memory).
- New feature: `gc` API function. Garbage collects all peer replicas and deletes old log entries from ZooKeeper.
- Enhancement: peers now automatically kill their currently running job if it throws an exception other than a Zookeeper or HornetQ connection failure. The latter cases still cause the peer to automatically reboot.

#### 0.4.1

- Fixes aggregate ignoring `:onyx/batch-timeout`. [#33](https://github.com/onyx-platform/onyx/issues/33)
- Adds log rotation to default Onyx logging configuration. [#35](https://github.com/onyx-platform/onyx/issues/35)
- Peer options available in pipeline event map under key `:onyx.core/peer-opts`

#### 0.4.0

- Grouper and Aggregate functions removed, replaced by catalog-level grouping and implicit aggregation. [#20](https://github.com/onyx-platform/onyx/issues/20)
- Support for directed, acylic graphs as workflows. [#26](https://github.com/onyx-platform/onyx/issues/26)
- Fix for peer live lock on task completion. [#23](https://github.com/onyx-platform/onyx/issues/23)
- Fixed bug where job submission silently fails due to malformed workflow [#24](https://github.com/onyx-platform/onyx/issues/24)
- `submit-job` throws exceptions on a malformed catalog submission. [#3](https://github.com/onyx-platform/onyx/issues/3)
- Fix HornetQ ipv6 multicast socket bind issue when running on hosts with ipv6 interfaces.
- Adds `:onyx/batch-timeout` option to all catalog entries. [#29](https://github.com/onyx-platform/onyx/issues/29)

#### 0.3.3

- Fixes a scenario where a virtual peer can deadlock on task completion. [#18](https://github.com/onyx-platform/onyx/issues/18)

#### 0.3.2

- Made peer shutdown function synchronous.

#### 0.3.1

- Performance improvement by eliminating superfluous decompression.

#### 0.3.0
- Coordinator can be made highly available via stand-by coordinators
- HornetQ connection via UDP multicast for clustering
- HornetQ connection via JGroups for clustering
- HornetQ embedded mode for development with an HQ cluster
- HornetQ VM mode for development with an in-JVM HQ instance
- ZooKeeper in-memory mode for for development without an external ZooKeeper running
- Concurrent tasks executed by a single v-peer no longer implies sequential message processing

#### 0.2.0

- Rename internal API extensions to use "node" instead of "place".
- Throw an explicit error on function resolution failure.
- Add state audits into test suite.
- Remove Datomic log component, replace with ZooKeeper.
- Return job ID on submission.
- Expose API function for blocking on job completion.
- Allow overriding of peer lifecycle methods, merge results back together.
- Add aggregate operation.
- Add grouping operation.
- Change lifecycle API functions.
- Log all Onyx output to file system.
