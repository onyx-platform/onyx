#### 0.6.0

- Dropped feature: support for `:sequential` tasks
- Dropped feature: support for `onyx.task-scheduler/greedy`
- Dropped feature: support for HornetQ messaging
- New messaging transport: http-kit websockets
- New messaging transport: Netty websockets
- New messaging transport: Aeron
- New feature: Percentage-based, elastically scalable acknowledgement configuration
- New feature: Input, output, and specific task name exemption from acting as an acker node
- Added metadata to all public API functions indicating which Onyx version they were added in.
- `onyx.api/start-peers!` API renamed to `onyx.api/start-peers`
- `onyx.api/shutdown-peers` is now idempotent
- The last task in a workflow no longer needs to be an `output` task.

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

- Fixes aggregate ignoring `:onyx/batch-timeout`. [#33](https://github.com/MichaelDrogalis/onyx/issues/33)
- Adds log rotation to default Onyx logging configuration. [#35](https://github.com/MichaelDrogalis/onyx/issues/35)
- Peer options available in pipeline event map under key `:onyx.core/peer-opts`

#### 0.4.0

- Grouper and Aggregate functions removed, replaced by catalog-level grouping and implicit aggregation. [#20](https://github.com/MichaelDrogalis/onyx/issues/20)
- Support for directed, acylic graphs as workflows. [#26](https://github.com/MichaelDrogalis/onyx/issues/26)
- Fix for peer live lock on task completion. [#23](https://github.com/MichaelDrogalis/onyx/issues/23)
- Fixed bug where job submission silently fails due to malformed workflow [#24](https://github.com/MichaelDrogalis/onyx/issues/24)
- `submit-job` throws exceptions on a malformed catalog submission. [#3](https://github.com/MichaelDrogalis/onyx/issues/3)
- Fix HornetQ ipv6 multicast socket bind issue when running on hosts with ipv6 interfaces.
- Adds `:onyx/batch-timeout` option to all catalog entries. [#29](https://github.com/MichaelDrogalis/onyx/issues/29)

#### 0.3.3

- Fixes a scenario where a virtual peer can deadlock on task completion. [#18](https://github.com/MichaelDrogalis/onyx/issues/18)

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

- Rename internal API extensions to use "node" instead of "place.
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
