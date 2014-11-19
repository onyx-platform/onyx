#### 0.5.0-SNAPSHOT

- Transducer support for Functions.

#### 0.4.1

- Fixes aggregate ignoring `:onyx/batch-timeout`. [#33](https://github.com/MichaelDrogalis/onyx/issues/33)
- Adds log rotation to default Onyx logging configuration. [#35](https://github.com/MichaelDrogalis/onyx/issues/35)
- Peer options available in pipeline event map under key `:onyx.core/peer-opts`.
- Support alternative workflow format e.g. `{:a #{:b :c} :b #{:d}}`. Convert to the regular workflow format prior to submit-job using onyx.api/map-set-workflow->workflow. [#47](https://github.com/MichaelDrogalis/onyx/pull/47)
- Throw an exception on `submit-job` when the catalog or worklow aren't serializable [#45](https://github.com/MichaelDrogalis/onyx/pull/45/files)
- Increased validation of workflow formats [#43](https://github.com/MichaelDrogalis/onyx/pull/43)
- Throw an exception when lifecycle methods don't return maps [#46](https://github.com/MichaelDrogalis/onyx/pull/46)
- Fixes logging of stack traces to the log file - no longer using Rotor [#46](https://github.com/MichaelDrogalis/onyx/pull/46)

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
