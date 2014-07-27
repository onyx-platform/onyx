<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [0.3.0](#030)
- [0.2.0](#020)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
