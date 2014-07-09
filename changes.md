#### 0.3.0
- HornetQ connects via UDP multicast for clustering
- A concurrent task with a single virtual peer executing no longer implies sequential message processing

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
