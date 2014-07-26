## I built this because I wanted...

- an information model, rather than an API, for describing distributed workflows
- temporal decoupling of workflow descriptions from workflow execution
- an elimination of macros from the API that is offered
- plain Clojure functions as building blocks for application logic
- a very easy way to test distributed workflows locally, without buckets of mocking code
- a decoupled technique for configuring workflows
- transactional, exactly-once semantics for moving data between nodes in a cluster
- transparent code reuse between streaming and batching workflows
- friendlier interfaces to plug into IO for data sources
- aspect orientation without a headache
- to get away from AOT complilation and avoid dependency hell
- heterogenous jar execution for performing rolling releases