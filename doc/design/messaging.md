## Messaging

This design document outlines the new approach that Onyx will take in implementing messaging from 0.6.0 forward.
For a refresher of how Onyx messaging works as of 0.5.0, please refer to the Internal Design section of the User Guide.

### The pros of using a message broker

As of 0.5.0, Onyx uses the HornetQ message broker for communication. There are certainly some benefits to this approach:

- Provides a mechanism for built-in message acknowledgement
- Provides a mechanism for built-in message grouping
- Out of the box load balacing strategies for both the server and the client
- Built-in failover and replication, bugs are HornetQ's problem, not mine

### The cons of using a message broker

- While bugs in HornetQ may be JBoss's problem, they're also sort of mine, because they affect my product
- We write to disk way too much as a result of transactionally moving messages between tasks
- Need to scale the message brokers alongside peers - an operational burden on the developer
- Good fault tolerance requires replication, increasing latency by a constant factor per replica
- Setting up HornetQ is an operational concern by itself
- Configuring HornetQ is hard
- Transactional message movement doesn't matter nearly as much as I thought it might

