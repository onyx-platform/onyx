## Reliability Guarantees

This chapter discusses the reliability features that you can expect out of Onyx. These features are tuneable and require the developer to make trade-offs.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Transactions](#transactions)
- [Replication](#replication)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Transactions

Onyx offers transactional processing semantics per batch. Messages are moved from one HornetQ server to the other using non-XA transacted sessions. Messages are eligible for replay if there is machine failure in between the time a batch has been read and committed back to storage.

### Replication

In the case of a HornetQ server kicking the bucket, you're likely going to want to be able to recover the messages that were lost and continue processing without it. To defend against data loss, you stand up one or more replicas per HornetQ server instance. This will raise the overall latency per message processed. If you're okay with losing some data for some reason, you can run each server without any replication for as low as possible latency. See the section on running a production Onyx environment for information on staging replicas.

