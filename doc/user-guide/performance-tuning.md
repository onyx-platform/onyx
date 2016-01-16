---
layout: user_guide_page
title: Performance Tuning
categories: [user-guide-page]
---

## Performance Tuning

This chapter details a few tips for getting your Onyx cluster to run as fast as possible.

### Onyx

- Use Clojure 1.7. This version of Clojure has some dramatic performance enhancements compared to 1.6.
- Start roughly 1 virtual peer per core per machine.  When using Aeron
  messaging, `# cores = # virtual peers + # subscribers` is a good guideline.
  This recommendation is a good starting point, however may not hold true when
  some virtual peers are largely idle, or spend much of their time I/O blocked.
- Batch size per task of roughly 2000 bytes of segments is a good starting point.
- For small segments, batch multiple segments into a single segment, and treat each new segment as a rolled up batch.
- Tweak the batch timeout in each catalog entry to trade off increased latency for higher throughput.
- Use a custom compression scheme, rather than Nippy. You can configure custom compression/decompression functions via the peer configuration.
- Increase the number of acker peers through the peer configuration as your cluster gets larger
- Tune the number of Aeron subscriber threads, if serialization is a large proportion of work performed in tasks.

### ZooKeeper

- Put the ZooKeeper journal on its own physical volume

Zookeeper tends to not get a huge amount of traffic, so this probably won't offer a huge performance boost. It's helpful if you're trying to make your processing latency as predictable as possible, though.

### Messaging

#### Aeron

Ensure you disable the embedded media driver, and instead use an independent
media driver (see [Media Driver](messaging.md#media-driver)).

When testing performance with a single node using the Aeron messaging layer,
connection short circuiting may cause very misleading results.

The peer-config option [`:onyx.messaging/allow-short-circuit?`](peer-config#onyxmessagingallow-short-circuit),
should be be set to false for realistic performance testing when only a single
node is available for testing. Ensure this option is set to true when operating
in production.

Please refer to the [Aeron messaging section](messaging.md#aeron-messaging) for general
discussion of the Aeron messaging implementation and its characterstics.
