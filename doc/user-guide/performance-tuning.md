## Performance Tuning

This chapter details a few tips for getting your Onyx cluster to run as fast as possible.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Onyx](#onyx)
- [ZooKeeper](#zookeeper)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Onyx

- Use Clojure 1.7. This version of Clojure has some dramatic performance enhancements compared to 1.6.
- Start roughly 1 virtual peer per core per machine.
- Set your batch size per task to roughly 100 bytes of segments.
- For small segments, batch multiple segments into a single segment, and treat each new segment as a rolled up batch.
- Tweak the batch timeout in each catalog entry to trade off increased latency for higher throughput.
- Use a custom compression scheme, rather than Nippy. You can configure custom compression/decompression functions via the peer configuration.
- Increase the number of acker peers through the peer configuration as your cluster gets larger

### ZooKeeper

- Put the ZooKeeper journal on its own physical volume

Zookeeper tends to not get a huge amount of traffic, so this probably won't offer a huge performance boost. It's helpful if you're trying to make your processing latency as predictable as possible, though.
