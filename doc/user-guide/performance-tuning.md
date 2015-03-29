## Performance Tuning

This chapter details a few tips for getting your Onyx cluster to run as fast as possible.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Onyx](#onyx)
- [ZooKeeper](#zookeeper)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Onyx

- Use Aeron for message transport instead of Netty, which requires Java 8.
- Adjust the number of virtual peers started on each physical node to avoid underworking or pinning the processors. As always, keep an eye on processor consumption. Increasing by a single virtual peer will add about 5 threads to the box, so you're likely going to want a lower number of v-peers per box.
- For small segments, batch multiple segments into a single segment, and treat each new segment as a rolled up batch.
- Tweak the batch timeout in each catalog entry to trade off increased latency for higher throughput.
- Use a custom compression scheme, rather than Nippy. You can configure custom compression/decompression functions via the peer configuration.

### ZooKeeper

- Put the ZooKeeper journal on its own physical volume

Zookeeper tends to not get a huge amount of traffic, so this probably won't offer a huge performance boost. It's helpful if you're trying to make your processing latency as predictable as possible, though.

