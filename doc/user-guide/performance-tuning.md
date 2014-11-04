## Performance Tuning

This chapter details a few tips for getting your Onyx cluster to run as fast as possible.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Performance Tuning](#performance-tuning)
  - [HornetQ Tuning](#hornetq-tuning)
  - [ZooKeeper](#zookeeper)
  - [Onyx](#onyx)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


### HornetQ Tuning

HornetQ is the number one thing that needs to be tuned correctly go to fast with Onyx. See the section in the [HornetQ docs on performance tuning](http://docs.jboss.org/hornetq/2.4.0.Final/docs/user-manual/html_single/#perf-tuning).

To boil it down to the essentials:

-  Put the HornetQ journal on its own physical volume

HornetQ performs sequential writes, so it's a good idea to let the volume only work on writing HornetQ data. This prevents the drive head from moving unnecessarily. Even on an SSD drive, random-writes are slower than their sequential counterpart.

-  Ensure that HornetQ is running on Linux with AIO enabled

HornetQ's journal is tuned specifically to go extraordinarily fast with AIO.

### ZooKeeper

- Put the ZooKeeper journal on its own physical volume

The same idea applies here as for Onyx. Zookeeper tends to not get a huge amount of traffic, so this probably won't offer a huge performance boost.

### Onyx

- Adjust the number of virtual peers started on each physical node to avoid under-working or pinning the processors.

As always, keep an eye on processor consumption. Increasing by a single virtual peer will add about 10-15 threads to the box, so you're likely going to want a lower number of v-peers per box.

- For small segments, batch multiple segments into a single segment, and treat each new segment as a rolled up batch.

- Use HornetQ's `batch-delay` configuration to increase throughput, at the cost of heightened latency.

