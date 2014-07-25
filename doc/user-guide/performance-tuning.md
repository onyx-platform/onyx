### Performance Tuning

- Put the HornetQ journal on its own physical volume
- Ensure that HornetQ is running on Linux with AIO enabled
- Put the ZooKeeper jornal on its own physical volume
- Adjust the number of virtual peers started on each physical node to avoid underworking or pinning the processors.
- For small segments, batch multiple segments into a single segment, and treat each new segment as a rolled up batch.
- Use HornetQ's `batch-delay` configuration to increase throughput, at the cost of heightened latency.
