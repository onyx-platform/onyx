## Backpressure

A common problem with streaming platforms is whether peers and tasks can exhibit
backpressure to upstream tasks.

When Onyx's internal messaging buffers overflow, the oldest segments in the
buffer are discarded i.e. a sliding buffer. While this ensures that the
freshest segments are likely to make it through the entire workflow, and be
fully acked, input segments are still likely to be retried.

One important form of backpressure, is the
[:onyx/max-pending](https://github.com/onyx-platform/onyx/blob/master/doc/user-guide/information-model.md#maps-with-onyxtype-set-to-input-may-optionally-have-these-keys),
task parameter, which may be configured on input tasks. Input tasks will only
produce new segments if there are fewer than max-pending pending (i.e. not
fully acked) input segments. 

One problem with max-pending as a form of backpressure, is that it doesn't take
into account the number of segments produced by the intermediate tasks, nor
whether these segments are filling up the inbound buffers of the later tasks
(due to slow processing, large numbers of produced segments, etc).

Onyx uses a simple scheme to allow backpressure to push back to upstream input
tasks. When a virtual peer fills up past a *high water mark*, the peer writes a
log message to say that its internal buffers are filling up (backpressure-on).
If any peer currently allocated to a job has set backpressure-on, then all
peers allocated to input tasks will stop reading from the input sources.

When a peers messaging buffer is reduced to below a *low water mark*, it writes
a backpressure-off log message. If no peers allocated to a job are currently
set to backpressure, then peers allocated to input tasks will resume reading
from their input sources.

Refer to [Peer Config](peer-config.md) for information regarding the default
backpressure settings, and how to override them.
