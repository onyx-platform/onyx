## Error Handling

This chapter describes what options the developer when an error occurs while a virtual peer is executing a task.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Error Handling](#error-handling)
  - [Infinite Retry](#infinite-retry)
  - [Bounded Retry](#bounded-retry)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


### Infinite Retry

Sometimes, an operation during task execution will fail on a particular node. We're not talking about the node crashing, we're talking about an exception being thrown, or a network partition arising. In some cases, you'll want to try to execute the segment again at a later point in time. This can be accomplished by requeuing the segment back onto its ingress queue. Requeuing the segment for retry will insert it *at the back of the queue*. You can see [an example of this here](https://github.com/MichaelDrogalis/onyx-examples/tree/master/error-retry).

In this example, we inject a function into the task that can be executed to requeue the node. If there's a failure, we invoke the function with the segment as a parameter. This segment will be retried infinitely until it succeeds. You'll probably want to bound retries - which are described in the next section.

Note that if Onyx is in batch mode and the sentinel value is on the queue, this sentinel value will get bumped back to the tail of the queue - meaning you don't need to worry about the ordering. Also note that this operation *is* executed in the current running transaction. That means segments move to their respect queues - errors or not - atomically.

### Bounded Retry

Other times, you'll want to retry an operation a preconfigured number of times. Bounded retry is a special case of infinite retry. This can be accomplished by tagging the failing segment with additional data. [Here is an example](https://github.com/MichaelDrogalis/onyx-examples/tree/master/bounded-retry) that retries operations at most 3 times. Since maps are the basic unit of data in Onyx, we can associate extra data onto them without messing up other operations.

