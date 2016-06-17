---
layout: user_guide_page
title: FAQ
categories: [user-guide-page]
---



## Frequently Asked Questions / Problems

- [My job's tasks never start.](#tasks-dont-start)
- [Peers repeatedly echo that they're warming up.](#peers-warming-up)
- [None of my messages are being processed.](#no-processed-messages)
- [The same messages are being replayed multiple times.](#message-replay)
- [My program starts running, but then it stalls](#stalled-program)
- [Peer fails to start, and throws `java.io.IOException: No space left on device`](#io-exception)
- [Peer fails to start, and throws `org.apache.bookkeeper.bookie.BookieException$InvalidCookieException: Cookie`](#cookie-exception)
- [Peer fails to start, and throws `java.lang.IllegalStateException: aeron cnc file version not understood`](#cnc-exception)
- [Peer fails to start, and throws `Failed to connect to the Media Driver - is it currently running?`](#failed-media-driver)
- [Peer fails to start, and throws `uk.co.real_logic.aeron.driver.exceptions.ActiveDriverException: active driver detected`](#active-driver-exception)
- [Peer fails to start, and throws `org.apache.bookkeeper.proto.WriteEntryProcessorV3: Error writing entry:X to ledger:Y`](#ledger-exception)
- [My program begins running, but throws `No implementation of method: :read-char of protocol: #'clojure.tools.reader.reader-types/Reader found for class`](#read-char-exception)
- [What does Onyx use internally for compression by default?](#compression)
- [How can I filter segments from being output from my tasks?](#filtering)
- [Can I return more than one segment from a function?](#mapcat)
- [Should I be worried about `user-level KeeperException` in ZooKeeper logs?](#zk-exceptions)
- [How should I benchmark on a single machine?](#benchmarking)

#### <a name="tasks-dont-start"></a>My job's tasks never start

When your job's tasks don't start, you won't see any messages in `onyx.log` on the peer that read something like:

```
Job fb2f2b80-2c5a-41b6-93c8-df35bffe6915 {:job-name :click-stream} - Task da5795a6-bd4c-41a1-843d-dda84cf5c615 :inc - Peer 2b35433a-b935-43b7-881b-4f4ec16672cc - Warming up task lifecycle {:id #uuid "da5795a6-bd4c-41a1-843d-dda84cf5c615", :name :inc, :egress-ids {:out #uuid "2c210c3e-6411-45fb-85a7-6cc733118215"}}
```

Instead, the log would show peers starting, but not doing anything. This looks like something:

```
Starting Virtual Peer 111dcc44-7f53-41a4-8548-047803e8d441
```

##### Resolutions

###### Do you have enough virtual peers provisioned?

When a job is submitted to Onyx, it will only begin when there are enough virtual peers to sustain its execution. How many are *enough*? That depends. One virtual peer may work on *at most* one task at any given time. By default, each task in your workflow requires *one* available virtual peer to run it. If a grouping task is used, or if you set `:onyx/min-peers` on the task's catalog entry, the number may be greater than 1. You can statically determine how many peers a job needs for execution with [this simple helper function](https://github.com/onyx-platform/onyx/blob/c311e4034897d5693e046e8223c66fcbd478312d/src/onyx/test_helper.clj#L33-L41) available in Onyx core.

As an example, if you submit a job with 10 tasks in its workflow, you need *at least* 10 available virtual peers for your job to begin running. If you only have 8, nothing will happen. If you have more than 10 (assuming that 10 is truly the minimum that you need), Onyx may attempt to dedicate more resources to your job beyond 10 peers, depending on which scheduler you used You can control the number of virtual peers that you're running by modifying the parameters to `onyx.api/start-peers`.

We don't emit a warning that your cluster is underprovisioned in terms of number of peers because its masterless design makes this difficult to asynchronously detect. This has been discussed [in an issue](https://github.com/onyx-platform/onyx/issues/452). Onyx is designed to handle under and over provisioning as part of its normal flow, so this is not considered an error condition.

See the [Scheduling chapter](http://www.onyxplatform.org/docs/user-guide/latest/scheduling.html) of the User Guide for a full explanation.

###### Are you using the Greedy job scheduler?

You've hit this case if you have more than one job running concurrently. When you start your peers, you specified a job scheduler through the configuration key `:onyx.peer/job-scheduler`. If you specified `:onyx.job-scheduler/greedy`, you've asked that Onyx dedicate *all* cluster resources to the first submitted job until that job is completed. Then the job is completed, all resources will be assigned to the next job available, and so on. If you want more than one job running at a time, consider using the Balanced (`onyx.job-scheduler/balanced`) or Percentage (`onyx.job-scheduler/percentage`) job schedulers. See the [Scheduling chapter](http://www.onyxplatform.org/docs/user-guide/latest/scheduling.html) of the User Guide for a full explanation.

#### <a name="peers-warming-up"></a>Peers repeatedly echo that they're warming up

If you see something like the following repeatedly being written to `onyx.log`, you're in the right place:

```
Job fb2f2b80-2c5a-41b6-93c8-df35bffe6915 {:job-name :click-stream} - Task 2c210c3e-6411-45fb-85a7-6cc733118215 :out - Peer 3b2c6667-8f41-47a9-ba6b-f97c81ade828 - Peer chose not to start the task yet. Backing off and retrying...
```

What's happening here is that your job's tasks have started execution. Peers have been assigned tasks, and are beginning to get things in order to perform work on them. Before a peer can truly begin running a task, it must do three things:

1. Call all lifecycle hooks registered under `lifecycle/start-task?` sequentially, accruing their results
2. Call the constructor for the input or output plugin, if this is an input or output task.
3. Emit a signal back to ZooKeeper indicating that this peer has opened up all of its sockets to receiving incoming communication from other peers.

The results from the first step are a sequence of boolean values designating whether the peer is allowed to start this task. If *any* of the `start-task?` lifecycle calls return false, the peer sleeps for a short period to back off, then performs step 1 again. This process continues until all lifecycle calls return true. Read the [Lifecycles chapter](http://www.onyxplatform.org/docs/user-guide/latest/lifecycles.html) of the User Guide to learn why it's useful to be repeatedly try and back-off from task launching.

The second step will only occur after step 1 successfully completes - that is, all lifecycle functions return true. The reason you see "Backing off and retrying..." in the logs is because step 1 is being repeated again.

##### Resolutions

###### Do all your `:lifecycle/start-task?` functions eventually return `true`?

If any of your lifecycle hooks for `:lifecycle/start-task?` return `false` or `nil`, you'll want to change them to eventually return `true` based upon some condition.

###### Are the connections that your plugin opens valid?

As per step 2, a plugin's constructor will be called to open any relevant stateful connections, such as a database or socket connection. Many connection calls, such as a JDBC SQL, will block for prolonged periods of time before failing, unless otherwise configured. If the task that appears to be stuck is an input or output task, this is likely the cause. You may want to reconfigure your initial connections to fail faster to make it more obvious as to what's happening.

#### <a name="no-processed-messages"></a>None of my messages are being processed

If you're `onyx.log` shows messages that read as follows, your job's tasks have successfully started:

```
Job fb2f2b80-2c5a-41b6-93c8-df35bffe6915 {:job-name :click-stream} - Task da5795a6-bd4c-41a1-843d-dda84cf5c615 :inc - Peer 2b35433a-b935-43b7-881b-4f4ec16672cc - Enough peers are active, starting the task
```

If you suspect that messages are not being processed, it heavily depends on the input plugin that you're using.

##### Resolutions

###### Try using `:onyx/fn` to log all incoming messages

One thing that you can do for extra visibility is to log all incoming messages from input tasks. This is inadvisable for production, but can be useful for temporary debugging. You can specify an `:onyx/fn` transformation function to *any* task, including inputs and outputs. It can be useful to specify a debug function on your input tasks to see which messages are entering the system. Be sure that this function returns the original segment! For example, you can define a function:

```clojure
(defn spy [segment]
  (println "Read from input:" segment)
  segment)
```

Then add `:onyx/fn ::spy` to your input catalog entries.

###### Has your job been killed?

Unless otherwise configured by your Lifecycles, if any user-level code throws an exception, the job will be killed and is no longer elligible for execution. Check `onyx.log` on all your peers to ensure that no exceptions were thrown. If this were the case, you'd see messages lower in the log reading:

```
Job fb2f2b80-2c5a-41b6-93c8-df35bffe6915 {:job-name :click-stream} - Task 1e83e005-3e2d-4307-907b-a3d66e3aa293 :in - Peer 111dcc44-7f53-41a4-8548-047803e8d441 - Stopping task lifecycle
```

###### Are you using the Kafka input plugin?

If you're using the Kafka input plugin, make sure that you're reading from a reasonable starting offset of the topic. If you've set `:kafka/force-reset?` to `true` in the catalog entry, and you've also set `:kafka/offset-reset` to `:largest`, you've instructed Onyx to begin reading messages from the *end* of the topic. Until you place more messages into the topic, Onyx will sit idle waiting for more input. The starting offset for each input task using Kafka is echoed out in `onyx.log`.

#### <a name="message-replay"></a>The same messages are being replayed multiple times

Message replay happens when a mesage enters Onyx from an input source, gets processed, and is seen again at a later point in time. Onyx replays messages for fault tolerance when it suspects that failure of some sort has occurred. You can read about how message replay is implemented, and why it is exists, in the [Architecture](http://www.onyxplatform.org/docs/user-guide/latest/architecture-low-level-design.html) chapter of the User Guide.

There are many reasons why a message may *need* to be replayed (every possible failure scenario), so we will limit our discussion to controlling replay frequency. See the performance tuning sections of this document for more context about what value is appropriate to set for the replay frequency.

##### Resolutions

###### Is your `:onyx/pending-timeout` too low?

Messages are replayed from the input source if they do not complete their route through the cluster within a particular period of time. This period is controlled by the `:onyx/pending-timeout` parameter to the catalog entry, and it's default is 60 seconds. You can read about its specifics [in the Cheatsheet](http://www.onyxplatform.org/docs/cheat-sheet/latest/#catalog-entry/:onyx/pending-timeout). You should set this value high enough such that any segment taking longer than this value to complete is highly likely to have encountered a failure scenario.

#### <a name="stalled-program"></a> My program starts running, but then it stalls

Programs that begin healthy by processing messages and then stall are out typically indicative of user-level code problems. We outline a few common cases here.

##### Resolutions

###### Does onyx.log have any exceptions in it?

Most exceptions will kill the job in question. If you are simply monitoring progress by reading from an output data source through Onyx, you should check all of the peer `onyx.log` files for exceptions that may have killed the job.

###### Are any user-level functions blocking?

Any implementations of `:onyx/fn` that are blocking will halt progress of all other segments that are directly lined up behind it. Ensure that user level functions finish up in a timely manner.

###### Are messages being replayed?

To get started, see the full section on how and why messages are being replayed. In short, messages will be replayed in 60 seconds if they are not completed. You may be experiencing initial success, followed by a runtime error that is causing temporarily lost segments before replay.

###### Are you using a core.async output plugin?

If you're using a core.async output plugin writing to a channel that will *block* writes when the buffer is full, you have run enough messages to put onto the channel such that core.async writes are now blocking, and hence stalling Onyx.

###### Are your peer hosts and ports advertised correctly?

Ensure that the host and port that the peer advertises to the rest of the cluster for incoming connections is correct. If it is incorrect, only tasks that are colocated on the same machine will have a chance of working. Remember that Onyx uses UDP as its port, so make sure that your security settings are allowing traffic to run through that protocol.

The host is configured via the `:onyx.messaging/bind-addr` key, and the port is configured via the `:onyx.messaging/peer-port` key.

#### <a name="io-exception"></a>Peer fails to start, and throws `java.io.IOException: No space left on device`

This exception commonly occurs when running Onyx inside of a Docker container. Aeron requires more shared memory than the container allocates by default. You can solve this problem by starting your container with a larger amount of shared memory by specifying `--shm-size` on Docker >= 1.10.

#### <a name="cookie-exception"></a>Peer fails to start, and throws `org.apache.bookkeeper.bookie.BookieException$InvalidCookieException: Cookie`

This exception occurs due to a bug in BookKeeper reconnection to ZooKeeper before it's ephemeral node expires. We are currently surveying our own workarounds until this is patched, but for now the thing to do is to delete `/tmp/bookkeeper_journal` and `/tmp/bookkeeper_ledger` on the host. Restart the peer, and all will be well.

#### <a name="cnc-exception"></a>Peer fails to start, and throws `java.lang.IllegalStateException: aeron cnc file version not understood`

This exception occurs when Aeron's version is upgraded or downgraded between incompatible versions. The exception will also provide a path on the OS to some Aeron files. Shutdown the peer, delete that directory, then restart the peer.

#### <a name="failed-media-driver"></a>Peer fails to start, and throws `Failed to connect to the Media Driver - is it currently running?`

This message is thrown when the peer tries to start, but can't engage Aeron in its local environment. Aeron can be run in embedded mode by switching `:onyx.messaging.aeron/embedded-driver?` to `true`, or by running it out of process on the peer machine, which is the recommended production setting. If you're running it out of process, ensure that it didn't go down when you encounter this message. You should run Aeron through a process monitoring tool such as `monit` when running it out of process.

#### <a name="active-driver-exception"></a>Peer fails to start, and throws `uk.co.real_logic.aeron.driver.exceptions.ActiveDriverException: active driver detected`

You have encountered the following exception:

```
uk.co.real_logic.aeron.driver.exceptions.ActiveDriverException: active driver detected
  clojure.lang.ExceptionInfo: Error in component :messaging-group in system onyx.system.OnyxPeerGroup calling #'com.stuartsierra.component/start
```

This is because you have started your peer-group twice without shutting it down. Alternatively, you may be using `:onyx.messaging.aeron/embedded-driver? true` in your peer-group and starting a media driver externally. Only one media driver can be started at a time.

#### <a name="ledger-exception"></a>Peer fails to start, and throws `org.apache.bookkeeper.proto.WriteEntryProcessorV3: Error writing entry:X to ledger:Y`

You have encountered the following exception:

```
2015-12-16 16:59:35 ERROR org.apache.bookkeeper.proto.WriteEntryProcessorV3: Error writing entry:0 to ledger:2
org.apache.bookkeeper.bookie.Bookie$NoLedgerException: Ledger 2 not found
```

Your ZooKeeper directory has been cleared out of information that points to the BookKeeper servers,
and the two processes can't sync up. This can be fixed by removing the data directory from the
BookKeeper servers and ZooKeeper servers.

#### <a name="read-char-exception"></a>My program begins running, but throws `No implementation of method: :read-char of protocol: #'clojure.tools.reader.reader-types/Reader found for class`

You'll encounter this exception when your `:onyx/fn` returns something that is not EDN and Nippy serializable, which is required to send it over the network. Ensure that return values from `:onyx/fn` return either a map, or a vector of maps. All values within must be EDN serializable.

#### <a name="compression"></a>What does Onyx use internally for compression by default?

Unless otherwise overridden in the Peer Pipeline API, Onyx will use [Nippy](https://github.com/ptaoussanis/nippy). This can be override by setting the peer configuration with `:onyx.messaging/compress-fn` and `:onyx.messaging/decompress-fn`. See the Information Model documentation for more information.

#### <a name="filtering"></a>How can I filter segments from being output from my tasks?

Use [Flow Conditions]({{ "/flow-conditions.html" | prepend: page.dir | prepend: site.baseurl }}) or return an empty vector from your `:onyx/fn`.

#### <a name="mapcat"></a>Can I return more than one segment from a function?

Return a vector of maps from `:onyx/fn` instead of a map. All maps at the top level of the vector will be unrolled and pushed downstream.

#### <a name="zk-exceptions"></a>Should I be worried about `user-level KeeperException` in ZooKeeper logs?

You should monitor these, however `KeeperErrorCode = NodeExists` are probably fine:

```
2015-11-05 15:12:51,332 [myid:] - INFO  [ProcessThread(sid:0 cport:-1)::PrepRequestProcessor@645] - Got user-level KeeperException when processing sessionid:0x150d67d0cd10003 type:create cxid:0xa zxid:0x50 txntype:-1 reqpath:n/a Error Path:/onyx/0e14715d-51b9-4e2b-af68-d5292f276afc/windows Error:KeeperErrorCode = NodeExists for /onyx/0e14715d-51b9-4e2b-af68-d5292f276afc/windows
```

This is a peer just trying to recreate a ZooKeeper path that was already created by another peer, and it can be safely ignored.

#### <a name="benchmarking"></a>How should I benchmark on a single machine?

Definitely turn off messaging short circuiting, as messaging short circuiting will improve performance in a way that is unrealistic for multi-node use. Remember to turn messaging short circuiting back on for production use, as it *does* improve performance overall.
