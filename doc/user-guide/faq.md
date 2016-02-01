---
layout: user_guide_page
title: FAQ
categories: [user-guide-page]
---

## Frequently Asked Questions

##### What does Onyx use internally for compression by default?

Unless otherwise overridden in the Peer Pipeline API, Onyx will use [Nippy](https://github.com/ptaoussanis/nippy). This can be override by setting the peer configuration with `:onyx.messaging/compress-fn` and `:onyx.messaging/decompress-fn`. See the Information Model documentation for more information.

##### Why does my job hang at the repl?

Onyx requires that for each task in a workflow, at least one vpeer is started. For example, the following workflow would require *at least* 4 peers

```clojure
(def workflow
  [[:in :add]
  [:add :inc]
  [:inc :out]])
```

#### How can I filter segments from being output from my tasks?

Use [Flow Conditions]({{ "/flow-conditions.html" | prepend: page.dir | prepend: site.baseurl }}) or return an empty vector from your `:onyx/fn`.


#### uk.co.real_logic.aeron.driver.exceptions.ActiveDriverException: active driver detected

You have encountered the following exception: 

```
uk.co.real_logic.aeron.driver.exceptions.ActiveDriverException: active driver detected
  clojure.lang.ExceptionInfo: Error in component :messaging-group in system onyx.system.OnyxPeerGroup calling #'com.stuartsierra.component/start
```

This is because you have started your peer-group twice without shutting it down. Alternatively, you may be using `:onyx.messaging.aeron/embedded-driver? true` in your peer-group and starting a media driver externally. Only one media driver can be started at a time.


#### Should I be worried about "user-level KeeperException"s in ZooKeeper logs

You should monitor these, however `KeeperErrorCode = NodeExists` are probably fine:

```
2015-11-05 15:12:51,332 [myid:] - INFO  [ProcessThread(sid:0 cport:-1)::PrepRequestProcessor@645] - Got user-level KeeperException when processing sessionid:0x150d67d0cd10003 type:create cxid:0xa zxid:0x50 txntype:-1 reqpath:n/a Error Path:/onyx/0e14715d-51b9-4e2b-af68-d5292f276afc/windows Error:KeeperErrorCode = NodeExists for /onyx/0e14715d-51b9-4e2b-af68-d5292f276afc/windows
```

This is a peer just trying to recreate a ZooKeeper path that was already created by another peer, and it can be safely ignored.

#### How should I benchmark on a single machine

Definitely turn off messaging short circuiting, as messaging short circuiting
will improve performance in a way that is unrealistic for multi-node use.
Remember to turn messaging short circuiting back on for production use, as it
*does* improve performance overall.

#### `org.apache.bookkeeper.proto.WriteEntryProcessorV3: Error writing entry:X to ledger:Y`

You have encountered the following exception:

```
2015-12-16 16:59:35 ERROR org.apache.bookkeeper.proto.WriteEntryProcessorV3: Error writing entry:0 to ledger:2
org.apache.bookkeeper.bookie.Bookie$NoLedgerException: Ledger 2 not found
```

Your ZooKeeper directory has been cleared out of information that points to the BookKeeper servers,
and the two processes can't sync up. This can be fixed by removing the data directory from the
BookKeeper servers and ZooKeeper servers.

