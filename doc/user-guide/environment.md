## Environment

In this chapter, we'll discuss what you need to set up a develop and production environment.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Environment](#environment)
  - [Development Environment](#development-environment)
    - [Dependencies](#dependencies)
    - [Explanation](#explanation)
    - [ZooKeeper](#zookeeper)
      - [Environment Launch of In-Memory ZooKeeper](#environment-launch-of-in-memory-zookeeper)
      - [Peer Connection to In-Memory ZooKeeper](#peer-connection-to-in-memory-zookeeper)
    - [Example](#example)
  - [Production Environment](#production-environment)
    - [Dependencies](#dependencies-1)
    - [Explanation](#explanation-1)
    - [Example](#example-1)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Development Environment

#### Dependencies

- Java 8+
- Clojure 1.7+

#### Explanation

One of the primary design goals of Onyx is to make the development environment as close as possible to production - without making the developer run a lot of services locally. A development environment in Onyx merely needs Clojure 1.7+ to operate. A ZooKeeper server is spun up in memory via Curator, so you don't need to install ZooKeeper locally if you don't want to.

#### Dependencies

- Java 8+
- Clojure 1.7+
- ZooKeeper 3.4.5+

#### Multi-node & Production Checklist

Congratulations! You're going to production, or at least testing your Onyx jobs with a multi-node setup.

We strongly recommend you run through this checklist before you do so, as it will likely save you a lot of time.

- [ ] **Ensure your JVM is running with JVM opts -server** Performance will be greatly decreased if you do not run Onyx via Java without at least `-server` JVM opts.
- [ ] **Disable embedded ZooKeeper**: when `onyx.api/start-env` is called with a config where `:zookeeper/server? true`, it will start an embedded ZooKeeper. `:zookeeper/server?` should be set to `false` in production.
- [ ] **Setup production ZooKeeper**: A full [ZooKeeper ensemble](https://zookeeper.apache.org/) should be used in lieu of the testing ZooKeeper embedded in Onyx.
- [ ] **Increase maximum ZK connections** Onyx establishes a large number of ZooKeeper connections, in which case you will see an exception like the following: `WARN org.apache.zookeeper.server.NIOServerCnxn: Too many connections from /127.0.0.1 - max is 10`. Increase the number of connections in zoo.cfg, via `maxClientCnxns`. This should be set to a number moderately larger than the number of virtual peers that you will start.
- [ ] **Configure ZooKeeper address to point an ensemble**: `:zookeeper/address` should be set in your peer-config e.g. `:zookeeper/address "server1:2181,server2:2181,server3:2181"`.
- [ ] **Ensure all nodes are using the same `:onyx/id`**: `:onyx/id` in the peer-config is used to denote which cluster a virtual peer should join. If all your nodes do not use the same `:onyx/id`, they will not be a part of the same cluster and will not run the same jobs. Any jobs submitted a cluster must also use the same `:onyx/id` to ensure that cluster runs the job.
- [ ] **Do not use core async tasks**: switch all input or output tasks from core.async as it is a not a suitable medium for multi-node use and will result in many issues when used in this way. The [Kafka plugin](https://github.com/onyx-platform/onyx-kafka) is one recommended alternative.
- [ ] **Test on a single node with without short circuiting**: when `:onyx.messaging/allow-short-circuit?` is `true`, Aeron messaging will be short circuited completely. To test messaging on a local mode as if it's in production, set `:onyx.messaging/allow-short-circuit? false`.
- [ ] **Ensure short circuiting is enabled in production**: short circuiting improves performance by locally messaging where possible. Ensure `:onyx.messaging/allow-short-circuit? true` is set in the peer-config on production.
- [ ] **Set messaging bind address**: the messaging layer must be bound to the network interface used by peers to communicate. To do so, set `:onyx.messaging/bind-addr` in peer-config to a string defining the interface's IP. On AWS, this IP can easily be obtained via `(slurp "http://169.254.169.254/latest/meta-data/local-ipv4")`.
- [ ] **Is your bind address external facing?**: If your bind address is something other than the one accessible to your other peers (e.g. docker, without net=host), then you will need to define an external address to advertise. This can be set via `:onyx.messaging/external-addr` in peer-config.
- [ ] **Open UDP ports for Aeron**: Aeron requires the port defined in `:onyx.messaging/peer-port` to be open for UDP traffic.
- [ ] **Setup an external Aeron Media Driver**: If messaging performance is a factor, it is recommended that the Aeron Media Driver is run out of process. First, disable the embedded driver by setting `:onyx.messaging.aeron/embedded-driver? false`. An example out of process media driver is included in [onyx-template](https://github.com/onyx-platform/onyx-template/blob/master/resources/leiningen/new/onyx_app/aeron_media_driver.clj). This media driver can be started via `lein run -m`, or via an uberjar, each by referencing the correct namespace, which contains a main entry point. Ensure that the media driver is started with JVM opts `-server`.
- [ ] **Setup metrics**: when in production, it is essential that you are able to measure retried messages, input message complete latency, throughput and batch latency. Setup Onyx to use [onyx-metrics](https://github.com/onyx-platform/onyx-metrics). We recommend at very least using the timbre logging plugin, which is easy to setup.

#### ZooKeeper

##### Environment Launch of In-Memory ZooKeeper

To launch an in-memory ZooKeeper instance, add `:zookeeper/server? true` to the environment options. Also, specify `:zookeeper.server/port <my port>` so that Curator knows what port to start running the server on.

If your deployment throws an exception and doesn't shut down ZooKeeper, it will remain open. Firing up the environment again will cause a port collision, so be sure to restart your repl in that case.

##### Peer Connection to In-Memory ZooKeeper

Add `:zookeeper/address "127.0.0.1:<my port>"` to the peer options as usual. In-memory Zookeeper is completely opaque to the peer.

#### Example

Here's an example of using ZooKeeper in-memory, with some non-ZooKeeper required parameters elided.

```clojure
(def env-config
  {:zookeeper/address "127.0.0.1:2182"
   :zookeeper/server? true
   :zookeeper.server/port 2182
   :onyx/id id})

(def peer-opts
  {:zookeeper/address "127.0.0.1:2182"
   :onyx/id id})
```
### Networking / Firewall

Messaging requires the *UDP* port to be open for port set `:onyx.messaging/peer-port`.

All peers require the ability to connect to the ZooKeeper instances over TCP.

#### Explanation

Running a ZooKeeper cluster is a requirement for a lot of fault tolerant systems. See [this link](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html) for getting set up. I won't go into detail since this is a particularly common set up. I recommend using [Exhibitor](https://github.com/Netflix/exhibitor) to manage clustered ZooKeeper.

#### Example

Notice that all we're doing is extending the address string to include more host:port pairs. This uses the standard ZooKeeper connection string, so you can use authentication here too if you need it.

```clojure
(def peer-opts
  {...
   :zookeeper/address "10.132.8.150:2181,10.132.8.151:2181,10.132.8.152:2181"
   ...})
```
