## Environment

In this chapter, we'll discuss what you need to test up a develop, testing, and production environment.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Development Environment](#development-environment)
  - [Dependencies](#dependencies)
  - [Explanation](#explanation)
  - [HornetQ](#hornetq)
    - [Environment Launch of In-VM HornetQ](#environment-launch-of-in-vm-hornetq)
    - [Peer Connection to In-VM HornetQ](#peer-connection-to-in-vm-hornetq)
  - [ZooKeeper](#zookeeper)
    - [Environment Launch of In-Memory ZooKeeper](#environment-launch-of-in-memory-zookeeper)
    - [Peer Connection to In-Memory ZooKeeper](#peer-connection-to-in-memory-zookeeper)
  - [Example](#example)
- [Production Environment](#production-environment)
  - [Dependencies](#dependencies-1)
  - [Explanation](#explanation-1)
    - [ZooKeeper clustering](#zookeeper-clustering)
      - [Example](#example-1)
    - [HornetQ UDP multicast clustering mode](#hornetq-udp-multicast-clustering-mode)
      - [Example](#example-2)
    - [HornetQ JGroups clustering mode](#hornetq-jgroups-clustering-mode)
      - [Example](#example-3)
    - [Fault Tolerancy Tuning](#fault-tolerancy-tuning)
- [Test Environment](#test-environment)
  - [Dependencies](#dependencies-2)
  - [Explanation](#explanation-2)
  - [HornetQ](#hornetq-1)
    - [Environment Launch of Embedded HornetQ](#environment-launch-of-embedded-hornetq)
    - [Peer Connection to Embedded HornetQ](#peer-connection-to-embedded-hornetq)
  - [Example](#example-4)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Development Environment

#### Dependencies

- Java 7+
- Clojure 1.6

#### Explanation

One of the primary design goals of Onyx is to make the development environment as close as possible to production - without making the developer run a lot of services locally. A development environment in Onyx merely needs Clojure 1.6 to operate. A ZooKeeper server is spun up in memory via Curator, and a HornetQ server is brought up using the In-VM connector - so you don't have to worry about running either of these things. As an added bonus, the In-VM HornetQ instance starts up and shuts down very quickly.

#### HornetQ

##### Environment Launch of In-VM HornetQ

To launch an In-VM HornetQ server, pass `hornetq/mode :vm` and `hornetq/server? true` to the environment options, and specify `:hornetq.server/type :vm` in the options too.

##### Peer Connection to In-VM HornetQ

Add `:hornetq/mode :vm` to the peer options. That's it - the virtual peers will find the appropriate HornetQ instance to connect to.

#### ZooKeeper

##### Environment Launch of In-Memory ZooKeeper

To launch an in-memory ZooKeeper instance, add `:zookeeper/server? true` to the environment options. Also, specify `:zookeeper.server/port <my port>` so that Curator knows what port to start running the server on.

If your deployment throws an exception and doesn't shut down ZooKeeper, it will remain open. Firing up the environment again will cause a port collision, so be sure to restart your repl in that case.

##### Peer Connection to In-Memory ZooKeeper

Add `:zookeeper/address "127.0.0.1:<my port>"` to the peer options as usual. In-memory Zookeeper is completely opaque to the peer.

#### Example

Here's an example of using both HornetQ In-VM and ZooKeeper in-memory. They're not mutually exclusive - you can run, both, or neither.

```clojure
(def env-config
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2182"
   :zookeeper/server? true
   :zookeeper.server/port 2182
   :onyx/id id})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2182"
   :onyx/id id})
```

### Production Environment
Running a good production Onyx cluster is mostly about running a good HornetQ cluster. To ensure that we're fault tolerant every step of the way, we need a 2+ node HornetQ cluster and a 3-5 node ZooKeeper cluster. I don't recommend running HornetQ in either VM or embedded mode for production as this hurts fault tolerancy significantly. Instead, [download HornetQ 2.4.0-final](http://hornetq.jboss.org/downloads) and configure each server. Stand each server up by running the typical `bin/run.sh` command.

Quick side note when you're starting up HornetQ servers - you might be tempted to copy and paste the entire HornetQ directory to make new nodes in the cluster. That's fine, but remember to *never* copy the `data` directory. The cluster will be super wonky if you do!

#### Dependencies

- Java 7+
- Clojure 1.6
- HornetQ 2.4.0-Final
- ZooKeeper 3.4.5+

#### Explanation

There are a lot of options for how to run HornetQ in the cloud. If you want an educated understand of all of them, I recommend [the HornetQ 2.4.0-Final documentation](http://docs.jboss.org/hornetq/2.4.0.Final/docs/user-manual/html_single/). 

##### ZooKeeper clustering

Running a ZooKeeper cluster is a requirement for a lot of fault tolerant systems. See [this link](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html) for getting set up. I won't go into detail since this is a particularly common set up.

###### Example

Notice that all we're doing is extending the address string to include more host:port pairs.

```clojure
(def peer-opts
  {...
   :zookeeper/address "10.132.8.150:2181,10.132.8.151:2181,10.132.8.152:2181"
   ...})
```

##### HornetQ UDP multicast clustering mode

One option for running a HornetQ cluster is to use UDP multicast for dynamic peer discovery. This is the route that the Onyx suite takes. You can consult the tests and configuration files in the resource path. We'll break down the options below for better understanding.

Note that if you're looking to run Onyx inside a cloud provider like AWS, you're going to need to use JGroups. Cloud providers typically don't allow UDP multicast, which is a limitation that a lot of clustered services face.

###### Example

Let's have a look at the three node cluster that the Onyx test suite uses.

- [Node 1 Configuration](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/resources/hornetq/clustered-1.xml)
- [Node 2 Configutation](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/resources/hornetq/clustered-2.xml)
- [Node 3 Configuration](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/resources/hornetq/clustered-3.xml)

There are a few things to take note of, but otherwise you can and should reuse these configuration files for your own cluster. It'll save you a lot of leg work in understanding HornetQ:

- Note that the ports in `hornetq.remoting.netty.port` and `hornetq.remoting.netty.batch.port`. If you're running 2 or more HornetQ servers on the same machine, you don't want the ports to collide.
- In all nodes, security is turned off via `<security-enabled>false</security-enabled>`. I presume you're running Onyx in a closed, trusted environment.
- In Node 1 *only*, you'll find `<grouping-handler name="onyx">` with `<type>LOCAL</type>`. In all other nodes, you'll find the same grouping handler with `<type>REMOTE</type>`. Only *one* node should have a local handler. All others *must* be remote. This is used for grouping and aggregation in Onyx. You might notice this creates a single point of failure - which we'll fix later on in the Fault Tolerancy Tuning section. You can read more about why this is necessary [in this section of the HornetQ docs](http://docs.jboss.org/hornetq/2.4.0.Final/docs/user-manual/html_single/#d0e5752).

Configure the peer with the details to dynamically discover other HornetQ nodes.

```clojure
(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name hornetq-cluster-name
   :hornetq.udp/group-address hornetq-group-address
   :hornetq.udp/group-port hornetq-group-port
   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
   ...})
```

##### HornetQ JGroups clustering mode

Another option for running HornetQ in clustered mode is to use JGroups. You're going to want to use JGroups if you're running on AWS due to their multicast restrictions. JGroups uses TCP and shared storage, such as S3, for peer discovery.

###### Example

Let's look at a single HornetQ node's configuration. The same advice in the UDP section with respect to replicating the grouping handler applies here:

- [Node 1 Configuration](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/resources/hornetq/jgroups-clustered-1.xml)
- [JGroups file for Node 1](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/resources/onyx-jgroups.xml)

Note: You can see the JGroups local storage being configured in the `FILE_PING` tag. I'd recommend using the S3 discovery mechanism if you're running in AWS.

Configure the peer with the details to dynamically discover other HornetQ nodes.

```clojure
(def peer-opts
  {:hornetq/mode :jgroups
   :hornetq.jgroups/cluster-name hornetq-cluster-name
   :hornetq.jgroups/file jgroups-file
   :hornetq.jgroups/channel-name jgroups-channel
   :hornetq.jgroups/refresh-timeout hornetq-refresh-timeout
   :hornetq.jgroups/discovery-timeout hornetq-discovery-timeout
   ...})
```

##### Fault Tolerancy Tuning

Onyx uses HornetQ to construct data pipelines that offer transactional execution semantics. By doing this, Onyx moves messages between queues on each node in the HornetQ cluster. We run the risk of losing a HornetQ node, and hence losing all of the messages on that box. To defend against this happening, it's typical to create at least one HornetQ replica per HornetQ server in the cluster. There are a lot of ways that you can accomplish this, and it's very much worth your time to read [all of the failover options that HornetQ offers](http://docs.jboss.org/hornetq/2.4.0.Final/docs/user-manual/html_single/#d0e11342). The trade-offs are yours to make.

Remember to always provide a replica for the node running the Local grouping handler! Losing this box in an under-replicated scenario will disallow Onyx from doing grouping operations.

### Test Environment

#### Dependencies

- Java 7+
- Clojure 1.6

#### Explanation

For the most part, you're going to want to write your tests using the options described in the development environment. In the in-memory machine starts up much faster, so you'll get more develoment cycles. One thing you're unable to simulate with that set up, though, is running an actual HornetQ cluster as the underlying messaging system. It's critical to be able to have this if you want to test idempotency of your workflow, or you're using functionality like grouping that behaves a bit differently with a HornetQ cluster.

To handle this, Onyx ships with an embedded option for HornetQ. Embeddeding HornetQ means spinning up one or more servers inside the application. This allows you to run a cluster without having to configure HornetQ outside your project. We use embedded HornetQ for the Onyx test suite for this reason.

#### HornetQ

##### Environment Launch of Embedded HornetQ

To launch one or more embedded HornetQ nodes, pass `:hornetq/server? true` to the environment options, and specify `:hornetq.server/type :embedded` in the options, too. Finally, embedded servers need a real configuration files to operate. Pass `:hornetq.embedded/config <config file 1, config file 2, ...>` to these options as well. The config file names must be available on the classpath.

To jump past writing these configuration files, you can use [the configuration files that Onyx uses in the test suite](https://github.com/MichaelDrogalis/onyx/tree/master/resources/hornetq).

##### Peer Connection to Embedded HornetQ

Embedded HornetQ is invisible to the peer. Simply connect via the normal means as if it were a live cluster. See the example below:

#### Example

Here's an example of using both HornetQ in embedded mode. Notice that we're running ZooKeeper in-memory for convenience:

```clojure
(def env-config
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.udp/cluster-name hornetq-cluster-name
   :hornetq.udp/group-address hornetq-group-address
   :hornetq.udp/group-port hornetq-group-port
   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
   :hornetq.server/type :embedded
   :hornetq.embedded/config ["hornetq/clustered-1.xml" "hornetq/clustered-2.xml" "hornetq/clustered-3.xml"]
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id})

(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name hornetq-cluster-name
   :hornetq.udp/group-address hornetq-group-address
   :hornetq.udp/group-port hornetq-group-port
   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id})
```

