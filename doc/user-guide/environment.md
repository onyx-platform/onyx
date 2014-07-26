## Environment

In this chapter, we'll discuss what you need to test up a develop, testing, and production environment.

### Development Environment

#### Dependencies

- Java 7+
- Clojure 1.6

#### Explanation

One of the primary design goals of Onyx is to make the development environment as close as possible to production - without making the developer run a lot of services locally. A development environment in Onyx merely needs Clojure 1.6 to operate. A ZooKeeper server is spun up in memory via Curator, and a HornetQ server is brought up using the In-VM connector - so you don't have to worry about running either of these things. As an added bonus, the In-VM HornetQ instance starts up and shuts down very quickly.

#### HornetQ

##### Coordinator Launch of In-VM HornetQ

To launch an In-VM HornetQ server, pass `hornetq/server? true` to the coordinator options, and specify `:hornetq.server/type :vm` in the options too. This will run the server as part of the Coordinator. Since we're presumably using a single Coordinator in development mode, this is the ideal entity to also start up the server.

##### Peer Connection to In-VM HornetQ

Add `:hornetq/mode :vm` to the peer options. That's it - the virtual peers will find the appropriate HornetQ instance to connect to.

#### ZooKeeper

##### Coordinator Launch of In-Memory ZooKeeper

To launch an in-memory ZooKeeper instance, add `:zookeeper/server? true` to the coordination options. Also, specify `:zookeeper.server/port <my port>` so that Curator knows what port to start running the server on. As usual, specify `:zookeeper/address "127.0.0.1:<myport>"` so the Coordinator knows what to connect to.

Since we're presumably using a single Coordinator in development mode, this is the ideal entity to also start up the server. Be sure to shut down the Coordinator after every test run when using ZooKeeper. If you're test throws an exception and doesn't shut down ZooKeeper, it will remain open. Firing up the Coordinator again will cause a port collision.

##### Peer Connection to In-Memory ZooKeeper

Add `:zookeeper/address "127.0.0.1:<my port>" to the peer options as usual. In-memory Zookeeper is completely opaque to the peer.

#### Example

Here's an example of using both HornetQ In-VM and ZooKeeper in-memory. They're not mutually exclusive - you can run, both, or neither.

```clojure
(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2182"
   :zookeeper/server? true
   :zookeeper.server/port 2182
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2182"
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))
```

### Test Environment

#### Dependencies

- Java 7+
- Clojure 1.6

#### Explanation

For the most part, you're going to want to write your tests using the options described in the development environment. In the in-memory machine starts up much faster, so you'll get more develoment cycles. One thing you're unable to simulate with that set up, though, is running an actual HornetQ cluster as the underlying messaging system. It's critical to be able to have this if you want to test idempotency of your workflow, or you're using functionality like grouping that behaves a bit differently with a HornetQ cluster.

To handle this, Onyx ships with an embedded option for HornetQ. Embeddeding HornetQ means spinning up one or more servers inside the application. This allows you to run a cluster without having to configure HornetQ outside your project. We use embedded HornetQ for the Onyx test suite for this reason.

#### HornetQ

##### Coordinator Launch of Embedded HornetQ

To launch one or more embedded HornetQ nodes, pass `:hornetq/server? true` to the Coordinator options, and specify `:hornetq.server/type :embedded` in the options, too. Finally, embedded servers need a real configuration files to operate. Pass `:hornetq.embedded/config <config file 1, config file 2, ...>` to these options as well. The config file names must be available on the classpath.

To jump past writing these configuration files, you can use [the configuration files that Onyx uses in the test suite](https://github.com/MichaelDrogalis/onyx/tree/master/resources/hornetq).

To connect to the Coordinator to HornetQ, use the normal means as if it were a live cluster. See the example below for the options that the test suite uses.

##### Peer Connection to Embedded HornetQ

Embedded HornetQ is invisible to the peer. Simply connect via the normal means as if it were a live cluster. See the example below:

#### Example

Here's an example of using both HornetQ in embedded mode. Notice that we're running ZooKeeper in-memory for convenience:

```clojure
(def coord-opts
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
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name hornetq-cluster-name
   :hornetq.udp/group-address hornetq-group-address
   :hornetq.udp/group-port hornetq-group-port
   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))
```



  - Test environment
    - Embedded configuration
  - Production environment
    - Link to the HornetQ documentation
    - chef-onyx
    - manual set-up
    - replicating grouping node
    - replication all nodes

