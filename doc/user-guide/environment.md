## Environment

In this section, we'll discuss what you need to test up a develop, testing, and production environment.


### Development Environment

#### Dependencies

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


  - Development environment
    - in-VM configuration
  - Test environment
    - Embedded configuration
  - Production environment
    - Link to the HornetQ documentation
    - chef-onyx
    - manual set-up
    - replicating grouping node
    - replication all nodes




In order to host a coordinator or peer on a node, Java 7+ must be installed.
Additionally, a ZooKeeper and HornetQ connection need to be available to all the nodes in the cluster.
See below for the set up each of these services.

#### Chef Recipe

See `chef-onyx`.

#### Manual set up

##### HornetQ 2.4.0-Final

- [Download HornetQ 2.4.0-Final here](http://hornetq.jboss.org/downloads). Grab the .tar.gz.
- Untar the downloaded file.
- Make the following adjustments to `config/stand-alone/non-clustered/hornetq-configuration.xml` `security-settings`. We need to enable permission to create and destroy durable queues:

```xml
<security-settings>
    <security-setting match="#">
        <permission type="createNonDurableQueue" roles="guest"/>
        <permission type="deleteNonDurableQueue" roles="guest"/>
        <permission type="createDurableQueue" roles="guest"/>
        <permission type="deleteDurableQueue" roles="guest"/>
        <permission type="consume" roles="guest"/>
        <permission type="send" roles="guest"/>
    </security-setting>
</security-settings>
```

- Make the following adjustment to `config/stand-alone/non-clustered/hornetq-configuration.xml`. We need to make HornetQ page messages to disk by default:

```xml
<address-settings>
    <address-setting match="#">
        .... more settings here ....
        <address-full-policy>PAGE</address-full-policy>
    </address-setting>
</address-settings>
```

##### ZooKeeper

There's a pretty good [installation guide for ZooKeeper here](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html). No special configuration is needed.


