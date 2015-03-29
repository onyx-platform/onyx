## Environment

In this chapter, we'll discuss what you need to set up a develop and production environment.

### Development Environment

#### Dependencies

- Java 7+ (Java 8 if you're using Aeron as the message transport)
- Clojure 1.6+

#### Explanation

One of the primary design goals of Onyx is to make the development environment as close as possible to production - without making the developer run a lot of services locally. A development environment in Onyx merely needs Clojure 1.6+ to operate. A ZooKeeper server is spun up in memory via Curator, so you don't need to install ZooKeeper locally if you don't want to.

#### ZooKeeper

##### Environment Launch of In-Memory ZooKeeper

To launch an in-memory ZooKeeper instance, add `:zookeeper/server? true` to the environment options. Also, specify `:zookeeper.server/port <my port>` so that Curator knows what port to start running the server on.

If your deployment throws an exception and doesn't shut down ZooKeeper, it will remain open. Firing up the environment again will cause a port collision, so be sure to restart your repl in that case.

##### Peer Connection to In-Memory ZooKeeper

Add `:zookeeper/address "127.0.0.1:<my port>"` to the peer options as usual. In-memory Zookeeper is completely opaque to the peer.

#### Example

Here's an example of using ZooKeeper in-memory.

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

### Production Environment

Running a good production Onyx cluster requires a multinode ZooKeeper cluster. Otherwise, your configuration will remain exactly the same.

#### Dependencies

- Java 7+ (Java 8 if you're using Aeron)
- Clojure 1.6+
- ZooKeeper 3.4.5+

#### Explanation

Running a ZooKeeper cluster is a requirement for a lot of fault tolerant systems. See [this link](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html) for getting set up. I won't go into detail since this is a particularly common set up.

###### Example

Notice that all we're doing is extending the address string to include more host:port pairs. This uses the standard ZooKeeper connection string, so you can use authentication here too if you need it.

```clojure
(def peer-opts
  {...
   :zookeeper/address "10.132.8.150:2181,10.132.8.151:2181,10.132.8.152:2181"
   ...})
```
