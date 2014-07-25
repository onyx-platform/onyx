### Environment set up

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


