(ns onyx.hornetq.spike
  (:import [org.hornetq.api.core TransportConfiguration]
           [org.hornetq.api.core.client ClientConsumer]
           [org.hornetq.api.core.client ClientMessage]
           [org.hornetq.api.core.client ClientProducer]
           [org.hornetq.api.core.client ClientSession]
           [org.hornetq.api.core.client ClientSessionFactory]
           [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core.client ServerLocator]
           [org.hornetq.core.config Configuration]
           [org.hornetq.core.config.impl ConfigurationImpl]
           [org.hornetq.core.remoting.impl.invm InVMAcceptorFactory]
           [org.hornetq.core.remoting.impl.invm InVMConnectorFactory]
           [org.hornetq.core.server HornetQServer]
           [org.hornetq.core.server HornetQServers]
           [org.hornetq.core.server.embedded EmbeddedHornetQ]))

(def server (EmbeddedHornetQ.))
(.setConfigResourcePath server)

(.start server)

(def tc (TransportConfiguration. (.getName InVMConnectorFactory)))
(def locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc])))

(def sf (.createSessionFactory locator))


