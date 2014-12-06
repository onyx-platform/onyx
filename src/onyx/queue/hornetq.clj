(ns ^:no-doc onyx.queue.hornetq
    (:require [clojure.string :refer [split]]
              [clojure.data.fressian :as fressian]
              [com.stuartsierra.component :as component]
              [onyx.extensions :as extensions]
              [taoensso.timbre :refer [info]])
    (:import [org.hornetq.api.core SimpleString]
             [org.hornetq.api.core TransportConfiguration]
             [org.hornetq.api.core HornetQQueueExistsException]
             [org.hornetq.api.core HornetQNonExistentQueueException]
             [org.hornetq.api.core DiscoveryGroupConfiguration]
             [org.hornetq.api.core UDPBroadcastGroupConfiguration]
             [org.hornetq.api.core JGroupsBroadcastGroupConfiguration]
             [org.hornetq.api.core.client HornetQClient]
             [org.hornetq.api.core.client ClientRequestor]
             [org.hornetq.api.core.management ManagementHelper]
             [org.hornetq.core.config.impl ConfigurationImpl]
             [org.hornetq.core.remoting.impl.invm InVMAcceptorFactory]
             [org.hornetq.core.remoting.impl.invm InVMConnectorFactory]
             [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]
             [org.hornetq.core.server JournalType]
             [org.hornetq.core.server HornetQServers]
             [org.hornetq.core.server.embedded EmbeddedHornetQ]))

(defmulti connect-to-locator :hornetq/mode)

(defmulti start-server :hornetq.server/type)

(defmulti stop-server (comp :opts :hornetq.server/type))

(defn connect-standalone [host port]
  (let [config {"host" host "port" port}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)]
    (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))))

(defmethod connect-to-locator :standalone
  [{:keys [hornetq.standalone/host hornetq.standalone/port]}]
  (connect-standalone host port))

(defmethod connect-to-locator :vm
  [_]
  (let [tc (TransportConfiguration. (.getName InVMConnectorFactory))]
    (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))))

(defmethod connect-to-locator :udp
  [{:keys [hornetq.udp/cluster-name hornetq.udp/group-address
           hornetq.udp/group-port hornetq.udp/refresh-timeout
           hornetq.udp/discovery-timeout]}]
  (let [udp (UDPBroadcastGroupConfiguration. group-address group-port nil -1)
        gdc (DiscoveryGroupConfiguration. cluster-name refresh-timeout discovery-timeout udp)]
    (HornetQClient/createServerLocatorWithHA gdc)))

(defmethod connect-to-locator :jgroups
  [{:keys [hornetq.jgroups/cluster-name hornetq.jgroups/refresh-timeout
           hornetq.jgroups/discovery-timeout hornetq.jgroups/file
           hornetq.jgroups/channel-name]}]
  (let [jgroups (JGroupsBroadcastGroupConfiguration. file channel-name)
        gdc (DiscoveryGroupConfiguration. cluster-name refresh-timeout discovery-timeout jgroups)]
    (HornetQClient/createServerLocatorWithHA gdc)))

(defmethod start-server :vm
  [_]
  (let [tc (TransportConfiguration. (.getName InVMAcceptorFactory))
        config
        (doto (ConfigurationImpl.)
          (.setJournalDirectory "target/journal")
          (.setJournalType (JournalType/NIO))
          (.setPersistenceEnabled true)
          (.setSecurityEnabled false))]
    (.add (.getAcceptorConfigurations config) tc)

    (let [server (HornetQServers/newHornetQServer config)]
      (.start server)
      server)))

(defmethod start-server :embedded
  [opts]
  (doall
   (pmap
    (fn [path]
      (doto (EmbeddedHornetQ.)
        (.setConfigResourcePath (str (clojure.java.io/resource path)))
        (.start)))
    (:hornetq.embedded/config opts))))

(defmethod start-server :default
  [_] nil)

(defmethod stop-server :vm
  [component]
  (.stop (:server component)))

(defmethod stop-server :embedded
  [component]
  (doseq [server (:server component)]
    (.stop server)))

(defmethod stop-server :default
  [_] nil)

(defn cluster-name [opts]
  (let [k (first (filter #(= (keyword (name %)) :cluster-name) (keys opts)))]
    (get opts k)))

(defn split-host-str [s]
  (let [[h p] (split s #":")]
    [(second (split h #"/")) p]))

(defn initial-connectors [locator]
  (map
   (fn [config]
     (let [params (.getParams config)]
       [(get params "host") (get params "port")]))
   (into [] (.getStaticTransportConfigurations locator))))

(defrecord HornetQConnection [onyx-id opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HornetQ connection")

    (let [server (start-server opts)
          locator (doto (connect-to-locator opts) (.setConsumerWindowSize 0))
          session-factory (.createSessionFactory locator)]
      (assoc component
        :server server
        :locator locator
        :session-factory session-factory
        :cluster-name (cluster-name opts))))

  (stop [{:keys [server locator session-factory] :as component}]
    (taoensso.timbre/info "Stopping HornetQ connection")

    (stop-server opts)
    (.close session-factory)
    (.close locator)
    
    component))

(defn hornetq [onyx-id opts]
  (map->HornetQConnection {:onyx-id onyx-id :opts opts}))

(defmethod extensions/optimize-concurrently HornetQConnection
  [queue event]
  (if (= (:onyx/consumption (:onyx.core/task-map event)) :concurrent)
    (do (.close (:session-factory queue))
        (.close (:locator queue))

        ;;; Start it up again without a 0 sized consumer window.
        (let [locator (connect-to-locator (:opts queue))]
          (assoc queue :locator locator :session-factory (.createSessionFactory locator))))
    queue))

(defmethod extensions/create-tx-session HornetQConnection
  [queue]
  (let [session-factory (:session-factory queue)
        session (.createTransactedSession session-factory)]
    (.start session)
    session))

(defmethod extensions/create-producer HornetQConnection
  [queue session queue-name]
  (extensions/create-queue-on-session queue session queue-name)
  (.createProducer session queue-name))

(defmethod extensions/create-consumer HornetQConnection
  [queue session queue-name]
  (extensions/create-queue-on-session queue session queue-name)
  (.createConsumer session queue-name))

(defmethod extensions/create-queue HornetQConnection
  [queue task]
  (let [session (extensions/create-tx-session queue)
        ingress-queue (vals (:ingress-queues task))
        egress-queues (vals (:egress-queues task))]
    (doseq [queue-name (concat egress-queues ingress-queue)]
      (extensions/create-queue-on-session queue session queue-name))
    (.close session)))

(defmethod extensions/create-queue-on-session HornetQConnection
  [queue session queue-name]
  (try
    (.createQueue session queue-name queue-name true)
    (catch HornetQQueueExistsException e)
    (catch Exception e
      (info e))))

(defmethod extensions/n-messages-remaining HornetQConnection
  [queue session queue-name]
  (let [query (.queueQuery session (SimpleString. queue-name))]
    (.getMessageCount query)))

(defmethod extensions/n-consumers HornetQConnection
  [queue queue-name]
  (if-not (:cluster-name queue)
    (let [session (.createSession (:session-factory queue))
          query (.queueQuery session (SimpleString. queue-name))
          n (.getConsumerCount query)]
      (.close session)
      n)
    (let [session (.createSession (:session-factory queue))
          requestor (ClientRequestor. session "onyx.queue.hornetq.management")
          message (.createMessage session false)
          attr (format "core.clusterconnection.%s" (:cluster-name queue))]
      (ManagementHelper/putAttribute message attr "nodes")
      (.start session)

      (let [reply (.request requestor message)
            result (ManagementHelper/getResult reply)
            host-port-pairs (map split-host-str (vals result))
            host-port-pairs (into #{} (concat host-port-pairs (initial-connectors (:locator queue))))
            locators (map (partial apply connect-standalone) host-port-pairs)
            session-factories (map #(.createSessionFactory %) locators)
            sessions (map (fn [sf] (let [s (.createSession sf)] (.start s) s)) session-factories)
            consumer-counts
            (doall
             (map (fn [s]
                    (.start s)
                    (let [query (.queueQuery s (SimpleString. queue-name))
                          n (.getConsumerCount query)]
                      (.close s)
                      n))
                  sessions))]
        (.close session)
        (doall (map #(.close %) sessions))
        (doall (map #(.close %) session-factories))
        (doall (map #(.close %) locators))
        (apply + consumer-counts)))))

(defmethod extensions/bootstrap-queue HornetQConnection
  [queue task]
  (let [session (extensions/create-tx-session queue)
        producer (extensions/create-producer queue session (:self (:ingress-queues task)))]
    (extensions/produce-message queue producer session (.array (fressian/write {})))
    (extensions/produce-message queue producer session (.array (fressian/write :done)))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue session)))

(def key->property
  {:group "_HQ_GROUP_ID"
   :uuid "_ONYX_UUID"})

(def key->put-fn
  {:group #(.putStringProperty %1 (:group key->property) (str %2))
   :uuid #(.putStringProperty %1 (:uuid key->property) (str %2))})

(defn add-properties [m opts]
  (doseq [[k v] opts]
    ((get key->put-fn k) m v))
  m)

(defmethod extensions/produce-message HornetQConnection
  ([queue producer session msg]
     (let [message (.createMessage session true)]
       (.writeBytes (.getBodyBuffer message) msg)
       (.send producer message)))
  ([queue producer session msg opts]
     (let [message (add-properties (.createMessage session true) opts)]
       (.writeBytes (.getBodyBuffer message) msg)
       (.send producer message))))

(defmethod extensions/consume-message HornetQConnection
  [queue consumer]
  (.receive consumer))

(defmethod extensions/read-message HornetQConnection
  [queue message]
  (fressian/read (.toByteBuffer (.getBodyBuffer message))))

(defmethod extensions/message-uuid HornetQConnection
  [queue message]
  (.getStringProperty message (:uuid key->property)))

(defmethod extensions/ack-message HornetQConnection
  [queue message]
  (.acknowledge message))

(defmethod extensions/commit-tx HornetQConnection
  [queue session]
  (.commit session))

(defmethod extensions/rollback-tx HornetQConnection
  [queue session]
  (.rollback session))

(defmethod extensions/close-resource HornetQConnection
  [queue resource]
  (.close resource))

(defmethod extensions/bind-active-session HornetQConnection
  [queue queue-name]
  (if-not (:cluster-name queue)
    (let [session (.createTransactedSession (:session-factory queue))]
      (.start session)
      session)
    (let [session (.createSession (:session-factory queue))
          requestor (ClientRequestor. session "onyx.queue.hornetq.management")
          message (.createMessage session false)
          attr (format "core.clusterconnection.%s" (:cluster-name queue))]
      (ManagementHelper/putAttribute message attr "nodes")
      (.start session)

      (let [reply (.request requestor message)
            result (ManagementHelper/getResult reply)
            host-port-pairs (map split-host-str (vals result))
            host-port-pairs (into #{} (concat host-port-pairs (initial-connectors (:locator queue))))
            locators (map (partial apply connect-standalone) host-port-pairs)
            session-factories (map #(.createSessionFactory %) locators)
            sessions (map (fn [sf] (let [s (.createSession sf)] (.start s) s)) session-factories)
            counts
            (doall
             (map (fn [s pair]
                    (.start s)
                    (let [query (.queueQuery s (SimpleString. queue-name))
                          c (.getConsumerCount query)
                          m (.getMessageCount query)]
                      (.close s)
                      {:route pair :consumers c :messages m}))
                  sessions host-port-pairs))]
        (.close session)
        (doall (map #(.close %) sessions))
        (doall (map #(.close %) session-factories))
        (doall (map #(.close %) locators))
        (let [active-queues (filter #(> (:messages %) 0) counts)
              pair (or (:route (first (sort-by :consumers < active-queues))) (first host-port-pairs))
              locator (apply connect-standalone pair)
              _ (.setConsumerWindowSize locator 0)
              sf (.createSessionFactory locator)
              s (.createTransactedSession sf)]
          (.start s)
          s)))))

(defmethod extensions/producer->queue-name HornetQConnection
  [queue producer] (.toString (.getAddress producer)))

(def sentinel-byte-array (.limit (fressian/write :done) 32))

(defn take-segments
  ;; Set limit of 32 to match HornetQ's byte buffer. If they don't
  ;; match, hashCode() doesn't work as expected.
  ([f n] (take-segments f n []))
  ([f n rets]
     (if (= n (count rets))
       rets
       (let [segment (f)]
         (if (nil? (:message segment))
           rets
           (let [m (.toByteBuffer (.getBodyBufferCopy (:message segment)))]
             (if (= m sentinel-byte-array)
               (conj rets segment)
               (recur f n (conj rets segment)))))))))

