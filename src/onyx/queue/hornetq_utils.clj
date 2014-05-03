(ns onyx.queue.hornetq-utils
  (:require [clojure.data.fressian :as fressian]
            [taoensso.timbre :refer [info]])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defn create-queue [session queue-name]
  (try
    (.createQueue session queue-name queue-name true)
    (catch Exception e)))

(defn write-and-cap! [config queue-name messages echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]
    
    (create-queue session queue-name)
    
    (let [producer (.createProducer session queue-name)]
      (.start session)
      (doseq [n (range (count messages))]
        (when (zero? (mod n echo))
          (info (format "[HQ Util] Wrote %s segments" n)))
        (let [message (.createMessage session true)]
          (.writeBytes (.getBodyBuffer message) (.array (fressian/write (nth messages n))))
          (.send producer message)))

      (let [sentinel (.createMessage session true)]
        (.writeBytes (.getBodyBuffer sentinel) (.array (fressian/write :done)))
        (.send producer sentinel))

      (.commit session)
      (.close producer)
      (.close session)
      (.close session-factory)
      (.close locator))))

(defn read! [config queue-name n echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]

    (create-queue session queue-name)
    
    (let [consumer (.createConsumer session queue-name)
          results (atom [])]
      (.start session)
      (doseq [k (range n)]
        (when (zero? (mod k echo))
          (info (format "[HQ Util] Read %s segments" k)))
        (let [message (.receive consumer)]
          (when message
            (.acknowledge message)
            (swap! results conj (fressian/read (.toByteBuffer (.getBodyBuffer message)))))))

      (info "[HQ Util] Done reading")
      (.commit session)
      (.close consumer)
      (.close session)

      @results)))

(defn consume-queue! [config queue-name echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]

    (create-queue session queue-name)

    (let [consumer (.createConsumer session queue-name)
          results (atom [])]
      (.start session)
      (while (not= (last @results) :done)
        (when (zero? (mod (count @results) echo))
          (info (format "[HQ Util] Read %s segments" (count @results))))
        (let [message (.receive consumer)]
          (when message
            (.acknowledge message)
            (swap! results conj (fressian/read (.toByteBuffer (.getBodyBuffer message)))))))

      (info "[HQ Util] Done reading")
      (.commit session)
      (.close consumer)
      (.close session)

      @results)))

