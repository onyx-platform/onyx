(ns onyx.queue.hornetq-utils
  (:require [taoensso.timbre :refer [info]]
            [taoensso.nippy :as nippy])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]
           [org.hornetq.api.core SimpleString]))

(defn create-queue [session queue-name]
  (try
    (.createQueue session queue-name queue-name true)
    (catch HornetQQueueExistsException e)
    (catch Exception e
      (.printStackTrace e))))

(defn create-queue! [config queue-name]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]
    (.start session)
    
    (create-queue session queue-name)
    
    (.commit session)
    (.close session)
    (.close session-factory)
    (.close locator)))

(defn write! [config queue-name messages echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]   
    
    (let [producer (.createProducer session queue-name)]
      (.start session)

      (doall
       (map-indexed
        (fn [i x]
          (when (zero? (mod i echo))
            (info (format "Wrote %s segments" i))
            (.commit session))
          (let [message (.createMessage session true)]
            (.writeBytes (.getBodyBuffer message) (nippy/freeze x))
            (.send producer message)))
        messages))

      (.commit session)
      (.close producer)
      (.close session)
      (.close session-factory)
      (.close locator))))

(defn write-and-cap! [config queue-name messages echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]   

    (let [producer (.createProducer session queue-name)]
      (.start session)

      (doall
       (map-indexed
        (fn [i x]
          (when (zero? (mod i echo))
            (info (format "Wrote %s segments" i))
            (.commit session))
          (let [message (.createMessage session true)]
            (.writeBytes (.getBodyBuffer message) (nippy/freeze x))
            (.send producer message)))
        messages))

      (let [sentinel (.createMessage session true)]
        (.writeBytes (.getBodyBuffer sentinel) (nippy/freeze :done))
        (.send producer sentinel)
        (.commit session))
      
      (.close producer)
      (.close session)
      (.close session-factory)
      (.close locator))))

(defn read! [config queue-name n echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]

    (let [consumer (.createConsumer session queue-name)
          results (atom [])]
      (.start session)
      (doseq [k (range n)]
        (when (zero? (mod k echo))
          (info (format "Read %s segments" k))
          (.commit session))
        (let [message (.receive consumer)]
          (when message
            (.acknowledge message)
            (swap! results conj (nippy/thaw (.toByteBuffer (.getBodyBuffer message)))))))

      (info "Done reading")
      (.commit session)
      (.close consumer)
      (.close session)
      (.close session-factory)
      (.close locator)

      @results)))

(defn consume-queue! [config queue-name echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]

    (let [consumer (.createConsumer session queue-name)
          results (atom [])]
      (.start session)
      (loop [last-message nil]
        (when (not= last-message :done)
          (when (zero? (mod (count @results) echo))
            (info (format "Read %s segments" (count @results)))
            (.commit session))
          (let [message (.receive consumer)]
            (when message
              (let [decompressed (nippy/thaw (.toByteBuffer (.getBodyBuffer message)))]
                (.acknowledge message)
                (swap! results conj decompressed)
                (recur decompressed))))))

      (info "Done reading")
      (.commit session)
      (.close consumer)
      (.close session)
      (.close session-factory)
      (.close locator)

      @results)))

(defn message-count [config queue-name]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)
        query (.queueQuery session (SimpleString. queue-name))]
    (.getMessageCount query)))

