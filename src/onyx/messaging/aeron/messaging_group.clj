(ns onyx.messaging.aeron.messaging-group
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.aeron.embedded-media-driver :as md]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.default-vals :refer [arg-or-default]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn] :as timbre])
  (:import [java.util.concurrent.locks LockSupport]
           [java.util.function Consumer]
           [io.aeron Aeron$Context CommonContext]))

(defn cleanup [ticket-counters short-circuit ks]
  (run! (fn [k] 
          (swap! ticket-counters dissoc k)
          (swap! short-circuit dissoc k))
        ks))

(def gc-sleep-ns (* 100 1000000))

(defn gc-state-loop [ticket-counters short-circuit replica]
  (loop [replica-val @replica old-replica-val @replica] 
    (when-not (Thread/interrupted)
      (->> (:allocation-version old-replica-val)
           (vec)
           (remove (set (vec (:allocation-version replica-val))))
           (cleanup ticket-counters short-circuit))
      (LockSupport/parkNanos gc-sleep-ns)
      (recur @replica replica-val))))

(defn media-driver-healthy? []
  (let [common-context (.conclude (CommonContext.))]
    (try
     (let [driver-timeout-ms (.driverTimeoutMs common-context)
           active? (.isDriverActive common-context
                                    driver-timeout-ms
                                    (reify Consumer
                                      (accept [this log])))]
       active?)
     (finally
      (.close common-context)))))

(defrecord AeronMessengerPeerGroup [peer-config ticket-counters short-circuit replica]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [ticket-counters (atom {})
          short-circuit (atom {})
          replica (atom {})
          embedded-media-driver (component/start (md/->EmbeddedMediaDriver peer-config))
          gc-fut (future (gc-state-loop ticket-counters short-circuit replica))]
      (assoc component
             :gc-fut gc-fut
             :replica replica
             :short-circuit short-circuit
             :ticket-counters ticket-counters
             :embedded-media-driver embedded-media-driver)))

  (stop [{:keys [embedded-media-driver gc-fut] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop embedded-media-driver)
    (some-> gc-fut future-cancel)
    (assoc component 
           :gc-fut nil
           :replica nil
           :short-circuit nil
           :ticket-counters nil
           :embedded-media-driver nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessengerPeerGroup {:peer-config peer-config}))

(defmethod clojure.core/print-method AeronMessengerPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
