(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout promise-chan dropping-buffer chan close! thread]]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-state :as ms]
            ;[onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn next-messenger-state [messenger old-replica new-replica]
  
  )

(defn start-coordinator! [peer-config allocation-ch shutdown-ch]
  (thread
   (try
    (let [;; Setup messenger
          messenger :MYMESSENGER
          ;; Generate from peer-config
          barrier-period-ms 5000] 
      (loop [prev-replica (onyx.log.replica/starting-replica peer-config)
             messenger messenger]
        (let [timeout-ch (timeout barrier-period-ms)
              [v ch] (alts!! [shutdown-ch timeout-ch allocation-ch])]
          (cond (= ch shutdown-ch)
                (do
                 (println "Falling out of coordinator thread")
                 :shutdown)

                (= ch timeout-ch)
                (do
                 ;; TODO emit barrier
                 (recur messenger prev-replica))
                (= ch allocation-ch)

                ;; re-arranger messenger then emit barrrier then recur
                (let [new-messenger :MYNEXTMESSENGER]
                  (recur v messenger))))))
    (catch Throwable e
      (fatal "Error in coordinator" e)))))

(defprotocol Coordinator
  (start [this])
  (stop [this])
  (emit-barrier [this])
  (next-state [this old-replica new-replica]))

(defrecord PeerCoordinator [peer-config peer-id job-id messenger allocation-ch shutdown-ch coordinator-thread]
  Coordinator
  (start [this] 
    (info "Starting coordinator on:" peer-id)
    (let [allocation-ch (chan (dropping-buffer 1))
          shutdown-ch (promise-chan)] 
      (assoc this 
             :allocation-ch allocation-ch
             :shutdown-ch shutdown-ch
             :coordinator-thread (start-coordinator! peer-config allocation-ch shutdown-ch))))
  (stop [this]
    (info "Stopping coordinator on:" peer-id)
    (some-> shutdown-ch close!)
    (some-> allocation-ch close!)
    (assoc this :allocation-ch nil :shutdown-ch nil :coordinator-thread nil))
  (emit-barrier [this])
  (next-state [this old-replica new-replica]
    (let [started? (= (get-in old-replica [:coordinators job-id]) 
                      peer-id)
          start? (= (get-in new-replica [:coordinators job-id]) 
                    peer-id)]
      (cond-> this
        (and (not started?) start?)
        (start)

        (and started? (not start?))
        (stop)

        (not= (get-in old-replica [:allocation-version job-id])
              (get-in new-replica [:allocation-version job-id]))
        ;; Setup new state 
        ;; Setup new messenger 
        ;; and then emit barrier
        (identity)


        ))))

(defn new-peer-coordinator [peer-config peer-id job-id]
  (map->PeerCoordinator {:peer-config peer-config :peer-id peer-id :job-id job-id}))
