(ns onyx.log.failure-detector
  (:require [clojure.core.async :refer [chan >!! <!! alts!! close! timeout thread]]
            [com.stuartsierra.component :as component]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.log.curator :as zk]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [fatal trace info]]))

;; See Peer Failure Detection Thread section of the Internal Design
;; chapter for the rationale of this component. In short, this component
;; defends against a deadlock that can occur during a specific window
;; of time due to a crashed peer.

(defn monitor-target-group! [log group-id shutdown-ch peer-config]
  (thread
    (try
      (let [interval (arg-or-default :onyx.zookeeper/prepare-failure-detection-interval peer-config)]
        (loop []
          (let [t-ch (timeout interval)
                [v ch] (alts!! [shutdown-ch t-ch] :priority true)]
            (cond (and (= ch t-ch) (not (extensions/group-exists? log group-id)))
                  (extensions/write-log-entry
                   log
                   {:fn :group-leave-cluster :args {:id group-id}
                    :peer-parent group-id})
                  (= ch t-ch)
                  (recur)))))
      (catch Throwable t
        (fatal t "Error in failure detector.")))
    (info "Stopping peer failure detector monitor thread")))

(defrecord FailureDetector [log group-id peer-config]
  component/Lifecycle

  (start [component]
    (info "Starting peer failure detector")
    (let [shutdown-ch (chan)
          monitor-ch (monitor-target-group! log group-id shutdown-ch peer-config)]
      (assoc component :monitor-ch monitor-ch :shutdown-ch shutdown-ch)))

  (stop [component]
    (info "Stopping peer failure detector")
    (close! (:shutdown-ch component))
    (<!! (:monitor-ch component))
    component))

(defn failure-detector [log group-id peer-config]
  (map->FailureDetector {:log log :group-id group-id :peer-config peer-config}))
