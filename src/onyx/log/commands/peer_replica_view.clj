(ns onyx.log.commands.peer-replica-view
  (:require [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [clj-tuple :as t]))

(defrecord PeerReplicaView [backpressure active-peers])

(defmethod extensions/peer-replica-view :default [entry old new diff peer-id]
  ;; This should be smarter about making a more personalised view
  ;; e.g. only calculate receivable peers for job the task is on and for downstream ids
  (let [allocations (:allocations new)
        peer-state (:peer-state new)
        backpressure (into (t/hash-map) 
                           (map (fn [job-id] 
                                  (t/vector job-id 
                                            (common/backpressure? new job-id)))
                                (keys allocations)))
        receivable-peers (into (t/hash-map)
                               (map (fn [[job-id job-allocations]]
                                      (t/vector job-id 
                                                (common/transform-job-allocations peer-state job-allocations)))
                                    allocations))] 
    (->PeerReplicaView backpressure receivable-peers)))
