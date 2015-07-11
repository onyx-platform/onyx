(ns onyx.log.commands.peer-replica-view
  (:require [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [clj-tuple :as t]))

(defrecord PeerReplicaView [backpressure? active-peers])

(defmethod extensions/peer-replica-view :default [entry old new diff old-view peer-id]
  (let [allocations (:allocations new)
        {:keys [job task]} (common/peer->allocated-job allocations peer-id)] 
    (if job 
      (let [peer-state (:peer-state new)
            backpressure? (common/backpressure? new job)
            ;;; these could be filtered down to outgoing nodes only
            receivable-peers (common/job-receivable-peers peer-state allocations job)] 
        (->PeerReplicaView backpressure? receivable-peers))
      (->PeerReplicaView nil nil))))
