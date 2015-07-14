(ns onyx.log.commands.peer-replica-view
  (:require [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [onyx.peer.operation :as operation]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defrecord PeerReplicaView [backpressure? active-peers acker-candidates])

(defmethod extensions/peer-replica-view :default [entry old new diff old-view peer-id opts]
  (let [allocations (:allocations new)
        {:keys [job task]} (common/peer->allocated-job allocations peer-id)] 
    (if job 
      (let [peer-state (:peer-state new)
            backpressure? (common/backpressure? new job)
            ;;; these could be filtered down to outgoing nodes only
            receivable-peers (common/job-receivable-peers peer-state allocations job)
            ;; acker peers (TODO: only required when peers are on input task)
            ;; Validate ackable needs to be re-implemented
            max-acker-links (arg-or-default :onyx.messaging/max-acker-links opts)
            job-ackers (get (:ackers new) job)
            acker-candidates (operation/select-n-peers peer-id job-ackers max-acker-links)] 
        (->PeerReplicaView backpressure? receivable-peers acker-candidates))
      (->PeerReplicaView nil nil nil))))
