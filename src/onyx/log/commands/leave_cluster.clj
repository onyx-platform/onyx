(ns onyx.log.commands.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.log.commands.kill-job :as kill]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(s/defmethod extensions/apply-log-entry :leave-cluster :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [{:keys [id]} args
        observer (get (map-invert (:pairs replica)) id)
        transitive (get (:pairs replica) id)
        pair (if (= observer transitive) {} {observer transitive})
        prep-observer (get (map-invert (:prepared replica)) id)
        accep-observer (get (map-invert (:accepted replica)) id)]
    (-> replica
        (kill/enforce-flux-policy id)
        (update-in [:peers] (partial remove #(= % id)))
        (update-in [:peers] vec)
        (update-in [:orphaned-peers] (partial remove #(= % id)))
        (update-in [:orphaned-peers] vec)
        (update-in [:peer-state] dissoc id)
        (update-in [:peer-sites] dissoc id)
        (update-in [:peer-tags] dissoc id)
        (update-in [:groups-index (:group-id args)] disj id)
        (common/remove-peers id)
        (reconfigure-cluster-workload))))

(s/defmethod extensions/replica-diff :leave-cluster :- ReplicaDiff
  [{:keys [args]} old new]
  {:died (:id args)})

(s/defmethod extensions/reactions :leave-cluster :- Reactions
  [entry old new diff state]
  [])

(s/defmethod extensions/fire-side-effects! :leave-cluster :- State
  [entry old new diff state]
  state)
