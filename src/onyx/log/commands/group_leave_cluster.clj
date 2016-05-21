(ns onyx.log.commands.group-leave-cluster
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

(s/defmethod extensions/apply-log-entry :group-leave-cluster :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [{:keys [id]} args
        observer (get (map-invert (:pairs replica)) id)
        transitive (get (:pairs replica) id)
        pair (if (= observer transitive) {} {observer transitive})
        prep-observer (get (map-invert (:prepared replica)) id)
        accep-observer (get (map-invert (:accepted replica)) id)
        peers (get-in replica [:groups-index id])]
    (-> replica
        (kill/enforce-flux-policy id)
        (update-in [:groups] (partial remove #(= % id)))
        (update-in [:groups] vec)
        (update-in [:peers] (partial remove #(some #{%} peers)))
        (update-in [:peers] vec)
        (update-in [:orphaned-peers] (partial remove #(some #{%} peers)))
        (update-in [:orphaned-peers] vec)
        (update-in [:prepared] dissoc id)
        (update-in [:prepared] dissoc prep-observer)
        (update-in [:accepted] dissoc id)
        (update-in [:accepted] dissoc accep-observer)
        (update-in [:pairs] merge pair)
        (update-in [:pairs] dissoc id)
        (update-in [:pairs] #(if-not (seq pair) (dissoc % observer) %))
        (update-in [:groups-index] dissoc id)
        (update-in [:peer-state] #(apply (partial dissoc %) peers))
        (update-in [:peer-sites] #(apply (partial dissoc %) peers))
        (update-in [:peer-tags] #(apply (partial dissoc %) peers))
        ((fn [rep] (reduce #(common/remove-peers %1 %2) rep peers)))
        (reconfigure-cluster-workload))))

(s/defmethod extensions/replica-diff :group-leave-cluster :- ReplicaDiff
  [{:keys [args]} old new]
  (let [observer (get (map-invert (:pairs old)) (:id args))
        subject (get (:pairs old) (:id args))]
    {:died (:id args)
     :updated-watch {:observer observer
                     :subject subject}}))

(defn abort? [replica state {:keys [args]}]
  (or (= (:id state) (get (:prepared replica) (:id args)))
      (= (:id state) (get (:accepted replica) (:id args)))))

(s/defmethod extensions/reactions :group-leave-cluster :- Reactions
  [entry old new diff state]
  (when (abort? old state entry)
    [{:fn :abort-join-cluster
      :args {:id (:id state)}}]))

(s/defmethod extensions/multiplexed-entry? :group-join-cluster :- s/Bool
  [_] true)

(s/defmethod extensions/fire-side-effects! :group-leave-cluster :- State
  [{:keys [args message-id] :as entry} old new {:keys [updated-watch] :as diff} state]
  (let [affected-peers (get-in old [:groups-index (:id args)])]
    (cond (and (= (:id state) (:id args)) 
               (not (abort? old state entry)))
          (close! (:restart-ch state))

          (some #{(:id state)} affected-peers)
          (when-let [job (:job (common/peer->allocated-job (:allocations new) (:id state)))]
            (common/should-seal? new job state message-id)
            (>!! (:seal-ch (:task-state state)) true)))
    state))
