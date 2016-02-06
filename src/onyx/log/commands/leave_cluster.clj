(ns onyx.log.commands.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.log.commands.kill-job :refer [apply-kill-job]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(defn enforce-flux-policy [replica id]
  (let [allocation (common/peer->allocated-job (:allocations replica) id)]
    (if (= (get-in replica [:flux-policies (:job allocation) (:task allocation)]) :kill)
      (apply-kill-job replica (:job allocation))
      replica)))

(s/defmethod extensions/apply-log-entry :leave-cluster :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [{:keys [id]} args
        observer (get (map-invert (:pairs replica)) id)
        transitive (get (:pairs replica) id)
        pair (if (= observer transitive) {} {observer transitive})
        prep-observer (get (map-invert (:prepared replica)) id)
        accep-observer (get (map-invert (:accepted replica)) id)]
    (-> replica
        (enforce-flux-policy id)
        (update-in [:peers] (partial remove #(= % id)))
        (update-in [:peers] vec)
        (update-in [:prepared] dissoc id)
        (update-in [:prepared] dissoc prep-observer)
        (update-in [:accepted] dissoc id)
        (update-in [:accepted] dissoc accep-observer)
        (update-in [:pairs] merge pair)
        (update-in [:pairs] dissoc id)
        (update-in [:pairs] #(if-not (seq pair) (dissoc % observer) %))
        (update-in [:peer-state] dissoc id)
        (update-in [:peer-sites] dissoc id)
        (update-in [:peer-tags] dissoc id)
        (common/remove-peers id)
        (reconfigure-cluster-workload))))

(s/defmethod extensions/replica-diff :leave-cluster :- ReplicaDiff
  [{:keys [args]} old new]
  (let [observer (get (map-invert (:pairs old)) (:id args))
        subject (get (:pairs old) (:id args))]
    {:died (:id args)
     :updated-watch {:observer observer
                     :subject subject}}))

(defn abort? [replica state {:keys [args]}]
  (or (= (:id state) (get (:prepared replica) (:id args)))
      (= (:id state) (get (:accepted replica) (:id args)))))

(s/defmethod extensions/reactions :leave-cluster :- Reactions
  [entry old new diff state]
  (when (abort? old state entry)
    [{:fn :abort-join-cluster
      :args {:id (:id state)}}]))

(s/defmethod extensions/fire-side-effects! :leave-cluster :- State
  [{:keys [args message-id] :as entry} old new {:keys [updated-watch] :as diff} state]
  (if (and (= (:id state) (:id args)) 
           (not (abort? old state entry)))
    (do (close! (:restart-ch state))
        state)
    (let [job (:job (common/peer->allocated-job (:allocations new) (:id state)))]
      (common/start-new-lifecycle
        old new diff
        (cond (common/should-seal? new job state message-id)
              (>!! (:seal-ch (:task-state state)) true)

              (and (= (:id state) (:observer updated-watch))
                   (not= (:observer updated-watch) (:subject updated-watch)))

              (let [ch (chan 1)]
                (extensions/on-delete (:log state) (:subject updated-watch) ch)
                (go (when (<! ch)
                      (extensions/write-log-entry
                        (:log state)
                        {:fn :leave-cluster :args {:id (:subject updated-watch)}
                         :entry-parent message-id
                         :peer-parent (:id state)}))
                    (close! ch))
                (close! (or (:watch-ch state) (chan)))
                (assoc state :watch-ch ch))

              :else state)))))
