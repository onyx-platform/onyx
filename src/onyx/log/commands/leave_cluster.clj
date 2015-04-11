(ns onyx.log.commands.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(defmethod extensions/apply-log-entry :leave-cluster
  [{:keys [args]} replica]
  (let [{:keys [id]} args
        observer (get (map-invert (:pairs replica)) id)
        transitive (get (:pairs replica) id)
        pair (if (= observer transitive) {} {observer transitive})
        prep-observer (get (map-invert (:prepared replica)) id)
        accep-observer (get (map-invert (:accepted replica)) id)]
    (-> replica
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
        (common/remove-peers (:id args))
        (reconfigure-cluster-workload))))

(defmethod extensions/replica-diff :leave-cluster
  [{:keys [args]} old new]
  (let [observer (get (map-invert (:pairs old)) (:id args))
        subject (get (:pairs old) (:id args))]
    {:died (:id args)
     :updated-watch {:observer observer
                     :subject subject}}))

(defmethod extensions/reactions :leave-cluster
  [{:keys [args]} old new diff state]
  (let [allocation (common/peer->allocated-job (:allocations old) (:id state))
        scheduler (get-in new [:task-schedulers (:job allocation)])]
    (when (or (= (:id state) (get (:prepared old) (:id args)))
              (= (:id state) (get (:accepted old) (:id args))))
      [{:fn :abort-join-cluster
        :args {:id (:id state)}
        :immediate? true}])))

(defmethod extensions/fire-side-effects! :leave-cluster
  [{:keys [message-id args]} old new {:keys [updated-watch] :as diff} state]
  (let [job (:job (common/peer->allocated-job (:allocations new) (:id state)))]
    (common/start-new-lifecycle
     old new diff
     (cond (common/should-seal? new {:job job} state message-id)
           (>!! (:seal-response-ch state) true)

           (and (= (:id state) (:observer updated-watch))
                (not= (:observer updated-watch) (:subject updated-watch)))

           (let [ch (chan 1)]
             (extensions/on-delete (:log state) (:subject updated-watch) ch)
             (go (when (<! ch)
                   (extensions/write-log-entry
                    (:log state)
                    {:fn :leave-cluster :args {:id (:subject updated-watch)}}))
                 (close! ch))
             (close! (or (:watch-ch state) (chan)))
             (assoc state :watch-ch ch))

           :else state))))
