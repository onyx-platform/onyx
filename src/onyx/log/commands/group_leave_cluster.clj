(ns onyx.log.commands.group-leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn fatal error]]
            [onyx.log.commands.common :as common]
            [onyx.log.commands.kill-job :as kill]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(defn deallocated-replica [args replica]
  (let [{:keys [id]} args
        observer (get (map-invert (:pairs replica)) id)
        transitive (get (:pairs replica) id)
        pair (if (= observer transitive) {} {observer transitive})
        prep-observer (get (map-invert (:prepared replica)) id)
        accep-observer (get (map-invert (:accepted replica)) id)
        active-prep (get (:prepared replica) id)
        active-accep (get (:accepted replica) id)
        peers (get-in replica [:groups-index id])]
    (-> replica
        (kill/enforce-flux-policy id)
        (update-in [:groups] (partial remove #(= % id)))
        (update-in [:groups] vec)
        (update-in [:peers] (partial remove #(some #{%} peers)))
        (update-in [:peers] vec)
        (update-in [:orphaned-peers] dissoc id)
        (update-in [:prepared] dissoc id)
        (update-in [:prepared] dissoc prep-observer)
        (update-in [:accepted] dissoc id)
        (update-in [:accepted] dissoc accep-observer)
        ;; We need to add to :aborted in case any
        ;; virtual peers try to get added to the cluster
        ;; between this entry and when the abort entry
        ;; actually gets executed.
        (update-in [:aborted] (fnil conj #{}) active-prep active-accep)
        (update-in [:aborted] #(set (remove nil? %)))
        (update-in [:pairs] merge pair)
        (update-in [:pairs] dissoc id)
        (update-in [:pairs] #(if-not (seq pair) (dissoc % observer) %))
        (update-in [:left] conj id)
        (update-in [:groups-index] dissoc id)
        (update-in [:groups-reverse-index] #(apply (partial dissoc %) peers))
        (update-in [:peer-sites] #(apply (partial dissoc %) peers))
        (update-in [:peer-tags] #(apply (partial dissoc %) peers))
        ((fn [rep] (reduce #(common/remove-peers %1 %2) rep peers))))))

(s/defmethod extensions/apply-log-entry :group-leave-cluster :- Replica
  [{:keys [args]} :- LogEntry replica]
  (reconfigure-cluster-workload (deallocated-replica args replica) replica))

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

(s/defmethod extensions/reactions [:group-leave-cluster :group] :- Reactions
  [entry old new diff state]
  (when (abort? old state entry)
    [{:fn :abort-join-cluster
      :args {:id (:id state)}}]))

(s/defmethod extensions/fire-side-effects! [:group-leave-cluster :peer] :- State
  [{:keys [args message-id] :as entry} old new {:keys [updated-watch] :as diff} state]
  (let [affected-peers (get-in old [:groups-index (:id args)])]
    (common/start-new-lifecycle old new diff state :peer-reallocated)))

(s/defmethod extensions/fire-side-effects! [:group-leave-cluster :group] :- State
  [{:keys [args message-id] :as entry} old new {:keys [updated-watch] :as diff} state]
  (cond (and (= (:id state) (:id args)) 
             (not (abort? old state entry)))
        (do (>!! (:group-ch state) [:restart-peer-group (:id args)])
            state)

        (and (= (:id state) (:observer updated-watch))
             (not= (:observer updated-watch) (:subject updated-watch)))

        (let [ch (chan 1)]
          (extensions/on-delete (:log state) (:subject updated-watch) ch)
          (go (when (<! ch)
                (extensions/write-log-entry
                 (:log state)
                 {:fn :group-leave-cluster :args {:id (:subject updated-watch)}
                  :peer-parent (:id state)
                  :entry-parent message-id}))
              (close! ch))
          (close! (or (:watch-ch state) (chan)))
          (assoc state :watch-ch ch))

        :else 
        state))
