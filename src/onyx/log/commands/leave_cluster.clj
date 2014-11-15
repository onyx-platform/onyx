(ns onyx.log.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :leave-cluster
  [{:keys [args]} replica]
  (let [{:keys [id]} args
        observer (get (map-invert (:pairs replica)) id)
        transitive (get (:pairs replica) id)]
    (-> replica
        (update-in [:peers] (partial remove #(= % id)))
        (update-in [:prepared] dissoc id)
        (update-in [:accepted] dissoc id)
        (update-in [:pairs] merge {observer transitive})
        (update-in [:pairs] dissoc id))))

(defmethod extensions/replica-diff :leave-cluster
  [{:keys [args]} old new]
  (let [observer (get (map-invert (:pairs old)) (:id args))
        subject (get (:pairs old) (:id args))]
    {:died (:id args)
     :updated-watch {:observer observer
                     :subject subject}}))

(defmethod extensions/reactions :leave-cluster
  [entry old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :leave-cluster
  [{:keys [args]} old new {:keys [updated-watch]} state]
  (when (= (:id args) (:observer updated-watch))
    (let [ch (chan 1)]
      (extensions/on-delete (:log state) (:subject updated-watch) ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log state)
             {:fn :leave-cluster :args {:id (:subject updated-watch)}}))
          (close! ch))
      (close! (:watch-ch state))
      ;; TODO: What if this peer already died?
      (assoc state :watch-ch ch))))

