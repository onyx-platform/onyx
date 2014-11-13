(ns onyx.log.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :leave-cluster
  [kw {:keys [id]}]
  [{:keys [args]} replica message-id]
  (fn [replica message-id]
    (let [observer (get (map-invert (:pairs replica)) id)
          transitive (get (:pairs replica) id)]
      (-> replica
          (update-in [:peers] (partial remove #(= % id)))
          (update-in [:prepared] dissoc id)
          (update-in [:accepted] dissoc id)
          (update-in [:pairs] merge {observer transitive})
          (update-in [:pairs] dissoc id)))))

(defmethod extensions/replica-diff :leave-cluster
  [kw old new {:keys [id]}]
  (let [observer (get (map-invert (:pairs old)) id)
        subject (get (:pairs old) id)]
    {:died id
     :updated-watch {:observer observer
                     :subject subject}}))

(defmethod extensions/reactions :leave-cluster
  [kw old new diff args]
  [])

(defmethod extensions/fire-side-effects! :leave-cluster
  [kw old new {:keys [updated-watch]} {:keys [env id]} state]
  (when (= id (:observer updated-watch))
    (let [ch (chan 1)]
      (extensions/on-delete (:log env) (:subject updated-watch) ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log env)
             {:fn :leave-cluster :args {:id (:subject updated-watch)}}))
          (close! ch))
      (close! (:watch-ch state))
      ;; TODO: What if this peer already died?
      (assoc state :watch-ch ch))))

