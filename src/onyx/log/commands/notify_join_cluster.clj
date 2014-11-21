(ns onyx.log.notify-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :notify-join-cluster
  [{:keys [args]} replica]
  (let [prepared (get (map-invert (:prepared replica)) (:observer args))]
    (-> replica
        (update-in [:accepted] merge {prepared (:observer args)})
        (update-in [:prepared] dissoc prepared))))

(defmethod extensions/replica-diff :notify-join-cluster
  [entry old new]
  (let [rets (second (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :notify-join-cluster
  [entry old new diff peer-args]
  (when (= (:id peer-args) (:subject diff))
    (let [transitive (get (:pairs old) (:observer diff))]
      [{:fn :accept-join-cluster
        :args {:accepted diff
               :updated-watch {:observer (:subject diff)
                               :subject transitive}}}])))

(defmethod extensions/fire-side-effects! :notify-join-cluster
  [{:keys [args]} old new diff state]
  (when (= (:id state) (:subject diff))
    (let [ch (chan 1)
          transitive (get (:pairs old) (:observer diff))]
      (extensions/on-delete (:log state) transitive ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log state)
             {:fn :leave-cluster :args {:id transitive}}))
          (close! ch))
      (close! (or (:watch-ch state) (chan)))
      ;; TODO: What if this peer already died?
      (assoc state :watch-ch ch))))

