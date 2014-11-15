(ns onyx.log.notify-watchers
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :notify-watchers
  [{:keys [args]} replica]
  (let [target (get-in replica [:prepared (:subject args)])]
    (-> replica
        (update-in [:accepted] merge {(:subject args) target})
        (update-in [:prepared] dissoc (:subject args)))))

(defmethod extensions/replica-diff :notify-watchers
  [entry old new]
  (let [rets (second (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :notify-watchers
  [entry old new diff peer-args]
  (let [rotator (or (get (map-invert (:pairs new)) (:subject diff))
                    (:subject diff))]
    (when (= (:id peer-args) rotator)
      [{:fn :accept-join-cluster
        :args {:accepted diff
               :updated-watch {:observer rotator
                               :subject (:observer diff)}}}])))

(defmethod extensions/fire-side-effects! :notify-watchers
  [{:keys [args]} old new diff state]
  (let [rotator (or (get (map-invert (:pairs new)) (:subject diff))
                    (:subject diff))]
    (when (= (:id state) rotator)
      (let [ch (chan 1)]
        (extensions/on-delete (:log state) (:observer diff) ch)
        (go (when (<! ch)
              (extensions/write-log-entry
               (:log state)
               {:fn :leave-cluster :args {:id (:observer diff)}}))
            (close! ch))
        (close! (or (:watch-ch state) (chan)))
        ;; TODO: What if this peer already died?
        (assoc state :watch-ch ch)))))

