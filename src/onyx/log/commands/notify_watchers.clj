(ns onyx.log.notify-watchers
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :notify-watchers
  [kw args]
  (fn [replica message-id]
    (let [target (get-in replica [:prepared (:subject args)])]
      (-> replica
          (update-in [:accepted] merge {(:subject args) target})
          (update-in [:prepared] dissoc (:subject args))))))

(defmethod extensions/replica-diff :notify-watchers
  [kw old new]
  (let [rets (second (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :notify-watchers
  [kw old new diff {:keys [id]}]
  (let [rotator (get (map-invert (:pairs new)) (:subject diff))]
    (when (= id rotator)
      [{:fn :accept-join-cluster
        :args {:accepted diff
               :updated-watch {:observer rotator
                               :subject (:observer diff)}}}])))

(defmethod extensions/fire-side-effects! :notify-watchers
  [kw old new diff {:keys [env id]} state]
  (let [rotator (get (map-invert (:pairs new)) (:subject diff))]
    (when (= id rotator)
      (let [ch (chan 1)]
        (extensions/on-delete (:log env) (:observer diff) ch)
        (go (when (<! ch)
              (extensions/write-log-entry
               (:log env)
               {:fn :leave-cluster :args {:id (:observer diff)}}))
            (close! ch))
        (close! (:watch-ch state))
        ;; TODO: What if this peer already died?
        (assoc state :watch-ch ch)))))

