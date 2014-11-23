(ns onyx.log.commands.notify-join-cluster
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
      {:observer (first (vals rets))
       :subject (get-in old [:pairs (first (keys rets))])
       :accepted-observer (first (keys rets))
       :accepted-joiner (first (vals rets))})))

(defmethod extensions/reactions :notify-join-cluster
  [entry old new diff peer-args]
  (when (= (:id peer-args) (:observer diff))
    [{:fn :accept-join-cluster
      :args diff
      :immediate? true}]))

(defmethod extensions/fire-side-effects! :notify-join-cluster
  [{:keys [args]} old new diff state]
  (if (= (:id state) (:observer diff))
    (let [ch (chan 1)]
      (extensions/on-delete (:log state) (:subject diff) ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log state)
             {:fn :leave-cluster :args {:id (:subject diff)}}))
          (close! ch))
      (close! (or (:watch-ch state) (chan)))
      (assoc state :watch-ch ch))
    state))

