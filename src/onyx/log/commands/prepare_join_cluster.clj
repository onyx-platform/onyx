(ns onyx.log.prepare-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :prepare-join-cluster
  [{:keys [args message-id]} replica]
  (let [n (count (:peers replica))]
    (if (> n 0)
      (let [joining-peer (:joiner args)
            all-joined-peers (into #{} (keys (:pairs replica)))
            lone-peer #{(:lone-peer replica)}
            all-prepared-peers #(into {} (keys (:prepared replica)))
            all-prepared-deps (into #{} (vals (:prepared replica)))
            cluster (union all-joined-peers lone-peer)
            candidates (difference cluster all-prepared-deps)
            sorted-candidates (sort (filter identity candidates))]
        (if (seq sorted-candidates)
          (let [index (mod message-id (count sorted-candidates))
                target (nth sorted-candidates index)]
            (update-in replica [:prepared] merge {joining-peer target}))
          replica)))))

(defmethod extensions/replica-diff :prepare-join-cluster
  [entry old new]
  (let [rets (second (diff (:prepared old) (:prepared new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/fire-side-effects! :prepare-join-cluster
  [{:keys [args]} old new diff state]
  (when (= (:id state) (:observer diff))
    (let [ch (chan 1)]
      (extensions/on-delete (:log state) (:subject diff) ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log state)
             {:fn :leave-cluster :args {:id (:subject diff)}}))
          (close! ch))
      (assoc state :watch-ch ch))))

(defmethod extensions/reactions :prepare-join-cluster
  [entry old new diff peer-args]
  (when (= (:id peer-args) (:observer diff))
    [{:fn :notify-watchers
      :args {:observer (get (map-invert (:pairs new)) (:subject diff))
             :subject (:observer diff)}}]))

