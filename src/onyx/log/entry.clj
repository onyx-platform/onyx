(ns onyx.log.entry
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defn create-log-entry [kw args]
  {:fn kw :args args})

(defmethod extensions/apply-log-entry :prepare-join-cluster
  [kw args]
  (fn [replica message-id]
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
            replica))))))

(defmethod extensions/replica-diff :prepare-join-cluster
  [kw old new]
  (let [rets (second (diff (:prepared old) (:prepared new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:watching (first (keys rets))
       :watched (first (vals rets))})))

(defmethod extensions/fire-side-effects! :prepare-join-cluster
  [kw old new diff {:keys [env id]}]
  (when (= id (:watching diff))
    (let [ch (chan 1)]
      (extensions/on-delete (:log env) (:watched diff) ch)
      (go (<! ch)
          (extensions/write-log-entry
           (:log env)
           {:fn :leave-cluster :args {:id (:watched diff)}})
          (close! ch)))))

(defmethod extensions/reactions :prepare-join-cluster
  [kw old new diff {:keys [id]}]
  (when (= id (:watching diff))
    [{:fn :notify-watchers
      :args {:watching (get (map-invert (:pairs new)) (:watched diff))
             :watched (:watching diff)}}]))

(defmethod extensions/apply-log-entry :notify-watchers
  [kw args]
  (fn [replica message-id]
    (let [target (get-in replica [:prepared (:watched args)])]
      (-> replica
          (update-in [:accepted] merge {(:watched args) target})
          (update-in [:prepared] dissoc (:watched args))))))

(defmethod extensions/replica-diff :notify-watchers
  [kw old new]
  (let [rets (second (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:watching (first (keys rets))
       :watched (first (vals rets))})))

(defmethod extensions/reactions :notify-watchers
  [kw old new diff {:keys [id]}]
  (let [rotator (get (map-invert (:pairs new)) (:watched diff))]
    (when (= id rotator)
      [{:fn :accept-join-cluster
        :args {:accepted diff
               :updated-watch {:watching rotator
                               :watched (:watching diff)}}}])))

(defmethod extensions/fire-side-effects! :notify-watchers
  [kw old new diff {:keys [env id]}]
  (let [rotator (get (map-invert (:pairs new)) (:watched diff))]
    (when (= id rotator)
      (let [ch (chan 1)]
        (extensions/on-delete (:log env) (:watching diff) ch)
        ;;; TODO: Remove watch from other node
        ;;; TODO: What if this peer already died?
        (go (<! ch)
            (extensions/write-log-entry
             (:log env)
             {:fn :leave-cluster :args {:id (:watching diff)}})
            (close! ch))))))

