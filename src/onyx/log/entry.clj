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
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/fire-side-effects! :prepare-join-cluster
  [kw old new diff {:keys [env id]} state]
  (when (= id (:observer diff))
    (let [ch (chan 1)]
      (extensions/on-delete (:log env) (:subject diff) ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log env)
             {:fn :leave-cluster :args {:id (:subject diff)}}))
          (close! ch))
      (assoc state :watch-ch ch))))

(defmethod extensions/reactions :prepare-join-cluster
  [kw old new diff {:keys [id]}]
  (when (= id (:observer diff))
    [{:fn :notify-watchers
      :args {:observer (get (map-invert (:pairs new)) (:subject diff))
             :subject (:observer diff)}}]))

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
        ;;; TODO: What if this peer already died?
        (assoc state :watch-ch ch)))))

(defmethod extensions/apply-log-entry :accept-join-cluster
  [kw {:keys [accepted updated-watch]}]
  (fn [replica message-id]
    (-> replica
        (update-in [:pairs] merge {(:observer accepted) (:subject accepted)})
        (update-in [:pairs] merge {(:observer updated-watch) (:subject updated-watch)})
        (update-in [:accepted] dissoc (:observer accepted))
        (update-in [:peers] conj (:observer accepted)))))

(defmethod extensions/replica-diff :accept-join-cluster
  [kw old new]
  (let [rets (first (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))})))

(defmethod extensions/reactions :accept-join-cluster
  [kw old new diff args]
  [])

(defmethod extensions/fire-side-effects! :accept-join-cluster
  [kw old new diff {:keys [env id]} state]
  (when (= id (:observer (:accepted state)))
    (extensions/flush-outbox (:outbox env))))

