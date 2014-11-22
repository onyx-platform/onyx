(ns onyx.log.prepare-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :prepare-join-cluster
  [{:keys [args message-id]} replica]
  (let [n (count (:peers replica))]
    (if (> n 0)
      (let [joining-peer (:joiner args)
            cluster (:peers replica)
            all-joined-peers (into #{} (concat (keys (:pairs replica)) cluster))
            all-prepared-deps (into #{} (keys (:prepared replica)))
            prep-watches (into #{} (map (fn [dep] (get (map-invert (:pairs replica)) dep)) all-prepared-deps))
            accepting-deps (into #{} (keys (:accepted replica)))
            candidates (difference all-joined-peers all-prepared-deps accepting-deps prep-watches)
            sorted-candidates (sort (filter identity candidates))]
        (if (seq sorted-candidates)
          (let [index (mod message-id (count sorted-candidates))
                watcher (nth sorted-candidates index)]
            (update-in replica [:prepared] merge {watcher joining-peer}))
          replica))
      (update-in replica [:peers] conj (:joiner args)))))

(defmethod extensions/replica-diff :prepare-join-cluster
  [entry old new]
  (let [rets (second (diff (:prepared old) (:prepared new)))]
    (assert (<= (count rets) 1))
    (cond (seq rets)
          {:observer (first (keys rets))
           :subject (first (vals rets))}
          (and (not (seq (:peers old))) (seq (:peers new)))
          (let [lone-peer (first (:peers new))]
            (assert (= (count (:peers old)) 0))
            (assert (= (count (:peers new)) 1))
            {:instant-join lone-peer}))))

(defmethod extensions/reactions :prepare-join-cluster
  [entry old new diff peer-args]
  (cond (and (= (:id peer-args) (:joiner (:args entry)))
             (nil? diff))
        [{:fn :abort-join-cluster
          :args {:id (:id peer-args)}
          :immediate? true}]
        (= (:id peer-args) (:observer diff))
        [{:fn :notify-join-cluster
          :args {:observer (:subject diff)
                 :subject (or (get (:pairs new) (:observer diff))
                              (:observer diff))}}]))

(defmethod extensions/fire-side-effects! :prepare-join-cluster
  [{:keys [args]} old new diff state]
  (cond (= (:id state) (:observer diff))
        (let [ch (chan 1)]
          (extensions/on-delete (:log state) (:subject diff) ch)
          (go (when (<! ch)
                (extensions/write-log-entry
                 (:log state)
                 {:fn :leave-cluster :args {:id (:subject diff)}}))
              (close! ch))
          (assoc state :watch-ch ch))
        (= (:id state) (:subject diff))
        (let [ch (chan 1)]
          (extensions/on-delete (:log state) (:observer diff) ch)
          (go (when (<! ch)
                (extensions/write-log-entry
                 (:log state)
                 {:fn :leave-cluster :args {:id (:observer diff)}}))
              (close! ch))
          (assoc state :watch-ch ch))
        (= (:id state) (:instant-join diff))
        (do (doseq [entry (:buffered-outbox state)]
              (>!! (:outbox-ch state) entry))
            (assoc (dissoc state :buffered-outbox) :stall-output? false))))

