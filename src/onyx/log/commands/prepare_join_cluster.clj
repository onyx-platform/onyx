(ns onyx.log.commands.prepare-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [taoensso.timbre :refer [info] :as timbre]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defn add-peer-site [replica args]
  (assoc-in replica [:peer-sites (:joiner args)] (:peer-site args)))

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
            (-> replica
                (update-in [:prepared] merge {watcher joining-peer})
                (add-peer-site args)))
          replica))
      (-> replica
          (add-peer-site args)))))

(defmethod extensions/replica-diff :prepare-join-cluster
  [entry old new]
  (let [rets (second (diff (:prepared old) (:prepared new)))]
    (assert (<= (count rets) 1))
    (if (seq rets)
      {:observer (first (keys rets))
       :subject (first (vals rets))}
      ;;; FIXME grabbing from peer sites isn't fantastic
      {:self-stitched (first (keys (second (diff (:peer-sites old) (:peer-sites new)))))})))

(defmethod extensions/reactions :prepare-join-cluster
  [entry old new diff peer-args]
  (let [observer (:observer diff)
        joiner (:joiner (:args entry))
        id (:id peer-args)] 
    (cond (and (= id joiner) (nil? diff))
            [{:fn :abort-join-cluster
              :args {:id id}
              :immediate? true}]
          (= id observer)
          [{:fn :notify-join-cluster
            :args {:observer (:subject diff)
                   :subject (or (get (:pairs new) observer)
                                observer)}
            :immediate? true}]
          (and (:self-stitched diff) 
               (= id joiner))
          (do
            (Thread/sleep 10)
            [{:fn :accept-join-cluster
              :args diff
              :site-resources (extensions/assign-site-resources (:messenger peer-args) (:peer-sites new))
              :immediate? true}]))))

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
        :else state))

