(ns onyx.log.commands.prepare-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.peer.operation :as operation]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [taoensso.timbre :refer [info] :as timbre]))

(defn add-site [replica {:keys [joiner peer-site]}]
  (assert (:messaging replica) ":messaging key missing in replica, cannot continue")
  (-> replica
      (assoc-in [:peer-sites joiner]
                (merge
                  peer-site
                  (extensions/assign-site-resources replica
                                                    joiner
                                                    peer-site
                                                    (:peer-sites replica))))))

(defmethod extensions/apply-log-entry :prepare-join-cluster
  [{:keys [args message-id]} replica]
  (let [peers (:peers replica)
        n (count peers)]
    (if (> n 0)
      (let [joining-peer (:joiner args)
            all-joined-peers (set (concat (keys (:pairs replica)) peers))
            all-prepared-deps (set (keys (:prepared replica)))
            prep-watches (set (map (fn [dep] (get (map-invert (:pairs replica)) dep)) all-prepared-deps))
            accepting-deps (set (keys (:accepted replica)))
            candidates (difference all-joined-peers all-prepared-deps accepting-deps prep-watches)
            sorted-candidates (sort (remove nil? candidates))]
        (if (seq sorted-candidates)
          (let [index (mod message-id (count sorted-candidates))
                watcher (nth sorted-candidates index)]
            (-> replica
                (update-in [:prepared] merge {watcher joining-peer})
                (add-site args)
                (reconfigure-cluster-workload)))
          replica))
      (-> replica
          (update-in [:peers] conj (:joiner args))
          (update-in [:peers] vec)
          (assoc-in [:peer-state (:joiner args)] :idle)
          (add-site args)
          (reconfigure-cluster-workload)))))

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
                              (:observer diff))}
          :immediate? true}]))

(defmethod extensions/fire-side-effects! :prepare-join-cluster
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  (common/start-new-lifecycle
   old new diff
   (cond (= (:id state) (:observer diff))
         (let [ch (chan 1)]
           (extensions/emit monitoring {:event :peer-prepare-join :id (:id state)})
           (extensions/on-delete (:log state) (:subject diff) ch)
           (go (when (<! ch)
                 (extensions/write-log-entry
                  (:log state)
                  {:fn :leave-cluster :args {:id (:subject diff)}}))
               (close! ch))
           (assoc state :watch-ch ch))
         ;; Handles the cases where a peer tries to attach to a dead
         ;; peer that hasn't been evicted for whatever reason.
         (= (:id state) (:subject diff))
         (if (not (extensions/peer-exists? (:log state) (:observer diff)))
           (do
             (extensions/write-log-entry
              (:log state)
              {:fn :leave-cluster :args {:id (:observer diff)}})
             state)
           state)
         (= (:id state) (:instant-join diff))
         (do (extensions/open-peer-site (:messenger state)
                                        (get-in new [:peer-sites (:id state)]))
             (doseq [entry (:buffered-outbox state)]
               (>!! (:outbox-ch state) entry))
             (assoc (dissoc state :buffered-outbox) :stall-output? false))
         :else state)))
