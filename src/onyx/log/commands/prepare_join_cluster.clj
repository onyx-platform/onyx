(ns onyx.log.commands.prepare-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.peer.operation :as operation]
            [onyx.log.failure-detector :refer [failure-detector]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [taoensso.timbre :refer [info] :as timbre]))

(defn add-site-acker [replica {:keys [joiner peer-site]}]
  (assert (:messaging replica) ":messaging key missing in replica, cannot continue")
  (-> replica
      (assoc-in [:peer-sites joiner]
                (merge
                  peer-site
                  (extensions/assign-acker-resources replica
                                                     joiner
                                                     peer-site
                                                     (:peer-sites replica))))))

(defn still-joining? [replica joiner]
  (or (get (map-invert (:prepared replica)) joiner)
      (get (map-invert (:accepted replica)) joiner)))

(defn already-joined? [replica joiner]
  (some #{joiner} (:peers replica)))

(s/defmethod extensions/apply-log-entry :prepare-join-cluster :- Replica
  [{:keys [args message-id]} :- LogEntry replica]
  (let [peers (:peers replica)
        joiner (:joiner args)
        n (count peers)]
    (if (> n 0)
      (let [all-joined-peers (set (into (keys (:pairs replica)) peers))
            all-prepared-deps (set (keys (:prepared replica)))
            prep-watches (set (map (fn [dep] (get (map-invert (:pairs replica)) dep)) all-prepared-deps))
            accepting-deps (set (keys (:accepted replica)))
            candidates (difference all-joined-peers all-prepared-deps accepting-deps prep-watches #{joiner})
            sorted-candidates (sort (remove nil? candidates))]
        (cond (already-joined? replica joiner)
              replica
              (still-joining? replica joiner)
              replica
              (seq sorted-candidates)
              (let [index (mod message-id (count sorted-candidates))
                    watcher (nth sorted-candidates index)]
                (-> replica
                    (update-in [:prepared] merge {watcher joiner})
                    (add-site-acker args)
                    (reconfigure-cluster-workload)))
              :else
              replica))
      (-> replica
          (update-in [:peers] conj joiner)
          (update-in [:peers] vec)
          (assoc-in [:peer-state joiner] :idle)
          (add-site-acker args)
          (reconfigure-cluster-workload)))))

(s/defmethod extensions/replica-diff :prepare-join-cluster :- ReplicaDiff
  [entry :- LogEntry old new]
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

(s/defmethod extensions/reactions :prepare-join-cluster :- Reactions
  [entry :- LogEntry old new diff peer-args]
  (let [joiner (:joiner (:args entry))] 
    (cond (already-joined? old joiner)
          []
          (still-joining? old joiner)
          []
          (and (= (:id peer-args) joiner) (nil? diff))
          [{:fn :abort-join-cluster
            :args {:id (:id peer-args)}}]
          (= (:id peer-args) (:observer diff))
          [{:fn :notify-join-cluster
            :args {:observer (:subject diff)}}])))

(s/defmethod extensions/fire-side-effects! :prepare-join-cluster :- State
  [{:keys [args message-id]} :- LogEntry old new diff {:keys [monitoring] :as state}]
  (common/start-new-lifecycle
   old new diff
   (cond (= (:id state) (:observer diff))
         (let [ch (chan 1)]
           (extensions/emit monitoring {:event :peer-prepare-join :id (:id state)})
           (extensions/on-delete (:log state) (:subject diff) ch)
           (go (when (<! ch)
                 (extensions/write-log-entry
                  (:log state)
                  {:fn :leave-cluster :args {:id (:subject diff)}
                   :peer-parent (:id state)
                   :entry-parent message-id}))
               (close! ch))
           (assoc state :watch-ch ch))
         ;; Handles the cases where a peer tries to attach to a dead
         ;; peer that hasn't been evicted for whatever reason.
         (= (:id state) (:subject diff))
         (if (not (extensions/peer-exists? (:log state) (:observer diff)))
           (do
             (extensions/write-log-entry
              (:log state)
              {:fn :leave-cluster :args {:id (:observer diff)}
               :peer-parent (:id state)})
             state)
           (let [fd (failure-detector (:log state) (:observer diff) (:opts state))]
             (assoc state :failure-detector (component/start fd))))
         (= (:id state) (:instant-join diff))
         (do (extensions/register-acker (:messenger state)
                                        (get-in new [:peer-sites (:id state)]))
             state)
         :else state)))
