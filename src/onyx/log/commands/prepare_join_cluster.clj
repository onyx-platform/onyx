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

(defn still-joining? [replica joiner]
  (or (get (map-invert (:prepared replica)) joiner)
      (get (map-invert (:accepted replica)) joiner)))

(defn already-joined? [replica joiner]
  (some #{joiner} (:peers replica)))

(defn disallowed-candidates [{:keys [peers pairs accepted prepared] :as replica}]
  (let [all-prepared-deps (keys prepared)
        prep-watches (map (fn [dep] (get (map-invert pairs) dep)) all-prepared-deps)
        accepting-deps (keys accepted)]
    (remove nil? (concat all-prepared-deps accepting-deps prep-watches))))

(s/defmethod extensions/apply-log-entry :prepare-join-cluster :- Replica
  [{:keys [args message-id]} :- LogEntry replica]
  (let [peers (:peers replica)
        joiner (:joiner args)
        n (count peers)]
    (if (> n 0)
      (let [all-joined-peers (set (into (keys (:pairs replica)) peers))
            candidates (difference all-joined-peers (set (disallowed-candidates replica)) #{joiner})
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
                    (assoc-in [:peer-tags joiner] (:tags args))
                    (reconfigure-cluster-workload)))
              :else
              replica))
      (-> replica
          (update-in [:peers] conj joiner)
          (update-in [:peers] vec)
          (assoc-in [:peer-state joiner] :idle)
          (assoc-in [:peer-tags joiner] (:tags args))
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
            :args {:id (:id peer-args)
                   :tags (:tags (:args entry))}}]
          (= (:id peer-args) (:observer diff))
          [{:fn :notify-join-cluster
            :args {:observer (:subject diff)}}])))

(s/defmethod extensions/fire-side-effects! :prepare-join-cluster :- State
  [{:keys [args message-id]} :- LogEntry old new diff {:keys [log monitoring] :as state}]
  (let [state-new (cond ;; Handles the cases where all peers are actually dead.
                        ;; This can happen if a single node cluster comes down
                        ;; and is rebooted. We pick a predictably-random peer
                        ;; and knock it down if it's not up. This guarantees
                        ;; progress even if the cluster has experiences total failure.
                        (and (= (:id state) (:joiner args)) (nil? diff))
                        (let [disallowed (distinct (disallowed-candidates new))
                              k (mod message-id (count disallowed))
                              target (nth disallowed k)]
                          (when-not (extensions/peer-exists? log target)
                            (extensions/write-log-entry
                              (:log state)
                              {:fn :leave-cluster :args {:id target}
                               :entry-parent message-id}))
                          state)

                        (= (:id state) (:observer diff))
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
                        state
                        :else state)] 
    (common/start-new-lifecycle old new diff state-new :peer-reallocated)))
