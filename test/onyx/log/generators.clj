(ns onyx.log.generators
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.messaging.dummy-messenger :refer [->DummyMessenger]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]))

(def messenger (->DummyMessenger))

(defn bump-forward-immediates 
  "If peer hasn't finished joining yet, bump all their 
  log entries to the head of their queue. Order is otherwise stable."
  [entries peers peer-id]
  (if ((set peers) peer-id)
    entries
    (vec (concat (filter :immediate? entries)
                 (remove :immediate? entries)))))

(defn peerless-entry? [log-entry]
  (#{:submit-job :kill-job :gc} (:fn log-entry)))

(defn active-peers [replica entry]
  (cond-> (set (concat (:peers replica)
                       ; might not need these with the below entries
                       (vals (or (:prepared replica) {})) 
                       (vals (or (:accepted replica) {}))))
    (and (not (peerless-entry? entry))
         (:id (:args entry))) (conj (:id (:args entry)))
    (and (not (peerless-entry? entry))
         (:joiner (:args entry))) (conj (:joiner (:args entry)))))

(defn apply-entry [replica entries entry]
  (let [new-replica (extensions/apply-log-entry entry replica)
        diff (extensions/replica-diff entry replica new-replica)
        peers (active-peers new-replica entry)
        peer-reactions (keep (fn [peer-id] 
                               (if-let [reactions (extensions/reactions entry 
                                                                        replica 
                                                                        new-replica 
                                                                        diff 
                                                                        {:messenger messenger
                                                                         :id peer-id})]
                                 (when (seq reactions)
                                   [peer-id reactions])))
                             peers)
        ; it does not matter that multiple reactions are processed
        ; together because they may be processed interleaved depending on 
        ; the choice of peer queue being popped
        unapplied (reduce (fn [new-entries [peer-id reactions]]
                            (-> new-entries 
                                (update-in [peer-id] vec)
                                (update-in [peer-id] into reactions)
                                (update-in [peer-id] bump-forward-immediates (:peers replica) peer-id)))
                          entries
                          peer-reactions)]
    (vector new-replica unapplied)))


(defn apply-peer-queue-entry 
  "Applies the next log message in the selected peer's queue.
  Effectively, the next peer that wrote its message to ZK"
  [{:keys [replica message-id entries peer-choices log]} next-peer] 
  (let [peer-queue (entries next-peer)
        next-entry (first peer-queue)
        new-peer-queue (vec (rest peer-queue))
        new-entries (if (empty? new-peer-queue)
                      (dissoc entries next-peer)
                      (assoc entries next-peer new-peer-queue))
        message (assoc next-entry :message-id message-id)
        [new-replica updated-entries] (apply-entry replica new-entries message)] 
    {:replica new-replica
     :message-id (inc message-id)
     :entries updated-entries
     :log (conj log message)
     :peer-choices (conj peer-choices next-peer)}))

(defn queue-select-gen 
  "Generator to look into all of the peer's write queues
  and pick an entry to get fake written next"
  [replica-state-gen]
  (gen/bind replica-state-gen 
            (fn [state]
              ; we only play back log messages from peers who have
              ; joined, or whose entry is :prepare-join-cluster 
              ; because non-immediate reactions are buffered til join
              (let [peerless-queues (->> (:entries state)
                                         (filter (fn [[queue-id queue]] (peerless-entry? (first queue))))
                                         (map key))
                    joined-peers (set (:peers (:replica state)))
                    selectable-peers (->> (:entries state)
                                          (filter (fn [[peer [entry]]]
                                                    (or (:immediate? entry)
                                                        (contains? joined-peers peer))))
                                          (map key)
                                          set)
                    selectable-queues (into selectable-peers peerless-queues)] 
                (when (empty? selectable-queues)
                  (println "No playable log messages. State: " state))
                (gen/elements selectable-queues)))))

(defn apply-entry-gen 
  "Apply an entry from one of the peers log queues
  to a replica generator "
  [replica-state-gen]
  (gen/fmap
    (fn [[state peer-id]]
      (apply-peer-queue-entry state peer-id))
    (gen/tuple replica-state-gen
               (queue-select-gen replica-state-gen))))

(defn apply-entries-gen 
  "Recurse over replica generator until entries
  are exhausted. Return the final replica, the log messages 
  in the order they were written and the order of the peers 
  that got to write"
  [replica-state-gen]
  (gen/bind replica-state-gen
            (fn [state]
              (let [g (gen/return state)] 
                (when (> (count (:log state))
                         1000)
                  (throw (Exception. (str "Log entry generator overflow. Likely issue with uncompletable log\n" state))))
                (if (empty? (:entries state))
                  g
                  (apply-entries-gen (apply-entry-gen g)))))))
