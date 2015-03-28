(ns onyx.log.generative-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.aeron :as aeron]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [clojure.set :refer [intersection]]
            [midje.sweet :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config (assoc (:peer-config config) :onyx/id onyx-id))

(def peer-group (onyx.api/start-peer-group peer-config))

(def messaging 
  (component/start (aeron/aeron {:opts peer-config})))

(defn bump-forward-immediates 
  "If peer hasn't finished joining yet, bump all their 
  log entries to the head of their queue. Order is otherwise stable."
  [entries peers peer-id]
  (if ((set peers) peer-id)
    entries
    (vec (concat (filter :immediate? entries)
                 (remove :immediate? entries)))))

(defn active-peers [replica entry]
  (cond-> (set (concat (:peers replica)
                       ; might not need these with the below entries
                       (vals (or (:prepared replica) {})) 
                       (vals (or (:accepted replica) {}))))
    (:id (:args entry)) (conj (:id (:args entry)))
    (:joiner (:args entry)) (conj (:joiner (:args entry)))))

(defn apply-entry [replica entries entry]
  (let [new-replica (extensions/apply-log-entry entry replica)
        diff (extensions/replica-diff entry replica new-replica)
        peers (active-peers replica entry)
        peer-reactions (keep (fn [peer-id] 
                               (if-let [reactions (extensions/reactions entry 
                                                                        replica 
                                                                        new-replica 
                                                                        diff 
                                                                        {:messenger messaging
                                                                         :id peer-id})]

                                 [peer-id reactions]))
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

(defn peer-gen 
  "Generator to look into all of the peer's write queues
  and pick an entry to get fake written next"
  [replica-state-gen]
  (gen/bind replica-state-gen 
            (fn [state]
              ; we only play back log messages from peers who have
              ; joined, or whose entry is :prepare-join-cluster 
              ; because non-immediate reactions are buffered til join
              (let [peers (set (:peers (:replica state)))] 
                (gen/elements 
                  (map key 
                       (filter (fn [[peer [entry]]]
                                 (or (:immediate? entry)
                                     (contains? peers peer)))
                               (:entries state))))))))

(defn apply-entry-gen 
  "Apply an entry from one of the peers log queues
  to a replica generator "
  [replica-state-gen]
  (gen/fmap
    (fn [[state peer-id]]
      (apply-peer-queue-entry state peer-id))
    (gen/tuple replica-state-gen
               (peer-gen replica-state-gen))))

(defn apply-entries-gen 
  "Recurse over replica generator until entries
  are exhausted. Return the final replica, the log messages 
  in the order they were written and the order of the peers 
  that got to write"
  [replica-state-gen]
  (gen/bind replica-state-gen
            (fn [state]
              (let [g (gen/return state)] 
                (if (empty? (:entries state))
                  g
                  (apply-entries-gen (apply-entry-gen g)))))))

(deftest joins
  (checking
    "Checking joins"
    10000
    [{:keys [replica log peer-choices]} 
     (apply-entries-gen 
       (gen/return
         {:replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)
                    :messaging (:onyx.messaging/config peer-config)}
          :message-id 0
          ; TODO: for scheduling we need a way to supply queues for entries written by non peers
          ; possibly could just check whether they are certain message types 
          ; in peer-gen (rename to queue-select-gen?)
          ; eg. queue with :z [{:fn :submit-job]
          ; if they are in separate queues they could be processed in any order which is what we want
          :entries {:a [{:fn :prepare-join-cluster 
                         :immediate? true
                         :args {:peer-site (extensions/peer-site messaging)
                                :joiner :a}}]
                    :b [{:fn :prepare-join-cluster 
                         :immediate? true
                         :args {:peer-site (extensions/peer-site messaging)
                                :joiner :b}}]
                    :c [{:fn :prepare-join-cluster 
                         :immediate? true
                         :args {:peer-site (extensions/peer-site messaging)
                                :joiner :c}}]
                    :d [{:fn :prepare-join-cluster 
                         :immediate? true
                         :args {:peer-site (extensions/peer-site messaging)
                                :joiner :d}}]}
          :log []
          :peer-choices []}))]
    (is (= (:prepared replica) {}))
    (is (= (:accepted replica) {}))
    (is (= (set (keys (:pairs replica)))
           (set (vals (:pairs replica)))))
    (is (= (count (:peers replica)) 4))))
