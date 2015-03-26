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

(def messaging 
  (component/start (aeron/aeron {:opts peer-config})))

(defn bump-forward-immediates [entries peers peer-id]
  (if ((set peers) peer-id)
    entries
    (vec (concat (filter :immediate? entries)
                 (remove :immediate? entries)))))

(defn apply-entry [replica entries entry]
  (let [new-replica (extensions/apply-log-entry entry replica)
        diff (extensions/replica-diff entry replica new-replica)
        peers (cond-> (set (concat (:peers replica)
                                   ; might not need these with the below entries
                                   (vals (or (:prepared replica) {})) 
                                   (vals (or (:accepted replica) {}))))
                (:id (:args entry)) (conj (:id (:args entry)))
                (:joiner (:args entry)) (conj (:joiner (:args entry))))
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

(defn apply-peer-queue-entry [{:keys [replica message-id entries peer-choices log]} next-peer] 
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

(defn peer-gen [replica-state-gen]
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

(defn apply-entry-gen [replica-state-gen]
  (gen/fmap
    (fn [[state peer-id]]
      (apply-peer-queue-entry state peer-id))
    (gen/tuple replica-state-gen
               (peer-gen replica-state-gen))))

(defn apply-entries-gen [replica-state-gen]
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
    [{:keys [replica peer-choices]} 
     (apply-entries-gen 
       (gen/return
         {:replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}
          :message-id 0
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
