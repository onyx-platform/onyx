(ns onyx.log.generative-peer-join
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.messaging.dummy-messenger :refer [->DummyMessenger]]
            [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.log.helper :refer [job-allocation-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def peer-config (assoc (:peer-config config) 
                        :onyx/id onyx-id
                        :onyx.messaging/impl :dummy-messenger
                        :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def messenger (->DummyMessenger))

(def job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")
(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/ident :core.async/read-from-chan
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :onyx.log.round-robin-multi-job-test/my-inc
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/ident :core.async/write-to-chan
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/round-robin})

(def job-2-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")
(def job-2
  {:workflow [[:d :e] [:e :f]]
   :catalog [{:onyx/name :d
              :onyx/ident :core.async/read-from-chan
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :e
              :onyx/fn :onyx.log.round-robin-multi-job-test/my-inc
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/ident :core.async/write-to-chan
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/round-robin})

(defn generate-join-entries [peer-ids]
  (zipmap peer-ids 
          (map (fn [id] [{:fn :prepare-join-cluster 
                          :immediate? true
                          :args {:peer-site (extensions/peer-site messenger)
                                 :joiner id}}])
               peer-ids)))

(deftest joins
  (checking
    "Checking joins"
    10000
    [{:keys [replica log peer-choices]} 
     (log-gen/apply-entries-gen 
       (gen/return
         {:replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)
                    :messaging peer-config}
          :message-id 0
          :entries (assoc
                     (generate-join-entries [:a :b :c :d :e :f :g :h])
                     :job-1 [(api/create-submit-job-entry job-1-id
                                                          peer-config 
                                                          job-1 
                                                          (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]
                     :job-2 [(api/create-submit-job-entry job-2-id
                                                          peer-config 
                                                          job-2 
                                                          (planning/discover-tasks (:catalog job-2) (:workflow job-2)))])
          :log []
          :peer-choices []}))]
    (is (= (apply + (map count (vals (get (:allocations replica) job-1-id)))) 4))
    (is (= (apply + (map count (vals (get (:allocations replica) job-2-id)))) 4))
    (is (= (:prepared replica) {}))
    (is (= (:accepted replica) {}))
    (is (= (set (keys (:pairs replica)))
           (set (vals (:pairs replica)))))
    (is (= (count (:peers replica)) 8))))
