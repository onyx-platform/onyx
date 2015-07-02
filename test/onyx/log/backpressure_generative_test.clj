(ns onyx.log.backpressure-generative-test
  (:require [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def onyx-id (java.util.UUID/randomUUID))

(def peer-config 
  {:onyx/id onyx-id
   :onyx.messaging/impl :dummy-messenger})

(def messenger (dummy-messenger {}))

(def job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/ident :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/ident :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-2-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")

(def job-2
  {:workflow [[:d :e] [:e :f]]
   :catalog [{:onyx/name :d
              :onyx/ident :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :e
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/ident :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-3-id #uuid "58d199e8-4ea4-4afd-a112-945e97235924")

(def job-3
  {:workflow [[:g :h] [:h :i]]
   :catalog [{:onyx/name :g
              :onyx/ident :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :h
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :i
              :onyx/ident :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest simple-backpressure
  (checking
    "Checking backpressure handled correctly"
    (times 50)
    [{:keys [entries replica log peer-choices]} 
     (log-gen/apply-entries-gen 
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (-> (log-gen/generate-join-queues (log-gen/generate-peer-ids 12))
                       (assoc :job-1 [(api/create-submit-job-entry job-1-id
                                                                   peer-config 
                                                                   job-1 
                                                                   (planning/discover-tasks (:catalog job-1) (:workflow job-1)))])
                       (assoc :bp1 [{:fn :backpressure-on :args {:peer :p1}}
                                    {:fn :backpressure-off :args {:peer :p1}}])
                       (assoc :bp2 [{:fn :backpressure-on :args {:peer :p2}}]))
          :log []
          :peer-choices []}))]
    ;(spit "log.edn" (pr-str log))
    (is (empty? (apply concat (vals entries))))
    (is (= :active (get (:peer-state replica) :p1)))
    (is (= :backpressure (get (:peer-state replica) :p2)))))

(deftest backpressure-kill-job
  (checking
    "Checking balanced allocation causes peers to be evenly split"
    (times 50)
    [{:keys [replica log peer-choices]} 
     (log-gen/apply-entries-gen 
       (gen/return
         {:replica {:job-scheduler :onyx.job-scheduler/balanced
                    :messaging {:onyx.messaging/impl :dummy-messenger}}
          :message-id 0
          :entries (-> (log-gen/generate-join-queues (log-gen/generate-peer-ids 12))
                       (assoc :job-1 [(api/create-submit-job-entry job-1-id
                                                                   peer-config 
                                                                   job-1 
                                                                   (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]
                              :job-2 [(api/create-submit-job-entry job-2-id
                                                                   peer-config 
                                                                   job-2 
                                                                   (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]
                              :job-3 [(api/create-submit-job-entry job-3-id
                                                                   peer-config 
                                                                   job-3 
                                                                   (planning/discover-tasks (:catalog job-3) (:workflow job-3)))
                                      {:fn :backpressure-on :args {:peer :p3}}
                                      {:fn :kill-job :args {:job job-3-id}}])
                       (assoc :bp1 [{:fn :backpressure-on :args {:peer :p1}}
                                    {:fn :backpressure-off :args {:peer :p1}}])
                       (assoc :bp2 [{:fn :backpressure-on :args {:peer :p2}}]))
          :log []
          :peer-choices []}))]
    (is (= :active (get (:peer-state replica) :p1)))
    (is (#{:backpressure :active} (get (:peer-state replica) :p2)))
    (is (not= :idle (get (:peer-state replica) :p3)))))
