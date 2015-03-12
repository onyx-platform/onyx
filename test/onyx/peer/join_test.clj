(ns onyx.peer.join-test
  (:require [com.stuartsierra.component :as component]
            [midje.sweet :refer :all]
            [onyx.system :refer [onyx-development-env]]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def dev (onyx-development-env env-config))

(def env (onyx.api/start-env env-config))

(def batch-size 2)

(def echo 1)

(def name-queue (str (java.util.UUID/randomUUID)))

(def age-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(hq-util/create-queue! hq-config name-queue)

(hq-util/create-queue! hq-config age-queue)

(hq-util/create-queue! hq-config out-queue)

(def people
  [{:id 1 :name "Mike" :age 23}
   {:id 2 :name "Dorrene" :age 24}
   {:id 3 :name "Benti" :age 23}
   {:id 4 :name "John" :age 19}
   {:id 5 :name "Shannon" :age 31}
   {:id 6 :name "Kristen" :age 25}])

(def names (map #(select-keys % [:id :name]) people))

(def ages (map #(select-keys % [:id :age]) people))

(hq-util/write-and-cap! hq-config name-queue names 1)

(hq-util/write-and-cap! hq-config age-queue ages 1)

(defn join-person [local-state segment]
  (let [state @local-state]
    (if-let [record (get state (:id segment))]
      (let [result (merge record segment)]
        (swap! local-state dissoc (:id segment))
        result)
      (do (swap! local-state assoc (:id segment) (dissoc segment :id))
          []))))

(def catalog
  [{:onyx/name :names
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :hornetq/queue-name name-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :ages
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :hornetq/queue-name age-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :join-person
    :onyx/fn :onyx.peer.join-test/join-person
    :onyx/type :function
    :onyx/group-by-key :id
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def workflow
  [[:names :join-person]
   [:ages :join-person]
   [:join-person :out]])

(defmethod l-ext/inject-lifecycle-resources :join-person
  [_ event]
  {:onyx.core/params [(atom {})]})

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-util/consume-queue! hq-config out-queue echo))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(fact (into #{} (butlast results)) => (into #{} people))

