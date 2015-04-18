(ns onyx.peer.join-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def dev (onyx-development-env env-config))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def batch-size 2)

(def people
  [{:id 1 :name "Mike" :age 23}
   {:id 2 :name "Dorrene" :age 24}
   {:id 3 :name "Benti" :age 23}
   {:id 4 :name "John" :age 19}
   {:id 5 :name "Shannon" :age 31}
   {:id 6 :name "Kristen" :age 25}])

(def names (map #(select-keys % [:id :name]) people))

(def ages (map #(select-keys % [:id :age]) people))

(def name-chan (chan (inc names)))

(def age-chan (chan (inc ages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(doseq [name names]
  (>!! name-chan name))

(doseq [age ages]
  (>!! age-chan age))

(>!! name-chan :done)
(>!! age-chan :done)

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
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :ages
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :join-person
    :onyx/fn :onyx.peer.join-test/join-person
    :onyx/type :function
    :onyx/group-by-key :id
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow
  [[:names :join-person]
   [:ages :join-person]
   [:join-person :out]])

(defmethod l-ext/inject-lifecycle-resources :join-person
  [_ event]
  {:onyx.core/params [(atom {})]})

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(fact (into #{} (butlast results)) => (into #{} people))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))
    
(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
