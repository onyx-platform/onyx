(ns onyx.peer.join-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]
            [taoensso.timbre :refer [info warn trace fatal] :as timbre]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

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

(def name-chan (chan (inc (count names))))

(def age-chan (chan (inc (count ages))))

(def out-chan (chan 10000 #_(sliding-buffer (inc n-messages))))

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
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :ages
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :join-person
    :onyx/fn :onyx.peer.join-test/join-person
    :onyx/type :function
    :onyx/group-by-key :id
    :onyx/min-peers 1
    :onyx/flux-policy :kill
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow
  [[:names :join-person]
   [:ages :join-person]
   [:join-person :out]])

(defn inject-names-ch [event lifecycle]
  {:core.async/chan name-chan})

(defn inject-ages-ch [event lifecycle]
  {:core.async/chan age-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(defn inject-join-state [event lifecycle]
  {:onyx.core/params [(atom {})]})

(def names-calls
  {:lifecycle/before-task-start inject-names-ch})

(def ages-calls
  {:lifecycle/before-task-start inject-ages-ch})

(def join-calls
  {:lifecycle/before-task-start inject-join-state})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :names
    :lifecycle/calls :onyx.peer.join-test/names-calls}
   {:lifecycle/task :names
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :ages
    :lifecycle/calls :onyx.peer.join-test/ages-calls}
   {:lifecycle/task :ages
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.peer.join-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :join-person
    :lifecycle/calls :onyx.peer.join-test/join-calls}])

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(fact (set (butlast results)) => (set people))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
