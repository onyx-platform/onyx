(ns onyx.peer.catalog-params-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group 
  (onyx.api/start-peer-group peer-config))

(def n-messages 1000)

(def batch-size 20)

(defn my-adder [factor {:keys [n] :as segment}]
  (assoc segment :n (+ n factor)))

(def workflow [[:in :add] [:add :out]])

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :add
    :onyx/fn :onyx.peer.catalog-params-test/my-adder
    :onyx/type :function
    :onyx/factor 42
    :onyx/params [:onyx/factor]
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task :onyx.peer.min-peers-test/inject-in-ch})

(def out-calls
  {:lifecycle/before-task :onyx.peer.min-peers-test/inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.peer.min-peers-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.peer.min-peers-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(doseq [n (range n-messages)]
  (>!! in-chan {:n n}))

(>!! in-chan :done)

(def v-peers (onyx.api/start-peers 3 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(fact results => (conj (vec (map (fn [x] {:n (+ x 42)}) (range n-messages))) :done))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

