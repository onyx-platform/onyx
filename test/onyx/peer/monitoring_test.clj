(ns onyx.peer.monitoring-test
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

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 100)

(def batch-size 20)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.monitoring-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :inc] [:inc :out]])

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.peer.monitoring-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.peer.monitoring-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(doseq [n (range n-messages)]
  (>!! in-chan {:n n}))

(>!! in-chan :done)
(close! in-chan)

(def state (atom {}))

(defn update-state [_ event]
  (swap! state update-in [(:event event)] conj (dissoc event :event)))

(def monitoring-config
  {:monitoring :custom
   :zookeeper-write-log-entry update-state
   :zookeeper-read-log-entry update-state
   :zookeeper-write-catalog update-state
   :zookeeper-write-workflow update-state
   :zookeeper-write-flow-conditions update-state
   :zookeeper-write-lifecycles update-state
   :zookeeper-write-task update-state
   :zookeeper-write-chunk update-state
   :zookeeper-write-job-scheduler update-state
   :zookeeper-write-messaging update-state
   :zookeeper-force-write-chunk update-state
   :zookeeper-read-catalog update-state
   :zookeeper-read-workflow update-state
   :zookeeper-read-flow-conditions update-state
   :zookeeper-read-lifecycles update-state
   :zookeeper-read-task update-state
   :zookeeper-read-chunk update-state
   :zookeeper-read-origin update-state
   :zookeeper-read-job-scheduler update-state
   :zookeeper-read-messaging update-state
   :zookeeper-write-origin update-state
   :zookeeper-gc-log-entry update-state})

(def v-peers (onyx.api/start-peers 3 peer-group monitoring-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(def metrics @state)

(fact (:zookeeper-read-task metrics) => seq)
(fact (:zookeeper-read-catalog metrics) => seq)
(fact (:zookeeper-read-log-entry metrics) => seq)
(fact (:zookeeper-read-workflow metrics) => seq)
(fact (:zookeeper-read-flow-conditions metrics) => seq)
(fact (:zookeeper-read-lifecycles metrics) => seq)
(fact (:zookeeper-read-messaging metrics) => seq)
(fact (:zookeeper-read-job-scheduler metrics) => seq)
(fact (:zookeeper-read-origin metrics) => seq)
(fact (:zookeeper-write-messaging metrics) => seq)
(fact (:zookeeper-write-job-scheduler metrics) => seq)
(fact (:zookeeper-write-log-entry metrics) => seq)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
