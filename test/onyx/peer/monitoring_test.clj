(ns onyx.peer.monitoring-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def n-messages 100)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def in-chan (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def state (atom {}))

(defn update-state [_ event]
  (swap! state update-in [(:event event)] conj (dissoc event :event)))

(deftest monitoring-test
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        catalog [{:onyx/name :in
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
                  :onyx/doc "Writes segments to a core.async channel"}]
        workflow [[:in :inc] [:inc :out]]
        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls :onyx.peer.monitoring-test/in-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.peer.monitoring-test/out-calls}]
        monitoring-config {:monitoring :custom
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
                           :zookeeper-gc-log-entry update-state}]

    (reset! in-chan (chan (inc n-messages)))
    (reset! out-chan (chan (sliding-buffer (inc n-messages))))

    (with-test-env [test-env [3 env-config peer-config monitoring-config]]
      (doseq [n (range n-messages)]
        (>!! @in-chan {:n n}))

      (close! @in-chan)

      (onyx.api/submit-job peer-config
                           {:catalog catalog
                            :workflow workflow
                            :lifecycles lifecycles
                            :task-scheduler :onyx.task-scheduler/balanced})

      (let [results (take-segments! @out-chan)
            expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
        (is (= expected (set results))))

      (let [metrics @state]
        (is (seq? (:zookeeper-read-task metrics)))
        (is (seq? (:zookeeper-read-catalog metrics)))
        (is (seq? (:zookeeper-read-log-entry metrics)))
        (is (seq? (:zookeeper-read-workflow metrics)))
        (is (seq? (:zookeeper-read-flow-conditions metrics)))
        (is (seq? (:zookeeper-read-lifecycles metrics)))
        (is (seq? (:zookeeper-read-messaging metrics)))
        (is (seq? (:zookeeper-read-job-scheduler metrics)))
        (is (seq? (:zookeeper-read-origin metrics)))
        (is (seq? (:zookeeper-write-messaging metrics)))
        (is (seq? (:zookeeper-write-job-scheduler metrics)))
        (is (seq? (:zookeeper-write-log-entry metrics))))))) 
