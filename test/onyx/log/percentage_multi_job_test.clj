(ns onyx.log.percentage-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [playback-log get-counts load-config]]
            [onyx.api :as api]
            [schema.test] 
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.log.curator :as zk]))

(use-fixtures :once schema.test/validate-schemas)

(def a-chan (chan 100))

(def b-chan (chan (sliding-buffer 100)))

(def c-chan (chan 100))

(def d-chan (chan (sliding-buffer 100)))

(defn inject-a-ch [event lifecycle]
  {:core.async/chan a-chan})

(defn inject-b-ch [event lifecycle]
  {:core.async/chan b-chan})

(defn inject-c-ch [event lifecycle]
  {:core.async/chan c-chan})

(defn inject-d-ch [event lifecycle]
  {:core.async/chan d-chan})

(def a-calls
  {:lifecycle/before-task-start inject-a-ch})

(def b-calls
  {:lifecycle/before-task-start inject-b-ch})

(def c-calls
  {:lifecycle/before-task-start inject-c-ch})

(def d-calls
  {:lifecycle/before-task-start inject-d-ch})

(deftest log-percentage-multi-job
  (let [onyx-id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id onyx-id)
        peer-config (assoc (:peer-config config)
                           :onyx/id onyx-id
                           :onyx.peer/job-scheduler :onyx.job-scheduler/percentage)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        catalog-1 [{:onyx/name :a
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size 20
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :b
                    :onyx/plugin :onyx.plugin.core-async/output
                    :onyx/type :output
                    :onyx/medium :core.async
                    :onyx/batch-size 20
                    :onyx/doc "Writes segments to a core.async channel"}]

        catalog-2 [{:onyx/name :c
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size 20
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :d
                    :onyx/plugin :onyx.plugin.core-async/output
                    :onyx/type :output
                    :onyx/medium :core.async
                    :onyx/batch-size 20
                    :onyx/doc "Writes segments to a core.async channel"}]
        lifecycles-1 [{:lifecycle/task :a
                       :lifecycle/calls :onyx.log.percentage-multi-job-test/a-calls}
                      {:lifecycle/task :a
                       :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                      {:lifecycle/task :b
                       :lifecycle/calls :onyx.log.percentage-multi-job-test/b-calls}
                      {:lifecycle/task :b
                       :lifecycle/calls :onyx.plugin.core-async/writer-calls}]
        lifecycles-2 [{:lifecycle/task :c
                       :lifecycle/calls :onyx.log.percentage-multi-job-test/c-calls}
                      {:lifecycle/task :c
                       :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                      {:lifecycle/task :d
                       :lifecycle/calls :onyx.log.percentage-multi-job-test/d-calls}
                      {:lifecycle/task :d
                       :lifecycle/calls :onyx.plugin.core-async/writer-calls}]
        j1 (onyx.api/submit-job peer-config
                                {:workflow [[:a :b]]
                                 :catalog catalog-1
                                 :lifecycles lifecycles-1
                                 :percentage 70
                                 :task-scheduler :onyx.task-scheduler/balanced})
        j2 (onyx.api/submit-job peer-config
                                {:workflow [[:c :d]]
                                 :catalog catalog-2
                                 :lifecycles lifecycles-2
                                 :percentage 30
                                 :task-scheduler :onyx.task-scheduler/balanced})
        n-peers 10
        v-peers-1 (onyx.api/start-peers n-peers peer-group)
        ch (chan 10000)
        replica (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 10000)
        v-peers-2 (onyx.api/start-peers n-peers peer-group)
        replica-2 (playback-log (:log env) replica ch 10000)]

    (testing "70/30% split for percentage job scheduler succeeded"
      (is (= (map (partial apply +)
                  (get-counts replica [j1 j2])) 
             [7 3])))

    (testing "70/30% split for percentage job scheduler succeeded after rebalance"
      (is (= (map (partial apply +)
                  (get-counts replica-2 [j1 j2])) 
             [14 6])))

(doseq [v-peer v-peers-1]
  (onyx.api/shutdown-peer v-peer))

(doseq [v-peer v-peers-2]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(onyx.api/shutdown-peer-group peer-group))) 
