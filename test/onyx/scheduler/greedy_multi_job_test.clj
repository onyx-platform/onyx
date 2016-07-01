(ns onyx.scheduler.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [playback-log get-counts]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api :as api]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]))

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

(deftest log-greedy-job
  (let [onyx-id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        batch-size 20
        catalog-1 [{:onyx/name :a
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :b
                    :onyx/plugin :onyx.plugin.core-async/output
                    :onyx/type :output
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/doc "Writes segments to a core.async channel"}]

        catalog-2 [{:onyx/name :c
                    :onyx/plugin :onyx.plugin.core-async/input
                    :onyx/type :input
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/doc "Reads segments from a core.async channel"}

                   {:onyx/name :d
                    :onyx/plugin :onyx.plugin.core-async/output
                    :onyx/type :output
                    :onyx/medium :core.async
                    :onyx/batch-size batch-size
                    :onyx/doc "Writes segments to a core.async channel"}]

        lifecycles-1 [{:lifecycle/task :a
                       :lifecycle/calls ::a-calls}
                      {:lifecycle/task :a
                       :lifecycle/calls :onyx.plugin.core-async/reader-calls
                       :core.async/allow-unsafe-concurrency? true}
                      {:lifecycle/task :b
                       :lifecycle/calls ::b-calls}
                      {:lifecycle/task :b
                       :lifecycle/calls :onyx.plugin.core-async/writer-calls
                       :core.async/allow-unsafe-concurrency? true}]

        lifecycles-2 [{:lifecycle/task :c
                       :lifecycle/calls ::c-calls}
                      {:lifecycle/task :c
                       :lifecycle/calls :onyx.plugin.core-async/reader-calls
                       :core.async/allow-unsafe-concurrency? true}
                      {:lifecycle/task :d
                       :lifecycle/calls ::d-calls}
                      {:lifecycle/task :d
                       :lifecycle/calls :onyx.plugin.core-async/writer-calls
                       :core.async/allow-unsafe-concurrency? true}]

        j1 (onyx.api/submit-job peer-config
                                {:workflow [[:a :b]]
                                 :catalog catalog-1
                                 :lifecycles lifecycles-1
                                 :task-scheduler :onyx.task-scheduler/balanced})

        j2 (onyx.api/submit-job peer-config
                                {:workflow [[:c :d]]
                                 :catalog catalog-2
                                 :lifecycles lifecycles-2
                                 :task-scheduler :onyx.task-scheduler/balanced})
        n-peers 10
        v-peers (onyx.api/start-peers n-peers peer-group)
        ch (chan n-peers)

        replica-1 (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 6000)
        counts-1 (get-counts replica-1 [j1 j2])
        _ (>!! a-chan :done)
        _ (close! a-chan)

        replica-2 (playback-log (:log env) replica-1 ch 6000)
        counts-2 (get-counts replica-2 [j1 j2])
        _ (>!! c-chan :done)
        _ (close! c-chan)

        replica-3 (playback-log (:log env) replica-2 ch 6000)
        counts-3 (get-counts replica-3 [j1 j2])
        _ (close! b-chan)
        _ (close! d-chan)]

    (testing  "5 peers were allocated to job 1, task A, 5 peers were allocated to job 1, task B"
      (is (= [{:a 5 :b 5}
              {}]
             counts-1)))

    (testing "5 peers were reallocated to job 2, task C, 5 peers were reallocated to job 2, task D"
      (is (= [{}
              {:c 5
               :d 5}]
             counts-2)))

    (testing "No peers are executing any tasks"
      (is (= [{} {}] counts-3)))

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-peer-group peer-group)

    (onyx.api/shutdown-env env)))
