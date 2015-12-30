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
                    :onyx/plugin :onyx.test-helper/dummy-input
                    :onyx/type :input
                    :onyx/medium :dummy
                    :onyx/batch-size 20}

                   {:onyx/name :b
                    :onyx/plugin :onyx.test-helper/dummy-output
                    :onyx/type :output
                    :onyx/medium :dummy
                    :onyx/batch-size 20}]

        catalog-2 [{:onyx/name :c
                    :onyx/plugin :onyx.test-helper/dummy-input
                    :onyx/type :input
                    :onyx/medium :dummy
                    :onyx/batch-size 20}

                   {:onyx/name :d
                    :onyx/plugin :onyx.test-helper/dummy-output
                    :onyx/type :output
                    :onyx/medium :dummy
                    :onyx/batch-size 20}]
        j1 (onyx.api/submit-job peer-config
                                {:workflow [[:a :b]]
                                 :catalog catalog-1
                                 :percentage 70
                                 :task-scheduler :onyx.task-scheduler/balanced})
        j2 (onyx.api/submit-job peer-config
                                {:workflow [[:c :d]]
                                 :catalog catalog-2
                                 :percentage 30
                                 :task-scheduler :onyx.task-scheduler/balanced})
        n-peers 10
        v-peers-1 (onyx.api/start-peers n-peers peer-group)
        ch (chan 10000)
        replica (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 10000)
        v-peers-2 (onyx.api/start-peers n-peers peer-group)
        replica-2 (playback-log (:log env) replica ch 10000)]

    (testing "70/30% split for percentage job scheduler succeeded"
      (is (= [7 3] 
             (map (partial apply +)
                  (get-counts replica [j1 j2])))))

    (testing "70/30% split for percentage job scheduler succeeded after rebalance"
      (is (= [14 6] 
             (map (partial apply +)
                  (get-counts replica-2 [j1 j2])))))

    (doseq [v-peer v-peers-1]
      (onyx.api/shutdown-peer v-peer))

    (doseq [v-peer v-peers-2]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-env env)

    (onyx.api/shutdown-peer-group peer-group)))
