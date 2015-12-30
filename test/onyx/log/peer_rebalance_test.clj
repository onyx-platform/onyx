(ns onyx.log.peer-rebalance-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [playback-log get-counts load-config]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [schema.test]
            [schema.core :as s]
            [onyx.log.curator :as zk]
            [onyx.api]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-peer-rebalance
  (let [onyx-id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id onyx-id)
        peer-config (assoc (:peer-config config)
                           :onyx/id onyx-id
                           :onyx.peer/job-scheduler :onyx.job-scheduler/balanced)
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
                                 :task-scheduler :onyx.task-scheduler/balanced})
        j2 (onyx.api/submit-job peer-config
                                {:workflow [[:c :d]]
                                 :catalog catalog-2
                                 :task-scheduler :onyx.task-scheduler/balanced})
        n-peers 12
        v-peers (onyx.api/start-peers n-peers peer-group)
        ch (chan 10000)
        replica-1 (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 8000)
        conn (zk/connect (:zookeeper/address (:env-config config)))
        task-b (second (get-in replica-1 [:tasks (:job-id j1)]))
        id (last (get (get (:allocations replica-1) (:job-id j1)) task-b))
        _ (zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" id))
        _ (zk/close conn)
        replica-2 (playback-log (:log env) replica-1 ch 8000)]

    (testing "the peers evenly balance" 
      (is (= [[3 3] [3 3]] 
             (get-counts replica-1 [j1 j2]))))

    (testing "the peers rebalance" 
      (is (= [[3 2] [3 3]] 
             (get-counts replica-2 [j1 j2]))))

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-env env)

    (onyx.api/shutdown-peer-group peer-group)))
