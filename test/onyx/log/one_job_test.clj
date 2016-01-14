(ns onyx.log.one-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [playback-log]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [schema.core :as s]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(defn my-inc [segment]
  {:n (inc (:n segment))})

(deftest log-one-job
  (let [config (load-config)
        onyx-id (java.util.UUID/randomUUID)
        env-config (assoc (:env-config config) :onyx/id onyx-id)
        peer-config (assoc (:peer-config config) :onyx/id onyx-id)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        n-peers 5
        batch-size 20
        v-peers (onyx.api/start-peers n-peers peer-group)
        catalog [{:onyx/name :a
                  :onyx/plugin :onyx.test-helper/dummy-input
                  :onyx/type :input
                  :onyx/medium :dummy
                  :onyx/batch-size batch-size}

                 {:onyx/name :b
                  :onyx/fn :onyx.log.one-job-test/my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :c
                  :onyx/plugin :onyx.test-helper/dummy-output
                  :onyx/type :output
                  :onyx/medium :dummy
                  :onyx/batch-size batch-size}]

        _ (onyx.api/submit-job peer-config
                               {:workflow [[:a :b] [:b :c]]
                                :catalog catalog
                                :task-scheduler :onyx.task-scheduler/balanced})
        ch (chan n-peers)
        replica (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000)]

    (testing "peers balanced on 1 job"
      (is (= #{1 2}
             (into #{} (map count (mapcat vals (vals (:allocations replica))))))))

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-peer-group peer-group)

    (onyx.api/shutdown-env env)))
