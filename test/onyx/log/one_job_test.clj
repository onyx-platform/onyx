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

(def in-chan (chan 100))

(def out-chan (chan (sliding-buffer 100)))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

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
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :b
                  :onyx/fn :onyx.log.one-job-test/my-inc
                  :onyx/type :function
                  :onyx/batch-size batch-size}

                 {:onyx/name :c
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/doc "Writes segments to a core.async channel"}]

        lifecycles [{:lifecycle/task :a
                     :lifecycle/calls :onyx.log.one-job-test/in-calls}
                    {:lifecycle/task :a
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :c
                     :lifecycle/calls :onyx.log.one-job-test/out-calls}
                    {:lifecycle/task :c
                     :lifecycle/calls :onyx.plugin.core-async/writer-calls}]

        _ (onyx.api/submit-job peer-config
                               {:workflow [[:a :b] [:b :c]]
                                :catalog catalog
                                :lifecycles lifecycles
                                :task-scheduler :onyx.task-scheduler/balanced})
        ch (chan n-peers)
        replica (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000)]

    (testing "peers balanced on 1 job"
      (is (= #{1 2}
             (into #{} (map count (mapcat vals (vals (:allocations replica))))))))

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-env env)

    (onyx.api/shutdown-peer-group peer-group)))
