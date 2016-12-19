(ns onyx.log.log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.peer.log-version]
            [onyx.test-helper :refer [load-config with-test-env]]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.api]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-log-test-1
  (let [onyx-id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
        scheduler :onyx.job-scheduler/balanced
        env (onyx.api/start-env env-config)]
    (try
     (extensions/write-chunk (:log env) :log-parameters {:onyx.messaging/impl :dummy-messenger
                                                         :log-version onyx.peer.log-version/version
                                                         :job-scheduler :onyx.job-scheduler/balanced} nil)

      (testing "We can write to the log and read the entries back out"
        (doseq [n (range 10)]
          (extensions/write-log-entry (:log env) {:n n}))

        (is (= 10 (count (map (fn [n] (extensions/read-log-entry (:log env) n)) (range 10))))))
      (finally
        (onyx.api/shutdown-env env)))))

(deftest log-log-test-2 
  (let [onyx-id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        env (onyx.api/start-env env-config)
        scheduler :onyx.job-scheduler/balanced
        entries 10000
        ch (chan entries)]
    (try
     (extensions/write-chunk (:log env) :log-parameters {:onyx.messaging/impl :dummy-messenger
                                                         :log-version onyx.peer.log-version/version
                                                         :job-scheduler :onyx.job-scheduler/balanced} nil)

      (extensions/subscribe-to-log (:log env) ch)

      (let [write-fut (future
                        (try
                          (doseq [n (range entries)]
                            (extensions/write-log-entry (:log env) {:n n}))
                          (catch Exception e
                            (.printStackTrace e))))]
        (testing "We can asynchronously write log entries and read them back in order"
          (is (= entries
                 (count (map (fn [n] (<!! ch))
                             (range entries))))))

        (deref write-fut))
      (finally
        (onyx.api/shutdown-env env)))))
