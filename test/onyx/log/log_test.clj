(ns onyx.log.log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.messaging.dummy-messenger]
            [onyx.test-helper :refer [load-config]]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.api]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-log-test-1
  (let [onyx-id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id onyx-id)
        peer-config (assoc (:peer-config config) :onyx/id onyx-id)
        scheduler :onyx.job-scheduler/balanced
        env (onyx.api/start-env env-config)]
    (try
      (extensions/write-chunk (:log env) :job-scheduler {:job-scheduler scheduler} nil)
      (extensions/write-chunk (:log env) :messaging {:onyx.messaging/impl :dummy-messaging} nil)

      (testing "We can write to the log and read the entries back out"
        (doseq [n (range 10)]
          (extensions/write-log-entry (:log env) {:n n}))

        (is (= (count (map (fn [n] (extensions/read-log-entry (:log env) n)) (range 10))) 10)))
      (finally
        (onyx.api/shutdown-env env)))))

(deftest log-log-test-2 
  (let [onyx-id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id onyx-id)
        env (onyx.api/start-env env-config)
        scheduler :onyx.job-scheduler/balanced
        entries 10000
        ch (chan entries)]
    (try
      (extensions/write-chunk (:log env) :job-scheduler {:job-scheduler scheduler} nil)
      (extensions/write-chunk (:log env) :messaging {:onyx.messaging/impl :dummy-messaging} nil)

      (extensions/subscribe-to-log (:log env) ch)

      (let [write-fut (future
                        (try
                          (doseq [n (range entries)]
                            (extensions/write-log-entry (:log env) {:n n}))
                          (catch Exception e
                            (.printStackTrace e))))]
        (testing "We can asynchronously write log entries and read them back in order"
          (is (= (count (map (fn [n] (<!! ch))
                             (range entries)))
                 entries)))

        (deref write-fut))
      (finally
        (onyx.api/shutdown-env env)))))
