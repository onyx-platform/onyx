(ns onyx.log.gc-log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api :as api]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.log.curator :as zk]))

(use-fixtures :once schema.test/validate-schemas)

(deftest gc-log-test
  (let [onyx-id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        n-peers 5
        v-peers (onyx.api/start-peers n-peers peer-group)
        _ (Thread/sleep 500)
        _ (onyx.api/gc peer-config)
        v-peers2 (onyx.api/start-peers n-peers peer-group)
        ch (chan 100)]
    (loop [replica (extensions/subscribe-to-log (:log env) ch)]
      (let [entry (<!! ch)
            _ (assert (> (:message-id entry) 5))
            new-replica (extensions/apply-log-entry entry replica)]
        (when-not (= (count (:peers new-replica)) 10)
          (recur new-replica))))

    (testing "Starting peers after GC succeeded" (is true))

    (doseq [v-peer (concat v-peers v-peers2)]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-peer-group peer-group)

    (onyx.api/shutdown-env env)))
