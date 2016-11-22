(ns onyx.log.zk-connection-state-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]
            [onyx.log.zookeeper :refer [start-zk-server stop-zk-server]]))

(deftest no-zk-server-test
  (testing "Env starting fail fast when no ZK server"
    (let [id (java.util.UUID/randomUUID)
          config (load-config)
          env-config  (assoc (:env-config config) :onyx/tenancy-id id
                                                  :zookeeper/server? false)]
      (try
        (onyx.api/start-env env-config)
        (is false "Should fail when no ZK server")
        (catch Throwable t
          (is true "Exception should be caught"))))))