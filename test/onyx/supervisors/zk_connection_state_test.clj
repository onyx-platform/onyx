(ns onyx.supervisors.zk-connection-state-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]
            [onyx.log.zookeeper :refer [start-zk-server stop-zk-server]]
            [taoensso.timbre :refer [fatal info]]))

;; start env
(deftest start-env-test-1
  (testing "Fail fast when no ZK server on start up"
    (let [id (java.util.UUID/randomUUID)
          config (load-config)
          env-config (assoc (:env-config config) :onyx/tenancy-id id
                                                  :zookeeper/server? false)]
      (try
        (onyx.api/start-env env-config)
        (Thread/sleep 30000)
        (is false "Should fail when no ZK server")
        (catch Throwable t
          ;(fatal t)
          (is true "Shouldn't be able to start system without ZooKeeper server connection")
          )))))

(deftest start-env-test-2
  (testing "Shutdown components when ZK server died"
    (let [id (java.util.UUID/randomUUID)
          config (load-config)
          env-config  (assoc (:env-config config) :onyx/tenancy-id id
                                                  :zookeeper/server? false)
          zk  (start-zk-server    env-config)
          env (onyx.api/start-env env-config)]
      (try
        (stop-zk-server zk)
        ;(start-zk-server env-config)
        (Thread/sleep 60000)
        (is true "Should fail when no ZK server")
        (catch Throwable t
          (fatal t)
          (is false "Exception should be caught"))
        (finally
          (println "===== stopping")
          (onyx.api/shutdown-env env)))))
  )

(deftest start-system-test-1
  (testing "Env fail fast when ZK server died"
    (let [id (java.util.UUID/randomUUID)
          config (load-config)
          env-config  (assoc (:env-config config) :onyx/tenancy-id id
                                                  :zookeeper/server? false)
          peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
          zk  (start-zk-server    env-config)
          env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          n-peers 5
          v-peers (onyx.api/start-peers n-peers peer-group)]
      (try
        (stop-zk-server zk)
        ;(start-zk-server env-config)
        (Thread/sleep 60000)
        (is true "Should fail when no ZK server")
        (catch Throwable t
          ;(fatal t)
          (is false "Exception should be caught"))
        (finally
          (println "===== stopping")
          (doseq [v-peer v-peers]
            (onyx.api/shutdown-peer v-peer))
          (onyx.api/shutdown-peer-group peer-group)
          (onyx.api/shutdown-env env)))))
  )