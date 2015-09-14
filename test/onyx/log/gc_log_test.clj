(ns onyx.log.gc-log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [load-config]]
            [onyx.api :as api]
            [clojure.test :refer [deftest is testing]]
            [onyx.log.curator :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config (assoc (:peer-config config) :onyx/id onyx-id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers 5)

(def v-peers (onyx.api/start-peers n-peers peer-group))

(onyx.api/gc peer-config)

(def v-peers2 (onyx.api/start-peers n-peers peer-group))

(def ch (chan 100))

(loop [replica (extensions/subscribe-to-log (:log env) ch)]
  (let [entry (<!! ch)
        new-replica (extensions/apply-log-entry entry replica)]
    (when-not (= (count (:peers new-replica)) 10)
      (recur new-replica))))

(fact "Starting peers after GC succeeded" true => true)

(doseq [v-peer (concat v-peers v-peers2)]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
