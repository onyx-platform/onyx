(ns onyx.batch.batch-test
  (:require [midje.sweet :refer :all]
            [onyx.test-helper :refer [load-batch-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-batch-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 100)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.local-file/input
    :onyx/type :input
    :onyx/medium :local-file
    :onyx/doc "Reads segments a local file"}

   {:onyx/name :identity
    :onyx/fn :onyx.batch/batch-test
    :onyx/type :function
    :onyx/batch "Maps over the segments, returning themselves"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.local-file/output
    :onyx/type :output
    :onyx/medium :local-file
    :onyx/doc "Writes segments to a local file"}])

(def workflow
  [[:in :inc]
   [:identity :out]])

(def v-peers (onyx.api/start-peers 1 peer-group))

(onyx.api/submit-job
 peer-config
 {:mode :batch
  :catalog catalog
  :workflow workflow
  :task-scheduler :onyx.task-scheduler/naive-batch})

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
