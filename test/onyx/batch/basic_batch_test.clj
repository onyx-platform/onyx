(ns onyx.batch.batch-test
  (:require [midje.sweet :refer :all]
            [onyx.test-helper :refer [load-batch-config]]
            [onyx.plugin.local-file]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-batch-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def base-dir "target/")

(def input-file-name (str base-dir (java.util.UUID/randomUUID) ".edn"))

(def output-file-name (str base-dir (java.util.UUID/randomUUID) ".edn"))

(def n-messages 100)

(doseq [n (range n-messages)]
  (spit input-file-name {:n n} :append true)
  (spit input-file-name "\n" :append true))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.local-file/input
    :onyx/type :input
    :onyx/medium :local-file
    :file/path input-file-name
    :onyx/doc "Reads segments a local file"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch "Maps over the segments, returning themselves"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.local-file/output
    :onyx/type :output
    :onyx/medium :local-file
    :file/path output-file-name
    :onyx/doc "Writes segments to a local file"}])

(def workflow
  [[:in :identity]
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
