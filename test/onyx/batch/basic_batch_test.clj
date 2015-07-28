(ns onyx.batch.basic-batch-test
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

(def input-file-name (str base-dir (java.util.UUID/randomUUID)))

(def output-file-name (str base-dir (java.util.UUID/randomUUID)))

(def n-messages 100)

(doseq [n (range n-messages)]
  (spit (str input-file-name ".edn") {:n n} :append true)
  (spit (str input-file-name ".edn") "\n" :append true))

(defn my-inc [segment]
  (update-in segment [:n] inc))

(prn "Input file is:" input-file-name ".edn")
(prn "Output file is:" output-file-name ".edn")

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.local-file/input
    :onyx/type :input
    :onyx/medium :local-file
    :file/path input-file-name
    :file/extension ".edn"
    :onyx/doc "Reads segments a local file"}

   {:onyx/name :inc
    :onyx/fn :onyx.batch.basic-batch-test/my-inc
    :onyx/type :function
    :onyx/batch "Maps over the segments, incrementing :n"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.local-file/output
    :onyx/type :output
    :onyx/medium :local-file
    :file/path output-file-name
    :file/extension ".edn"
    :onyx/doc "Writes segments to a local file"}])

(def workflow
  [[:in :inc]
   [:inc :out]])

(def v-peers (onyx.api/start-peers 1 peer-group))

(def job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    {:mode :batch
     :catalog catalog
     :workflow workflow
     :task-scheduler :onyx.task-scheduler/naive})))

(onyx.api/await-job-completion peer-config job-id)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
