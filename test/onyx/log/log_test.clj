(ns onyx.log.log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]
            [onyx.api]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(extensions/write-chunk (:log env) :job-scheduler {:job-scheduler scheduler} nil)

(facts
 "We can write to the log and read the entries back out"
 (doseq [n (range 10)]
   (extensions/write-log-entry (:log env) {:n n}))

 (fact (count (map (fn [n] (extensions/read-log-entry (:log env) n)) (range 10))) => 10))

(onyx.api/shutdown-env env)

(def env (onyx.api/start-env env-config))

(extensions/write-chunk (:log env) :job-scheduler {:job-scheduler scheduler} nil)

(def entries 10000)

(def ch (chan entries))

(extensions/subscribe-to-log (:log env) ch)

(future
  (try
    (doseq [n (range entries)]
      (extensions/write-log-entry (:log env) {:n n}))
    (catch Exception e
      (.printStackTrace e))))

(facts
 "We can asynchronously write log entries and read them back in order"
 (fact (count (map (fn [n] (<!! ch) (extensions/read-log-entry (:log env) n))
                   (range entries)))
       => entries))

(onyx.api/shutdown-env env)

