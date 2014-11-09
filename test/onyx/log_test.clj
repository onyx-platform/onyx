(ns onyx.log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(facts
 "We can write to the log and read the entries back out"
 (doseq [n (range 10)]
   (extensions/write-log-entry (:log env) n))

 (fact (map (fn [n] (extensions/read-log-entry (:log env) n)) (range 10))
       => (range 10)))

(component/stop env)

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(def entries 10000)

(def ch (chan entries))

(extensions/subscribe-to-log (:log env) 0 ch)

(future
  (try
    (doseq [n (range entries)]
      (extensions/write-log-entry (:log env) n))
    (catch Exception e
      (.printStackTrace e))))

(facts
 "We can asynchronously write log entries and read them back in order"
 (fact (map (fn [n] (<!! ch) (extensions/read-log-entry (:log env) n))
            (range entries))
       => (range entries)))

(component/stop env)

