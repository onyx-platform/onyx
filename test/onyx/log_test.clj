(ns onyx.log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env (component/start (onyx-development-env onyx-id (:env config))))

(facts
 "We can write to the log and read the entries back out"
 (doseq [n (range 10)]
   (extensions/write-log-entry (:log env) n))

 (fact (map (fn [n] (extensions/read-log-entry (:log env) n)) (range 10))
       => (range 10)))

(component/stop env)

