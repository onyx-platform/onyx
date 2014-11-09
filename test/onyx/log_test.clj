(ns onyx.log-test
  (:require [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env (component/start (onyx-development-env onyx-id (:env config))))

(component/stop env)

