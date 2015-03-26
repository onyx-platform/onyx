(ns onyx.log.full-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.aeron :as aeron]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config (assoc (:peer-config config) :onyx/id onyx-id))

(def messaging 
  (component/start (aeron/aeron {:opts peer-config})))

(def n-peers 3)

(facts "peers all join and watch each other"
       (let [env (onyx.api/start-env env-config)
             v-peers (onyx.api/start-peers n-peers peer-config)] 
         (try 
           (let [ch (chan n-peers)
                 replica (loop [replica (extensions/subscribe-to-log (:log env) ch)]
                           (let [position (<!! ch)
                                 entry (extensions/read-log-entry (:log env) position)
                                 new-replica (extensions/apply-log-entry entry replica)]
                             (if (< (count (:pairs new-replica)) n-peers)
                               (recur new-replica)
                               new-replica)))]
             (fact (:prepared replica) => {})
             (fact (:accepted replica) => {})
             (fact (set (keys (:pairs replica)))
                   => (set (vals (:pairs replica))))
             (fact (count (:peers replica)) => n-peers))

           (finally
             (doseq [v-peer v-peers]
               (onyx.api/shutdown-peer v-peer)) 
             (onyx.api/shutdown-env env)))))


