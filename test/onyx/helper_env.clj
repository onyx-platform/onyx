(ns onyx.helper-env
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer go-loop]]
            [taoensso.timbre :refer [fatal] :as timbre]
            [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(defprotocol Manager
  (run-job [this job]
           [this job inputs])
  (lookup-peer [this peer-id])
  (remove-n-peers [this n])
  (remove-peer [this peer])
  (add-peers [this n-peers])
  (remove-peers [this n-peers]))

(defrecord TestEnv [env-config peer-config]
  Manager
  (add-peers [component n-peers]
    (swap! (:v-peers component) into (onyx.api/start-peers n-peers (:peer-group component))))

  (lookup-peer [component peer-id]
    (first (filter #(= peer-id
                       (:id (:virtual-peer @(:started-peer %))))
                   @(:v-peers component))))

  (remove-peer [component v-peer]
    (onyx.api/shutdown-peer v-peer)
    (swap! (:v-peers component) disj v-peer))

  (remove-n-peers [component n]
    (let [v-peers (take n @(:v-peers component))]
      (doseq [v-peer v-peers]
        (remove-peer component v-peer))))

  (run-job [component {:keys [workflow catalog lifecycles task-scheduler] :as job}]
    (let [n-peers-required (count (distinct (flatten workflow)))
          n-peers (count @(:v-peers component))]
      (when (< n-peers n-peers-required)
        (throw (Exception. (format "%s peers required to run this job and only %s peers are available."
                                   n-peers-required
                                   n-peers)))))
    (onyx.api/submit-job (:peer-config component)
                         {:workflow workflow
                          :catalog catalog
                          :lifecycles lifecycles
                          :task-scheduler task-scheduler}))

  component/Lifecycle
  (component/start [component]
    (let [id (java.util.UUID/randomUUID)
          env-config (assoc env-config :onyx/id id)
          peer-config (assoc peer-config :onyx/id id)
          log-ch (chan)
          env (onyx.api/start-env env-config)]
      (try
        (let [peer-group (onyx.api/start-peer-group peer-config)
              !replica (atom {})
              _ (go-loop [replica (extensions/subscribe-to-log (:log env) log-ch)]
                         (let [entry (<!! log-ch)
                               new-replica (extensions/apply-log-entry entry replica)]
                           (reset! !replica new-replica)
                           (recur new-replica)))]
          (println "Started helper environment:" id)
          (assoc component
                 :id id
                 :env-config env-config
                 :peer-config peer-config
                 :env env
                 :peer-group peer-group
                 :replica !replica
                 :v-peers (atom #{})
                 :log-ch log-ch))
        (catch Exception e
          (fatal e "Could not start peer group")
          (when log-ch (close! log-ch))
          (onyx.api/shutdown-env env)))))
  (component/stop [component]
    (when-let [log-ch (:log-ch component)]
      (close! log-ch))
    (doseq [v-peer @(:v-peers component)]
      (try
        (onyx.api/shutdown-peer v-peer)
        (catch Exception e
          (fatal e "Could not shutdown v-peer " v-peer))))
    (try
      (onyx.api/shutdown-peer-group (:peer-group component))
      (catch Exception e
        (fatal e "Could not stop peer-group")))
    (try
      (onyx.api/shutdown-env (:env component))
      (catch Exception e
        (fatal e "Could not stop environment")))
    (assoc component
           :env nil
           :id nil
           :peer-group nil
           :replica nil
           :v-peers nil
           :log-ch nil)))
