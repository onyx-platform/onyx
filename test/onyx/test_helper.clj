(ns onyx.test-helper
  (:require [clojure.core.async :refer [chan >!! alts!! timeout <!! close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [yeller.timbre-appender]
            [taoensso.timbre :refer  [info warn trace fatal error] :as timbre]
            [onyx.extensions :as extensions]
            [onyx.api]))

;; Following with macros marked for deletion
(defmacro with-env [[symbol-name env-config] & body]
  `(let [~symbol-name (onyx.api/start-env ~env-config)]
     (try
       ~@body
       (finally
         (onyx.api/shutdown-env ~symbol-name)))))

(defmacro with-peer-group [[symbol-name peer-config] & body]
  `(let [~symbol-name (onyx.api/start-peer-group ~peer-config)]
     (try
       ~@body
       (finally
         (onyx.api/shutdown-peer-group ~symbol-name)))))

(defmacro with-peers [[symbol-name n-peers peer-group] & body]
  `(let [~symbol-name (onyx.api/start-peers ~n-peers ~peer-group)]
     (try
       ~@body
       (finally
         (doseq [v-peer# ~symbol-name]
           (onyx.api/shutdown-peer v-peer#))))))

(defn playback-log [log replica ch timeout-ms]
  (loop [replica replica]
    (if-let [entry (first (alts!! [ch (timeout timeout-ms)]))]
      (let [new-replica (extensions/apply-log-entry entry replica)]
        (recur new-replica))
      replica)))

(defn job-allocation-counts [replica job-info]
  (if-let [allocations (get-in replica [:allocations (:job-id job-info)])]
    (mapv (comp count allocations :id)
          (:task-ids job-info))
    []))

(defn get-counts [replica job-infos]
  (mapv (partial job-allocation-counts replica) job-infos))

(defn load-config
  ([]
     (load-config "test-config.edn"))
  ([filename]
     (let [yeller-token (System/getenv "YELLER_TOKEN")
           impl (System/getenv "TEST_TRANSPORT_IMPL")]
       (cond-> (read-string (slurp (clojure.java.io/resource filename)))
               (not-empty yeller-token)
               (assoc-in [:peer-config :onyx.log/config :appenders :yeller]
                         (yeller.timbre-appender/make-yeller-appender
                          {:token yeller-token
                           :environment "citests"}))
               (= impl "aeron")
               (assoc-in [:peer-config :onyx.messaging/impl] :aeron)))))

(defn try-start-env [env-config]
  (try
    (onyx.api/start-env env-config)
    (catch Throwable e
      nil)))

(defn try-start-group [peer-config]
  (try
    (onyx.api/start-peer-group peer-config)
    (catch Throwable e
      nil)))

(defn try-start-peers [n-peers peer-group]
  (try
    (onyx.api/start-peers n-peers peer-group)
    (catch Throwable e
      nil)))

(defrecord OnyxTestEnv [n-peers]
  component/Lifecycle

  (start [component]
    (println "Starting Onyx test environment")
    (let [onyx-id (java.util.UUID/randomUUID)
          {:keys [env-config peer-config]} (load-config)
          env (try-start-env env-config)
          peer-group (try-start-group peer-config)
          peers (try-start-peers n-peers peer-group)]
      (assoc component
        :env env :peer-group peer-group
        :peers peers :onyx-id onyx-id)))

  (stop [component]
    (println "Stopping Onyx test environment")

    (doseq [v-peer (:peers component)]
      (try
        (onyx.api/shutdown-peer v-peer)
        (catch InterruptedException e)))

    (when-let [pg (:peer-group component)]
      (try
        (onyx.api/shutdown-peer-group pg)
        (catch InterruptedException e)))

    (when-let [env (:env component)]
      (try
        (onyx.api/shutdown-env env)
        (catch InterruptedException e)))

    (assoc component :env nil :peer-group nil :peers nil)))
