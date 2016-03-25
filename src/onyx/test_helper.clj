(ns onyx.test-helper
  (:require [clojure.core.async :refer [chan >!! alts!! timeout <!! close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info warn trace fatal error] :as timbre]
            [schema.core :as s]
            [onyx.extensions :as extensions]
            [onyx.peer.function :as function]
            [onyx.system :as system]
            [onyx.api]))

(defn feedback-exception!
  "Feeds an exception that killed a job back to a client. 
   Blocks until the job is complete."
  ([peer-config job-id]
   (let [client (component/start (system/onyx-client peer-config))]
     (try 
       (feedback-exception! peer-config job-id (:log client))
       (finally
         (component/stop client)))))
  ([peer-config job-id log]
   (when-not (onyx.api/await-job-completion peer-config job-id)
     (throw (extensions/read-chunk log :exception job-id)))))


(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (when-not (seq matches)
      (throw (ex-info (format "Couldn't find task %s in catalog" task-name)
                      {:catalog catalog :task-name task-name})))
    (first matches)))

(defn n-peers
  "Takes a workflow and catalog, returns the minimum number of peers
   needed to execute this job."
  [{:keys[catalog workflow] :as job}]
  (let [task-set (into #{} (apply concat workflow))]
    (reduce
     (fn [sum t]
       (+ sum (or (:onyx/min-peers (find-task catalog t)) 1)))
     0 task-set)))

(defn validate-enough-peers!  
  "Checks that the test environment will start enough peers to start the job.  Do
  not use this validation function in production as it does not check for the
  number of peers running over a cluster, and the number of peers that has joined
  is subject to change as nodes come online and go offline." 
  [test-env job]
  (let [required-peers (n-peers job)] 
    (when (< (:n-peers test-env) required-peers)
      (throw (ex-info (format "test-env requires at least %s peers to start the job" required-peers)
                      {:job job
                       :n-peers (:n-peers test-env)
                       :required-n-peers required-peers})))))

(defn playback-log [log replica ch timeout-ms]
  (loop [replica replica]
    (if-let [entry (first (alts!! [ch (timeout timeout-ms)]))]
      (let [new-replica (extensions/apply-log-entry entry replica)]
        (recur new-replica))
      replica)))

(defn job-allocation-counts [replica job-info]
  (if-let [allocations (get-in replica [:allocations (:job-id job-info)])]
    (reduce-kv (fn [result task-id peers]
                 (assoc result task-id (count peers)))
               {}
               allocations)
    {}))

(defn get-counts [replica job-infos]
  (mapv (partial job-allocation-counts replica) job-infos))

(defn load-config
  ([]
     (load-config "test-config.edn"))
  ([filename]
     (let [impl (System/getenv "TEST_TRANSPORT_IMPL")]
       (cond-> (read-string (slurp (clojure.java.io/resource filename)))
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

(defn try-start-peers [n-peers peer-group monitoring-config]
  (try
    (onyx.api/start-peers n-peers peer-group (or monitoring-config {:monitoring :custom}))
    (catch Throwable e
      nil)))

(defn add-test-env-peers! 
  "Add peers to an OnyxTestEnv component"
  [{:keys [peer-group peers monitoring-config] :as component} n-peers]
  (swap! peers into (try-start-peers n-peers peer-group monitoring-config)))

(defrecord OnyxTestEnv [env-config peer-config monitoring-config n-peers]
  component/Lifecycle

  (start [component]
    (println "Starting Onyx test environment")
    (let [env (try-start-env env-config)
          peer-group (try-start-group peer-config)
          peers (try-start-peers n-peers peer-group monitoring-config)]
      (assoc component :env env :peer-group peer-group :peers (atom peers))))

  (stop [component]
    (println "Stopping Onyx test environment")

    (doseq [v-peer @(:peers component)]
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

(defmacro with-test-env 
  "Start a test env in a way that shuts down after body is completed. 
   Useful for running tests that can be killed, and re-run without bouncing the repl."
  [[symbol-name [n-peers env-config peer-config monitoring-config]] & body]
  `(let [~symbol-name (component/start (map->OnyxTestEnv {:n-peers ~n-peers 
                                                          :env-config ~env-config 
                                                          :peer-config ~peer-config
                                                          :monitoring-config ~monitoring-config}))]
     (try
       (s/with-fn-validation ~@body)
       (catch InterruptedException e#
         (Thread/interrupted))
       (finally
         (component/stop ~symbol-name)))))
