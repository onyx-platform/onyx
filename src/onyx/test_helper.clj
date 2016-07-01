(ns onyx.test-helper
  (:require [clojure.core.async :refer [chan >!! alts!! timeout <!! close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info warn trace fatal error] :as timbre]
            [schema.core :as s]
            [onyx.extensions :as extensions]
            [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
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

(defn job->min-peers-per-task 
  [{:keys [catalog workflow] :as job}]
  (let [task-set (into #{} (apply concat workflow))]
    (mapv (fn [t]
           {:task t :min-peers (or (:onyx/min-peers (find-task catalog t)) 1)})
         task-set)))

(defn validate-enough-peers!  
  "Checks that the test environment will start enough peers to start the job.  Do
  not use this validation function in production as it does not check for the
  number of peers running over a cluster, and the number of peers that has joined
  is subject to change as nodes come online and go offline." 
  [test-env job]
  (let [peers-per-task (job->min-peers-per-task job)
        required-peers (reduce + (map :min-peers peers-per-task))] 
    (when (< (:n-peers test-env) required-peers)
      (throw (ex-info (format "test-env requires at least %s peers to start the job. 
                               validate-enough-peers! checks your job to see whether you've started enough peers before submitting a job to the test cluster that might hang.
                               Tasks each require at least one peer to be started, and may require more if :onyx/n-peers or :onyx/min-peers is set.
                               Minimum peers for each task: %s." 
                              required-peers
                              peers-per-task)
                      {:required-n-peers required-peers
                       :peers-peer-task peers-per-task
                       :job job
                       :n-peers (:n-peers test-env)})))))

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
    (catch InterruptedException e)
    (catch Throwable e
      (throw e))))

(defn try-start-group [peer-config monitoring-config]
  (try
    (let [m-cfg (or monitoring-config {:monitoring :no-op})]
      (onyx.api/start-peer-group peer-config m-cfg))
    (catch InterruptedException e)
    (catch Throwable e
      (throw e))))

(defn try-start-peers 
  [n-peers peer-group]
  (try
   (onyx.api/start-peers n-peers peer-group)
   (catch InterruptedException e)
   (catch Throwable e
     (throw e))))

(defn add-test-env-peers! 
  "Add peers to an OnyxTestEnv component"
  [{:keys [peer-group peers monitoring-config] :as component} n-peers]
  (swap! peers into (try-start-peers n-peers peer-group)))

(defn shutdown-peer [v-peer]
  (try
    (onyx.api/shutdown-peer v-peer)
    (catch InterruptedException e)))

(defn shutdown-peer-group [peer-group]
  (try
    (onyx.api/shutdown-peer-group peer-group)
    (catch InterruptedException e)))

(defn shutdown-env [env]
  (try
    (onyx.api/shutdown-env env)
    (catch InterruptedException e)))

(defrecord OnyxTestEnv [env-config peer-config monitoring-config n-peers]
  component/Lifecycle

  (start [component]
    (println "Starting Onyx test environment")
    (let [env (try-start-env env-config)
          peer-group (try-start-group peer-config monitoring-config)
          peers (try-start-peers n-peers peer-group)]
      (assoc component :env env :peer-group peer-group :peers (atom peers))))

  (stop [component]
    (println "Stopping Onyx test environment")

    (doseq [v-peer @(:peers component)]
      (shutdown-peer v-peer))

    (when-let [pg (:peer-group component)]
      (shutdown-peer-group pg))

    (when-let [env (:env component)]
      (shutdown-env env))

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
       (catch ThreadDeath e#
         (Thread/interrupted))
       (finally
         (component/stop ~symbol-name)))))

(defrecord DummyInput []
  p-ext/Pipeline
  (write-batch
    [this event])

  (read-batch [_ event]
    (Thread/sleep 500)
    {:onyx.core/batch []})

  p-ext/PipelineInput
  (ack-segment [_ _ message-id])

  (retry-segment
    [_ _ message-id])

  (pending?
    [_ _ message-id]
    false)

  (drained?
    [_ _]
    false))

(defn dummy-input [pipeline-data]
  (->DummyInput))

(defrecord DummyOutput []
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ event]
    {})

  (seal-resource
    [_ event]))

(defn dummy-output [pipeline-data]
  (->DummyOutput))
