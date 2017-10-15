(ns onyx.monitoring.metrics-monitoring
  (:require [metrics.core :refer [new-registry]]
            [metrics.meters :as m :refer [meter rates]]
            [metrics.histograms :as h]
            [metrics.timers :as t]
            [metrics.gauges :as g]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.extensions :as extensions]
            [metrics.counters :as c]
            [onyx.protocol.task-state :as task]
            [onyx.static.util :refer [ns->ms]]
            [taoensso.timbre :refer [warn info]]
            [com.stuartsierra.component :as component])
  (:import [com.codahale.metrics JmxReporter]
           [java.util.concurrent.atomic AtomicLong AtomicInteger]
           [com.codahale.metrics Gauge]
           [java.util.concurrent TimeUnit]))

(defn job-metric->metric-str [s attribute value]
  (let [[t job task lifecycle] (-> s
                                   (clojure.string/replace #"^name=" "")
                                   (clojure.string/split #"[.]"))
        lifecycle (clojure.string/replace lifecycle #"-" "_")
        tags (format "{job=\"%s\", task=\"%s\",}" job task)]
    (format "onyx_job_task_%s_%s%s %s" lifecycle (name attribute) tags value)))

(defn update-timer! [^com.codahale.metrics.Timer timer ms]
  (.update timer ms TimeUnit/MILLISECONDS))

(defn update-timer-ns! [^com.codahale.metrics.Timer timer ns]
  (.update timer ns TimeUnit/NANOSECONDS))

(defrecord Monitoring []
  extensions/IEmitEvent
  (extensions/registered? [this event-type]
    (get this event-type))
  (extensions/emit [this event]
    (when-let [f (get this (:event event))]
      (f this event)))
  component/Lifecycle
  (component/start [component]
    (let [reg (new-registry)
          write-log-entry-bytes (h/histogram reg ["zookeeper" "write-log-entry" "bytes"])
          write-log-entry-latency (t/timer reg ["zookeeper" "write-log-entry" "latency"])
          write-catalog-bytes (h/histogram reg ["zookeeper" "write-catalog" "bytes"])
          write-catalog-latency (t/timer reg ["zookeeper" "write-catalog" "latency"])
          write-workflow-bytes (h/histogram reg ["zookeeper" "write-workflow" "bytes"])
          write-workflow-latency (t/timer reg ["zookeeper" "write-workflow" "latency"])
          write-flow-conditions-bytes (h/histogram reg ["zookeeper" "write-flow-conditions" "bytes"])
          write-flow-conditions-latency (t/timer reg ["zookeeper" "write-flow-conditions" "latency"])
          write-lifecycles-bytes (h/histogram reg ["zookeeper" "write-lifecycles" "bytes"])
          write-lifecycles-latency (t/timer reg ["zookeeper" "write-lifecycles" "latency"])
          write-task-bytes (h/histogram reg ["zookeeper" "write-task" "bytes"])
          write-task-latency (t/timer reg ["zookeeper" "write-task" "latency"])
          write-chunk-bytes (h/histogram reg ["zookeeper" "write-chunk" "bytes"])
          write-chunk-latency (t/timer reg ["zookeeper" "write-chunk" "latency"])
          write-job-scheduler-bytes (h/histogram reg ["zookeeper" "write-job-scheduler" "bytes"])
          write-job-scheduler-latency (t/timer reg ["zookeeper" "write-job-scheduler" "latency"])
          write-messaging-bytes (h/histogram reg ["zookeeper" "write-messaging" "bytes"])
          write-messaging-latency (t/timer reg ["zookeeper" "write-messaging" "latency"])
          force-write-chunk-bytes (h/histogram reg ["zookeeper" "force-write-chunk" "bytes"])
          force-write-chunk-latency (t/timer reg ["zookeeper" "force-write-chunk" "latency"])
          write-origin-bytes (h/histogram reg ["zookeeper" "write-origin" "bytes"])
          write-origin-latency (t/timer reg ["zookeeper" "write-origin" "latency"])
          read-log-entry-latency (t/timer reg ["zookeeper" "read-log-entry" "latency"])
          read-catalog-latency (t/timer reg ["zookeeper" "read-catalog" "latency"])
          read-workflow-latency (t/timer reg ["zookeeper" "read-workflow" "latency"])
          read-flow-conditions-latency (t/timer reg ["zookeeper" "read-flow-conditions" "latency"])
          read-lifecycles-latency (t/timer reg ["zookeeper" "read-lifecycles" "latency"])
          read-task-latency (t/timer reg ["zookeeper" "read-task" "latency"])
          read-chunk-latency (t/timer reg ["zookeeper" "read-chunk" "latency"])
          read-job-scheduler-latency (t/timer reg ["zookeeper" "read-job-scheduler" "latency"])
          read-messaging-latency (t/timer reg ["zookeeper" "read-messaging" "latency"])
          force-read-chunk-latency (t/timer reg ["zookeeper" "force-read-chunk" "latency"])
          read-origin-latency (t/timer reg ["zookeeper" "read-origin" "latency"])
          gc-log-entry-position (g/gauge reg ["zookeeper" "gc-log-entry" "position"])
          gc-log-entry-latency (t/timer reg ["zookeeper" "gc-log-entry" "latency"])
          group-prepare-join-cnt (c/counter reg ["group" "prepare-join" "event"])
          group-accept-join-cnt (c/counter reg ["group" "accept-join" "event"])
          group-notify-join-cnt (c/counter reg ["group" "notify-join" "event"])
          peer-error-rate (m/meter reg ["peer-group" "peer" "errors"])

          peer-group-peer-allocated-proportion (atom 0)
          peer-group-gg (g/gauge-fn reg
                                    ["peer-group" "peer-allocated-proportion"] 
                                    (fn [] 
                                      (double (deref peer-group-peer-allocated-proportion))))

          last-heartbeat (AtomicLong.)
          peer-group-heartbeat (g/gauge-fn reg
                                           ["peer-group" "since-heartbeat"] 
                                           (fn [] 
                                             (ns->ms (- (System/nanoTime)
                                                        (.get ^AtomicLong last-heartbeat)))))
          scheduler-lag (AtomicLong. 0)
          peer-group-scheduler-lag (g/gauge-fn reg
                                               ["peer-group" "scheduler-lag"] 
                                               (fn [] 
                                                 (.get ^AtomicLong scheduler-lag)))
          number-peer-shutdowns (AtomicLong. 0)
          peer-group-num-peer-shutdowns (g/gauge-fn reg
                                                    ["peer-group" "peers-shutting-down"] 
                                                    (fn [] 
                                                      (.get ^AtomicLong number-peer-shutdowns)))
          peers-shutdown-duration-ms (AtomicLong. 0)
          peer-group-num-peer-shutdowns (g/gauge-fn reg
                                                    ["peer-group" "peers-max-shutdown-duration-ms"] 
                                                    (fn [] 
                                                      (.get ^AtomicLong peers-shutdown-duration-ms)))
          reporter (-> (JmxReporter/forRegistry reg)
                       (.inDomain "org.onyxplatform")
                       (.build))
          _ (.start ^JmxReporter reporter)] 
      (info "Started Metrics Reporting to JMX.")
      (assoc component
             :monitoring :custom
             :registry reg
             :reporter reporter
             :set-scheduler-lag! (fn [^long v] (.set ^AtomicLong scheduler-lag v))
             :set-num-peer-shutdowns! (fn [^long v] (.set ^AtomicLong number-peer-shutdowns v))
             :set-peer-shutdown-duration-ms! (fn [^long v] (.set ^AtomicLong peers-shutdown-duration-ms v))
             :set-peer-group-allocation-proportion! (fn [ratio] (reset! peer-group-peer-allocated-proportion ratio))
             :peer-group-heartbeat! (fn [] (.set ^AtomicLong last-heartbeat ^long (System/nanoTime)))
             :peer-error! (fn [] (m/mark! peer-error-rate))
             :zookeeper-write-log-entry (fn [config metric] 
                                          (h/update! write-log-entry-bytes (:bytes metric))
                                          (update-timer! write-log-entry-latency (:latency metric)))
             :zookeeper-write-catalog (fn [config metric] 
                                        (h/update! write-catalog-bytes (:bytes metric))
                                        (update-timer! write-catalog-latency (:latency metric)))
             :zookeeper-write-workflow (fn [config metric] 
                                         (h/update! write-workflow-bytes (:bytes metric))
                                         (update-timer! write-workflow-latency (:latency metric)))
             :zookeeper-write-flow-conditions (fn [config metric] 
                                                (h/update! write-flow-conditions-bytes (:bytes metric))
                                                (update-timer! write-flow-conditions-latency (:latency metric)))
             :zookeeper-write-lifecycles (fn [config metric] 
                                           (h/update! write-lifecycles-bytes (:bytes metric))
                                           (update-timer! write-lifecycles-latency (:latency metric)))
             :zookeeper-write-task (fn [config metric] 
                                     (h/update! write-task-bytes (:bytes metric))
                                     (update-timer! write-task-latency (:latency metric)))
             :zookeeper-write-chunk (fn [config metric] 
                                      (h/update! write-task-bytes (:bytes metric))
                                      (update-timer! write-task-latency (:latency metric)))
             :zookeeper-write-job-scheduler (fn [config metric] 
                                              (h/update! write-job-scheduler-bytes (:bytes metric))
                                              (update-timer! write-job-scheduler-latency (:latency metric)))
             :zookeeper-write-messaging (fn [config metric] 
                                          (h/update! write-messaging-bytes (:bytes metric))
                                          (update-timer! write-messaging-latency (:latency metric)))
             :zookeeper-force-write-chunk (fn [config metric] 
                                            (h/update! force-write-chunk-bytes (:bytes metric))
                                            (update-timer! force-write-chunk-latency (:latency metric)))
             :zookeeper-write-origin (fn [config metric] 
                                       (h/update! write-origin-bytes (:bytes metric))
                                       (update-timer! write-origin-latency (:latency metric)))
             :zookeeper-read-log-entry (fn [config metric] 
                                         (update-timer! read-log-entry-latency (:latency metric)))
             :zookeeper-read-catalog (fn [config metric] 
                                       (update-timer! read-catalog-latency (:latency metric)))
             :zookeeper-read-workflow (fn [config metric] 
                                        (update-timer! read-workflow-latency (:latency metric)))
             :zookeeper-read-flow-conditions (fn [config metric] 
                                               (update-timer! read-flow-conditions-latency (:latency metric)))
             :zookeeper-read-lifecycles (fn [config metric] 
                                          (update-timer! read-lifecycles-latency (:latency metric)))
             :zookeeper-read-task (fn [config metric] 
                                    (update-timer! read-task-latency (:latency metric)))
             :zookeeper-read-chunk (fn [config metric] 
                                     (update-timer! read-chunk-latency (:latency metric)))
             :zookeeper-read-origin (fn [config metric] 
                                      (update-timer! read-origin-latency (:latency metric)))
             :zookeeper-read-job-scheduler (fn [config metric] 
                                             (update-timer! read-job-scheduler-latency (:latency metric)))
             :zookeeper-read-messaging (fn [config metric] 
                                         (update-timer! read-messaging-latency (:latency metric)))
             :zookeeper-gc-log-entry (fn [config metric] 
                                       (h/update! gc-log-entry-position (:position metric))
                                       (update-timer! gc-log-entry-latency (:latency metric)))
             :group-prepare-join (fn [config metric] 
                                   (c/inc! group-prepare-join-cnt))
             :group-notify-join (fn [config metric] 
                                  (c/inc! group-notify-join-cnt))
             :group-accept-join (fn [config metric] 
                                  (c/inc! group-accept-join-cnt)))))
  (component/stop [{:keys [registry reporter] :as component}]
    (info "Stopping Metrics Reporting to JMX.")
    (.stop ^JmxReporter reporter)
    (metrics.core/remove-metrics registry)
    (assoc component :registry nil :reporter nil)))

(defn new-monitoring []
  (map->Monitoring {}))

(defn new-lifecycle-latency [reg tag lifecycle]
  (let [timer ^com.codahale.metrics.Timer (t/timer reg (into tag ["task-lifecycle" (name lifecycle)]))] 
    (fn [state latency-ns]
      (.update timer latency-ns TimeUnit/NANOSECONDS))))

(defn new-read-batch [reg tag lifecycle]
  (let [throughput (m/meter reg (conj tag (clojure.string/join "_" ["task-lifecycle" (name lifecycle) "throughput"])))
        timer ^com.codahale.metrics.Timer (t/timer reg (into tag ["task-lifecycle" (name lifecycle)]))] 
    (fn [state latency-ns]
      (let [size (count (:onyx.core/batch (task/get-event state)))] 
        (when-not (zero? size)
          (m/mark! throughput size)
          (.update timer latency-ns TimeUnit/NANOSECONDS))))))

(defn count-written-batch [state]
  (reduce (fn [c {:keys [leaves]}]
            (unchecked-add c (count leaves)))
          (long 0)
          (:tree (:onyx.core/results (task/get-event state)))))

(defn new-write-batch [reg tag lifecycle]
  (let [throughput (m/meter reg (conj tag (clojure.string/join "_" ["task-lifecycle" (name lifecycle) "throughput"])))
        timer ^com.codahale.metrics.Timer (t/timer reg (into tag ["task-lifecycle" (name lifecycle)]))
        accum (volatile! (long 0))]
    (fn [state latency-ns]
      (vswap! accum (fn [a] (unchecked-add a latency-ns)))
      (when (task/advanced? state)
        (let [cnt (count-written-batch state)]
          (when-not (zero? cnt)
            (.update timer @accum TimeUnit/NANOSECONDS)
            (vreset! accum (long 0))
            (m/mark! throughput cnt)))))))

(defn update-rv-epoch [^AtomicLong replica-version ^AtomicLong epoch epoch-rate]
  (fn [state latency-ns]
    (m/mark! epoch-rate 1)
    (.set ^AtomicLong replica-version (task/replica-version state))
    (.set ^AtomicLong epoch (task/epoch state))))

(defn cleanup-keyword [k]
  (if-let [n (namespace k)]
    (str n "/" (name k))
    (name k)))

(defn normalize-tag [tag]
  (-> tag
      (clojure.string/replace "." "_")
      (clojure.string/replace ":" "")))

(defrecord TaskMonitoring [event]
  extensions/IEmitEvent
  (extensions/registered? [this event-type]
    (get this event-type))
  (extensions/emit [this event]
    (when-let [f (get this (:event event))]
      (f this event)))
  component/Lifecycle
  (component/start [component]
    (let [{:keys [onyx.core/job-id onyx.core/id onyx.core/slot-id onyx.core/monitoring 
                  onyx.core/task onyx.core/metadata onyx.core/peer-opts]} event
          lifecycles (arg-or-default :onyx.peer.metrics/lifecycles peer-opts)
          job-name (get metadata :job-name job-id)
          job-name (cond-> job-name 
                     (keyword? job-name) cleanup-keyword)
          extra-tags (get-in metadata [:tags task])
          task-name (cleanup-keyword task)
          task-registry (new-registry)
          tag (into (mapv str ["job-name" job-name "job-id" job-id "task" task-name "slot-id" slot-id "peer-id" id])
                    (map normalize-tag (reduce into [] extra-tags)))
          replica-version (AtomicLong.)
          epoch (AtomicLong.)
          task-state-index (AtomicInteger.)
          task-state-index-gg (g/gauge-fn task-registry (conj tag "lifecycle-index") (fn [] (.get ^AtomicInteger task-state-index)))

          written-bytes (AtomicLong.)
          written-bytes-gg (g/gauge-fn task-registry (conj tag "written-bytes") (fn [] (.get ^AtomicLong written-bytes)))

          gg-replica-version (g/gauge-fn task-registry (conj tag "replica-version") (fn [] (.get ^AtomicLong replica-version)))
          gg-epoch (g/gauge-fn task-registry (conj tag "epoch") (fn [] (.get ^AtomicLong epoch)))
          epoch-rate (m/meter task-registry (conj tag "epoch-rate"))
          update-rv-epoch-fn (update-rv-epoch replica-version epoch epoch-rate)
          batch-serialization-latency ^com.codahale.metrics.Timer (t/timer task-registry (into tag ["serialization-latency"]))
          written-bytes (AtomicLong.)
          written-bytes-gg (g/gauge-fn task-registry (conj tag "written-bytes") (fn [] (.get ^AtomicLong written-bytes)))

          publication-errors (AtomicLong.)
          publication-errors-gg (g/gauge-fn task-registry (conj tag "publication-errors") (fn [] (.get ^AtomicLong publication-errors)))

          checkpoint-written-bytes (AtomicLong.)
          checkpoint-written-bytes-gg (g/gauge-fn task-registry (conj tag "checkpoint-written-bytes") (fn [] (.get ^AtomicLong checkpoint-written-bytes)))
          checkpoint-read-bytes (AtomicLong.)
          checkpoint-read-bytes-gg (g/gauge-fn task-registry (conj tag "checkpoint-read-bytes") (fn [] (.get ^AtomicLong checkpoint-read-bytes)))
          read-bytes (AtomicLong.)
          read-bytes-gg (g/gauge-fn task-registry (conj tag "read-bytes") (fn [] (.get ^AtomicLong read-bytes)))

          subscription-errors (AtomicLong.)
          subscription-errors-gg (g/gauge-fn task-registry (conj tag "subscription-errors") (fn [] (.get ^AtomicLong subscription-errors)))
          since-received-heartbeat ^com.codahale.metrics.Timer (t/timer task-registry (into tag ["since-received-heartbeat"]))
          last-heartbeat (AtomicLong. (System/nanoTime))
          peer-heartbeat (g/gauge-fn task-registry
                                     (conj tag "since-heartbeat")
                                     (fn []
                                       (ns->ms (- (System/nanoTime)
                                                  (.get ^AtomicLong last-heartbeat)))))
          time-init-state (AtomicLong. (System/nanoTime))
          time-in-state-gg (g/gauge-fn task-registry
                                       (conj tag "current-lifecycle-duration")
                                       (fn []
                                         (ns->ms (- (System/nanoTime) 
                                                    (.get ^AtomicLong time-init-state)))))
          checkpoint-serialization-latency ^com.codahale.metrics.Timer (t/timer task-registry (into tag ["checkpoint-serialization-latency"]))
          checkpoint-store-latency ^com.codahale.metrics.Timer (t/timer task-registry (into tag ["checkpoint-store-latency"]))
          checkpoint-size (AtomicLong.)
          checkpoint-size-gg (g/gauge-fn task-registry (conj tag "checkpoint-size") (fn [] (.get ^AtomicLong checkpoint-size)))
          read-offset (AtomicLong.)
          read-offset-gg (g/gauge-fn task-registry (conj tag "offset") (fn [] (.get ^AtomicLong read-offset)))

          lag-gauge (AtomicLong. -1)
          lag-gauge-gg (g/gauge-fn task-registry (conj tag "lag") (fn [] (.get ^AtomicLong lag-gauge)))

          coordinator-watermark-gauge (AtomicLong. -1)
          coordinator-watermark-gg (g/gauge-fn task-registry (conj tag "coordinator-watermark") 
                                               (fn [] (.get ^AtomicLong coordinator-watermark-gauge)))

          workflow-watermark-gauge (AtomicLong. -1)
          workflow-watermark-gg (g/gauge-fn task-registry (conj tag "workflow-watermark") 
                                            (fn [] (.get ^AtomicLong workflow-watermark-gauge)))

          recover-latency ^com.codahale.metrics.Timer (t/timer task-registry (into tag ["recover-latency"]))
          reporter (-> (JmxReporter/forRegistry task-registry)
                       (.inDomain "org.onyxplatform")
                       (.build))
          _ (.start ^JmxReporter reporter)] 
      (info "Starting Task Metrics Reporter. Starting reporting to JMX.")
      (reduce 
       (fn [mon lifecycle]
         (assoc mon 
                lifecycle 
                (case lifecycle
                  :lifecycle/unblock-subscribers update-rv-epoch-fn
                  :lifecycle/read-batch (new-read-batch task-registry tag :lifecycle/read-batch) 
                  :lifecycle/write-batch (new-write-batch task-registry tag :lifecycle/write-batch) 
                  (new-lifecycle-latency task-registry tag lifecycle))))
       (assoc component
              :id id
              :written-bytes written-bytes
              :publication-errors publication-errors
              :read-bytes read-bytes
              :subscription-errors subscription-errors
              :task-state-index task-state-index
              :checkpoint-written-bytes checkpoint-written-bytes
              :checkpoint-read-bytes checkpoint-read-bytes
              :checkpoint-serialization-latency checkpoint-serialization-latency
              :checkpoint-store-latency checkpoint-store-latency
              :checkpoint-size checkpoint-size
              :checkpoint-written-bytes checkpoint-written-bytes
              :coordinator-watermark coordinator-watermark-gauge
              :workflow-watermark workflow-watermark-gauge
              :read-offset read-offset
              :lag-gauge lag-gauge
              :recover-latency recover-latency
              :last-heartbeat last-heartbeat
              :time-init-state time-init-state
              :since-received-heartbeat since-received-heartbeat
              :monitoring :custom
              :registry task-registry
              :reporter reporter)
       lifecycles)))
  (component/stop [{:keys [registry reporter id] :as component}]
    (info id "Stopping Task Metrics Reporter. Stopped reporting to JMX.")
    (.stop ^JmxReporter reporter)
    (metrics.core/remove-metrics registry)
    (assoc component :registry nil :reporter nil)))

(defn new-task-monitoring [event]
  (->TaskMonitoring event))
