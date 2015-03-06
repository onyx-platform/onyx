(ns ^:no-doc onyx.peer.function
    (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
              [onyx.static.planning :refer [find-task]]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [taoensso.timbre :refer [debug]]
              [dire.core :refer [with-post-hook!]])
    (:import [java.util UUID]
             [java.security MessageDigest]))

(defn hash-value [x]
  (let [md5 (MessageDigest/getInstance "MD5")]
    (apply str (.digest md5 (.getBytes (pr-str x) "UTF-8")))))

(defn group-message [segment catalog task]
  (let [t (find-task catalog task)]
    (if-let [k (:onyx/group-by-key t)]
      (hash-value (get segment k))
      (when-let [f (:onyx/group-by-fn t)]
        (hash-value ((operation/resolve-fn {:onyx/fn f}) segment))))))

(defn compress-segment [next-tasks catalog message]
  ;;   :hash-group (reduce (fn [groups t] (assoc groups t (group-message segment catalog t))) {} next-tasks)
  message)

(defn write-batch [queue session msgs paths task->producer]
  (doseq [[msg path] (map vector msgs paths)]
    (doseq [output-path (:paths path)]
      (let [p (get task->producer output-path)]
        (if-let [group (get (:hash-group msg) output-path)]
          (extensions/produce-message queue p session (:compressed msg) {:group group})
          (extensions/produce-message queue p session (:compressed msg))))))
  {:onyx.core/written? true})

(defn read-batch-shim
  [{:keys [onyx.core/queue onyx.core/session onyx.core/ingress-queues
           onyx.core/catalog onyx.core/task-map] :as event}]
  (let [drained (:drained-inputs @(:onyx.core/pipeline-state event))
        consumer-f (partial extensions/create-consumer queue session)
        uncached (into {} (remove (fn [[t _]] (get drained t)) ingress-queues))
        consumers (reduce-kv (fn [all k v] (assoc all k (consumer-f v))) {} uncached)
        batch (read-batch queue consumers task-map event)]
    (merge event {:onyx.core/batch batch :onyx.core/consumers consumers})))

(defn decompress-batch-shim [{:keys [onyx.core/queue onyx.core/batch] :as event}]
  (let [decompressed-msgs (map (partial decompress-segment queue) (map :message batch))]
    (merge event {:onyx.core/decompressed decompressed-msgs})))

(defn strip-sentinel-shim [event]
  (operation/on-last-batch
   event
   (fn [{:keys [onyx.core/queue onyx.core/session onyx.core/ingress-queues]}]
     (let [task-name (:input (last (:onyx.core/batch event)))
           queue-name (get ingress-queues task-name)]
       (extensions/n-messages-remaining queue session queue-name)))))

(defn requeue-sentinel-shim
  [{:keys [onyx.core/queue onyx.core/session onyx.core/ingress-queues] :as event}]
  (let [uuid (or (extensions/message-uuid queue (:message (last (:onyx.core/batch event))))
                 (UUID/randomUUID))]
    (let [task-name (:input (last (:onyx.core/batch event)))
          queue-name (get ingress-queues task-name)]
      (requeue-sentinel queue session queue-name uuid)))
  (merge event {:onyx.core/requeued? true}))

(defn apply-fn-shim
  [{:keys [onyx.core/decompressed onyx.function/fn onyx.core/params
           onyx.core/task-map] :as event}]
  (let [results (flatten (map (partial operation/apply-fn fn params) decompressed))]
    (merge event {:onyx.core/results results})))

(defn compress-batch-shim
  [{:keys [onyx.core/results onyx.core/catalog
           onyx.core/serialized-task onyx.core/result-paths] :as event}]
  (let [next-tasks (keys (:egress-queues serialized-task))
        compressed-msgs (map (fn [[result path]] (compress-segment next-tasks catalog result path))
                             (map vector results result-paths))]
    (merge event {:onyx.core/compressed compressed-msgs})))

(defn write-batch-shim
  [{:keys [onyx.core/queue onyx.core/egress-queues onyx.core/serialized-task
           onyx.core/catalog onyx.core/session onyx.core/compressed
           onyx.core/result-paths] :as event}]
  (let [task->producer (into {} (map (fn [[task queue-name]]
                                       {task (extensions/create-producer queue session queue-name)})
                                     (:egress-queues serialized-task)))
        batch (write-batch queue session compressed result-paths task->producer)]
    (merge event {:onyx.core/producers (vals task->producer)})))

(defn seal-resource-shim [{:keys [onyx.core/queue onyx.core/egress-queues] :as event}]
  (merge event (seal-queue queue (vals egress-queues))))

(defmethod l-ext/start-lifecycle? :function
  [_ event]
  {:onyx.core/start-lifecycle? (operation/start-lifecycle? event)})

(defmethod l-ext/inject-lifecycle-resources :function
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.function/fn (operation/resolve-fn task-map)})

(defmethod p-ext/read-batch :default
  [{:keys [onyx.core/messenger] :as event}]
  {:onyx.core/batch (onyx.extensions/receive-messages messenger event)})

(defmethod p-ext/decompress-batch :default
  [{:keys [onyx.core/queue onyx.core/batch] :as event}]
  {:onyx.core/decompressed batch})

(defmethod p-ext/apply-fn :default
  [{:keys [onyx.core/params] :as event} segment]
  (operation/apply-fn (:onyx.function/fn event) params segment))

(defmethod p-ext/compress-batch :default
  [{:keys [onyx.core/results onyx.core/catalog onyx.core/serialized-task]
    :as event}]
  (let [next-tasks (:egress-ids serialized-task)
        compressed-msgs (map (partial compress-segment next-tasks catalog) results)]
    (merge event {:onyx.core/compressed compressed-msgs})))

(defmethod p-ext/write-batch :default
  [{:keys [onyx.core/messenger onyx.core/job-id] :as event}]
  (let [replica @(:onyx.core/replica event)]
    (doseq [task-id (vals (:egress-ids (:onyx.core/serialized-task event)))]
      (let [peers (get-in replica [:allocations job-id task-id])
            active-peers (filter #(= (get-in replica [:peer-state %]) :active) peers)
            target (rand-nth active-peers)
            link (operation/peer-link event target :send-peer-site)]
        (onyx.extensions/send-messages messenger event link)))))

(defmethod p-ext/seal-resource :default
  [{:keys [] :as event}]
  {})

