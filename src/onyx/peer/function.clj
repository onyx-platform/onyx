(ns ^:no-doc onyx.peer.function
    (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
              [clojure.data.fressian :as fressian]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.queue.hornetq :refer [take-segments]]
              [onyx.planning :refer [find-task]]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [taoensso.timbre :refer [debug]]
              [dire.core :refer [with-post-hook!]])
    (:import [java.util UUID]
             [java.security MessageDigest]))

(defn requeue-sentinel [queue session queue-name uuid]
  (let [p (extensions/create-producer queue session queue-name)]
    (extensions/produce-message queue p session (.array (fressian/write :done)) {:uuid uuid})
    (extensions/close-resource queue p)))

(defn seal-queue [queue queue-names]
  (let [session (extensions/create-tx-session queue)]
    (doseq [queue-name queue-names]
      (let [producer (extensions/create-producer queue session queue-name)]
        (extensions/produce-message queue producer session (.array (fressian/write :done)))
        (extensions/close-resource queue producer)))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue session)))

(defn reader-thread [event queue reader-ch input consumer]
  (go
   (try
    (loop []
      (let [segment (extensions/consume-message queue consumer)]
        (when segment
          (>! reader-ch {:input input :message segment})
          (recur))))
    (finally
     (close! reader-ch)))))

(defn read-batch [queue consumers task-map event]
  (if-not (empty? consumers)
    (let [timeout-ms (or (:onyx/batch-timeout task-map) 1000)
          reader-chs (map (fn [_] (chan (:onyx/batch-size task-map))) (vals consumers))
          reader-threads (doall (map (fn [ch [input consumer]]
                                       (reader-thread event queue ch input consumer))
                                     reader-chs consumers))
          read-f #(first (alts!! (conj reader-chs (timeout timeout-ms))))
          segments (doall (take-segments read-f (:onyx/batch-size task-map)))]

      ;; Ack each of the segments. Closing a consumer with unacked tasks will send
      ;; all the tasks back onto the queue.
      (doseq [segment segments]
        (extensions/ack-message queue (:message segment)))

      ;; Close each consumer. If any consumers are left open with unacked messages,
      ;; the next round of reading will skip the messages that the consumers are hanging
      ;; onto - hence breaking the sequential reads for task that require it.
      (doseq [consumer (vals consumers)]
        (extensions/close-resource queue consumer))
      segments)
    []))

(defn decompress-segment [queue message]
  (extensions/read-message queue message))

(defn hash-value [x]
  (let [md5 (MessageDigest/getInstance "MD5")]
    (apply str (.digest md5 (.getBytes (pr-str x) "UTF-8")))))

(defn group-message [segment catalog task]
  (let [t (find-task catalog task)]
    (if-let [k (:onyx/group-by-key t)]
      (hash-value (get segment k))
      (when-let [f (:onyx/group-by-fn t)]
        (hash-value ((operation/resolve-fn {:onyx/fn f}) segment))))))

(defn compress-segment [next-tasks catalog segment]
  {:compressed (.array (fressian/write segment))
   :hash-group (reduce (fn [groups t]
                         (assoc groups t (group-message segment catalog t)))
                       {} next-tasks)})

(defn write-batch [queue session msgs paths task->producer]
  (doseq [[msg path] (map vector msgs paths)]
    (doseq [output-path path]
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

(defn choose-output-paths [flow-conditions event old all-new new]
  (reduce
   (fn [all entry]
     (if ((:flow/predicate entry) [event old all-new new])
       (if (:flow/short-circuit? entry)
         (reduced (conj all (:flow/to entry)))
         (conj all (:flow/to entry)))
       all))
   #{}
   flow-conditions))

(defn apply-fn-shim
  [{:keys [onyx.core/decompressed onyx.core/params
           onyx.core/task-map onyx.core/compiled-flow-conditions] :as event}]
  (reduce
   (fn [result input-segment]
     (let [new-segments (operation/apply-fn (:onyx.function/fn event) params input-segment)
           new-segments (if coll? new-segments) new-segments (vector new-segments)
           paths (map (fn [segment]
                        (choose-output-paths compiled-flow-conditions
                                             event input-segment new-segments
                                             segment))
                      new-segments)]
       (-> result
           (update-in [:onyx.core/results] concat new-segments)
           (update-in [:onyx.core/result-paths concat paths]))))
   {:onyx.core/results [] :onyx.core/result-paths []}
   decompressed))

(defn compress-batch-shim
  [{:keys [onyx.core/results onyx.core/catalog onyx.core/serialized-task] :as event}]
  (let [next-tasks (keys (:egress-queues serialized-task))
        compressed-msgs (map (partial compress-segment next-tasks catalog) results)]
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
  [event] (read-batch-shim event))

(defmethod p-ext/decompress-batch :default
  [event] (decompress-batch-shim event))

(defmethod p-ext/strip-sentinel :default
  [event] (strip-sentinel-shim event))

(defmethod p-ext/requeue-sentinel :default
  [event] (requeue-sentinel-shim event))

(defmethod p-ext/apply-fn :default
  [event] (apply-fn-shim event))

(defmethod p-ext/compress-batch :default
  [event] (compress-batch-shim event))

(defmethod p-ext/write-batch :default
  [event] (write-batch-shim event))

(defmethod p-ext/seal-resource :default
  [event] (seal-resource-shim event))

