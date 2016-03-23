(ns onyx.plugin.buffered-reader
  (:require [clojure.core.async :refer [chan >! >!! <!! close! poll! thread timeout alts!! go-loop sliding-buffer]]
            [onyx.plugin.simple-input :as i]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.plugin.utils :as u]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]]))

(defn completed? [batch pending-messages read-ch]
  (and (u/all-done? (vals @pending-messages))
       (u/all-done? batch)
       (zero? (count (.buf read-ch)))
       (or (not (empty? @pending-messages))
           (not (empty? batch)))))

(defn feedback-producer-exception! [e]
  (when (instance? java.lang.Throwable e)
    (throw e)))

;; Implemented as a manual loop to maintain lazy.
;; Clojure's chunking was making this difficult.
(defn take-until-barrier
  ([max-segments read-ch timeout-ch]
   (take-until-barrier max-segments read-ch timeout-ch []))
  ([max-segments read-ch timeout-ch results]
   (cond (>= (count results) max-segments)
         results

         :else
         (let [[v ch] (alts!! [read-ch timeout-ch] :priority true)]
           (cond (instance? onyx.types.Barrier v)
                 (conj results v)

                 (= ch timeout-ch)
                 results

                 v
                 (recur max-segments read-ch timeout-ch (conj results v))

                 :else
                 (recur max-segments read-ch timeout-ch results))))))

(defrecord BufferedInput 
  [reader log task-id max-pending batch-size batch-timeout pending-messages drained? 
   read-ch complete-ch]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (take-until-barrier max-segments read-ch timeout-ch)
          barriers (filter #(instance? onyx.types.Barrier %) batch)
          batch (remove #(instance? onyx.types.Barrier %) batch)]
      (assert (<= (count barriers) 1))
      (doseq [m batch]
        (feedback-producer-exception! m)
        (swap! pending-messages assoc (:id m) m))
      (when (completed? batch pending-messages read-ch) 
        (extensions/force-write-chunk log :chunk :complete task-id)
        (reset! drained? true))
      {:onyx.core/batch batch
       :onyx.core/barrier (first barriers)}))

  p-ext/PipelineInput
  (ack-segment [_ event segment-id]
    (let [input (get @pending-messages segment-id)] 
      (>!! complete-ch input) 
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! read-ch (assoc msg :id (random-uuid))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn new-buffered-input [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event}]
  (let [max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        reader-builder (kw->fn (:simple-input/build-input task-map))
        buf-size (or (:buffered-reader/read-buffer-size task-map) 1000)
        read-ch (chan buf-size)
        complete-ch (chan buf-size)
        reader (onyx.plugin.simple-input/start (reader-builder event))]
    (->BufferedInput (atom reader) log task-id max-pending batch-size batch-timeout (atom {}) (atom false) read-ch complete-ch)))

(defn inject-buffered-reader
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id
           onyx.core/job-id onyx.core/pipeline onyx.core/id
           onyx.core/peer-replica-view onyx.core/replica]
    :as event} 
   lifecycle]
  (let [barrier-gap 5
        shutdown-ch (chan 1)
        {:keys [reader read-ch complete-ch]} pipeline
        ;; Attempt to write initial checkpoint
        _ (extensions/write-chunk log :chunk (i/checkpoint @reader) task-id)
        checkpoint-content (extensions/read-chunk log :chunk task-id)]
    (if (= :complete checkpoint-content)
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [reader-val @reader
            producer-ch
            (thread
              (try
                (loop [reader-val (i/next-state (i/recover reader-val checkpoint-content) event)
                       segment (i/segment reader-val)
                       segment-id (i/segment-id reader-val)
                       n 0]
                  (let [safe-spot? (zero? (mod (inc n) barrier-gap))]
                    (when safe-spot?
                      (extensions/force-write-chunk log :chunk (i/checkpoint reader-val) task-id))
                    (reset! reader reader-val)
                    (let [write-result (if segment
                                         (let [res (>!! read-ch (t/input (random-uuid) segment segment-id))]
                                           (when safe-spot?
                                             (let [downstream-task-ids (vals (:egress-ids (:task @peer-replica-view)))
                                                   replica @replica
                                                   downstream-peers (mapcat #(get-in replica [:allocations job-id %]) downstream-task-ids)]
                                               (doseq [p downstream-peers]
                                                 (>!! read-ch (t/->Barrier p id n nil nil)))))
                                           res)
                                         :backoff)
                          reader-val* (u/process-completed! reader-val complete-ch)]
                      (when (and write-result (not= :done segment))
                        (let [reader-val** (i/next-state reader-val* event)]
                          (recur reader-val**
                                 (i/segment reader-val**)
                                 (i/segment-id reader-val**)
                                 (inc n)))))))
                (catch Exception e
                  ;; feedback exception to read-batch
                  (>!! read-ch e))))]
        {:buffered-reader/reader reader
         :buffered-reader/read-ch read-ch
         :buffered-reader/complete-ch complete-ch
         :buffered-reader/shutdown-ch shutdown-ch
         :buffered-reader/producer-ch producer-ch}))))

(defn close-buffered-reader
  [{:keys [buffered-reader/producer-ch buffered-reader/reader 
           buffered-reader/complete-ch buffered-reader/read-ch 
           buffered-reader/shutdown-ch] :as event} 
   lifecycle]
  (close! read-ch)
  ;; Drain the read-ch to unblock it
  (while (poll! read-ch))
  (while (poll! complete-ch))
  (close! producer-ch)
  (close! shutdown-ch)
  (i/stop @reader)
  {})
