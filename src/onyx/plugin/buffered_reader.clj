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
          batch (->> (range max-segments)
                     (keep (fn [_] (first (alts!! [read-ch timeout-ch] :priority true)))))]
      (doseq [m batch]
        (feedback-producer-exception! m)
        (swap! pending-messages assoc (:id m) m))
      (when (completed? batch pending-messages read-ch) 
        (extensions/force-write-chunk log :chunk :complete task-id)
        (reset! drained? true))
      {:onyx.core/batch batch}))

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
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} 
   lifecycle]
  (let [shutdown-ch (chan 1)
        {:keys [reader read-ch complete-ch]} pipeline
        ;; Attempt to write initial checkpoint
        _ (extensions/write-chunk log :chunk (i/checkpoint @reader) task-id)
       checkpoint-content (extensions/read-chunk log :chunk task-id)]
    (if (= :complete checkpoint-content)
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [commit-ms 500
            commit-loop-ch (u/start-commit-loop! reader shutdown-ch commit-ms log task-id)
            reader-val @reader
            producer-ch (thread
                          (try
                            (loop [reader-val (i/next-state (i/recover reader-val checkpoint-content))
                                   segment (i/segment reader-val)
                                   segment-id (i/segment-id reader-val)]
                              (reset! reader reader-val)
                              (let [write-result (>!! read-ch (t/input (random-uuid) segment segment-id))
                                    reader-val* (u/process-completed! reader-val complete-ch)]
                                (when (and write-result (not= :done segment))
                                  (let [reader-val** (i/next-state reader-val*)]
                                    (recur reader-val**
                                           (i/segment reader-val**)
                                           (i/segment-id reader-val**))))))
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
