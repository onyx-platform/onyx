(ns onyx.plugin.timeout-reader
  (:require [clojure.core.async :refer [chan >! >!! <!! close! poll! thread timeout alts!! go-loop sliding-buffer]]
            [onyx.plugin.simple-input :as i]
            [onyx.static.swap-pair :refer [swap-pair!]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.plugin.utils :as u]
            [taoensso.timbre :refer [info debug fatal]]))

(defn completed? [batch pending-messages]
  (and (u/all-done? (vals @pending-messages))
       (u/all-done? batch)
       (or (not (empty? @pending-messages))
           (not (empty? batch)))))

(def complete-ch-size 500)
(def commit-ms 500)
(def backoff-ms 50)

(defrecord TimeoutInput 
  [reader log task-id max-pending batch-size batch-timeout pending-messages drained? retry-buffer complete-ch]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          ;; Timeout does not currently work
          timeout-ch (timeout batch-timeout)
          [old-buffer new-buffer] (swap-pair! retry-buffer (fn [v] (drop max-segments v)))
          retry-batch (transient (vec (take max-segments old-buffer)))
          reader-val (-> @reader 
                         (u/process-completed! complete-ch)
                         (i/next-state))
          [new-reader 
           batch] (if (= max-segments (count retry-batch))
                    (persistent! retry-batch)
                    (loop [batch retry-batch 
                           reader-val reader-val
                           segment (i/segment reader)
                           offset (i/segment-id reader)]
                      (let [new-input (t/input (random-uuid) segment offset)
                            new-batch (conj! batch new-input)]
                        (if-not (and (= :done segment) 
                                     (< (count batch) max-segments))
                          (let [new-reader-val (i/next-state reader-val)] 
                            (recur new-batch 
                                   new-reader-val
                                   (i/segment new-reader-val)
                                   (i/segment-id new-reader-val)))
                          (list reader (persistent! new-batch))))))]
      (reset! reader new-reader)
      (when (empty? batch)
        (Thread/sleep backoff-ms))
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      (when (completed? batch pending-messages) 
        (extensions/force-write-chunk log :chunk :complete task-id)
        (reset! drained? true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput
  (ack-segment [_ event segment-id]
    (let [input (get @pending-messages segment-id)] 
      (>!! complete-ch input) 
      (swap! pending-messages dissoc segment-id)
      (i/segment-complete! @reader (:message input))))

  (retry-segment
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! retry-buffer conj (assoc msg :id (random-uuid))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn new-timeout-input [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event}]
  (let [max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        reader-builder (kw->fn (:simple-input/build-input task-map))
        reader (onyx.plugin.simple-input/start (reader-builder event))
        complete-ch (chan complete-ch-size)]
    (->TimeoutInput (atom reader) log task-id max-pending batch-size batch-timeout 
                    (atom {}) (atom false) (atom []) complete-ch)))

(defn inject-timeout-reader
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} 
   lifecycle]
  (let [shutdown-ch (chan 1)
        {:keys [reader read-ch complete-ch]} pipeline
        ;; Attempt to write initial checkpoint
        _ (extensions/write-chunk log :chunk (i/checkpoint @reader) task-id)
        read-offset (extensions/read-chunk log :chunk task-id)]
    (swap! reader i/recover read-offset)
    (if (= :complete read-offset)
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [commit-loop-ch (u/start-commit-loop! reader shutdown-ch commit-ms log task-id)]
        {:timeout-reader/reader reader
         :timeout-reader/complete-ch complete-ch
         :timeout-reader/shutdown-ch shutdown-ch}))))

(defn close-timeout-reader
  [{:keys [timeout-reader/reader timeout-reader/shutdown-ch complete-ch] :as event} 
   lifecycle]
  (close! shutdown-ch)
  ;; Drain the read-ch to unblock it
  (while (poll! complete-ch))
  (i/stop @reader)
  {})
