(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout go <! alts! close!]]
            [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.static.uuid :as uuid]
            [onyx.types :as t]
            [taoensso.timbre :refer [debug info] :as timbre]))

(defn inject-reader
  [event lifecycle]
  (when-not (:core.async/chan event)
    (throw (ex-info ":core.async/chan not found - add it using a :before-task-start lifecycle"
                    {:event-map-keys (keys event)})))

  (let [task (:onyx.core/task-map event)]
    (when (and (not= (:onyx/max-peers task) 1)
               (not (:core.async/allow-unsafe-concurrency? lifecycle)))
      (throw (ex-info ":onyx/max-peers must be set to 1 in the task map for core.async readers" {:task-map task}))))

  (let [pipeline (:onyx.core/pipeline event)]
    {:core.async/pending-messages (:pending-messages pipeline)
     :core.async/drained (:drained pipeline)
     :core.async/retry-ch (:retry-ch pipeline)
     :core.async/retry-count (:retry-count pipeline)}))

(defn log-retry-count
  [event lifecycle]
  (info "core.async input plugin stopping. Retry count:" @(:core.async/retry-count event))
  {})

(defn inject-writer
  [event lifecycle]
  (when-not (:core.async/chan event)
    (throw (ex-info ":core.async/chan not found - add it using a :before-task-start lifecycle"
                    {:event-map-keys (keys event)})))

  (let [task (:onyx.core/task-map event)]
    (when (and (not= (:onyx/max-peers task) 1)
               (not (:core.async/allow-unsafe-concurrency? lifecycle)))
      (throw (ex-info ":onyx/max-peers must be set to 1 in the task map for core.async writers" {:task-map task}))))

  {})

(def reader-calls
  {:lifecycle/before-task-start inject-reader
   :lifecycle/after-task-stop log-retry-count})

(def writer-calls
  {:lifecycle/before-task-start inject-writer})

(defrecord CoreAsyncInput [max-pending batch-size batch-timeout pending-messages
                           drained retry-ch retry-count]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch [_ {:keys [core.async/chan] :as event}]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          ;; We reuse a single timeout channel. This allows us to
          ;; continually block against one thread that is continually
          ;; expiring. This property lets us take variable amounts of
          ;; time when reading each segment and still allows us to return
          ;; within the predefined batch timeout limit.
          timeout-ch (timeout batch-timeout)
          batch (if (pos? max-segments)
                  (loop [segments [] cnt 0]
                    (if (= cnt max-segments)
                      segments
                      (if-let [message (first (alts!! [retry-ch chan timeout-ch] :priority true))]
                        (recur (conj segments
                                     (t/input (uuid/random-uuid) message))
                               (inc cnt))
                        segments)))
                  (<!! timeout-ch))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) (:message m)))
      (when (and (= 1 (count @pending-messages))
                 (= (count batch) 1)
                 (= (:message (first batch)) :done)
                 (zero? (count (.buf retry-ch))))
        (reset! drained true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ message-id]
    (swap! pending-messages dissoc message-id))

  (retry-segment
    [_ _ message-id]
    (when-let [msg (get @pending-messages message-id)]
      (when-not (= msg :done)
        (swap! retry-count inc))
      (>!! retry-ch msg)
      (swap! pending-messages dissoc message-id)))

  (pending?
    [_ _ message-id]
    (get @pending-messages message-id))

  (drained?
    [_ _]
    @drained))

(defn input [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))]
    (->CoreAsyncInput max-pending batch-size batch-timeout
                      (atom {}) (atom false) (chan 10000) (atom 0))))

(defrecord CoreAsyncOutput []
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results core.async/chan] :as event}]
    (doseq [msg (mapcat :leaves (:tree results))]
      (>!! chan (:message msg)))
    {})

  (seal-resource
    [_ {:keys [core.async/chan]}]
    (>!! chan :done)))

(defn output [pipeline-data]
  (->CoreAsyncOutput))

(defn take-segments!
  "Takes segments off the channel until :done is found.
   Returns a seq of segments, including :done."
  ([ch] (take-segments! ch nil))
  ([ch timeout-ms]
   (when-let [tmt (if timeout-ms
                    (timeout timeout-ms)
                    (chan))]
     (loop [ret []]
       (let [[v c] (alts!! [ch tmt] :priority true)]
         (if (= c tmt)
           ret
           (if (and v (not= v :done))
             (recur (conj ret v))
             (conj ret :done))))))))
