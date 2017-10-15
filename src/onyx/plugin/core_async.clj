(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [timeout chan alts!! offer! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.protocol.task-state :refer :all]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.plugin.protocols :as p]))

(defrecord AbsCoreAsyncReader [event chan completed? checkpoint 
                               resumed replica-version epoch] 
  p/Plugin
  (start [this event] this)

  (stop [this event] this)

  p/Checkpointed
  (checkpoint [this]
    [@replica-version @epoch])

  (recover! [this replica-version* checkpoint]
    (when-not (map? @(:core.async/buffer event))
      (throw (Exception. "A buffer atom must now be supplied to the core.async plugin under :core.async/buffer.
                          This atom must contain a map.")))
    (let [buf @(:core.async/buffer event)
          resume-to (or checkpoint (first (sort (keys buf))))
          resumed* (get buf resume-to)]
      (reset! completed? false)
      (reset! epoch 0)
      (reset! replica-version replica-version*)
      (reset! resumed resumed*)))

  (checkpointed! [this cp-epoch]
    (swap! (:core.async/buffer event)
           (fn [buf]
             (->> buf
                  (remove (fn [[[rv e] _]]
                            (or (<= rv @replica-version)
                                (<= e cp-epoch))))
                  (into {}))))
    true)

  p/BarrierSynchronization
  (synced? [this epoch*]
    (reset! epoch epoch*)
    true)

  (completed? [this]
    @completed?)

  p/Input
  (poll! [this {:keys [core.async/buffer]} _]
    (let [r @resumed
          reread-seg (when-not (empty? r)
                       (swap! resumed rest)
                       (first r))
          segment (or reread-seg (clojure.core.async/poll! chan))]
      ;; Add each newly read segment, to all the previous epochs as well. 
      ;; Then if we resume there we have all of the messages read to this point.
      ;; When we go past the epoch far enough, then we can discard those checkpoint buffers.
      ;; Resume buffer is only filled in on recover, doesn't need to be part of the buffer.
      (when (and segment (not reread-seg)) 
        (swap! buffer 
               (fn [buf]
                 (->> (update buf [@replica-version @epoch] vec)
                      (reduce-kv (fn [bb k v]
                                   (assoc bb k (conj (or v []) segment)))
                                 {})))))
      (when (= segment :done)
        (throw (Exception. ":done message is no longer supported on core.async.")))
      (when (and (not segment) (clojure.core.async.impl.protocols/closed? chan))
        (reset! completed? true))
      segment)))

(defrecord AbsCoreAsyncWriter [event prepared]
  p/Plugin
  (start [this event] this)

  (stop [this event] this)

  p/Checkpointed
  (checkpoint [this])
  (recover! [this replica-version checkpointed])
  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)
  (completed? [this] true)

  p/Output
  (prepare-batch [this event _ _]
    (let [{:keys [onyx.core/results] :as event} event] 
      (reset! prepared (mapcat :leaves (:tree results)))
      true))

  (write-batch
    [this {:keys [core.async/chan] :as event} _ _]
    (loop [msg (first @prepared)]
      (if msg
        (do
         (debug "core.async: writing message to channel" msg)
         (if (offer! chan msg)
           (recur (first (swap! prepared rest)))
           ;; Blocked, return without advancing
           (do
            (Thread/sleep 1)
            (when (zero? (rand-int 5000))
              (info "core.async: writer is blocked. Signalling every 5000 writes."))
            false)))
        true))))

(defn input [event]
  (map->AbsCoreAsyncReader {:event event
                            :chan (:core.async/chan event) 
                            :completed? (atom false)
                            :watermark (atom nil)
                            :epoch (atom 0)
                            :replica-version (atom 0)
                            :resumed (atom nil)}))

(defn output [event]
  (map->AbsCoreAsyncWriter {:event event :prepared (atom nil)}))

(defn take-segments!
  "Takes segments off the channel until nothing is read for timeout-ms."
  ([ch] (throw (Exception. "The core async plugin no longer automatically closes the output channel, nor emits a :done message. 
                            Thus a timeout must now be supplied. 
                            In order to receive all results, please use onyx.api/await-job-completion to ensure the job is finished before reading.")))
  ([ch timeout-ms]
   (loop [ret []
          tmt (timeout timeout-ms)]
     (let [[v c] (alts!! [ch tmt] :priority true)]
       (if (= c tmt)
         ret
         (if v
           (recur (conj ret v) (timeout timeout-ms))
           ret))))))

(def channels (atom {}))
(def buffers (atom {}))

(def default-channel-size 1000)

(defn get-channel
  ([id] (get-channel id default-channel-size))
  ([id size]
   (if-let [id (get @channels id)]
     id
     (do (swap! channels assoc id (chan (or size default-channel-size)))
         (get-channel id)))))

(defn get-buffer
  [id]
   (if-let [id (get @buffers id)]
     id
     (do (swap! buffers assoc id (atom {}))
         (get-buffer id))))

(defn inject-in-ch
  [_ lifecycle]
  {:core.async/buffer (get-buffer (:core.async/id lifecycle))
   :core.async/chan (get-channel (:core.async/id lifecycle)
                                 (or (:core.async/size lifecycle)
                                     default-channel-size))})

(defn inject-out-ch
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle)
                                 (or (:core.async/size lifecycle)
                                     default-channel-size))})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn get-core-async-channels
  [{:keys [catalog lifecycles]}]
  (let [lifecycle-catalog-join (join catalog lifecycles {:onyx/name :lifecycle/task})]
    (reduce (fn [acc item]
              (assoc acc
                     (:onyx/name item)
                     (get-channel (:core.async/id item)
                                  (:core.async/size item)))) 
            {} 
            (filter :core.async/id lifecycle-catalog-join))))

;; no op lifecycle to maintain compatibility with 0.9.x
(def reader-calls
  {:lifecycle/before-task-start (fn [_ _] {})})

;; no op lifecycles to maintain compatibility with 0.9.x
(def writer-calls
  {:lifecycle/before-task-start (fn [_ _] {})})


