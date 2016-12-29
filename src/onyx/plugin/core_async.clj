(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [poll! timeout chan alts!! offer! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.protocol.task-state :refer :all]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

(defrecord AbsCoreAsyncReader [event closed? segment offset checkpoint resumed replica-version epoch]
  p/Plugin
  (start [this event]
    (assoc this :checkpoint 0 :offset 0))

  (stop [this event] this)

  i/Input
  (checkpoint [{:keys [checkpoint] :as this}]
    [replica-version epoch])

  (recover [this replica-version checkpoint]
    (let [buf @(:core.async/buffer event)
          resume-to (or checkpoint (first (sort (keys buf))))
          resumed (get buf resume-to)]
      (info "RESUMED" resumed resume-to (keys buf) (sort (keys buf)))
      (println "RESUMED" resumed resume-to (keys buf) (sort (keys buf)))
      (-> this 
          (assoc :epoch 0)
          (assoc :replica-version replica-version)
          (assoc :resumed resumed))))

  (segment [{:keys [segment]}]
    segment)

  (synced? [this epoch]
    (swap! (:core.async/buffer event)
           (fn [buf]
             (->> ;(update buf [replica-version (:epoch this)] #(into (vec %) resumed))
                  buf
                  (remove (fn [[[rv e] _]]
                            false 
                            #_(or (< rv replica-version)
                                  (< e (- epoch 4))))) ;; fixme -4
                  (into {}))))
    [true (assoc this :epoch epoch) {}])

  (next-state [this {:keys [core.async/chan core.async/buffer]}]
    (let [segment (if-not (empty? resumed)
                    (do
                     (println "READ FROM RESUMED" (first resumed))
                     (first resumed))
                    (poll! chan))]
      ;; Add each newly read segment, to all the previous epochs as well. Then if we resume there
      ;; we have all of the messages read to this point
      ;; When we go past the epoch far enough, then we can discard those checkpoint buffers
      ;; Resume buffer is only filled in on recover, doesn't need to be part of the buffer.
      (when (and segment (empty? resumed)) 
        (swap! buffer 
               (fn [buf]
                 (reduce-kv (fn [bb k v]
                              (assoc bb k (conj (or v []) segment)))
                            {}
                            buf))))
      (when (= segment :done)
        (throw (Exception. ":done message is no longer supported on core.async.")))
      (assoc this
             :channel chan
             :segment segment
             :resumed (rest resumed)
             :offset (if segment (inc offset) offset)
             :closed? (clojure.core.async.impl.protocols/closed? chan))))

  (completed? [{:keys [closed? segment offset checkpoint]}]
    (and closed? (nil? segment))))

(defrecord AbsCoreAsyncWriter [event prepared]
  p/Plugin
  (start [this event] this)

  (stop [this event] this)

  o/Output

  (synced? [this epoch]
    [true this])

  (prepare-batch [this event _]
    (let [{:keys [onyx.core/results] :as event} event] 
      (reset! prepared (mapcat :leaves (:tree results)))
      [true this {}]))

  (write-batch
    [this {:keys [core.async/chan] :as event} _ _]
    (loop [msg (first @prepared)]
      (if msg
        (do
         (debug "core.async: writing message to channel" (:message msg))
         (if (offer! chan (:message msg))
           (recur (first (swap! prepared rest)))
           ;; Blocked, return without advancing
           (do
            (Thread/sleep 1)
            (when (zero? (rand-int 5000))
              (info "core.async: writer is blocked. Signalling every 5000 writes."))
            [false this {}])))
        [true this {}]))))

(defn input [event]
  (map->AbsCoreAsyncReader {:event event}))

(defn output [event]
  (map->AbsCoreAsyncWriter {:event event :prepared (atom nil)}))

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
