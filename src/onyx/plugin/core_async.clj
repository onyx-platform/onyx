(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [poll! timeout chan alts!! offer! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

(defrecord AbsCoreAsyncReader [event closed? segment offset checkpoint]
  p/Plugin
  (start [this]
    (assoc this :checkpoint 0 :offset 0))

  (stop [this event] this)

  i/Input
  (checkpoint [{:keys [checkpoint]}]
    checkpoint)

  (recover [this checkpoint]
    this)

  (offset-id [{:keys [offset]}]
    offset)

  (segment [{:keys [segment]}]
    segment)

  (next-state [{:keys [segment offset] :as this} state]
    (let [{:keys [core.async/chan] :as event} (get-event state)
          segment (poll! chan)]
      (assoc this
             :channel chan
             :segment segment
             :offset (if segment (inc offset) offset)
             :closed? (clojure.core.async.impl.protocols/closed? chan))))

  (segment-complete! [{:keys [conn]} segment])

  (completed? [{:keys [closed? segment offset checkpoint]}]
    (and closed? (nil? segment))))

(defrecord AbsCoreAsyncWriter [event]
  p/Plugin

  (start [this] this)

  (stop [this event] this)

  o/Output

  (prepare-batch
    [_ state]
    (let [{:keys [results] :as event} (get-event state)] 
      (set-context! state (mapcat :leaves (:tree results)))))

  (write-batch
    [_ state]
    (let [{:keys [core.async/chan] :as event} (get-event state)
          messages (get-context state)]
      (loop [msgs messages]
        (if-let [msg (first msgs)]
          (do
           (info "core.async: writing message to channel" (:message msg))
           (if (offer! chan (:message msg))
             (recur (rest msgs))
             ;; Blocked, return without advancing
             (set-context! state msgs)))
          (advance state))))))

(defn input [event]
  (map->AbsCoreAsyncReader {:event event}))

(defn output [event]
  (map->AbsCoreAsyncWriter {:event event}))

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

(def default-channel-size 1000)

(defn get-channel
  ([id] (get-channel id default-channel-size))
  ([id size]
   (if-let [id (get @channels id)]
     id
     (do (swap! channels assoc id (chan size))
         (get-channel id)))))

(defn inject-in-ch
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle) 
                                 (or (:core.async/size lifecycle)
                                     default-channel-size))})

(defn inject-out-ch
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle)
                                 (or (:core.async/size lifecycle)
                                     default-channel-size))})

(def reader-calls
  {:lifecycle/before-task-start inject-in-ch})

(def writer-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn get-core-async-channels
  [{:keys [catalog lifecycles]}]
  (let [lifecycle-catalog-join (join catalog lifecycles {:onyx/name :lifecycle/task})]
    (reduce (fn [acc item]
              (assoc acc
                     (:onyx/name item)
                     (get-channel (:core.async/id item)))) {} (filter :core.async/id lifecycle-catalog-join))))
