(ns ^:no-doc onyx.peer.read-batch
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.log.commands.common :as common]
            [onyx.plugin.protocols :as p]
            [onyx.protocol.task-state :refer [get-messenger set-event! get-event advance get-input-pipeline]]
            [clj-tuple :as t]
            [onyx.types :as types]
            [primitive-math :as pm]
            [onyx.static.uuid :refer [random-uuid]]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util.concurrent.locks LockSupport]
           [org.agrona.concurrent IdleStrategy]
           [java.util.concurrent.atomic AtomicLong]))

(defn read-function-batch [state 
                           since-barrier-count
                           batch-size
                           batch-timeout-ns
                           read-batch-idle-ns]
  (let [end-time (pm/+ (System/nanoTime) ^long batch-timeout-ns)
        tbatch (transient [])
        batch (loop []
                (when-let [polled (m/poll (get-messenger state))]
                  (reduce conj! tbatch (persistent! polled)))

                (if (or (pm/>= (count tbatch) ^long batch-size)
                        (pm/> (System/nanoTime) end-time))
                  (persistent! tbatch)
                  (do
                   (LockSupport/parkNanos ^long read-batch-idle-ns)
                   (recur))))]
    (.addAndGet ^AtomicLong since-barrier-count (count batch))
      (-> state 
          (set-event! (assoc (get-event state) :onyx.core/batch batch))
          (advance))))

(defn ns->ms [^long ns]
  (pm// ns 1000000))

(defn read-input-batch [state batch-size batch-timeout-ns max-segments-per-barrier since-barrier-count]
  (let [pipeline (get-input-pipeline state)
        event (get-event state)
        outgoing (transient [])
        batch-size-rest (pm/min ^long batch-size 
                                (pm/- ^long max-segments-per-barrier 
                                      (.get ^AtomicLong since-barrier-count)))
        end-time (pm/+ (System/nanoTime) ^long batch-timeout-ns)
        _ (if (p/completed? pipeline)
            outgoing
            (loop [remaining batch-timeout-ns
                   n 0]
              (when (and (pm/< n batch-size-rest)
                         (pos? remaining)) 
                (let [segment (p/poll! pipeline event (ns->ms remaining))]
                  (when-not (nil? segment)
                    (conj! outgoing segment)
                    (recur (pm/- end-time (System/nanoTime))
                           (pm/inc n)))))))
        batch (persistent! outgoing)]
    (.addAndGet ^AtomicLong since-barrier-count (count batch))
    (-> state
        (set-event! (assoc event :onyx.core/batch batch))
        (advance))))
