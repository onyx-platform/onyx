(ns ^:no-doc onyx.peer.read-batch
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.log.commands.common :as common]
            [onyx.plugin.protocols :as p]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t]
            [onyx.types :as types]
            [primitive-math :as pm]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.util :refer [ns->ms ms->ns]]
            [onyx.types]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util.concurrent.locks LockSupport]
           [java.util.concurrent.atomic AtomicLong]))

(defn read-function-batch [state ^long empty-poll-timeout-ns ^AtomicLong since-barrier-count]
  (if-let [outgoing (m/poll (get-messenger state))]
    (let [batch (persistent! outgoing)]
      (.addAndGet since-barrier-count (count batch))
      (-> state 
          (set-event! (assoc (get-event state) :onyx.core/batch batch))
          (advance))) 
    (do (LockSupport/parkNanos empty-poll-timeout-ns)
        (advance state))))

(defn read-input-batch [state batch-size batch-timeout-ns max-segments-per-barrier since-barrier-count]
  (let [pipeline (get-input-pipeline state)
        event (get-event state)
        outgoing (transient [])
        batch-size-rest (pm/min ^long batch-size 
                                (pm/- ^long max-segments-per-barrier 
                                      (.get ^AtomicLong since-barrier-count)))
        end-time (pm/+ (System/nanoTime) ^long batch-timeout-ns)
        _ (loop [remaining batch-timeout-ns
                 n 0]
            (when (and (pm/< n batch-size-rest)
                       (pos? remaining)) 
              (let [remaining-ms (pm// ^long remaining 1000000)
                    segment (p/poll! pipeline event remaining-ms)]
                (when-not (nil? segment)
                  (conj! outgoing segment)
                  (recur (pm/- end-time (System/nanoTime))
                         (pm/inc n))))))
        batch (persistent! outgoing)]
    (.addAndGet ^AtomicLong since-barrier-count (count batch))
    (-> state
        (set-event! (assoc event :onyx.core/batch batch))
        (advance))))
