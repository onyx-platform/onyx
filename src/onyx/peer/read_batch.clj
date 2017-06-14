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
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.util :refer [ns->ms ms->ns]]
            [onyx.types]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util.concurrent.locks LockSupport]))

(defn read-function-batch [state]
  (if-let [batch (m/poll (get-messenger state))]
    (let [pbatch (persistent! batch)]
      (-> state 
          (set-event! (assoc (get-event state) :onyx.core/batch pbatch))
          (advance))) 
    (do
     ;; not ideal to park here, as it's a bit of a special case, however 
     ;; this is the easiest way to achieve a backoff.
     ;; It should be parking for batch-timeout.
     ;; We can't simply block as we will not continue reading barriers.
     (LockSupport/parkNanos (* 2 1000000))
     (advance state))))

(defn read-input-batch [state batch-size batch-timeout-ns]
  (let [pipeline (get-input-pipeline state)
        event (get-event state)
        end-time (+ (System/nanoTime) batch-timeout-ns)
        outgoing (transient [])
        _ (loop [remaining (- end-time (System/nanoTime))]
            (when (and (< (count outgoing) batch-size)
                       (pos? remaining)) 
              (let [segment (p/poll! pipeline event (long (ns->ms remaining)))]
                (when-not (nil? segment)
                  (conj! outgoing segment)
                  (recur (- end-time (System/nanoTime)))))))
        batch (persistent! outgoing)]
    (-> state
        (set-event! (assoc event :onyx.core/batch batch))
        (advance))))
