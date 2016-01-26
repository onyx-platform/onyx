(ns ^:no-doc onyx.lifecycles.lifecycle-invoke
  (:require [clojure.core.async :refer [>!!]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]))

(defn restartable-invocation [event phase handler-fn f & args]
  (try
    (apply f args)
    (catch Throwable t
      (let [action (handler-fn event phase t)]
        (cond (= action :kill)
              (throw t)

              (= action :restart)
              (do (>!! (:onyx.core/restart-ch event) true)
                  (throw (ex-info "Jumping out of task lifecycle for a clean restart."
                                  {:onyx.core/lifecycle-restart? true
                                   :original-exception t})))

              :else
              (throw (ex-info
                      (format "Internal error, cannot handle exception with policy %s, must be one of #{:kill :restart :defer}"
                              action)
                      {})))))))

(defn invoke-start-task [event compiled]
  (restartable-invocation
   event
   :lifecycle/start-task?
   (:compiled-handle-exception-fn compiled)
   (:compiled-start-task-fn compiled)
   event))

(defn invoke-before-task-start [event compiled]
  (merge
   event
   (restartable-invocation
    event
    :lifecycle/before-task-start
    (:compiled-handle-exception-fn compiled)
    (:compiled-before-task-start-fn compiled)
    event)))

(defn invoke-before-batch [compiled event]
  (merge
   event
   (restartable-invocation
    event
    :lifecycle/before-batch
    (:compiled-handle-exception-fn compiled)
    (:compiled-before-batch-fn compiled)
    event)))

(defn invoke-after-read-batch [compiled event]
  (merge
   event
   (restartable-invocation
    event
    :lifecycle/after-read-batch
    (:compiled-handle-exception-fn compiled)
    (:compiled-after-read-batch-fn compiled)
    event)))

(defn invoke-after-batch [compiled event]
  (merge
   event
   (restartable-invocation
    event
    :lifecycle/after-batch
    (:compiled-handle-exception-fn compiled)
    (:compiled-after-batch-fn compiled)
    event)))

(defn invoke-read-batch
  [f compiled task-type replica peer-replica-view job-id pipeline event]
  (restartable-invocation
   event
   :lifecycle/read-batch
   (:compiled-handle-exception-fn compiled)
   f
   task-type replica peer-replica-view job-id pipeline event))

(defn invoke-write-batch [f compiled pipeline event]
  (restartable-invocation
   event
   :lifecycle/write-batch
   (:compiled-handle-exception-fn compiled)
   f pipeline event))

(defn invoke-after-ack [event compiled message-id ack-rets]
  (restartable-invocation
   event
   :lifecycle/after-ack-segment
   (:compiled-handle-exception-fn compiled)
   (:compiled-after-ack-segment-fn compiled)
   event
   message-id
   ack-rets))

(defn invoke-after-retry [event compiled message-id retry-rets]
  (restartable-invocation
   event
   :lifecycle/after-retry-segment
   (:compiled-handle-exception-fn compiled)
   (:compiled-after-retry-segment-fn compiled)
   event
   message-id
   retry-rets))

(defn invoke-assign-windows
  [f compiled event]
  (restartable-invocation
   event
   :lifecycle/assign-windows
   (:compiled-handle-exception-fn compiled)
   f
   compiled event))

; (defn invoke-after-task-stop [event]
;   ;; This function intentionally does not execute
;   ;; the lifecycle as a restartable-invocation. If
;   ;; the task reaches this stage, it is already considered
;   ;; complete. Restarting the task would cause the peer
;   ;; to deadlock.
;   (merge event ((:onyx.core/compiled-after-task-fn event) event)))
