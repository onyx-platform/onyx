(ns ^:no-doc onyx.lifecycles.lifecycle-invoke
  (:require [clojure.core.async :refer [>!! close!]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]))

(defn restartable-invocation [event phase handler-fn f & args]
  (try
    (apply f args)
    (catch Throwable t
      (let [action (handler-fn event phase t)]
        (cond (= action :kill)
              (throw t)

              (= action :restart)
              (throw (ex-info "Jumping out of task lifecycle for a clean restart."
                              {:onyx.core/lifecycle-restart? true
                               :original-exception t}))

              :else
              (throw (ex-info
                      (format "Internal error, cannot handle exception with policy %s, must be one of #{:kill :restart :defer}"
                              action)
                      {})))))))

(defn invoke-lifecycle-gen [phase compiled-key]
  (fn invoke-lifecycle [compiled event] 
    (restartable-invocation
      event
      phase
      (:compiled-handle-exception-fn compiled)
      (compiled-key compiled)
      event)))

(def invoke-start-task
  (invoke-lifecycle-gen :lifecycle/start-task? :compiled-start-task-fn))

(def invoke-before-task-start
  (invoke-lifecycle-gen :lifecycle/before-task-start :compiled-before-task-start-fn))

(def invoke-after-read-batch
  (invoke-lifecycle-gen :lifecycle/after-read-batch :compiled-after-read-batch-fn))

(def invoke-before-batch
  (invoke-lifecycle-gen :lifecycle/before-batch :compiled-before-batch-fn))

(def invoke-after-batch
  (invoke-lifecycle-gen :lifecycle/after-batch :compiled-after-batch-fn))

(defn invoke-task-lifecycle-gen [phase]
  (fn invoke-task-lifecycle [f compiled event] 
    (restartable-invocation
      event
      phase
      (:compiled-handle-exception-fn compiled)
      f
      compiled
      event)))

(def invoke-assign-windows
  (invoke-task-lifecycle-gen :lifecycle/assign-windows))

(def invoke-read-batch
  (invoke-task-lifecycle-gen :lifecycle/read-batch))

(def invoke-write-batch
  (invoke-task-lifecycle-gen :lifecycle/write-batch))

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

;; This function intentionally does not execute
;; the lifecycle as a restartable-invocation. If
;; the task reaches this stage, it is already considered
;; complete. Restarting the task would cause the peer
;; to deadlock.
; (defn invoke-after-task-stop [event]
;   (merge event ((:onyx.core/compiled-after-task-fn event) event)))
