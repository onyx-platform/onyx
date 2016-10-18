(ns ^:no-doc onyx.lifecycles.lifecycle-invoke
  (:require [clojure.core.async :refer [>!! close!]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]))

(defn handle-exception [event phase t handler-fn]
  (let [action (handler-fn event phase t)]
    (cond (= action :kill)
          (throw t)

          (= action :restart)
          (throw (ex-info "Jumping out of task lifecycle for a clean restart."
                          {:onyx.core/lifecycle-restart? true}
                          t))

          :else
          (throw (ex-info
                  (format "Internal error, cannot handle exception with policy %s, must be one of #{:kill :restart :defer}"
                          action)
                  {})))))

(defn restartable-invocation [event phase handler-fn f & args]
  (try
    (apply f args)
    (catch Throwable t
      (handle-exception event phase t handler-fn))))

(defn invoke-lifecycle-gen [phase compiled-key]
  (fn invoke-lifecycle [event]
    (restartable-invocation
      event
      phase
      (:compiled-handle-exception-fn event)
      (compiled-key event)
      event)))

(def invoke-start-task
  (invoke-lifecycle-gen :lifecycle/start-task? :compiled-start-task-fn))

(def invoke-before-task-start
  (invoke-lifecycle-gen :lifecycle/before-task-start :compiled-before-task-start-fn))

(def invoke-build-plugin
  (invoke-lifecycle-gen :lifecycle/build-plugin :compiled-handle-exception-fn))

(defn invoke-task-lifecycle-gen [phase]
  (fn invoke-task-lifecycle [f event]
    (restartable-invocation
      event
      phase
      (:compiled-handle-exception-fn event)
      f
      event)))

(defn invoke-task-windows-lifecycle-gen [phase]
  (fn invoke-task-windows-lifecycle [f event event-type] 
    (restartable-invocation
      event
      phase
      (:compiled-handle-exception-fn event)
      f
      event
      event-type)))

; (def invoke-read-batch
;   (invoke-task-lifecycle-gen :lifecycle/read-batch))

; (def invoke-write-batch
;   (invoke-task-lifecycle-gen :lifecycle/write-batch))

; (defn invoke-after-retry [event message-id retry-rets]
;   (restartable-invocation
;    event
;    :lifecycle/after-retry-segment
;    (:compiled-handle-exception-fn event)
;    (:compiled-after-retry-segment-fn event)
;    event
;    message-id
;    retry-rets))

(defn invoke-flow-conditions
  [f event result root leaves accum leaf]
  (restartable-invocation
   event
   :lifecycle/execute-flow-conditions
   (:compiled-handle-exception-fn event)
   f
   event result root leaves accum leaf))
