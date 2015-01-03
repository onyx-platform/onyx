(ns onyx.log.util
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info]]))

(defrecord StubbedTaskLifeCycle [args state]
  component/Lifecycle

  (start [component]
    (info (format "[%s] Starting stubbed Task Lifecycle for job %s task %s" (:id state) (:job args) (:task args)))
    component)

  (stop [component]
    (info (format "[%s] Stopping stubbed Task Lifecycle for job %s task %s" (:id state) (:job args) (:task args)))
    component))

(defn stub-task-lifecycle [args state]
  (map->StubbedTaskLifeCycle {:args args :state state}))

