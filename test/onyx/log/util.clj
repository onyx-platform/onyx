(ns onyx.log.util
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info]]))

(defrecord StubbedTaskLifeCycle [args state]
  component/Lifecycle

  (start [component]
    (info (format "Starting stubbed Task Lifecycle for job %s task %s" (:job args) (:task args)))
    component)

  (stop [component]
    (info (format "Stopping stubbed Task Lifecycle for job %s task %s" (:job args) (:task args)))
    component))

(defn stub-task-lifecycle [args state]
  (map->StubbedTaskLifeCycle {:args args :state state}))

