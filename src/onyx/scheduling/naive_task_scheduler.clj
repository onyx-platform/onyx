(ns onyx.scheduling.naive-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.log.commands.common :as common]))

(defmethod cts/drop-peers :onyx.task-scheduler/naive
  [replica job n]
  [])

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/naive
  [replica job n]
  (let [tasks (get-in replica [:tasks job])]
    (merge (zipmap tasks (repeat 0)) {(first tasks) n})))