(ns onyx.scheduling.colocated-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.log.commands.common :as common]))

(defmethod cts/task-constraints :onyx.task-scheduler/colocated
  [replica peer->vm task->node job-id])

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/colocated
  [replica job n]
  {})



#_(let [peers (remove (fn [p] (= p peer)) (:peers replica))
        peer-site (extensions/get-peer-site replica peer)]
    (filter
     (fn [p]
       (= (extensions/get-peer-site replica p) peer-site))
     peers))
