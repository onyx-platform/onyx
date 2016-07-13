(ns onyx.log.replica)

(def base-replica 
  {:peers []
   :orphaned-peers {}
   :groups []
   :groups-index {}
   :groups-reverse-index {}
   :peer-state {}
   :peer-sites {}
   :prepared {}
   :accepted {}
   :aborted #{}
   :left #{}
   :pairs {}
   :jobs []
   :task-schedulers {}
   :tasks {}
   :task-name->id {}
   :allocations {}
   :task-saturation {}
   :saturation {}
   :flux-policies {}
   :min-required-peers {}
   :input-tasks {}
   :output-tasks {}
   :exempt-tasks {}
   :exhausted-inputs {}
   :ackers {}
   :acker-percentage {}
   :acker-exclude-inputs {}
   :acker-exclude-outputs {}
   :task-percentages {}
   :task-metadata {}
   :percentages {}
   :completed-jobs []
   :killed-jobs []
   :state-logs {} 
   :state-logs-marked #{}
   :task-slot-ids {}
   :required-tags {}
   :peer-tags {}
   :allocation-version {}
   :allocation-counter -1})

(defn starting-replica [peer-config]
  (assoc base-replica 
         :job-scheduler (:onyx.peer/job-scheduler peer-config)
         :messaging (select-keys peer-config [:onyx.messaging/impl])))
