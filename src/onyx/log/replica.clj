(ns onyx.log.replica)

(def base-replica 
  {:peers []
   :orphaned-peers {}
   :groups []
   :groups-index {}
   :groups-reverse-index {}
   :peer-sites {}
   :prepared {}
   :accepted {}
   :aborted #{}
   :left #{}
   :pairs {}
   :jobs []
   :task-schedulers {}
   :tasks {}
   :allocations {}
   :task-saturation {}
   :saturation {}
   :flux-policies {}
   :min-required-peers {}
   :input-tasks {}
   :output-tasks {}
   :coordinators {}
   :sealed-outputs {}
   :task-percentages {}
   :task-metadata {}
   :percentages {}
   :grouped-tasks {}
   :state-tasks {}
   :completed-jobs []
   :completed-job-coordinates {}
   :killed-jobs []
   :in->out {}
   :state-logs {} 
   :state-logs-marked #{}
   :task-slot-ids {}
   :message-short-ids {}
   :required-tags {}
   :peer-tags {}
   :version -1
   :allocation-version {}})

(defn starting-replica [peer-config]
  (assoc base-replica 
         :job-scheduler (:onyx.peer/job-scheduler peer-config)
         :messaging (select-keys peer-config [:onyx.messaging/impl])))
