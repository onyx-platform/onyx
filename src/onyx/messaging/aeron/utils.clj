(ns onyx.messaging.aeron.utils
  (:import [io.aeron.logbuffer ControlledFragmentHandler$Action]))

(defn action->kw [action]
  (cond (= action ControlledFragmentHandler$Action/CONTINUE)
        :CONTINUE
        (= action ControlledFragmentHandler$Action/BREAK)
        :BREAK
        (= action ControlledFragmentHandler$Action/ABORT)
        :ABORT
        (= action ControlledFragmentHandler$Action/COMMIT)
        :COMMIT))

(def heartbeat-stream-id 0)

(defn stream-id [job-id task-id slot-id site]
  (hash [job-id task-id slot-id site]))
