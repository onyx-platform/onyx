(ns onyx.messaging.aeron.utils
  (:require [onyx.messaging.common :refer [bind-addr bind-port]])
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

(defn channel [peer-config]
  (println "CHannel a" )
  (println "bind addr" (bind-addr peer-config))
  (println "bind port" (bind-port peer-config))
  (format "aeron:udp?endpoint=%s:%s" (bind-addr peer-config) (bind-port peer-config)))
