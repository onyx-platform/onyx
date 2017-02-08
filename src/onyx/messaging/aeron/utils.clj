(ns onyx.messaging.aeron.utils
  (:require [onyx.messaging.common :refer [bind-addr bind-port]])
  (:import [io.aeron.logbuffer ControlledFragmentHandler$Action]
           [io.aeron Subscription Image]))

(defn action->kw [action]
  (cond (= action ControlledFragmentHandler$Action/CONTINUE)
        :CONTINUE
        (= action ControlledFragmentHandler$Action/BREAK)
        :BREAK
        (= action ControlledFragmentHandler$Action/ABORT)
        :ABORT
        (= action ControlledFragmentHandler$Action/COMMIT)
        :COMMIT
        :else
        (throw (Exception. "Invalid action " action))))

(def heartbeat-stream-id 0)

(defn stream-id [task-id slot-id site]
  (hash [task-id slot-id site]))

(defn channel 
  ([addr port]
   (format "aeron:udp?endpoint=%s:%s" addr port))
  ([peer-config]
   (channel (bind-addr peer-config) (bind-port peer-config))))

(defn image->map [^Image image]
  {:pos (.position image) 
   :term-id (.initialTermId image)
   :session-id (.sessionId image) 
   :closed? (.isClosed image) 
   :correlation-id (.correlationId image) 
   :source-id (.sourceIdentity image)})


