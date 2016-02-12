(ns ^:no-doc onyx.static.logging
  (:require [taoensso.timbre :refer [info warn fatal error]]))

(defn log-prefix [event]
  (format "Job %s %s - Task %s %s - Peer %s - "
          (:onyx.core/job-id event)
          (:onyx.core/metadata event)
          (:onyx.core/task-id event)
          (:onyx.core/task event)
          (:onyx.core/id event)))

(defn info-from-task [event msg]
  (info (str (log-prefix event) msg)))

(defn warn-from-task [event msg]
  (warn (str (log-prefix event) msg)))

(defn exception-msg [event msg]
  (str (log-prefix event) msg))
