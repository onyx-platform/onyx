(ns ^:no-doc onyx.static.logging
  (:require [taoensso.timbre :refer [info warn fatal error]]))

(defn log-prefix [task-info]
  (format "Job %s %s - Task %s %s - Peer %s - "
          (:job-id task-info)
          (:metadata task-info)
          (:task-id task-info)
          (:task task-info)
          (:id task-info)))

(defn merge-error-keys [e task-info msg]
  (let [d (ex-data e)
        ks [:job-id :metadata :task-id :task :id]
        error-keys (merge (select-keys task-info ks) d)
        msg (str msg " -> " (.getMessage ^clojure.lang.ExceptionInfo e))]
    (ex-info msg error-keys (.getCause ^clojure.lang.ExceptionInfo e))))

(defn task-log-info [task-information msg]
  (info (str (log-prefix task-information) msg)))

(defn task-log-warn [task-information msg]
  (warn (str (log-prefix task-information) msg)))

(defn exception-msg [task-information msg]
  (str (log-prefix task-information) msg))
