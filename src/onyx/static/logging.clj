(ns ^:no-doc onyx.static.logging
  (:require [taoensso.timbre :refer [trace info warn fatal error]]))

(defn log-prefix [task-info]
  (format "Job %s %s - Task %s %s - Peer %s - "
          (:job-id task-info)
          (:metadata task-info)
          (:task-id task-info)
          (:task-name task-info)
          (:id task-info)))

(defn merge-error-keys
  ([e task-info]
   (merge-error-keys e task-info ""))
  ([e task-info msg]
   (let [d (ex-data e)
         ks [:job-id :metadata :task-id :id]
         helpful-keys {:job-id (:job-id task-info)
                       :metadata (:metadata task-info)
                       :task-id (:task-id task-info)
                       :task-name (:name (:task task-info))
                       :peer-id (:id task-info)}
         error-keys (merge helpful-keys d)
         msg (str msg " -> " (.getMessage ^clojure.lang.ExceptionInfo e))]
     (ex-info msg error-keys (.getCause ^clojure.lang.ExceptionInfo e)))))

(defn task-log-trace [task-information msg]
  (trace (str (log-prefix task-information) msg)))

(defn task-log-info [task-information msg]
  (info (str (log-prefix task-information) msg)))

(defn task-log-warn [task-information msg]
  (warn (str (log-prefix task-information) msg)))

(defn exception-msg [task-information msg]
  (str (log-prefix task-information) msg))
