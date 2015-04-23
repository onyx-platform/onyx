(ns onyx.test-helper
  (:require [clojure.core.async :refer [chan >!! alts!! timeout <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]))

(defmacro with-env [[symbol-name env-config] & body]
  `(let [~symbol-name (onyx.api/start-env ~env-config)]
     (try
       ~@body
       (finally
         (onyx.api/shutdown-env ~symbol-name)))))

(defmacro with-peer-group [[symbol-name peer-config] & body]
  `(let [~symbol-name (onyx.api/start-peer-group ~peer-config)]
     (try
       ~@body
       (finally
         (onyx.api/shutdown-peer-group ~symbol-name)))))

(defmacro with-peers [[symbol-name n-peers peer-group] & body]
  `(let [~symbol-name (onyx.api/start-peers ~n-peers ~peer-group)]
     (try
       ~@body
       (finally
         (doseq [v-peer# ~symbol-name]
           (onyx.api/shutdown-peer v-peer#))))))

(defn load-config [filename]
  (read-string (slurp (clojure.java.io/resource filename))))

(defn playback-log [log replica ch timeout-ms]
  (loop [replica replica]
    (if-let [position (first (alts!! [ch (timeout timeout-ms)]))]
      (let [entry (extensions/read-log-entry log position)
            new-replica (extensions/apply-log-entry entry replica)]
        (recur new-replica))
      replica)))

(defn job-allocation-counts [replica job-info]
  (if-let [allocations (get-in replica [:allocations (:job-id job-info)])]
    (map (comp count allocations :id) 
         (:task-ids job-info))
    []))

(defn get-counts [replica job-infos]
  (map (partial job-allocation-counts replica)
       job-infos))

(defn load-config []
  (let [cfg (read-string (slurp (clojure.java.io/resource "test-config.edn")))]
    (let [impl (System/getenv "TEST_TRANSPORT_IMPL")]
      (cond (= impl "aeron")
            (assoc-in cfg [:peer-config :onyx.messaging/impl] :aeron)
            (= impl "netty")
            (assoc-in cfg [:peer-config :onyx.messaging/impl] :netty)
            (= impl "core-async")
            (assoc-in cfg [:peer-config :onyx.messaging/impl] :core.async)
            :else cfg))))
