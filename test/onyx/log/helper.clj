(ns onyx.log.helper
  (:require [clojure.core.async :refer [chan >!! alts!! timeout <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]))

(defn playback-log [log replica ch timeout-ms]
  (loop [replica replica]
    (if-let [position (first (alts!! [ch (timeout timeout-ms)]))]
      (let [entry (extensions/read-log-entry log position)
            new-replica (extensions/apply-log-entry entry replica)]
        (recur new-replica))
      replica)))
