(ns onyx.peer.status
  (:require [taoensso.timbre :refer [debug info error warn trace fatal]]))

(defn merge-statuses
  "Combines many statuses into one overall status that conveys the
   minimum/worst case of all of the statuses"
  [[fst & rst]]
  (reduce (fn [c s]
            {:ready? (and (:ready? s) (:ready? c))
             :drained? (and (:drained? s) (:drained? c))
             :replica-version (if-let [rvs (seq (keep :replica-version [c s]))]
                                (apply min rvs)
                                -1)
             :checkpointing? (or (:checkpointing? s) (:checkpointing? c))
             :heartbeat (min (:heartbeat c) (:heartbeat s))
             :epoch (min (:epoch c) (:epoch s))
             :min-epoch (min (:min-epoch c) (:min-epoch s))})
          fst
          rst))
