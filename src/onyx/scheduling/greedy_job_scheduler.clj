(ns onyx.scheduling.greedy-job-scheduler
  (:require [clojure.set :refer [subset?]]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [taoensso.timbre :refer [debug info warn spy]]))

; {JobId -> Int}
(defmethod cjs/job-offer-n-peers :onyx.job-scheduler/greedy
  [replica jobs]
  (if (seq jobs)
    (let [[active & passive] jobs]
      (merge {active (cjs/n-qualified-peers replica (:peers replica) active)}
             (zipmap passive (repeat 0))))
    {}))

; {JobId -> Int}
(defmethod cjs/claim-spare-peers :onyx.job-scheduler/greedy
  [replica jobs n]
  ;; Take all the claimable peers
  ;; 1. Allocate as many to the first coverable job that it may maximally use
  ;; 2. Continue with the rest of the jobs until all jobs or peers are exhausted.
  (loop [claims jobs
         jobs* jobs
         n* n]
    (if (zero? n*)
      claims
      (if-let [[job-id peer-count] (first jobs*)]
        (let [n-requested (- (cjs/job-upper-bound replica job-id) peer-count)
              n-required (- (cjs/job-lower-bound replica job-id) peer-count)
              n-alloc (if (<= n-required n*) (min n* n-requested) 0)]
          (if (pos? n-alloc)
            (recur
              (update claims job-id + n-alloc)
              (rest jobs*)
              (- n* n-alloc))
            (recur claims (rest jobs*) n*)))
        claims))))
