(ns onyx.state.core
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.state.state-extensions :as state-extensions]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]))

(defn apply-seen-id 
  "Update the buckets with a new id. 
   Currently only updates the first set and does not update any of the bloom filters."
  [seen-buckets id]
  (update-in seen-buckets [:sets 0] conj id))

(defn seen? 
  "Determine whether an id has been seen before. First check the bloom filter buckets, if
  a bloom filter thinks it has been seen before, check the corresponding set bucket.
  An optimisation is to let the sets expire before the bloom filters do, so we
  get most of the benefit without full memory usage. Currently this
  function just checks the sets."
  [{:keys [blooms sets] :as buckets} id]
  ;; do some pass on bloom-buckets, if any are maybe seen, then check corresponding id-bucket
  (boolean 
    (first 
      (filter (fn [set-bucket]
                (set-bucket id)) 
              sets))))

(defn peer-slot-id 
  [event]
  (let [replica (:onyx.core/replica event)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)] 
    (get-in @replica [:task-slot-ids job-id task-id peer-id])))
