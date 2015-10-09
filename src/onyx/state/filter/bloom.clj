(ns onyx.state.filter.bloom
  (:import [orestes.bloomfilter BloomFilter FilterBuilder])
  (:require [onyx.state.state-extensions :as state-extensions]))


(comment
  (def uuid-set (set (repeatedly 200000 #(java.util.UUID/randomUUID))))

  (def one-val (first uuid-set))

  (identity one-val)

  (def bloom-filter (.buildBloomFilter (FilterBuilder. 200000 0.01)))

  (time (mapv #(.add bloom-filter %) uuid-set))
  (time (into #{} uuid-set))

  (time (count (filter true? (repeatedly 200000 #(.contains bloom-filter (java.util.UUID/randomUUID))))))
  (time (count (filter true? (repeatedly 200000 #(uuid-set-set (java.util.UUID/randomUUID)))))))

(defrecord BloomFilters [bloom-buckets])

(defmethod state-extensions/initialise-filter :bloom [_ _] 
  (->BloomFilters []))

(defmethod state-extensions/apply-filter-id clojure.lang.PersistentHashSet [filter-state _ id] 
  (conj filter-state id))

(defmethod state-extensions/filter? clojure.lang.PersistentHashSet [filter-state _ id] 
  (filter-state id))

(defmethod state-extensions/close-filter clojure.lang.PersistentHashSet [_ _])
