(ns onyx.state.filter.bloom
  (:require [onyx.state.state-extensions :as state-extensions]))

(defrecord BloomFilters [bloom-buckets])

(defmethod state-extensions/initialise-filter :bloom [_ _] 
  (->BloomFilters []))

(defmethod state-extensions/apply-filter-id clojure.lang.PersistentHashSet [filter-state _ id] 
  (conj filter-state id))

(defmethod state-extensions/filter? clojure.lang.PersistentHashSet [filter-state _ id] 
  (filter-state id))

(defmethod state-extensions/close-filter clojure.lang.PersistentHashSet [_ _])
