(ns onyx.state.filter.set
  (:require [onyx.state.state-extensions :as state-extensions]))

(defmethod state-extensions/initialise-filter :set [_ _] 
  #{})

(defmethod state-extensions/apply-filter-id clojure.lang.PersistentHashSet [filter-state _ id] 
  (conj filter-state id))

(defmethod state-extensions/filter? clojure.lang.PersistentHashSet [filter-state _ id] 
  (filter-state id))

(defmethod state-extensions/close-filter clojure.lang.PersistentHashSet [_ _])
