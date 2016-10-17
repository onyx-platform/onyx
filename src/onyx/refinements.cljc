(ns onyx.refinements
  (:require [clojure.spec :as s]
            [clojure.future :refer [any?]]))

(defn discarding-create-state-update [trigger state state-event])

(defn discarding-apply-state-update [trigger state entry])

(defn accumulating-create-state-update [trigger state state-event])

(defn accumulating-apply-state-update [trigger state entry]
  state)

(def discarding
  {:refinement/create-state-update discarding-create-state-update 
   :refinement/apply-state-update discarding-apply-state-update})

(def accumulating
  {:refinement/create-state-update accumulating-create-state-update 
   :refinement/apply-state-update accumulating-apply-state-update})

(s/fdef discarding-create-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :state-event :onyx.core/state-event))

(s/fdef discarding-apply-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :entry any?))

(s/fdef accumulating-create-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :state-event :onyx.core/state-event))

(s/fdef accumulating-apply-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :entry any?))
