(ns onyx.refinements)

(defn discarding-create-state-update [trigger state state-event])

(defn discarding-apply-state-update [trigger state entry])

(defn accumulating-create-state-update [trigger state state-event])

(defn accumulating-apply-state-update [trigger state entry]
  state)

(def ^:export discarding
  {:refinement/create-state-update discarding-create-state-update 
   :refinement/apply-state-update discarding-apply-state-update})

(def ^:export accumulating
  {:refinement/create-state-update accumulating-create-state-update 
   :refinement/apply-state-update accumulating-apply-state-update})
