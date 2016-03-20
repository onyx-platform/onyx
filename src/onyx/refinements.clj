(ns onyx.refinements
  (:require [onyx.schema :refer [Trigger StateEvent]]
            [schema.core :as s]))

(def Opts {s/Any s/Any})

(def discarding 
  {:refinement/create-state-update (s/fn discarding-create-state-update 
                                     [trigger :- Trigger state state-event :- StateEvent])
   :refinement/apply-state-update (s/fn discarding-apply-state-update 
                                    [trigger :- Trigger state entry]
                                    nil)})

(def accumulating
  {:refinement/create-state-update (s/fn accumulating-create-state-update 
                                     [trigger :- Trigger state state-even :- StateEvent])
   :refinement/apply-state-update (s/fn accumulating-apply-state-update
                                    [trigger :- Trigger state entry]
                                    state)})
