(ns onyx.peer.task-lifecycles-test
  (:require [onyx.peer.task-lifecycle :as tl]
            [clojure.test :refer [deftest is testing]]))

(deftest build-task-state-machine-test
  (let [event {:task-map {:onyx/type :input}
               :windows [:some-window]}
        state-machine (tl/new-task-state-machine event nil)]
    state-machine

    ))
  
  
  
