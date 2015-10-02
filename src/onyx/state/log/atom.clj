(ns onyx.state.log.atom
  (:require [onyx.state.state-extensions :as state-extensions]
            [onyx.state.core :as state])) 

(defmethod state-extensions/initialise-log :atom [_ event] 
  (get @(:onyx.core/test-entries-log event) (state/peer-log-id event)))

(defmethod state-extensions/store-log-entries clojure.lang.Atom [log _ entries]
  (swap! log into entries))

(defmethod state-extensions/playback-log-entries clojure.lang.Atom [log _ state id->log-resolve]
  (reduce (fn [state' [window-id extent entry]] 
            (update-in state' 
                       [window-id extent] 
                       (fn [ext-state] 
                         (let [apply-fn (id->log-resolve window-id)] 
                           (assert apply-fn (str "Apply fn does not exist for window-id " window-id))
                           (apply-fn ext-state entry))))) 
          state 
          @log))

(defmethod state-extensions/store-seen-ids clojure.lang.Atom [log _ seen-ids]
  (swap! log into seen-ids))

(defmethod state-extensions/playback-seen-ids clojure.lang.Atom [seen-log _ bucket-state apply-fn]
  bucket-state
  #_(reduce apply-fn bucket-state @seen-log))
