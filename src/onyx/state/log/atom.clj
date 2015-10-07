(ns onyx.state.log.atom
  (:require [onyx.state.state-extensions :as state-extensions]
            [onyx.state.core :as state]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre])) 

(defmethod state-extensions/initialise-log :atom [_ event] 
  (let [test-entries-log (:onyx.core/test-entries-log event) 
        path [(:onyx.core/task-id event) (state/peer-slot-id event)]
        rets (swap! test-entries-log
                    update-in 
                    path
                    (fn [log]
                      (or log (atom []))))]
    (get-in rets path)))

(defmethod state-extensions/store-log-entry clojure.lang.Atom [log _ entry]
  (swap! log conj entry))

(defmethod state-extensions/playback-log-entries clojure.lang.Atom [log {:keys [onyx.core/windows :as event]} state]
  (let [id->window (into {} 
                         (map (juxt :window/id identity) 
                              windows))
        id->log-resolve (into {} 
                              (map (juxt :window/id :window/log-resolve) 
                                   windows))] 
    (reduce (fn [state' [window-id extent entry]] 
              (update-in state' 
                         [window-id extent] 
                         (fn [ext-state] 
                           (let [ext-state' (if ext-state
                                              ext-state
                                              (let [w (id->window window-id)] 
                                                ((:window/agg-init w) w)))
                                 apply-fn (id->log-resolve window-id)] 
                             (assert apply-fn (str "Apply fn does not exist for window-id " window-id))
                             (apply-fn ext-state' entry))))) 
            state 
            @log)))

(defmethod state-extensions/close-log clojure.lang.Atom
  [log event])
