(ns ^:no-doc onyx.peer.transform
  (:require [onyx.types :refer [->Result]]
            [taoensso.timbre :refer [tracef trace]]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t]))

(defn collect-next-segments [f input]
  (let [segments (try (f input)
                      (catch Throwable e
                        (ex-info "Segment threw exception"
                                 {:exception e :segment input})))]
    (if (sequential? segments) segments (t/vector segments))))

(defn apply-fn-single [f {:keys [onyx.core/batch onyx.core/task-map] :as event}]
  (assoc event 
         :onyx.core/transformed 
         (doall (map (fn [leaf] (collect-next-segments f leaf)) batch))))

(defn collect-next-segments-batch [f input]
  (try (f input)
       (catch Throwable e
         (mapv (fn [segment] 
                 (ex-info "Batch threw exception"
                          {:exception e 
                           :segment segment}))
               input))))

(defn apply-fn-batch [f {:keys [onyx.core/batch] :as event}]
  (let [batch-results (collect-next-segments-batch f batch)] 
    (when-not (= (count batch-results) (count batch))
      (throw (ex-info ":onyx/batch-fn? functions must return the same number of elements as its input argment."
                      {:input-elements batch
                       :output-elements batch-results
                       :task (:onyx/name (:onyx.core/task-map event))})))
    (assoc event 
           :onyx.core/transformed
           (doall
            (map
             (fn [leaf output]
               (if (sequential? output) output (t/vector output)))
             batch
             batch-results)))))

(defn curry-params [f params]
  (reduce partial f params))

(defn apply-fn [a-fn f state]
  (-> state
      (set-event! (let [event (get-event state) 
                        ;; dynamic param currying is pretty slow
                        ;; maybe we can make a separate task-map option to turn this on
                        g (curry-params f (:onyx.core/params event))]
                    (a-fn g event)))
      (advance)))
