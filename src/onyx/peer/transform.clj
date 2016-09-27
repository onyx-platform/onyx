(ns ^:no-doc onyx.peer.transform
  (:require [onyx.types :refer [->Result ->Results]]
            [taoensso.timbre :refer [tracef]]
            [clj-tuple :as t]))

(defn collect-next-segments [f input]
  (let [segments (try (f input)
                      (catch Throwable e
                        (ex-info "Segment threw exception"
                                 {:exception e :segment input})))]
    (if (sequential? segments) segments (t/vector segments))))

(defn apply-fn-single [f {:keys [onyx.core/batch] :as event}]
  (assoc
   event
   :onyx.core/results
    (->Results (doall
                 (map
                   (fn [segment]
                     (let [segments (collect-next-segments f (:message segment))
                           leaves (map (fn [message]
                                         (-> segment
                                             (assoc :message message)
                                             ;; not actually required, but it's safer
                                             (assoc :ack-val nil)))
                                       segments)]
                       (->Result segment leaves)))
                   batch))
               (transient (t/vector))
               (transient (t/vector))
               (transient (t/vector)))))

(defn collect-next-segments-batch [f input]
  (try (f input)
       (catch Throwable e
         (ex-info "Segments threw exception"
                  {:exception e :segments input}))))

(defn apply-fn-batch [f {:keys [onyx.core/batch] :as event}]
  (let [batch-results (collect-next-segments f (map :message batch))] 
    (when-not (= (count batch-results) (count batch))
      (throw (ex-info ":onyx/batch-fn? functions must return the same number of elements as its input argment."
                      {:input-elements batch
                       :output-elements batch-results
                       :task (:onyx/name (:onyx.core/task-map event))})))
    (assoc
     event
     :onyx.core/results
     (->Results (doall
                 (map
                  (fn [leaf output]
                    (let [segments (if (sequential? output) output (t/vector output))
                          leaves (map (fn [message]
                                        (-> leaf
                                            (assoc :message message)
                                            ;; not actually required, but it's safer
                                            (assoc :ack-val nil)))
                                      segments)]
                      (->Result leaf leaves)))
                  batch
                  batch-results))
                (transient (t/vector))
                (transient (t/vector))
                (transient (t/vector))))))

(defn curry-params [f params]
  (reduce partial f params))

(defn apply-fn [compiled event]
  (let [f (:fn compiled)
        g (curry-params f (:onyx.core/params event))
        rets (if (:batch-fn? compiled)
               (apply-fn-batch g event) 
               (apply-fn-single g event))]
    (tracef "[%s / %s] Applied fn to %s segments, returning %s new segments"
      (:onyx.core/id rets)
      (:onyx.core/lifecycle-id rets)
      (count (:onyx.core/batch event))
      (count (mapcat :leaves (:tree (:onyx.core/results rets)))))
    rets))
