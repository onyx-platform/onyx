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

(defn apply-fn-bulk [f {:keys [onyx.core/batch] :as event}]
  ;; Bulk functions intentionally ignore their outputs.
  (let [segments (map :message batch)]
    (when (seq segments) (f segments))
    (assoc
      event
      :onyx.core/results
      (->Results (doall
                   (map
                     (fn [segment]
                       (->Result segment (t/vector (assoc segment :ack-val nil))))
                     batch))
                 (transient (t/vector))
                 (transient (t/vector))
                 (transient (t/vector))))))

(defn curry-params [f params]
  (reduce partial f params))

(defn apply-fn [compiled event]
  (let [f (:fn compiled)
        bulk? (:bulk? compiled)
        g (curry-params f (:onyx.core/params event))
        rets
        (if bulk?
          (apply-fn-bulk g event)
          (apply-fn-single g event))]
    (tracef "[%s / %s] Applied fn to %s segments, returning %s new segments"
      (:onyx.core/id rets)
      (:onyx.core/lifecycle-id rets)
      (count (:onyx.core/batch event))
      (count (mapcat :leaves (:tree (:onyx.core/results rets)))))
    rets))
