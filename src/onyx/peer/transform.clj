(ns ^:no-doc onyx.peer.transform
  (:require [onyx.types :refer [->Result ->Results]]
            [taoensso.timbre :refer [trace]]
            [clj-tuple :as t]))

(defn collect-next-segments [f input]
  (let [segments (try (f input)
                      (catch Throwable e
                        (ex-info "Segment threw exception"
                                 {:exception e :segment input})))]
    (if (sequential? segments) segments (t/vector segments))))

(defn apply-fn-single [f {:keys [batch] :as event}]
  (assoc
   event
   :results
    (->Results (doall
                 (map
                   (fn [leaf]
                     (let [segments (collect-next-segments f (:message leaf))
                           leaves (map (fn [segment]
                                         (println "Associng " leaf segment)
                                         (assoc leaf :message segment))
                                       segments)]
                       (->Result leaf leaves)))
                   batch))
               (transient (t/vector))
               (transient (t/vector)))))

(defn apply-fn-bulk [f {:keys [batch] :as event}]
  ;; Bulk functions intentionally ignore their outputs.
  (let [segments (map :message batch)]
    (when (seq segments) (f segments))
    (assoc
      event
      :results
      (->Results (doall
                   (map
                     (fn [leaf]
                       (->Result leaf (t/vector leaf)))
                     batch))
                 (transient (t/vector))
                 (transient (t/vector))))))

(defn curry-params [f params]
  (reduce partial f params))

(defn apply-fn [event]
  (let [f (:fn event)
        g (curry-params f (:params event))
        apply-fn (:apply-fn event)
        rets (apply-fn f event)]
    (trace (format "[%s / %s] Applied fn to %s segments"
                   (:id rets)
                   (:lifecycle-id rets)
                   (count (:results rets))))
    rets))
