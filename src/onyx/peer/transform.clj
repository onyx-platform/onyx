(ns ^:no-doc onyx.peer.transform
  (:require [onyx.types :refer [->Result ->Results]]
            [taoensso.timbre :refer [tracef trace]]
            [onyx.protocol.task-state :refer :all]
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
                   (fn [leaf]
                     (let [segments (collect-next-segments f (:message leaf))
                           leaves (map (fn [segment]
                                         (assoc leaf :message segment))
                                       segments)]
                       (->Result leaf leaves)))
                   batch))
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
                     (fn [leaf]
                       (->Result leaf (t/vector leaf)))
                     batch))
                 (transient (t/vector))
                 (transient (t/vector))))))

(defn curry-params [f params]
  (reduce partial f params))

(defn apply-fn [state]
  (-> state
      (set-event! (let [event (get-event state) 
                        f (:onyx.core/fn event)
                        g (curry-params f (:onyx.core/params event))
                        rets ((:apply-fn event) g event)]
                    (tracef "[%s / %s] Applied fn to %s segments, returning %s new segments"
                            (:onyx.core/id rets)
                            (:onyx.core/lifecycle-id rets)
                            (count (:onyx.core/batch event))
                            (count (mapcat :leaves (:tree (:onyx.core/results rets)))))
                    rets))
      (advance)))
