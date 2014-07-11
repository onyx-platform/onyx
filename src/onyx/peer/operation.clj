(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]))

(defn apply-fn [f params segment]
  ((reduce #(partial %1 %2) f params) segment))

(defn resolve-fn [task-map]
  (try
    (let [user-ns (symbol (name (namespace (:onyx/fn task-map))))
          user-fn (symbol (name (:onyx/fn task-map)))]
      (or (ns-resolve user-ns user-fn) (throw (Exception.))))
    (catch Exception e
      (throw (ex-info "Could not resolve function in catalog" {:fn (:onyx/fn task-map)})))))

(defn on-last-batch
  [{:keys [onyx.core/decompressed onyx.core/ingress-queues onyx.core/task-map] :as event} f]
  (cond (= (last decompressed) :done)
        (let [n-messages (f event)]
          (assoc event
            :onyx.core/tail-batch? (= n-messages (count decompressed))
            :onyx.core/requeue? true
            :onyx.core/decompressed (or (butlast decompressed) [])))
        (some #{:done} decompressed)
        (assoc event
          :onyx.core/tail-batch? false
          :onyx.core/requeue? true
          :onyx.core/decompressed (remove (partial = :done) decompressed))
        :else
        (assoc event :onyx.core/tail-batch? false :onyx.core/requeue? false)))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  (if (= (:onyx/consumption task-map) :sequential)
    (zero? (extensions/n-consumers queue (first ingress-queues)))
    true))

