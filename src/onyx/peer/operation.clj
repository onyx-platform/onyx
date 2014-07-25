(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]))

(defn apply-fn [f params segment]
  ((reduce #(partial %1 %2) f params) segment))

(defn resolve-fn [task-map]
  (try
    (let [user-ns (symbol (name (namespace (:onyx/fn task-map))))
          user-fn (symbol (name (:onyx/fn task-map)))]
      (or (ns-resolve user-ns user-fn) (throw (Exception.))))
    (catch Exception e
      (throw (ex-info "Could not resolve function in catalog" {:fn (:onyx/fn task-map)})))))

(defn dead-peers [states task-node]
  (filter
   (fn [state]
     (and (= (:task-node (:content state)) task-node)
          (= (:state (:content state)) :dead)))
   states))

(defn previously-sealing? [sync state]
  (let [previous-node (extensions/previous-node sync :peer-state (:node state))]
    (= (:state (extensions/read-node sync previous-node)) :sealing)))

(defn n-seal-failures [sync task-node]
  (let [peers (extensions/bucket sync :peer-state)
        states (map (partial extensions/dereference sync) peers)
        dead (dead-peers states task-node)]
    (count (filter (partial previously-sealing? sync) dead))))

(defn on-last-batch
  [{:keys [onyx.core/sync onyx.core/decompressed onyx.core/ingress-queues
           onyx.core/task-map] :as event} f]
  (cond (= (last decompressed) :done)
        (let [n-messages (f event)
              max-failures (n-seal-failures sync (:onyx.core/task-node event))]
          (assoc event
            :onyx.core/tail-batch? (or (= n-messages (count decompressed))
                                       (<= n-messages (inc max-failures)))
            :onyx.core/requeue? true
            :onyx.core/decompressed (remove (partial = :done) decompressed)))
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

