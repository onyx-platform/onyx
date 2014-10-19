(ns onyx.peer.operation
  (:require [onyx.extensions :as extensions]
            [onyx.coordinator.impl :as impl]
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

(defn vote-for-sentinel-leader [sync task-node uuid]
  (extensions/create-node sync (str task-node impl/sentinel-marker) {:uuid uuid}))

(defn filter-sentinels [decompressed]
  (remove (partial = :done) decompressed))

(defn on-last-batch
  [{:keys [onyx.core/sync onyx.core/queue onyx.core/decompressed
           onyx.core/pipeline-state onyx.core/task-node] :as event} f]
  (if (= (last decompressed) :done)
    (if (= (:onyx/type (:onyx.core/task-map event)) :input)
      (let [n-messages (f event)]
        {:onyx.core/tail-batch? (= n-messages (count decompressed))
         :onyx.core/requeue? true
         :onyx.core/decompressed (filter-sentinels decompressed)})
      (let [uuid (extensions/message-uuid queue (:message (last (:onyx.core/batch event))))
            filtered-segments (filter-sentinels decompressed)
            n-messages (f event)
            state @pipeline-state]
        (if uuid
          (if-not (:learned-sentinel state)
            (do (vote-for-sentinel-leader sync task-node uuid)
                (let [learned (:uuid (extensions/read-node sync (str task-node impl/sentinel-marker)))]
                  (swap! pipeline-state assoc :learned-sentinel learned)
                  (if (= learned uuid)
                    {:onyx.core/tail-batch? (= n-messages (count decompressed))
                     :onyx.core/requeue? true
                     :onyx.core/decompressed filtered-segments}
                    {:onyx.core/tail-batch? false
                     :onyx.core/requeue? false
                     :onyx.core/decompressed filtered-segments})))
            (if (= (:learned-sentinel state) uuid)
              {:onyx.core/tail-batch? (= n-messages (count decompressed))
               :onyx.core/requeue? true
               :onyx.core/decompressed filtered-segments}
              {:onyx.core/tail-batch? false
               :onyx.core/requeue? false
               :onyx.core/decompressed filtered-segments}))
          {:onyx.core/tail-batch? false
           :onyx.core/requeue? true
           :onyx.core/decompressed filtered-segments})))
    {:onyx.core/tail-batch? false
     :onyx.core/requeue? false}))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  (if (= (:onyx/consumption task-map) :sequential)
    (zero? (extensions/n-consumers queue (first ingress-queues)))
    true))

