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

(defn sentinel-node [task-node input]
  (str task-node (impl/tag-sentinel-node input)))

(defn vote-for-sentinel-leader [sync task-node input uuid]
  (extensions/create-node sync (sentinel-node task-node input) {:uuid uuid}))

(defn filter-sentinels [decompressed]
  (remove (partial = :done) decompressed))

(defn learned-all-sentinels? [event state]
  (= (into #{} (keys (:learned-sentinel state)))
     (into #{} (keys (:onyx.core/ingress-queues event)))))

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
            input (:input (last (:onyx.core/batch event)))
            n-messages (f event)
            state @pipeline-state]
        (if uuid
          (if-not (get-in state [:learned-sentinel input])
            (do (vote-for-sentinel-leader sync task-node input uuid)
                (let [node (sentinel-node task-node input)
                      learned (:uuid (extensions/read-node sync node))]
                  (swap! pipeline-state assoc-in [:learned-sentinel input] learned)
                  (if (= learned uuid)
                    {:onyx.core/tail-batch? (and (= n-messages (count decompressed))
                                                 (learned-all-sentinels? event state))
                     :onyx.core/requeue? true
                     :onyx.core/decompressed filtered-segments}
                    {:onyx.core/tail-batch? false
                     :onyx.core/requeue? false
                     :onyx.core/decompressed filtered-segments})))
            (if (= (get-in state [:learned-sentinel input]) uuid)
              {:onyx.core/tail-batch? (and (= n-messages (count decompressed))
                                           (learned-all-sentinels? event state))
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
    (every? zero? (map #(extensions/n-consumers queue %) (vals ingress-queues)))
    true))

