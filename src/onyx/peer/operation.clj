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

(defn sentinel-node-name [task-id input-name]
  (format "%s-%s" task-id input-name))

(defn vote-for-sentinel-leader [log task-id input-name uuid]
  (let [node (sentinel-node-name task-id input-name)]
    (extensions/write-chunk log :sentinel {:leader uuid} node)))

(defn filter-sentinels [decompressed]
  (remove (partial = :done) decompressed))

(defn learned-all-sentinels? [event state]
  (= (into #{} (keys (:learned-sentinel state)))
     (into #{} (keys (:onyx.core/ingress-queues event)))))

(defn drained-all-inputs? [event state]
  (let [drained-inputs (keys (into {} (filter second (:drained-inputs state))))]
    (= (into #{} drained-inputs)
       (into #{} (keys (:onyx.core/ingress-queues event))))))

(defn on-last-batch
  [{:keys [onyx.core/log onyx.core/queue onyx.core/decompressed
           onyx.core/task-id onyx.core/pipeline-state onyx.core/task-node]
    :as event} f]
  (if (= (last decompressed) :done)
    (if (= (:onyx/type (:onyx.core/task-map event)) :input)
      ;; Input task, delegate out to its implementation for what "done" means
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
          ;; UUID has been preassigned to sentinel value
          (if-not (get-in state [:learned-sentinel input])
            ;; Don't have a cached value for the sentinel leader
            (do (vote-for-sentinel-leader log task-id input uuid)
                (let [node (sentinel-node-name task-id input)
                      learned (:leader (extensions/read-chunk log :sentinel node))
                      successor
                      (swap! pipeline-state
                             (fn [v]
                               (-> v
                                   (assoc-in [:learned-sentinel input] learned)
                                   (assoc-in [:drained-inputs input]
                                             (and (= n-messages (count decompressed))
                                                  (= learned uuid))))))]
                  (if (= learned uuid)
                    ;; My sentinel is the leader
                    {:onyx.core/tail-batch? (and (learned-all-sentinels? event successor)
                                                 (drained-all-inputs? event successor))
                     :onyx.core/requeue? true
                     :onyx.core/decompressed filtered-segments}
                    ;; My sentinel isn't the leader, throw it out
                    {:onyx.core/tail-batch? false
                     :onyx.core/requeue? false
                     :onyx.core/decompressed filtered-segments})))
            ;; We've got the cached leader already
            (let [successor (swap! pipeline-state assoc-in [:drained-inputs input]
                                   (and (= n-messages (count decompressed))
                                        (= (get-in state [:learned-sentinel input]) uuid)))]
              (if (= (get-in state [:learned-sentinel input]) uuid)
                ;; Our sentinel is the leader
                {:onyx.core/tail-batch? (and (learned-all-sentinels? event successor)
                                             (drained-all-inputs? event successor))
                 :onyx.core/requeue? true
                 :onyx.core/decompressed filtered-segments}
                ;; Our sentinel isn't the leader, chuck it
                {:onyx.core/tail-batch? false
                 :onyx.core/requeue? false
                 :onyx.core/decompressed filtered-segments})))
          ;; This sentinel doesn't have a UUID associated with it, funnel it through again
          {:onyx.core/tail-batch? false
           :onyx.core/requeue? true
           :onyx.core/decompressed filtered-segments})))
    ;; Not a sentinel, nothing to requeue
    {:onyx.core/tail-batch? false
     :onyx.core/requeue? false}))

(defn start-lifecycle?
  [{:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map]}]
  (if (= (:onyx/consumption task-map) :sequential)
    (every? zero? (map #(extensions/n-consumers queue %) (vals ingress-queues)))
    true))

