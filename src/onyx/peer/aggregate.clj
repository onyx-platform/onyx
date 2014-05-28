(ns ^:no-doc onyx.peer.aggregate
  (:require [clojure.core.async :refer [chan go >! <! <!! >!! close!]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.pipeline-internal-extensions :as internal-ext]
            [onyx.extensions :as extensions]
            [onyx.coordinator.planning :refer [find-task]]
            [onyx.queue.hornetq :refer [take-segments]]
            [onyx.peer.transform :as transformer]
            [taoensso.timbre :refer [info]]))

(def n-pipeline-threads 1)

(defn consumer-loop [event session consumers halting-ch session-ch]
  (go (loop []
        (let [task (find-task (:catalog event) (:task event))
              consumer (first consumers)
              f #(extensions/consume-message (:queue event) consumer)
              msgs (doall (take-segments f (:onyx/batch-size task)))]
          (>! session-ch {:session session :halting-ch halting-ch :msgs msgs}))
        (when (<! halting-ch)
          (recur)))))

(defmethod internal-ext/inject-pipeline-resources :aggregator
  [{:keys [queue ingress-queues] :as event}]
  (let [rets
        {:onyx.aggregate/queue
         (->> (range n-pipeline-threads)
              (map (fn [x] {:session (extensions/create-tx-session queue)}))
              (map (fn [x] (assoc x :consumers (map (partial extensions/create-consumer queue (:session x))
                                                   ingress-queues))))
              (map (fn [x] (assoc x :halting-ch (chan 0)))))
         :onyx.aggregate/read-ch (chan n-pipeline-threads)
         :reserve? true}]
    (doseq [queue-bundle (:onyx.aggregate/queue rets)]
      (consumer-loop event
                     (:session queue-bundle)
                     (:consumers queue-bundle)
                     (:halting-ch queue-bundle)
                     (:onyx.aggregate/read-ch rets)))
    rets))

(defmethod p-ext/read-batch [:aggregator nil]
  [event]
  (let [{:keys [session halting-ch msgs]} (<!! (:onyx.aggregate/read-ch event))]
    {:session session
     :batch msgs
     :onyx.aggregate/halting-ch halting-ch
     :onyx.pipeline/session-origin (:session event)}))

(defmethod internal-ext/close-temporal-resources :aggregator
  [event]
  (>!! (:onyx.aggregate/halting-ch event) true)
  (extensions/close-resource (:queue event) (:onyx.pipeline/session-origin event))
  {})

(defmethod internal-ext/close-pipeline-resources :aggregator
  [{:keys [queue] :as event}]
  (close! (:onyx.aggregate/read-ch event))
  (doseq [queue-bundle (:onyx.aggregate/queue event)]
    (close! (:halting-ch queue-bundle))
    (doseq [c (:consumers queue-bundle)] (extensions/close-resource queue c))
    (extensions/close-resource queue (:session queue-bundle)))
  {})

