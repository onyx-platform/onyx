(ns ^:no-doc onyx.peer.backpressure-poll
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn trace fatal error] :as timbre]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t])
  (:import [clojure.core.async.impl.buffers SlidingBuffer]
           [clojure.core.async.impl.channels ManyToManyChannel]))

(defrecord BackpressurePoll [peer-state messenger-buffer task-monitoring]
  component/Lifecycle
  (start [component]
    (let [{:keys [opts id outbox-ch]} peer-state
          track-fut (future 
                      (let [low-water-pct (arg-or-default :onyx.peer/backpressure-low-water-pct opts)
                            high-water-pct (arg-or-default :onyx.peer/backpressure-high-water-pct opts)
                            check-interval (arg-or-default :onyx.peer/backpressure-check-interval opts)
                            on? (atom false)
                            buf ^SlidingBuffer (.buf ^ManyToManyChannel (:inbound-ch messenger-buffer))
                            low-water-ratio (/ low-water-pct 100)
                            high-water-ratio (/ high-water-pct 100)]
                        (while (not (Thread/interrupted))
                          (let [buf-count (count buf)
                                ratio (/ buf-count (.n buf))
                                on-val @on?]
                            (extensions/emit task-monitoring 
                                             (t/->MonitorTaskEventCount :messenger-queue-count buf-count))
                            (cond (and (not on-val) (> ratio high-water-ratio))
                                  (do (reset! on-val true)
                                      (>!! outbox-ch (create-log-entry :backpressure-on {:peer id})))
                                  (and on-val (< ratio low-water-ratio))
                                  (do (reset! on? false)
                                      (>!! outbox-ch (create-log-entry :backpressure-off {:peer id})))))
                          (Thread/sleep check-interval))))] 
      (assoc component :track-backpressure-fut track-fut)))
  (stop [component]
    (extensions/emit (:monitoring peer-state)
                     (t/->MonitorTaskEventCount :messenger-queue-count-unregister nil))
    (future-cancel (:track-backpressure-fut component))
    (assoc component :track-backpressure-fut nil)))


(defn backpressure-poll [peer-state]
  (map->BackpressurePoll {:peer-state peer-state}))
