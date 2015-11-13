(ns ^:no-doc onyx.peer.backpressure-poll
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [clojure.core.async.impl.buffers SlidingBuffer]
           [clojure.core.async.impl.channels ManyToManyChannel]))

(defrecord BackpressurePoll [peer-state messenger-buffer]
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
                          (let [ratio (/ (count buf) (.n buf))
                                on-val @on?]
                            (cond (and (not on-val) (> ratio high-water-ratio))
                                  (do (reset! on-val true)
                                      (>!! outbox-ch (create-log-entry :backpressure-on {:peer id})))
                                  (and on-val (< ratio low-water-ratio))
                                  (do (reset! on? false)
                                      (>!! outbox-ch (create-log-entry :backpressure-off {:peer id})))))
                          (Thread/sleep check-interval))))] 
      (assoc component :track-backpressure-fut track-fut)))
  (stop [component]
    (future-cancel (:track-backpressure-fut component))
    (assoc component :track-backpressure-fut nil)))


(defn backpressure-poll [peer-state]
  (map->BackpressurePoll {:peer-state peer-state}))
