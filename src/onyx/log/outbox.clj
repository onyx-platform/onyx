(ns onyx.log.outbox
  (:require [clojure.core.async :refer [chan close!]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]))

(defrecord Outbox [capacity]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Outbox")
    (let [ch (chan capacity)]
      (assoc component :ch ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Outbox")
    (close! (:ch component))

    component))

(defn outbox [capacity starting-position]
  (map->Outbox {:capacity capacity}))

