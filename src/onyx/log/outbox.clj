(ns onyx.log.outbox
  (:require [clojure.core.async :refer [chan >!! <! go close!]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]))

(defn write-to-log-loop [log ch]
  (loop []
    (when-let [entry (<! ch)]
      (extensions/write-log-entry log entry)
      (recur))))

(defrecord Outbox [capacity]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Outbox")
    (let [ch (chan capacity)]
      (go (write-to-log-loop (:log component) ch))
      (assoc component :ch ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Outbox")
    (close! (:ch component))

    component))

(defn outbox [capacity starting-position]
  (map->Outbox {:capacity capacity}))

(defmethod extensions/write-to-outbox Outbox
  [outbox entry]
  (>!! (:ch outbox) entry))

