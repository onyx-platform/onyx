(ns onyx.log.inbox
  (:require [clojure.core.async :refer [chan <!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info]]
            [onyx.extensions :as extensions]))

(defrecord Inbox [capacity starting-position]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Inbox")
    (let [ch (chan capacity)]
      (extensions/subscribe-to-log (:log component) starting-position ch)
      (assoc component :ch ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Inbox")
    (close! (:ch component))

    component))

(defn inbox [capacity starting-position]
  (map->Inbox {:capacity capacity :starting-position starting-position}))

(defmethod extensions/read-next-entry Inbox
  [inbox]
  (let [position (<!! (:ch inbox))
        entry (extensions/read-log-entry (:log inbox) position)]
    {:message-id position
     :fn (:fn entry)
     :args (:args entry)}))

