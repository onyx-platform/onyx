(ns ^:no-doc onyx.messaging.messenger-buffer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.static.default-vals :refer [defaults]]
            [taoensso.timbre :as timbre]))

(defrecord MessengerBuffer [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Messenger Buffer")

    (let [inbound-ch (chan (sliding-buffer (or (:onyx.messaging/inbound-buffer-size opts) 
                                               (:onyx.messaging/inbound-buffer-size defaults))))]
      (assoc component :inbound-ch inbound-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Messenger Buffer")

    (close! (:inbound-ch component))

    component))

(defn messenger-buffer [opts]
  (map->MessengerBuffer {:opts opts}))

