(ns ^:no-doc onyx.messaging.messenger-buffer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]))

(defrecord MessengerBuffer [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Messenger Buffer")

    (let [inbound-ch (chan (or (:onyx.messenger/inbound-capacity opts) 20000))]
      (assoc component :inbound-ch inbound-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Messenger Buffer")

    (close! (:inbound-ch component))

    component))

(defn messenger-buffer [opts]
  (map->MessengerBuffer {:opts opts}))

