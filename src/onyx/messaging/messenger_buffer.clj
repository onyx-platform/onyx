(ns ^:no-doc onyx.messaging.messenger-buffer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [taoensso.timbre :as timbre]))

(defrecord MessengerBuffer [inbound-ch release-ch retry-ch opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Messenger Buffer")

    (let [retry-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/retry-ch-buffer-size opts)))
          release-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/release-ch-buffer-size opts)))
          inbound-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/inbound-buffer-size opts)))]
      (assoc component :inbound-ch inbound-ch :release-ch release-ch :retry-ch retry-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Messenger Buffer")
    (close! (:release-ch component))
    (close! (:retry-ch component))
    (close! (:inbound-ch component))
    (assoc component :release-ch nil :inbound-ch nil :retry-ch nil)))

(defn messenger-buffer [opts]
  (map->MessengerBuffer {:opts opts}))
