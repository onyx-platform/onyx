(ns ^:no-doc onyx.messaging.messaging-buffer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]))

(defrecord MessagingBuffer [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Messaging Buffer")

    (let [inbound-ch (chan (or (:onyx.messaging/inbound-capacity opts) 20000))
;          outbound-ch (chan (or (:onyx.messaging/outbound-capacity opts) 20000))
          ]
      (assoc component :inbound-ch inbound-ch; :outbound-ch outbound-ch
             )))

  (stop [component]
    (taoensso.timbre/info "Stopping Messaging Buffer")

    (close! (:inbound-ch component))
;    (close! (:outbound-ch) component)

    component))

(defn messaging-buffer [opts]
  (map->MessagingBuffer {:opts opts}))

