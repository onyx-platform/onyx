(ns onyx.messaging.aeron.messaging-group
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.aeron.embedded-media-driver :as md]
            [onyx.messaging.messenger :as m]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn] :as timbre]))

(defrecord AeronMessagingPeerGroup [peer-config]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [bind-addr (common/bind-addr peer-config)
          external-addr (common/external-addr peer-config)
          port (:onyx.messaging/peer-port peer-config)
          ticket-counters (atom {})
          embedded-media-driver (component/start (md/->EmbeddedMediaDriver peer-config))]
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :ticket-counters ticket-counters
             :embedded-media-driver embedded-media-driver
             :port port)))

  (stop [{:keys [embedded-media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop embedded-media-driver)
    (assoc component :embedded-media-driver nil :bind-addr nil 
           :external-addr nil :external-channel nil :ticket-counters nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessagingPeerGroup {:peer-config peer-config}))

(defmethod clojure.core/print-method AeronMessagingPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
