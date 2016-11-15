(ns onyx.messaging.aeron.messaging-group
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.aeron.embedded-media-driver :as md]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn] :as timbre]))

(defrecord AeronMessengerPeerGroup [peer-config]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [ticket-counters (atom {})
          embedded-media-driver (component/start (md/->EmbeddedMediaDriver peer-config))]
      (assoc component
             :ticket-counters ticket-counters
             :embedded-media-driver embedded-media-driver)))

  (stop [{:keys [embedded-media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop embedded-media-driver)
    (assoc component 
           :embedded-media-driver nil 
           :ticket-counters nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessengerPeerGroup {:peer-config peer-config}))

(defmethod clojure.core/print-method AeronMessengerPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
