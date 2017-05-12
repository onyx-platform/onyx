(ns onyx.messaging.aeron.messaging-group
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.aeron.embedded-media-driver :as md]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.default-vals :refer [arg-or-default]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn] :as timbre]))

(defrecord AeronMessengerPeerGroup [peer-config image-reference-count ticket-counters]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [ticket-counters (atom {})
          image-reference-count (atom {})
          embedded-media-driver (component/start (md/->EmbeddedMediaDriver peer-config))]
      (assoc component
             :image-reference-count image-reference-count
             :ticket-counters ticket-counters
             :embedded-media-driver embedded-media-driver)))

  (stop [{:keys [embedded-media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop embedded-media-driver)
    (when-not (empty? (remove (fn [v] (= :closed (:status v))) (mapcat vals (vals @image-reference-count))))
      (info "Shutdown subscribers did not reduce the image reference count to zero.
             Please notify the Onyx team as this should not occur."))
    (assoc component 
           :image-reference-count nil
           :ticket-counters nil
           :embedded-media-driver nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessengerPeerGroup {:peer-config peer-config}))

(defmethod clojure.core/print-method AeronMessengerPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
