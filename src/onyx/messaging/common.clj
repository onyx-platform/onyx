(ns ^:no-doc onyx.messaging.common)

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn bind-addr [peer-messaging-config]
  (:peer/bind-addr peer-messaging-config))

(defn external-addr [peer-messaging-config]
  (or (:peer/external-addr peer-messaging-config)
      (bind-addr peer-messaging-config)))

(defmulti messenger :messaging/impl)

(defmethod messenger :aeron [_]
  (require 'onyx.messaging.aeron)
  (ns-resolve 'onyx.messaging.aeron 'aeron))

(defmethod messenger :http-kit-websockets [_]
  (require 'onyx.messaging.http-kit)
  (ns-resolve 'onyx.messaging.http-kit 'http-kit-websockets))

(defmethod messenger :netty-tcp [_]
  (require 'onyx.messaging.netty-tcp)
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-tcp-sockets))
