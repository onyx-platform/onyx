(ns ^:no-doc onyx.messaging.common)

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn bind-addr [peer-messaging-config]
  (:peer/bind-addr peer-messaging-config))

(defn external-addr [peer-messaging-config]
  (or (:peer/external-addr peer-messaging-config)
      (bind-addr peer-messaging-config)))

(defmulti messaging-require :messaging/impl)

(defmulti messenger :messaging/impl)

(defmulti messaging-peer-group :messaging/impl)

(defmethod messaging-require :aeron [_]
  (require 'onyx.messaging.aeron))

(defmethod messenger :aeron [_]
  (ns-resolve 'onyx.messaging.aeron 'aeron))

(defmethod messaging-peer-group :aeron [_]
  (ns-resolve 'onyx.messaging.aeron 'aeron-peer-group))

(defmethod messaging-require :http-kit-websockets [_]
  (require 'onyx.messaging.http-kit))

(defmethod messenger :http-kit-websockets [_]
  (ns-resolve 'onyx.messaging.http-kit 'http-kit-websockets))

(defmethod  messaging-peer-group :http-kit-websockets [_]
  (ns-resolve 'onyx.messaging.http-kit 'http-kit-peer-group))

(defmethod messenger :netty-tcp [_]
  (require 'onyx.messaging.netty-tcp))

(defmethod messenger :netty-tcp [_]
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-tcp-sockets))

(defmethod  messaging-peer-group :netty-tcp [_]
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-tcp-peer-group))


