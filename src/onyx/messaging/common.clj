(ns ^:no-doc onyx.messaging.common)

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn bind-addr [peer-config]
  (:onyx.messaging/bind-addr peer-config))

(defn external-addr [peer-config]
  (or (:onyx.messaging/external-addr peer-config)
      (bind-addr peer-config)))

(defn allowable-ports [peer-config]
  (let [port-range (if-let [port-range (:onyx.messaging/peer-port-range peer-config)]
                     (range (first port-range) 
                            (inc (second port-range)))
                     [])
        ports-static (:onyx.messaging/peer-ports peer-config)]
    (into (set port-range) 
          ports-static)))

(defmulti messaging-require :onyx.messaging/impl)

(defmulti messenger :onyx.messaging/impl)

(defmulti messaging-peer-group :onyx.messaging/impl)

(defmethod messaging-require :aeron [_]
  (require 'onyx.messaging.aeron))

(defmethod messenger :aeron [_]
  (ns-resolve 'onyx.messaging.aeron 'aeron))

(defmethod messaging-peer-group :aeron [_]
  (ns-resolve 'onyx.messaging.aeron 'aeron-peer-group))

(defmethod messaging-require :netty-tcp [_]
  (require 'onyx.messaging.netty-tcp))

(defmethod messenger :netty-tcp [_]
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-tcp-sockets))

(defmethod messaging-peer-group :netty-tcp [_]
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-peer-group))

