(ns ^:no-doc onyx.messaging.common
  (:require [taoensso.timbre :refer [fatal]]))

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

(defn safe-require [sym]
  (try (require sym)
       (catch Throwable e
         (fatal e
                (str "Error loading messaging. "
                     "If your peer is AOT compiled you will need to manually require " sym))
         (throw e))))

(defmethod messaging-require :aeron [_]
  (safe-require 'onyx.messaging.aeron))

(defmethod messenger :aeron [_]
  (ns-resolve 'onyx.messaging.aeron 'aeron))

(defmethod messaging-peer-group :aeron [_]
  (ns-resolve 'onyx.messaging.aeron 'aeron-peer-group))

(defmethod messaging-require :netty [_]
  (safe-require 'onyx.messaging.netty-tcp))

(defmethod messenger :netty [_]
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-tcp-sockets))

(defmethod messaging-peer-group :netty [_]
  (ns-resolve 'onyx.messaging.netty-tcp 'netty-peer-group))

(defmethod messaging-require :core.async [_]
  (safe-require 'onyx.messaging.core-async))

(defmethod messenger :core.async [_]
  (ns-resolve 'onyx.messaging.core-async 'core-async))

(defmethod messaging-peer-group :core.async [_]
  (ns-resolve 'onyx.messaging.core-async 'core-async-peer-group))
