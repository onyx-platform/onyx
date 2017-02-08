(ns ^:no-doc onyx.messaging.common
  (:require [onyx.static.default-vals :refer [arg-or-default]]
            [taoensso.timbre :refer [fatal]]))

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn bind-addr [peer-config]
  (arg-or-default :onyx.messaging/bind-addr peer-config))

(defn bind-port [peer-config]
  (arg-or-default :onyx.messaging/peer-port peer-config))

(defn external-addr [peer-config]
  (or (:onyx.messaging/external-addr peer-config)
      (bind-addr peer-config)))

