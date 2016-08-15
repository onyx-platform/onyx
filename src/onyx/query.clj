(ns onyx.query
  (:require [onyx.static.default-vals :refer [arg-or-default]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info infof]]))

(defrecord QueryServer [replica server peer-config]
  component/Lifecycle
  (start [this]
    (let [ip (arg-or-default :onyx.query.server/ip peer-config )
          port (arg-or-default :onyx.query.server/port peer-config)]
      (infof "Starting http query server on %s:%s" ip port)
      this))
  (stop [this]
    (info "Stopping http query server")
    (assoc this 
           :replica nil
           :loggin-config nil
           :peer-config nil)))

(defrecord DummyServer [replica loggin-config peer-config])

(defn query-server [peer-config]
  (if (:onyx.query/server? peer-config)
    (map->QueryServer {:peer-config peer-config})
    (map->DummyServer {})))
