(ns onyx.query
  (:require [onyx.static.default-vals :refer [arg-or-default]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info infof]]))

(defrecord DummyQueryServer [replica loggin-config peer-config]
  component/Lifecycle
  (start [this]
    (assoc this :replica (atom nil)))
  (stop [this]
    (assoc this :replica nil)))

(defmulti query-server 
  (fn [peer-config]
    (boolean (:onyx.query/server? peer-config))))

(defmethod query-server false [_]
  (map->DummyQueryServer {}))

(defmethod query-server :default [peer-config]
  (throw (ex-info (str "No http query server implementation found. "
                       "Did you include org.onyxplatform/onyx-peer-http-query in your dependencies "
                       "and require onyx.http-query in your peer bootup namespace?")
                  {:peer-config peer-config})))

(defmethod clojure.core/print-method DummyQueryServer
  [system ^java.io.Writer writer]
  (.write writer "#<Dummy Query Server>"))
