(ns ^{:no-doc true} onyx.static.onyx-component
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal]]))

(defn rethrow-component [f]
  (try
    (f)
    (catch Throwable e
      (fatal e)
      (throw (.getCause e)))))

(defrecord ComponentSystem []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this))))
