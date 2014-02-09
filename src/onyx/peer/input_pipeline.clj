(ns onyx.peer.input-pipeline
  (:require [clojure.core.async :refer [alts!! <!! >!! >! chan close! go] :as async]
            [com.stuartsierra.component :as component]
            [dire.core :as dire]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.extensions :as extensions]))

(defrecord InputPipeline [payload sync queue]
  component/Lifecycle

  (start [component]
    (prn "Starting Input Pipeline")
    
    component)

  (stop [component]
    (prn "Stopping Input Pipeline")

    component))

(defn input-pipeline [payload sync queue]
  (map->InputPipeline {:payload payload :sync sync :queue queue}))

