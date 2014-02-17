(ns onyx.peer.pipeline-extensions
  (:require [onyx.coordinator.planning :refer [find-task]]))

(defn storage-dispatch [event]
  (let [catalog-task (find-task (:catalog event) (:task event))]
    (select-keys catalog-task [:onyx/type :onyx/medium :onyx/direction])))

(defmulti read-batch storage-dispatch)

(defmulti decompress-batch storage-dispatch)

(defmulti apply-fn storage-dispatch) 

(defmulti compress-batch storage-dispatch)

(defmulti write-batch storage-dispatch)

