(ns onyx.peer.storage-api
  (:require [onyx.coordinator.planning :refer [find-task]]))

(defn storage-dispatch [event]
  (let [catalog-task (find-task (:catalog event) (:task event))]
    (select-keys catalog-task [:onyx/type :onyx/medium :onyx/direction])))

(defmulti munge-read-batch storage-dispatch)

(defmulti munge-decompress-tx storage-dispatch)

(defmulti munge-apply-fn storage-dispatch) 

(defmulti munge-compress-tx storage-dispatch)

(defmulti munge-write-batch storage-dispatch)

