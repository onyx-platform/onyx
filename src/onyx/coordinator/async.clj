(ns onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread >!! <!!]]
            [onyx.coordinator.extensions :as extensions]))

(def planning-ch (chan 1000))

(def new-peer-ch (chan 1000))

(def dead-peer-ch (chan 1000))

(def evict-ch (chan 1000))

(def offer-ch (chan 1000))

(def ack-ch (chan 1000))

(def completion-ch (chan 1000))

(defn acknowledge-task [log task]
  (extensions/ack log task))

(defn evict-task [log sync task]
  (extensions/delete sync task)
  (extensions/evict log task))

(defn complete-task [log sync queue task]
  (extensions/delete sync task)
  (extensions/complete log task)
  (extensions/cap-queue queue task))

(defn ack-ch-loop [log]
  (loop []
    (let [task (<!! ack-ch)]
      (acknowledge-task log task)
      (recur))))

(defn evict-ch-loop [log sync]
  (loop []
    (let [task (<!! evict-ch)]
      (evict-task log sync task)
      (>!! offer-ch task)
      (recur))))

(defn completion-ch-loop [log sync queue]
  (loop []
    (let [task (<!! completion-ch)]
      (complete-task log sync queue task)
      (>!! offer-ch task)
      (recur))))

(defn ch-graph [log sync queue]
  (thread (ack-ch-loop log))
  (thread (evict-ch-loop log sync))
  (thread (completion-ch-loop log sync queue)))

