(ns onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread timeout >!! <!!]]
            [onyx.coordinator.extensions :as extensions]))

(def eviction-delay 5000)

(def planning-ch (chan 1000))

(def born-peer-ch (chan 1000))

(def dead-peer-ch (chan 1000))

(def evict-ch (chan 1000))

(def offer-ch (chan 1000))

(def ack-ch (chan 1000))

(def completion-ch (chan 1000))

(defn mark-peer-birth [log sync peer]
  (extensions/await-death sync dead-peer-ch)
  (extensions/mark-peer-born log peer))

(defn mark-peer-death [log peer]
  (extensions/mark-peer-dead log peer))

(defn plan-job [log job]
  (comment "Planning here.")
  (extensions/plan-job log job))

(defn acknowledge-task [log task]
  (extensions/ack log task))

(defn evict-task [log sync task]
  (extensions/delete sync task)
  (extensions/evict log task))

(defn offer-task [log sync]
  (when (extensions/next-task log)
    (extensions/create-ack-node sync)
    (extensions/create-completion-node sync)
    (extensions/await-ack sync ack-ch)
    (extensions/await-completion sync completion-ch)
    (extensions/mark-offered log)
    (extensions/write-payload sync)))

(defn complete-task [log sync queue task]
  (extensions/delete sync task)
  (extensions/complete log task)
  (extensions/cap-queue queue task))

(defn born-peer-ch-loop [log sync]
  (loop []
    (let [peer (<!! born-peer-ch)]
      (mark-peer-birth log sync peer)
      (>!! offer-ch peer)
      (recur))))

(defn dead-peer-ch-loop [log]
  (loop []
    (let [peer (<!! dead-peer-ch)]
      (mark-peer-death log peer)
      (>!! evict-ch peer)
      (recur))))

(defn planning-ch-loop [log]
  (loop []
    (let [job (<!! planning-ch)]
      (plan-job log job)
      (<!! offer-ch job)
      (recur))))

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

(defn offer-ch-loop [log sync]
  (loop []
    (let [event (<!! offer-ch)]
      (when (offer-task log sync)
        (thread (<!! (timeout eviction-delay))
                (>!! evict-ch nil)))
      (recur))))

(defn completion-ch-loop [log sync queue]
  (loop []
    (let [task (<!! completion-ch)]
      (complete-task log sync queue task)
      (>!! offer-ch task)
      (recur))))

(defn ch-graph [log sync queue]
  (thread (born-peer-ch-loop log sync))
  (thread (dead-peer-ch-loop log))
  (thread (planning-ch-loop))
  (thread (ack-ch-loop log))
  (thread (evict-ch-loop log sync))
  (thread (offer-ch-loop log sync))
  (thread (completion-ch-loop log sync queue)))

