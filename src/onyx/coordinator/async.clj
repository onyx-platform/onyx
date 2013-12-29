(ns onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread mult tap timeout >!! <!!]]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic]
            [onyx.coordinator.sync.zookeeper]))

(def eviction-delay 5000)

(def ch-capacity 1000)

(def planning-ch-head (chan ch-capacity))

(def born-peer-ch-head (chan ch-capacity))

(def dead-peer-ch-head (chan ch-capacity))

(def evict-ch-head (chan ch-capacity))

(def offer-ch-head (chan ch-capacity))

(def ack-ch-head (chan ch-capacity))

(def completion-ch-head (chan ch-capacity))

(def planning-ch-tail (chan ch-capacity))

(def born-peer-ch-tail (chan ch-capacity))

(def dead-peer-ch-tail (chan ch-capacity))

(def evict-ch-tail (chan ch-capacity))

(def offer-ch-tail (chan ch-capacity))

(def ack-ch-tail (chan ch-capacity))

(def completion-ch-tail (chan ch-capacity))

(def planning-mult (mult planning-ch-head))

(def born-peer-mult (mult born-peer-ch-head))

(def dead-peer-mult (mult dead-peer-ch-head))

(def evict-mult (mult evict-ch-head))

(def offer-mult (mult offer-ch-head))

(def ack-mult (mult ack-ch-head))

(def completion-mult (mult completion-ch-head))

(defn mark-peer-birth [log sync peer death-cb]
  (extensions/on-change sync death-cb)
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

(defn offer-task [log sync ack-cb complete-cb]
  (when (extensions/next-task log)
    (extensions/create sync)
    (extensions/create sync)
    (extensions/on-change sync ack-cb)
    (extensions/on-change sync complete-cb)
    (extensions/mark-offered log)
    (extensions/write-place sync)))

(defn complete-task [log sync queue task]
  (extensions/delete sync task)
  (extensions/complete log task)
  (extensions/cap-queue queue task))

(defn born-peer-ch-loop [log sync]
  (loop []
    (let [place (<!! born-peer-ch-head)]
      (mark-peer-birth log sync place #(>!! dead-peer-ch-head %))
      (>!! offer-ch-head place)
      (recur))))

(defn dead-peer-ch-loop [log]
  (loop []
    (let [peer (<!! dead-peer-ch-head)]
      (mark-peer-death log peer)
      (>!! evict-ch-head peer)
      (recur))))

(defn planning-ch-loop [log]
  (loop []
    (let [job (<!! planning-ch-head)]
      (plan-job log job)
      (<!! offer-ch-head job)
      (recur))))

(defn ack-ch-loop [log]
  (loop []
    (let [task (<!! ack-ch-head)]
      (acknowledge-task log task)
      (recur))))

(defn evict-ch-loop [log sync]
  (loop []
    (let [task (<!! evict-ch-head)]
      (evict-task log sync task)
      (>!! offer-ch-head task)
      (recur))))

(defn offer-ch-loop [log sync]
  (loop []
    (let [event (<!! offer-ch-head)]
      (when (offer-task log sync
                        #(>!! ack-ch-head %)
                        #(>!! completion-ch-head %))
        (thread (<!! (timeout eviction-delay))
                (>!! evict-ch-head nil)))
      (recur))))

(defn completion-ch-loop [log sync queue]
  (loop []
    (let [task (<!! completion-ch-head)]
      (complete-task log sync queue task)
      (>!! offer-ch-head task)
      (recur))))

(def start-async!
  (memoize
   (fn [log sync queue]
     (tap planning-mult planning-ch-tail)
     (tap born-peer-mult born-peer-ch-tail)
     (tap dead-peer-mult dead-peer-ch-tail)
     (tap evict-mult evict-ch-tail)
     (tap offer-mult offer-ch-tail)
     (tap ack-mult ack-ch-tail)
     (tap completion-mult completion-ch-tail)

     (thread (born-peer-ch-loop log sync))
     (thread (dead-peer-ch-loop log))
     (thread (planning-ch-loop))
     (thread (ack-ch-loop log))
     (thread (evict-ch-loop log sync))
     (thread (offer-ch-loop log sync))
     (thread (completion-ch-loop log sync queue)))))

(start-async! :datomic :zookeeper :hornetq)

