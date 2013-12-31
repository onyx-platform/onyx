(ns onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread mult tap timeout >!! <!!]]
            [com.stuartsierra.component :as component]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic]
            [onyx.coordinator.sync.zookeeper]))

(def eviction-delay 5000)

(def ch-capacity 1000)

(defn mark-peer-birth [log sync place death-cb]
  (extensions/on-change sync place death-cb)
  (extensions/mark-peer-born log place))

(defn mark-peer-death [log peer]
  (extensions/mark-peer-dead log peer))

(defn plan-job [log job]
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

(defn born-peer-ch-loop [log sync born-tail offer-head dead-head]
  (loop []
    (when-let [place (<!! born-tail)]
      (mark-peer-birth log sync place (fn [_] (>!! dead-head place)))
      (>!! offer-head place)
      (recur))))

(defn dead-peer-ch-loop [log dead-tail evict-head]
  (loop []
    (when-let [peer (<!! dead-tail)]
      (mark-peer-death log peer)
      (>!! evict-head peer)
      (recur))))

(defn planning-ch-loop [log planning-tail offer-head]
  (loop []
    (when-let [job (<!! planning-tail)]
      (plan-job log job)
      (>!! offer-head job)
      (recur))))

(defn ack-ch-loop [log ack-tail]
  (loop []
    (when-let [task (<!! ack-tail)]
      (acknowledge-task log task)
      (recur))))

(defn evict-ch-loop [log sync evict-tail offer-head]
  (loop []
    (when-let [task (<!! evict-tail)]
      (evict-task log sync task)
      (>!! offer-head task)
      (recur))))

(defn offer-ch-loop
  [log sync offer-tail ack-head complete-head evict-head]
  (loop []
    (when-let [event (<!! offer-tail)]
      (when (offer-task log sync
                        #(>!! ack-head %)
                        #(>!! complete-head %))
        (thread (<!! (timeout eviction-delay))
                (>!! evict-head nil)))
      (recur))))

(defn completion-ch-loop
  [log sync queue complete-tail offer-head]
  (loop []
    (when-let [task (<!! complete-tail)]
      (complete-task log sync queue task)
      (>!! offer-head task)
      (recur))))

(defrecord Coordinator [log sync queue]
  component/Lifecycle

  (start [component]
    (let [planning-ch-head (chan ch-capacity)
          born-peer-ch-head (chan ch-capacity)
          dead-peer-ch-head (chan ch-capacity)
          evict-ch-head (chan ch-capacity)
          offer-ch-head (chan ch-capacity)
          ack-ch-head (chan ch-capacity)
          completion-ch-head (chan ch-capacity)

          planning-ch-tail (chan ch-capacity)
          born-peer-ch-tail (chan ch-capacity)
          dead-peer-ch-tail (chan ch-capacity)
          evict-ch-tail (chan ch-capacity)
          offer-ch-tail (chan ch-capacity)
          ack-ch-tail (chan ch-capacity)
          completion-ch-tail (chan ch-capacity)

          planning-mult (mult planning-ch-head)
          born-peer-mult (mult born-peer-ch-head)
          dead-peer-mult (mult dead-peer-ch-head)
          evict-mult (mult evict-ch-head)
          offer-mult (mult offer-ch-head)
          ack-mult (mult ack-ch-head)
          completion-mult (mult completion-ch-head)]
      
      (tap planning-mult planning-ch-tail)
      (tap born-peer-mult born-peer-ch-tail)
      (tap dead-peer-mult dead-peer-ch-tail)
      (tap evict-mult evict-ch-tail)
      (tap offer-mult offer-ch-tail)
      (tap ack-mult ack-ch-tail)
      (tap completion-mult completion-ch-tail)

      (thread (born-peer-ch-loop log sync born-peer-ch-tail offer-ch-head dead-peer-ch-head))
      (thread (dead-peer-ch-loop log dead-peer-ch-tail evict-ch-head))
      (thread (planning-ch-loop log planning-ch-tail offer-ch-head))
      (thread (ack-ch-loop log ack-ch-tail))
      (thread (evict-ch-loop log sync evict-ch-tail offer-ch-head))
      (thread (offer-ch-loop log sync offer-ch-tail ack-ch-head completion-ch-head evict-ch-head))
      (thread (completion-ch-loop log sync queue completion-ch-tail offer-ch-head))

      (assoc component
        :planning-ch-head planning-ch-head
        :born-peer-ch-head born-peer-ch-head
        :dead-peer-ch-head dead-peer-ch-head
        :evict-ch-head evict-ch-head
        :offer-ch-head offer-ch-head
        :ack-ch-head ack-ch-head
        :completion-ch-head completion-ch-head

        :planning-mult planning-mult
        :born-peer-mult born-peer-mult
        :dead-peer-mult dead-peer-mult
        :evict-mult evict-mult
        :offer-mult offer-mult
        :ack-mult ack-mult
        :completion-mult completion-mult)))

  (stop [component]
    (clojure.core.async/close! (:planning-ch-head component))
    (clojure.core.async/close! (:born-peer-ch-head component))
    (clojure.core.async/close! (:dead-peer-ch-head component))
    (clojure.core.async/close! (:evict-ch-head component))
    (clojure.core.async/close! (:offer-ch-head component))
    (clojure.core.async/close! (:ack-ch-head component))
    (clojure.core.async/close! (:completion-ch-head component))
    
    component))

(defn coordinator [log sync queue]
  (map->Coordinator {:log log :sync sync :queue queue}))

(def components [:coordinator])

(defrecord OnyxSystem [log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this components))
  (stop [this]
    (component/stop-system this components)))

(defn onyx-system [{:keys [log sync queue]}]
  (map->OnyxSystem
   {:coordinator (Coordinator. log sync queue)
    :log log
    :sync sync
    :queue queue}))

