(ns onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread mult tap timeout close! >!! <!!]]
            [com.stuartsierra.component :as component]
            [dire.core :as dire]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.planning :as planning]))

(def ch-capacity 1000)

(defn mark-peer-birth [log sync place death-cb]
  (extensions/on-change sync place death-cb)
  (extensions/mark-peer-born log place))

(defn mark-peer-death [log peer]
  (extensions/mark-peer-dead log peer))

(defn plan-job [log {:keys [catalog workflow]}]
  (let [tasks (planning/discover-tasks catalog workflow)]
    (extensions/plan-job log catalog workflow tasks)))

(defn acknowledge-task [log task]
  (extensions/ack log task))

(defn evict-task [log sync task]
  (extensions/delete sync task)
  (extensions/evict log task))

(defn offer-task [log sync ack-cb complete-cb]
  (if-let [task (extensions/next-task log)]
    (if-let [peer (extensions/idle-peer log)]
      (let [payload-node (extensions/read-place sync peer)
            ack-node (extensions/create sync :ack)
            complete-node (extensions/create sync :completion)
            status-node (extensions/create sync :status)
            nodes {:payload payload-node :ack ack-node
                   :completion complete-node :status status-node}]
        (extensions/on-change sync ack-node ack-cb)
        (extensions/on-change sync complete-node complete-cb)
        (if (extensions/mark-offered log task peer nodes)
          (extensions/write-place sync payload-node {:task task :nodes nodes})
          false))
      false)
    false))

(defn complete-task [log sync queue complete-place]
  (if (extensions/complete log complete-place)
    (do (extensions/delete sync complete-place)
        (extensions/cap-queue queue complete-place))
    false))

(defn born-peer-ch-loop [log sync born-tail offer-head dead-head]
  (loop []
    (when-let [place (<!! born-tail)]
      (when (mark-peer-birth log sync place (fn [_] (>!! dead-head place)))
        (>!! offer-head place))
      (recur))))

(defn dead-peer-ch-loop [log dead-tail evict-head]
  (loop []
    (when-let [peer (<!! dead-tail)]
      (when (mark-peer-death log peer)
        (>!! evict-head peer))
      (recur))))

(defn planning-ch-loop [log planning-tail offer-head]
  (loop []
    (when-let [job (<!! planning-tail)]
      (let [job-id (plan-job log job)]
        (>!! offer-head job-id)
        (recur)))))

(defn ack-ch-loop [log ack-tail]
  (loop []
    (when-let [ack-place (:path (<!! ack-tail))]
      (acknowledge-task log ack-place)
      (recur))))

(defn evict-ch-loop [log sync evict-tail offer-head]
  (loop []
    (when-let [peer (<!! evict-tail)]
      (when (evict-task log sync peer)
        (>!! offer-head peer))
      (recur))))

(defn offer-ch-loop
  [log sync eviction-delay offer-tail ack-head complete-head evict-head]
  (loop []
    (when-let [event (<!! offer-tail)]
      (when-let [peer (offer-task log sync
                                  #(>!! ack-head %)
                                  #(>!! complete-head %))]
        (thread (<!! (timeout eviction-delay))
                (>!! evict-head peer)))
      (recur))))

(defn completion-ch-loop
  [log sync queue complete-tail offer-head]
  (loop []
    (when-let [place (:path (<!! complete-tail))]
      (when (complete-task log sync queue place)
        (>!! offer-head place))
      (recur))))

(defn failure-ch-loop [failure-tail]
  (loop []
    (when-let [failure (<!! failure-tail)]
      (prn "Failed: " (:ch failure))
      (prn "Details: " (:e failure))
      (recur))))

(defn print-if-not-thread-death [e & _]
  (if-not (instance? java.lang.InterruptedException e)
    (.printStackTrace e)))

(dire/with-handler! #'born-peer-ch-loop
  java.lang.Exception print-if-not-thread-death)

(dire/with-handler! #'dead-peer-ch-loop
  java.lang.Exception print-if-not-thread-death)

(dire/with-handler! #'planning-ch-loop
  java.lang.Exception print-if-not-thread-death)

(dire/with-handler! #'ack-ch-loop
  java.lang.Exception print-if-not-thread-death)

(dire/with-handler! #'evict-ch-loop
  java.lang.Exception print-if-not-thread-death)

(dire/with-handler! #'offer-ch-loop
  java.lang.Exception print-if-not-thread-death)

(dire/with-handler! #'completion-ch-loop
  java.lang.Exception print-if-not-thread-death)

(defrecord Coordinator []
  component/Lifecycle

  (start [{:keys [log sync queue eviction-delay] :as component}]
    (prn "Starting Coordinator")
    (let [planning-ch-head (chan ch-capacity)
          born-peer-ch-head (chan ch-capacity)
          dead-peer-ch-head (chan ch-capacity)
          evict-ch-head (chan ch-capacity)
          offer-ch-head (chan ch-capacity)
          ack-ch-head (chan ch-capacity)
          completion-ch-head (chan ch-capacity)
          failure-ch-head (chan ch-capacity)

          planning-ch-tail (chan ch-capacity)
          born-peer-ch-tail (chan ch-capacity)
          dead-peer-ch-tail (chan ch-capacity)
          evict-ch-tail (chan ch-capacity)
          offer-ch-tail (chan ch-capacity)
          ack-ch-tail (chan ch-capacity)
          completion-ch-tail (chan ch-capacity)
          failure-ch-tail (chan ch-capacity)

          planning-mult (mult planning-ch-head)
          born-peer-mult (mult born-peer-ch-head)
          dead-peer-mult (mult dead-peer-ch-head)
          evict-mult (mult evict-ch-head)
          offer-mult (mult offer-ch-head)
          ack-mult (mult ack-ch-head)
          completion-mult (mult completion-ch-head)
          failure-mult (mult failure-ch-head)]
      
      (tap planning-mult planning-ch-tail)
      (tap born-peer-mult born-peer-ch-tail)
      (tap dead-peer-mult dead-peer-ch-tail)
      (tap evict-mult evict-ch-tail)
      (tap offer-mult offer-ch-tail)
      (tap ack-mult ack-ch-tail)
      (tap completion-mult completion-ch-tail)
      (tap failure-mult failure-ch-tail)

      (dire/with-handler! #'mark-peer-birth
        java.util.concurrent.ExecutionException
        (fn [e log sync place death-cb]
          (>!! failure-ch-head {:ch :peer-birth :e e})
          false))

      (dire/with-handler! #'mark-peer-death
        java.util.concurrent.ExecutionException
        (fn [e log peer]
          (>!! failure-ch-head {:ch :peer-death :e e})
          false))

      (dire/with-handler! #'acknowledge-task
        java.util.concurrent.ExecutionException
        (fn [e log task]
          (>!! failure-ch-head {:ch :ack :e e})
          false))

      (dire/with-handler! #'offer-task
        java.util.concurrent.ExecutionException
        (fn [e log sync ack-cb complete-cb]
          (>!! failure-ch-head {:ch :offer :e e})
          false))

      (dire/with-handler! #'complete-task
        java.util.concurrent.ExecutionException
        (fn [e log sync queue complete-place]
          (>!! failure-ch-head {:ch :complete :e e})
          false))

      (assoc component
        :planning-ch-head planning-ch-head
        :born-peer-ch-head born-peer-ch-head
        :dead-peer-ch-head dead-peer-ch-head
        :evict-ch-head evict-ch-head
        :offer-ch-head offer-ch-head
        :ack-ch-head ack-ch-head
        :completion-ch-head completion-ch-head
        :failure-ch-head failure-ch-head

        :planning-mult planning-mult
        :born-peer-mult born-peer-mult
        :dead-peer-mult dead-peer-mult
        :evict-mult evict-mult
        :offer-mult offer-mult
        :ack-mult ack-mult
        :completion-mult completion-mult
        :failure-mult failure-mult

        :born-peer-thread (future (born-peer-ch-loop log sync born-peer-ch-tail offer-ch-head dead-peer-ch-head))
        :dead-peer-thread (future (dead-peer-ch-loop log dead-peer-ch-tail evict-ch-head))
        :planning-thread (future (planning-ch-loop log planning-ch-tail offer-ch-head))
        :ack-thread (future (ack-ch-loop log ack-ch-tail))
        :evict-thread (future (evict-ch-loop log sync evict-ch-tail offer-ch-head))
        :offer-thread (future (offer-ch-loop log sync eviction-delay offer-ch-tail ack-ch-head completion-ch-head evict-ch-head))
        :completion-thread (future (completion-ch-loop log sync queue completion-ch-tail offer-ch-head))
        :failure-thread (future (failure-ch-loop failure-ch-tail)))))

  (stop [component]
    (prn "Stopping Coordinator")
    
    (close! (:born-peer-ch-head component))
    (close! (:dead-peer-ch-head component))
    (close! (:planning-ch-head component))
    (close! (:evict-ch-head component))
    (close! (:offer-ch-head component))
    (close! (:ack-ch-head component))
    (close! (:completion-ch-head component))
    (close! (:failure-ch-head component))

    (future-cancel (:born-peer-thread component))
    (future-cancel (:dead-peer-thread component))
    (future-cancel (:planning-thread component))
    (future-cancel (:evict-thread component))
    (future-cancel (:offer-thread component))
    (future-cancel (:ack-thread component))
    (future-cancel (:completion-thread component))
    (future-cancel (:failure-thread component))

    component))

(defn coordinator [eviction-delay]
  (map->Coordinator {:eviction-delay eviction-delay}))

