(ns onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread mult tap timeout close! >!! <!!]]
            [com.stuartsierra.component :as component]
            [dire.core :as dire]
            [onyx.extensions :as extensions]
            [onyx.coordinator.planning :as planning]))

(def ch-capacity 10000)

(defn mark-peer-birth [log sync peer death-cb]
  (let [pulse (:pulse (extensions/read-place sync peer))]
    (extensions/on-delete sync pulse death-cb)
    (extensions/mark-peer-born log peer)))

(defn mark-peer-death [log peer]
  (extensions/mark-peer-dead log peer))

(defn plan-job [log queue {:keys [catalog workflow]}]
  (let [tasks (planning/discover-tasks catalog workflow)]
    (doseq [task tasks] (extensions/create-queue queue task))
    (extensions/plan-job log catalog workflow tasks)))

(defn acknowledge-task [log sync ack-place]
  (let [nodes (extensions/node-basis log :node/ack ack-place)]
    (extensions/ack log ack-place)
    (extensions/touch-place sync (:node/status nodes))))

(defn evict-peer [log sync peer]
  (if-let [status-node (:node/status (extensions/nodes log peer))]
    (extensions/delete sync status-node)))

(defn offer-task [log sync ack-cb complete-cb revoke-cb]
  (loop [[task :as tasks] (extensions/next-tasks log)
         [peer :as peers] (extensions/idle-peers log)]
    (when (and (seq tasks) (seq peers))
      (let [payload-node (:payload (extensions/read-place sync peer))
            ack-node (extensions/create sync :ack)
            complete-node (extensions/create sync :completion)
            status-node (extensions/create sync :status)
            nodes {:payload payload-node :ack ack-node
                   :completion complete-node :status status-node}]
        (extensions/on-change sync ack-node ack-cb)
        (extensions/on-change sync complete-node complete-cb)
        (if (extensions/mark-offered log task peer nodes)
          (do (extensions/write-place sync payload-node {:task task :nodes nodes})
              (revoke-cb {:peer-node peer :ack-node ack-node})
              (recur (rest tasks) (rest peers)))
          (recur tasks (rest peers)))))))

(defn revoke-offer [log sync peer-node ack-node evict-cb]
  (when (extensions/revoke-offer log ack-node)
    (evict-cb peer-node)))

(defn complete-task [log sync queue complete-place]
  (let [task (extensions/node->task log :node/completion complete-place)]
    (if-let [result (extensions/complete log complete-place)]
      (do (extensions/delete sync complete-place)
          (extensions/cap-queue queue (:egress-queues task))
          result)
      false)))

(defn shutdown-peer [sync peer]
  (let [shutdown (:shutdown (extensions/read-place sync peer))]
    (extensions/delete sync shutdown)))

(defn born-peer-ch-loop [log sync born-tail offer-head dead-head]
  (loop []
    (when-let [peer (<!! born-tail)]
      (when (mark-peer-birth log sync peer (fn [_] (>!! dead-head peer)))
        (>!! offer-head peer))
      (recur))))

(defn dead-peer-ch-loop [log dead-tail evict-head offer-head]
  (loop []
    (when-let [peer (<!! dead-tail)]
      (when (mark-peer-death log peer)
        (>!! evict-head peer)
        (>!! offer-head peer))
      (recur))))

(defn planning-ch-loop [log queue planning-tail offer-head]
  (loop []
    (when-let [job (<!! planning-tail)]
      (let [job-id (plan-job log queue job)]
        (>!! offer-head job-id)
        (recur)))))

(defn ack-ch-loop [log sync ack-tail]
  (loop []
    (when-let [ack-place (:path (<!! ack-tail))]
      (acknowledge-task log sync ack-place)
      (recur))))

(defn evict-ch-loop [log sync evict-tail offer-head shutdown-head]
  (loop []
    (when-let [peer (<!! evict-tail)]
      (evict-peer log sync peer)
      (>!! shutdown-head peer)
      (>!! offer-head peer)
      (recur))))

(defn offer-ch-loop
  [log sync revoke-delay offer-tail ack-head complete-head revoke-head]
  (loop []
    (when-let [event (<!! offer-tail)]
      (offer-task log sync
                  #(>!! ack-head %)
                  #(>!! complete-head %)
                  #(thread (<!! (timeout revoke-delay))
                           (>!! revoke-head %)))
      (recur))))

(defn offer-revoke-ch-loop [log sync offer-revoke-tail evict-head]
  (loop []
    (when-let [{:keys [peer-node ack-node]} (<!! offer-revoke-tail)]
      (revoke-offer log sync peer-node ack-node
                    #(>!! evict-head %))
      (recur))))

(defn completion-ch-loop
  [log sync queue complete-tail offer-head]
  (loop []
    (when-let [place (:path (<!! complete-tail))]
      (when-let [result (complete-task log sync queue place)]
        (>!! offer-head result))
      (recur))))

(defn shutdown-ch-loop [sync shutdown-tail]
  (loop []
    (when-let [peer (<!! shutdown-tail)]
      (shutdown-peer sync peer)
      (recur))))

(defn failure-ch-loop [failure-tail]
  (loop []
    (when-let [failure (<!! failure-tail)]
      ;; (prn "Failed: " (:ch failure))
      ;; (prn "Details: " (:e failure))
      (recur))))

(defn print-if-not-thread-death [e & _]
  (if-not (instance? java.lang.InterruptedException e)
    (.printStackTrace e)))

(defrecord Coordinator []
  component/Lifecycle

  (start [{:keys [log sync queue revoke-delay] :as component}]
    (prn "Starting Coordinator")
    (let [planning-ch-head (chan ch-capacity)
          born-peer-ch-head (chan ch-capacity)
          dead-peer-ch-head (chan ch-capacity)
          evict-ch-head (chan ch-capacity)
          offer-ch-head (chan ch-capacity)
          offer-revoke-ch-head (chan ch-capacity)
          ack-ch-head (chan ch-capacity)
          completion-ch-head (chan ch-capacity)
          failure-ch-head (chan ch-capacity)
          shutdown-ch-head (chan ch-capacity)

          planning-ch-tail (chan ch-capacity)
          born-peer-ch-tail (chan ch-capacity)
          dead-peer-ch-tail (chan ch-capacity)
          evict-ch-tail (chan ch-capacity)
          offer-ch-tail (chan ch-capacity)
          offer-revoke-ch-tail (chan ch-capacity)
          ack-ch-tail (chan ch-capacity)
          completion-ch-tail (chan ch-capacity)
          failure-ch-tail (chan ch-capacity)
          shutdown-ch-tail (chan ch-capacity)

          planning-mult (mult planning-ch-head)
          born-peer-mult (mult born-peer-ch-head)
          dead-peer-mult (mult dead-peer-ch-head)
          evict-mult (mult evict-ch-head)
          offer-mult (mult offer-ch-head)
          offer-revoke-mult (mult offer-revoke-ch-head)
          ack-mult (mult ack-ch-head)
          completion-mult (mult completion-ch-head)
          failure-mult (mult failure-ch-head)
          shutdown-mult (mult shutdown-ch-head)]
      
      (tap planning-mult planning-ch-tail)
      (tap born-peer-mult born-peer-ch-tail)
      (tap dead-peer-mult dead-peer-ch-tail)
      (tap evict-mult evict-ch-tail)
      (tap offer-mult offer-ch-tail)
      (tap offer-revoke-mult offer-revoke-ch-tail)
      (tap ack-mult ack-ch-tail)
      (tap completion-mult completion-ch-tail)
      (tap failure-mult failure-ch-tail)
      (tap shutdown-mult shutdown-ch-tail)

      (dire/with-handler! #'mark-peer-birth
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :peer-birth :e e})
          false))

      (dire/with-handler! #'mark-peer-death
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :peer-death :e e})
          false))

      (dire/with-handler! #'acknowledge-task
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :ack :e e})
          false))

      (dire/with-handler! #'offer-task
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :offer :e e})
          false))

      (dire/with-handler! #'revoke-offer
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :revoke-offer :e e})
          false))

      (dire/with-handler! #'complete-task
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :complete :e e})
          false))

      (dire/with-handler! #'evict-peer
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :evict :e e})
          false))

      (dire/with-handler! #'shutdown-peer
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :shutdown :e e})
          false))

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

      (dire/with-handler! #'offer-revoke-ch-loop
        java.lang.Exception print-if-not-thread-death)

      (dire/with-handler! #'completion-ch-loop
        java.lang.Exception print-if-not-thread-death)

      (dire/with-handler! #'shutdown-ch-loop
        java.lang.Exception print-if-not-thread-death)

      (assoc component
        :planning-ch-head planning-ch-head
        :born-peer-ch-head born-peer-ch-head
        :dead-peer-ch-head dead-peer-ch-head
        :evict-ch-head evict-ch-head
        :offer-ch-head offer-ch-head
        :offer-revoke-ch-head offer-revoke-ch-head
        :ack-ch-head ack-ch-head
        :completion-ch-head completion-ch-head
        :failure-ch-head failure-ch-head
        :shutdown-ch-head shutdown-ch-head

        :planning-mult planning-mult
        :born-peer-mult born-peer-mult
        :dead-peer-mult dead-peer-mult
        :evict-mult evict-mult
        :offer-mult offer-mult
        :offer-revoke-mult offer-revoke-mult
        :ack-mult ack-mult
        :completion-mult completion-mult
        :failure-mult failure-mult
        :shutdown-mult shutdown-mult

        :born-peer-thread (future (born-peer-ch-loop log sync born-peer-ch-tail offer-ch-head dead-peer-ch-head))
        :dead-peer-thread (future (dead-peer-ch-loop log dead-peer-ch-tail evict-ch-head offer-ch-head))
        :planning-thread (future (planning-ch-loop log queue planning-ch-tail offer-ch-head))
        :ack-thread (future (ack-ch-loop log sync ack-ch-tail))
        :evict-thread (future (evict-ch-loop log sync evict-ch-tail offer-ch-head shutdown-ch-head))
        :offer-thread (future (offer-ch-loop log sync revoke-delay offer-ch-tail ack-ch-head completion-ch-head offer-revoke-ch-head))
        :offer-revoke-thread (future (offer-revoke-ch-loop log sync offer-revoke-ch-tail evict-ch-head))
        :completion-thread (future (completion-ch-loop log sync queue completion-ch-tail offer-ch-head))
        :failure-thread (future (failure-ch-loop failure-ch-tail))
        :shutdown-thread (future (shutdown-ch-loop sync shutdown-ch-tail)))))

  (stop [component]
    (prn "Stopping Coordinator")
    
    (close! (:born-peer-ch-head component))
    (close! (:dead-peer-ch-head component))
    (close! (:planning-ch-head component))
    (close! (:evict-ch-head component))
    (close! (:offer-ch-head component))
    (close! (:offer-revoke-ch-head component))
    (close! (:ack-ch-head component))
    (close! (:completion-ch-head component))
    (close! (:failure-ch-head component))
    (close! (:shutdown-ch-head component))

    (future-cancel (:born-peer-thread component))
    (future-cancel (:dead-peer-thread component))
    (future-cancel (:planning-thread component))
    (future-cancel (:evict-thread component))
    (future-cancel (:offer-thread component))
    (future-cancel (:offer-revoke-thread component))
    (future-cancel (:ack-thread component))
    (future-cancel (:completion-thread component))
    (future-cancel (:failure-thread component))
    (future-cancel (:shutdown-thread component))

    component))

(defn coordinator [revoke-delay]
  (map->Coordinator {:revoke-delay revoke-delay}))

