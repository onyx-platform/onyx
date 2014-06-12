(ns ^:no-doc onyx.coordinator.async
  (:require [clojure.core.async :refer [chan thread mult tap timeout close! >!! <!!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info]]
            [dire.core :as dire]
            [onyx.extensions :as extensions]
            [onyx.coordinator.planning :as planning]
            [onyx.coordinator.impl]))

(def ch-capacity 10000)

(defn mark-peer-birth [sync peer death-cb]
  (let [pulse (:pulse (extensions/read-place sync peer))]
    (extensions/on-delete sync pulse death-cb)
    (extensions/mark-peer-born sync peer)))

(defn mark-peer-death [sync pulse]
  (extensions/mark-peer-dead sync pulse))

(defn plan-job [sync queue {:keys [catalog workflow]}]
  (let [tasks (planning/discover-tasks catalog workflow)]
    (extensions/plan-job sync catalog workflow tasks)

    (doseq [task tasks]
      (extensions/create-queue queue task))

    (doseq [task tasks]
      (let [task-map (planning/find-task catalog (:name task))]
        (when (:onyx/bootstrap? task-map)
          (extensions/bootstrap-queue queue task))))))

(defn acknowledge-task [sync ack-place]
  (let [nodes (extensions/nodes sync ack-place)]
    (when (extensions/ack sync ack-place)
      (extensions/touch-place sync (:node/status nodes)))))

(defn evict-peer [sync peer]
  (if-let [status-node (:node/status (extensions/nodes sync peer))]
    (extensions/delete sync status-node)))

(defn offer-task [sync ack-cb exhaust-cb complete-cb revoke-cb]
  (loop [[task :as tasks] (extensions/next-tasks sync)
         [peer :as peers] (extensions/idle-peers sync)]
    (when (and (seq tasks) (seq peers))
      (let [id (:id peer)
            task-attrs (dissoc task :workflow :catalog)
            payload-node (:payload (extensions/read-place sync peer))
            ack-node (extensions/create sync :ack)
            exhaust-node (extensions/create sync :exhaust)
            seal-node (extensions/create sync :seal)
            complete-node (extensions/create sync :completion)
            status-node (extensions/create sync :status)
            catalog-node (extensions/create sync :catalog)
            workflow-node (extensions/create sync :workflow)
            nodes {:node/peer peer
                   :node/payload payload-node
                   :node/ack ack-node
                   :node/exhaust exhaust-node
                   :node/seal seal-node
                   :node/completion complete-node
                   :node/status status-node
                   :node/catalog catalog-node
                   :node/workflow workflow-node}]

        (extensions/write-place sync ack-node {:id id})
        (extensions/write-place sync exhaust-node {:id id})
        (extensions/write-place sync seal-node {:id id})
        (extensions/write-place sync complete-node {:id id})
        (extensions/write-place sync status-node {:id id})

        (extensions/write-place sync catalog-node (:catalog task))
        (extensions/write-place sync workflow-node (:workflow task))
        
        (extensions/on-change sync ack-node ack-cb)
        (extensions/on-change sync exhaust-node exhaust-cb)
        (extensions/on-change sync complete-node complete-cb)
        
        (if (extensions/mark-offered sync task peer nodes)
          (do (extensions/write-place sync payload-node {:task task-attrs :nodes nodes})
              (revoke-cb {:peer-node peer :ack-node ack-node})
              (recur (rest tasks) (rest peers)))
          (recur tasks (rest peers)))))))

(defn revoke-offer [sync peer-node ack-node evict-cb]
  (when (extensions/revoke-offer sync ack-node)
    (evict-cb peer-node)))

(defn exhaust-queue [sync exhaust-place]
  (extensions/seal-resource? sync exhaust-place))

(defn seal-resource [sync seal? seal-place]
  (extensions/write-place sync seal-place seal?))

(defn complete-task [sync complete-place]
  (if-let [result (extensions/complete sync complete-place)]
    (when (= (:n-peers result) 1)
      (extensions/delete sync complete-place)
      result)
    false))

(defn shutdown-peer [sync peer]
  (let [shutdown (:shutdown (extensions/read-place sync peer))]
    (extensions/delete sync shutdown)))

(defn born-peer-ch-loop [sync born-tail offer-head dead-head]
  (loop []
    (when-let [peer (<!! born-tail)]
      (when (mark-peer-birth sync peer (fn [_] (>!! dead-head peer)))
        (>!! offer-head peer))
      (recur))))

(defn dead-peer-ch-loop [sync dead-tail evict-head offer-head]
  (loop []
    (when-let [pulse (<!! dead-tail)]
      (when (mark-peer-death sync pulse)
        (>!! evict-head pulse)
        (>!! offer-head pulse))
      (recur))))

(defn planning-ch-loop [sync queue planning-tail offer-head]
  (loop []
    (when-let [job (<!! planning-tail)]
      (let [job-id (plan-job sync queue job)]
        (>!! offer-head job-id)
        (recur)))))

(defn ack-ch-loop [sync ack-tail]
  (loop []
    (when-let [ack-place (:path (<!! ack-tail))]
      (acknowledge-task sync ack-place)
      (recur))))

(defn evict-ch-loop [sync evict-tail offer-head shutdown-head]
  (loop []
    (when-let [peer (<!! evict-tail)]
      (evict-peer sync peer)
      (>!! shutdown-head peer)
      (>!! offer-head peer)
      (recur))))

(defn offer-ch-loop
  [sync revoke-delay offer-tail ack-head exhaust-head complete-head revoke-head]
  (loop []
    (when-let [event (<!! offer-tail)]
      (offer-task sync
                  #(>!! ack-head %)
                  #(>!! exhaust-head %)
                  #(>!! complete-head %)
                  #(thread (<!! (timeout revoke-delay))
                           (>!! revoke-head %)))
      (recur))))

(defn offer-revoke-ch-loop [sync offer-revoke-tail evict-head]
  (loop []
    (when-let [{:keys [peer-node ack-node]} (<!! offer-revoke-tail)]
      (revoke-offer sync peer-node ack-node
                    #(>!! evict-head %))
      (recur))))

(defn exhaust-queue-loop [sync exhaust-tail seal-head]
  (loop []
    (when-let [place (:path (<!! exhaust-tail))]
      (when-let [result (exhaust-queue sync place)]
        (>!! seal-head result))
      (recur))))

(defn seal-resource-loop [sync seal-tail]
  (loop []
    (when-let [result (<!! seal-tail)]
      (seal-resource sync (:seal? result) (:seal-node result))
      (recur))))

(defn completion-ch-loop
  [sync complete-tail offer-head]
  (loop []
    (when-let [place (:path (<!! complete-tail))]
      (when-let [result (complete-task sync place)]
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
      (info (:e failure) (:ch failure))
      (recur))))

(defn log-if-not-interrupted [e & _]
  (if-not (instance? java.lang.InterruptedException e)
    (info e)))

(defrecord Coordinator []
  component/Lifecycle

  (start [{:keys [sync queue revoke-delay] :as component}]
    (info "Starting Coordinator")
    (let [planning-ch-head (chan ch-capacity)
          born-peer-ch-head (chan ch-capacity)
          dead-peer-ch-head (chan ch-capacity)
          evict-ch-head (chan ch-capacity)
          offer-ch-head (chan ch-capacity)
          offer-revoke-ch-head (chan ch-capacity)
          ack-ch-head (chan ch-capacity)
          exhaust-ch-head (chan ch-capacity)
          seal-ch-head (chan ch-capacity)
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
          exhaust-ch-tail (chan ch-capacity)
          seal-ch-tail (chan ch-capacity)
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
          exhaust-mult (mult exhaust-ch-head)
          seal-mult (mult seal-ch-head)
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
      (tap exhaust-mult exhaust-ch-tail)
      (tap seal-mult seal-ch-tail)
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

      (dire/with-handler! #'exhaust-queue
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :exhaust-queue :e e})))

      (dire/with-handler! #'seal-resource
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :seal-resource :e e})))

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
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'dead-peer-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'planning-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'ack-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'evict-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'offer-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'offer-revoke-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'exhaust-queue-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'seal-resource-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'completion-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (dire/with-handler! #'shutdown-ch-loop
        java.lang.Exception log-if-not-interrupted)

      (assoc component
        :planning-ch-head planning-ch-head
        :born-peer-ch-head born-peer-ch-head
        :dead-peer-ch-head dead-peer-ch-head
        :evict-ch-head evict-ch-head
        :offer-ch-head offer-ch-head
        :offer-revoke-ch-head offer-revoke-ch-head
        :ack-ch-head ack-ch-head
        :exhaust-ch-head exhaust-ch-head
        :seal-ch-head seal-ch-head
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
        :exhaust-mult exhaust-mult
        :seal-mult seal-mult
        :completion-mult completion-mult
        :failure-mult failure-mult
        :shutdown-mult shutdown-mult

        :born-peer-thread (thread (born-peer-ch-loop sync born-peer-ch-tail offer-ch-head dead-peer-ch-head))
        :dead-peer-thread (thread (dead-peer-ch-loop sync dead-peer-ch-tail evict-ch-head offer-ch-head))
        :planning-thread (thread (planning-ch-loop sync queue planning-ch-tail offer-ch-head))
        :ack-thread (thread (ack-ch-loop sync ack-ch-tail))
        :evict-thread (thread (evict-ch-loop sync evict-ch-tail offer-ch-head shutdown-ch-head))
        :offer-revoke-thread (thread (offer-revoke-ch-loop sync offer-revoke-ch-tail evict-ch-head))
        :exhaust-thread (thread (exhaust-queue-loop sync exhaust-ch-tail seal-ch-head))
        :seal-thread (thread (seal-resource-loop sync seal-ch-tail))
        :completion-thread (thread (completion-ch-loop sync completion-ch-tail offer-ch-head))
        :failure-thread (thread (failure-ch-loop failure-ch-tail))
        :shutdown-thread (thread (shutdown-ch-loop sync shutdown-ch-tail))
        :offer-thread (thread (offer-ch-loop sync revoke-delay offer-ch-tail ack-ch-head
                                             exhaust-ch-head completion-ch-head offer-revoke-ch-head)))))

  (stop [component]
    (info "Stopping Coordinator")
    
    (close! (:born-peer-ch-head component))
    (close! (:dead-peer-ch-head component))
    (close! (:planning-ch-head component))
    (close! (:evict-ch-head component))
    (close! (:offer-ch-head component))
    (close! (:offer-revoke-ch-head component))
    (close! (:ack-ch-head component))
    (close! (:exhaust-ch-head component))
    (close! (:seal-ch-head component))
    (close! (:completion-ch-head component))
    (close! (:failure-ch-head component))
    (close! (:shutdown-ch-head component))

    component))

(defn coordinator [revoke-delay]
  (map->Coordinator {:revoke-delay revoke-delay}))

