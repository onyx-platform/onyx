(ns ^:no-doc onyx.coordinator.async
    (:require [clojure.core.async :refer [chan thread mult tap timeout close! >!! <!!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info warn]]
              [dire.core :as dire]
              [onyx.extensions :as extensions]
              [onyx.coordinator.planning :as planning]
              [onyx.coordinator.repair :as r]
              [onyx.coordinator.impl]))

(def ch-capacity 10000)

(defn serialize [ch f & args]
  (let [p (promise)]
    (>!! ch [p f args])
    @p))

(defn apply-serial-fn [f args]
  (apply f args))

(defn mark-peer-birth [sync sync-ch peer death-cb]
  (let [pulse (:pulse-node (extensions/read-node sync peer))]
    (extensions/on-delete sync pulse death-cb)
    (serialize sync-ch extensions/mark-peer-born sync peer)))

(defn mark-peer-death [sync sync-ch peer-node]
  (serialize sync-ch extensions/mark-peer-dead sync peer-node))

(defn plan-job [sync queue {:keys [catalog workflow]}]
  (let [job-id (java.util.UUID/randomUUID)
        tasks (planning/discover-tasks catalog workflow)]

    (doseq [task tasks]
      (extensions/create-queue queue task))

    (doseq [task tasks]
      (let [task-map (planning/find-task catalog (:name task))]
        (when (:onyx/bootstrap? task-map)
          (extensions/bootstrap-queue queue task))))

    (extensions/plan-job sync job-id tasks catalog workflow)
    job-id))

(defn acknowledge-task [sync sync-ch ack-node]
  (let [nodes (:nodes (extensions/read-node sync ack-node))]
    (serialize
     sync-ch
     #(when (extensions/ack sync ack-node)
        (extensions/touch-node sync (:node/status nodes))))))

(defn evict-peer [sync sync-ch peer-node]
  (let [node-data (extensions/read-node sync peer-node)
        state-path (extensions/resolve-node sync :peer-state (:id node-data))]
    (serialize
     sync-ch
     #(let [peer-state (:content (extensions/dereference sync state-path))]
        (if-let [status-node (:node/status (:nodes peer-state))]
          (extensions/delete sync status-node))))))

(defn offer-task [sync sync-ch ack-cb exhaust-cb complete-cb revoke-cb]
  (loop [[task-node :as task-nodes] (or (serialize sync-ch #(extensions/next-tasks sync)) [])
         [peer :as peers] (or (serialize sync-ch #(extensions/idle-peers sync)) [])]
    (when (and (seq task-nodes) (seq peers))
      (let [peer-node (:node peer)
            peer-content (:content peer)
            payload-node (:payload-node (extensions/read-node sync (:peer-node peer-content)))
            task (extensions/read-node sync task-node)
            ack (extensions/create sync :ack)
            exhaust (extensions/create sync :exhaust)
            seal (extensions/create sync :seal)
            complete (extensions/create sync :completion)
            cooldown (extensions/create sync :cooldown)
            status (extensions/create sync :status)
            nodes {:node/peer (:peer-node peer-content)
                   :node/payload payload-node
                   :node/ack (:node ack)
                   :node/exhaust (:node exhaust)
                   :node/seal (:node seal)
                   :node/completion (:node complete)
                   :node/cooldown (:node cooldown)
                   :node/status (:node status)
                   :node/catalog (:task/catalog-node task)
                   :node/workflow (:task/workflow-node task)
                   :node/task task-node}
            snapshot {:id (:id peer-content) :peer-node peer-node
                      :task-node task-node :nodes nodes}]

        (extensions/write-node sync (:node ack) snapshot)
        (extensions/write-node sync (:node exhaust) snapshot)
        (extensions/write-node sync (:node seal) snapshot)
        (extensions/write-node sync (:node complete) snapshot)
        (extensions/write-node sync (:node status) snapshot)

        (extensions/on-change sync (:node ack) ack-cb)
        (extensions/on-change sync (:node exhaust) exhaust-cb)
        (extensions/on-change sync (:node complete) complete-cb)
        
        (if (serialize sync-ch #(extensions/mark-offered sync task-node (:id peer-content) nodes))
          (let [node (extensions/resolve-node sync :peer (:id peer-content))]
            (extensions/write-node sync payload-node {:task task :nodes nodes})
            (revoke-cb {:peer-node (:peer-node peer-content) :ack-node (:node ack)})
            (recur (rest task-nodes) (rest peers)))
          (recur task-nodes (rest peers)))))))

(defn revoke-offer [sync sync-ch peer-node ack-node evict-cb]
  (serialize
   sync-ch
   #(when (extensions/revoke-offer sync ack-node)
      (extensions/create sync :evict-log peer-node)
      (evict-cb ack-node))))

(defn exhaust-queue [sync sync-ch exhaust-node]
  (serialize
   sync-ch
   #(extensions/seal-resource? sync exhaust-node)))

(defn seal-resource [sync seal? seal-node exhaust-node exhaust-head]
  (when-not seal?
    (extensions/on-change sync exhaust-node #(>!! exhaust-head %)))
  (extensions/write-node sync seal-node seal?))

(defn complete-task [sync sync-ch complete-node cb]
  (serialize
   sync-ch
   #(let [nodes (:nodes (extensions/read-node sync complete-node))
          cooldown-node (:node/cooldown nodes)]
      (if-let [result (extensions/complete sync complete-node cooldown-node cb)]
        (when (<= (:n-peers result) 1)
          result)
        false))))

(defn shutdown-peer [sync peer]
  (let [shutdown (:shutdown (extensions/read-node sync peer))]
    (extensions/delete sync shutdown)))

(defn born-peer-ch-loop [sync sync-ch born-tail offer-head dead-head]
  (loop []
    (when (<!! born-tail)
      (let [offset (extensions/next-offset sync :born-log)]
        (when-let [peer (extensions/log-entry-at sync :born-log offset)]
          (when (mark-peer-birth sync sync-ch peer (fn [_] (>!! dead-head peer)))
            (extensions/create sync :offer-log peer)
            (extensions/checkpoint sync :born-log offset)
            (>!! offer-head peer)))
        (recur)))))

(defn dead-peer-ch-loop [sync sync-ch dead-tail evict-head offer-head]
  (loop []
    (when-let [peer-node (<!! dead-tail)]
      (when (mark-peer-death sync sync-ch peer-node)
        (extensions/create sync :evict-log peer-node)
        (extensions/create sync :offer-log peer-node)
        (>!! evict-head peer-node)
        (>!! offer-head peer-node))
      (recur))))

(defn planning-ch-loop [sync queue planning-tail offer-head]
  (loop []
    (when (<!! planning-tail)
      (let [offset (extensions/next-offset sync :planning-log)]
        (when-let [{:keys [job node]}
                   (extensions/log-entry-at sync :planning-log offset)]
          (let [job-id (plan-job sync queue job)]
            (extensions/write-node sync node job-id)
            (extensions/create sync :offer-log job-id)
            (extensions/checkpoint sync :planning-log offset)
            (>!! offer-head job-id)))
        (recur)))))

(defn ack-ch-loop [sync sync-ch ack-tail]
  (loop []
    (when-let [ack-node (:path (<!! ack-tail))]
      (acknowledge-task sync sync-ch ack-node)
      (recur))))

(defn evict-ch-loop [sync sync-ch evict-tail offer-head shutdown-head]
  (loop []
    (when (<!! evict-tail)
      (let [offset (extensions/next-offset sync :evict-log)]
        (when-let [node (extensions/log-entry-at sync :evict-log offset)]
          (evict-peer sync sync-ch node)
          (extensions/create sync :shutdown-log node)
          (extensions/create sync :offer-log node)
          (extensions/checkpoint sync :evict-log offset)
          (>!! shutdown-head node)
          (>!! offer-head node)))
      (recur))))

(defn offer-ch-loop
  [sync sync-ch revoke-delay offer-tail ack-head exhaust-head
   complete-head revoke-head]
  (loop []
    (when (<!! offer-tail)
      (let [offset (extensions/next-offset sync :offer-log)]
        (offer-task sync sync-ch
                    #(>!! ack-head %)
                    #(>!! exhaust-head %)
                    #(>!! complete-head %)
                    #(thread (<!! (timeout revoke-delay))
                             (extensions/create sync :revoke-log %)
                             (>!! revoke-head %)))
        (extensions/checkpoint sync :offer-log offset)
        (recur)))))

(defn offer-revoke-ch-loop [sync sync-ch offer-revoke-tail evict-head]
  (loop []
    (when (<!! offer-revoke-tail)
      (let [offset (extensions/next-offset sync :revoke-log)]
        (when-let [{:keys [peer-node ack-node]}
                   (extensions/log-entry-at sync :revoke-log offset)]
          (revoke-offer sync sync-ch peer-node ack-node #(>!! evict-head %))
          (extensions/checkpoint sync :revoke-log offset))
        (recur)))))

(defn exhaust-queue-loop [sync sync-ch exhaust-tail seal-head]
  (loop []
    (when-let [node (:path (<!! exhaust-tail))]
      (when-let [result (exhaust-queue sync sync-ch node)]
        (extensions/create sync :seal-log result)
        (>!! seal-head result))
      (recur))))

(defn seal-resource-loop [sync seal-tail exhaust-head]
  (loop []
    (when (<!! seal-tail)
      (let [offset (extensions/next-offset sync :seal-log)]
        (when-let [result (extensions/log-entry-at sync :seal-log offset)]
          (seal-resource
           sync
           (:seal? result) (:seal-node result)
           (:exhaust-node result) exhaust-head)
          (extensions/checkpoint sync :seal-log offset))
        (recur)))))

(defn completion-ch-loop
  [sync sync-ch complete-tail offer-head complete-head]
  (loop []
    (when-let [node (:path (<!! complete-tail))]
      (when-let [result (complete-task sync sync-ch node #(>!! complete-head %))]
        (>!! offer-head result))
      (recur))))

(defn shutdown-ch-loop [sync shutdown-tail]
  (loop []
    (when-let [peer (<!! shutdown-tail)]
      (shutdown-peer sync peer)
      (recur))))

(defn sync-ch-loop [sync sync-ch failure-ch]
  (loop []
    (when-let [[p f args] (<!! sync-ch)]
      (try
        (deliver p (apply-serial-fn f args))
        (catch Exception e
          (>!! failure-ch {:ch sync-ch :e e})
          (deliver p nil)))
      (recur))))

(defn failure-ch-loop [failure-tail]
  (loop []
    (when-let [failure (<!! failure-tail)]
      (warn (:e failure) (:ch failure))
      (recur))))

(defrecord Coordinator []
  component/Lifecycle

  (start [{:keys [sync queue revoke-delay] :as component}]
    (info "Starting Coordinator")
    
    (let [sync-ch (chan 0)

          planning-ch-head (chan ch-capacity)
          born-peer-ch-head (chan ch-capacity)
          dead-peer-ch-head (chan ch-capacity)
          evict-ch-head (chan ch-capacity)
          offer-ch-head (chan ch-capacity)
          offer-revoke-ch-head (chan ch-capacity)
          ack-ch-head (chan ch-capacity)
          exhaust-ch-head (chan ch-capacity)
          seal-ch-head (chan ch-capacity)
          completion-ch-head (chan ch-capacity)
          shutdown-ch-head (chan ch-capacity)
          failure-ch-head (chan ch-capacity)

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
          shutdown-ch-tail (chan ch-capacity)
          failure-ch-tail (chan ch-capacity)

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
          shutdown-mult (mult shutdown-ch-head)
          failure-mult (mult failure-ch-head)]
      
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
      (tap shutdown-mult shutdown-ch-tail)
      (tap failure-mult failure-ch-tail)

      (dire/with-handler! #'apply-serial-fn
        java.lang.Exception
        (fn [e & _]
          (>!! failure-ch-head {:ch :serial-fn :e e})
          false))
      
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

      (dire/with-handler! #'sync-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'born-peer-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'dead-peer-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'planning-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'ack-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'evict-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'offer-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'offer-revoke-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'exhaust-queue-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'seal-resource-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'completion-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (dire/with-handler! #'shutdown-ch-loop
        java.lang.Exception (fn [e & _] (warn e)))

      (try
        (r/repair-planning-messages! sync #(>!! planning-ch-head %))
        (r/repair-birth-messages! sync #(>!! born-peer-ch-head %))
        (r/repair-evict-messages! sync #(>!! evict-ch-head %))
        (r/repair-offer-messages! sync #(>!! offer-ch-head %))
        (r/repair-revoke-messages! sync #(>!! offer-revoke-ch-head %))
        (r/repair-ack-messages! sync #(>!! ack-ch-head %))
        (r/repair-exhaust-messages! sync #(>!! exhaust-ch-head %))
        (r/repair-seal-messages! sync #(>!! seal-ch-head %))
        (r/repair-completion-messages! sync #(>!! completion-ch-head %))
        (r/repair-shutdown-messages! sync #(>!! shutdown-ch-head %))
        (catch Exception e
          (warn e "Failure in repairing")))

      (assoc component
        :sync-ch sync-ch
        
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
        :shutdown-ch-head shutdown-ch-head
        :failure-ch-head failure-ch-head

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
        :shutdown-mult shutdown-mult
        :failure-mult failure-mult

        :sync-thread (thread (sync-ch-loop sync sync-ch failure-ch-head))
        :born-peer-thread (thread (born-peer-ch-loop sync sync-ch born-peer-ch-tail offer-ch-head dead-peer-ch-head))
        :dead-peer-thread (thread (dead-peer-ch-loop sync sync-ch dead-peer-ch-tail evict-ch-head offer-ch-head))
        :planning-thread (thread (planning-ch-loop sync queue planning-ch-tail offer-ch-head))
        :ack-thread (thread (ack-ch-loop sync sync-ch ack-ch-tail))
        :evict-thread (thread (evict-ch-loop sync sync-ch evict-ch-tail offer-ch-head shutdown-ch-head))
        :offer-revoke-thread (thread (offer-revoke-ch-loop sync sync-ch offer-revoke-ch-tail evict-ch-head))
        :exhaust-thread (thread (exhaust-queue-loop sync sync-ch exhaust-ch-tail seal-ch-head))
        :seal-thread (thread (seal-resource-loop sync seal-ch-tail exhaust-ch-head))
        :completion-thread (thread (completion-ch-loop sync sync-ch completion-ch-tail offer-ch-head completion-ch-head))
        :failure-thread (thread (failure-ch-loop failure-ch-tail))
        :shutdown-thread (thread (shutdown-ch-loop sync shutdown-ch-tail))
        :offer-thread (thread (offer-ch-loop sync sync-ch revoke-delay offer-ch-tail ack-ch-head
                                             exhaust-ch-head completion-ch-head offer-revoke-ch-head)))))

  (stop [component]
    (info "Stopping Coordinator")

    (close! (:sync-ch component))
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
    (close! (:shutdown-ch-head component))
    (close! (:failure-ch-head component))

    component))

(defn coordinator [{:keys [onyx.coordinator/revoke-delay]}]
  (map->Coordinator {:revoke-delay revoke-delay}))

