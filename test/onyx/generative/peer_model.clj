(ns onyx.generative.peer-model
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.test-helper :refer [load-config with-test-env playback-log]]
            [onyx.peer.task-lifecycle :as tl]
            [onyx.peer.communicator]
            [onyx.log.zookeeper]
            [onyx.mocked.zookeeper]
            [onyx.log.failure-detector]
            [onyx.mocked.failure-detector]
            [onyx.log.replica]
            [onyx.extensions :as extensions]
            [onyx.system :as system]
            [onyx.peer.peer-group-manager :as pm]
            [clojure.test.check.generators :as gen]))

(def n-max-groups 5)
(def n-max-peers 1)
(def max-iterations 10)

(def peer-group-num-gen
  (gen/fmap (fn [oid]
              (keyword (str "g" oid)))
            (gen/resize (dec n-max-groups) gen/pos-int)))

(def peer-num-gen
  (gen/fmap (fn [oid]
              (keyword (str "p" oid)))
            (gen/resize (dec n-max-peers) gen/pos-int)))

(def command-commands
  #{:add-peer-group})

(def add-peer-group-gen
  (gen/fmap (fn [g] [:add-peer-group g])
            peer-group-num-gen))

(def group-commands 
  #{:write-outbox-entries :apply-log-entries :play-group-commands :group-command})

(def add-peer-gen
  (gen/fmap (fn [[g p]] [:group-command g [:add-peer [g p]]]) 
            (gen/tuple peer-group-num-gen 
                       peer-num-gen)))

(def remove-peer-gen
  (gen/fmap (fn [[g p]] [:group-command g [:remove-peer [g p]]]) 
            (gen/tuple peer-group-num-gen
                       peer-num-gen)))

(def write-outbox-entries
  (gen/fmap (fn [[g n]] [:write-outbox-entries g n])
            (gen/tuple peer-group-num-gen
                       (gen/resize max-iterations gen/pos-int))))

(def play-group-commands
  (gen/fmap (fn [[g n]] [:play-group-commands g n])
            (gen/tuple peer-group-num-gen
                       (gen/resize max-iterations gen/pos-int))))

(def apply-log-entries
  (gen/fmap (fn [[g n]] [:apply-log-entries g n])
            (gen/tuple peer-group-num-gen
                       (gen/resize max-iterations gen/pos-int))))

(def remove-peer-group-gen
  (gen/fmap (fn [g] [:remove-peer-group g])
            peer-group-num-gen))

(def restart-peer-group-gen 
  (gen/fmap (fn [g] [:group-command g [:restart-peer-group]])
            peer-group-num-gen))

(def task-iteration-gen 
  (gen/tuple (gen/return :task-iteration) 
             (gen/return nil)
             (gen/tuple peer-group-num-gen
                        peer-num-gen)))

(def peer-commands 
  #{:task-iteration})

(def event-commands
  #{:submit-job})

(defn apply-group-command [group-model [command arg]]
  (update group-model 
          :state
          (fn [state]
            (case command 
              :write-outbox-entries
              (reduce (fn [s entry]
                        (update-in s 
                                   [:comm :log]
                                   (fn [log]
                                     (if log
                                       (extensions/write-log-entry log entry))))) 
                      state
                      (keep (fn [_] 
                              (when-let [ch (:outbox-ch state)]
                                (poll! ch)))
                            (range arg)))

              :apply-log-entries
              (reduce (fn [s _]
                        (if (and (not (:stopped? s))
                                 (:connected? s))
                          (let [log (get-in s [:comm :log])] 
                            (if-let [entry (extensions/read-log-entry log (:entry-num log))]
                              (-> s
                                  (update-in [:comm :log :entry-num] inc)
                                  (pm/action [:apply-log-entry entry])) 
                              s))
                          s)) 
                      state
                      (range arg))

              :play-group-commands
              (reduce pm/action
                      state
                      (keep (fn [_] (poll! (:group-ch state)))
                            (range arg)))

              :group-command
              (pm/action state arg)))))

(defn new-group [peer-config]
  {:peer-state {} ;; Will contain
   ;; {peer-id {:prev-event event :outbox outbox}
   ;; Need to assoc in messenger?
   :state (-> (onyx.api/start-peer-group peer-config)
              :peer-group-manager 
              :initial-state 
              (pm/action [:start-peer-group]))})

(defn get-peer-id [group peer-owner-id]
  (get-in group [:state :peer-owners peer-owner-id]))

(defn get-peer-system [group peer-owner-id]
  (get-in group [:state :vpeers (get-peer-id group peer-owner-id)]))

(defn peer-system->event [peer-system]
  (:event (:task-lifecycle (:started-task-ch (:state (:virtual-peer peer-system))))))

(defn apply-peer-commands [groups group-id peer-owner-id]
  ;; TODO ONLY TASK ITERATION HERE
   (let [group (get groups group-id)
         peer-id (get-peer-id group [group-id peer-owner-id])]
     (if peer-id
       (let [peer-system (get-peer-system group [group-id peer-owner-id])
             init-event (peer-system->event peer-system)] 
         (if init-event
           (let [previous-event (or (get-in group [:peer-state peer-id :prev-event])
                                    init-event)
                 current-replica (:replica (:state group))
                 new-event (tl/event-iteration init-event 
                                               (:replica previous-event) 
                                               current-replica 
                                               (:messenger previous-event)
                                               (:pipeline previous-event)
                                               (:barriers previous-event))]
             #_(println "applying peer command" peer-id group-id peer-owner-id)
             ;(println "event" init-event)
             #_(println "task type" 
                      (:onyx/type (:task-map init-event))
                      (:onyx/type (:task-map new-event)))
             (when-let [written (seq (:null/not-written new-event))]
               (println "written: " written))
             (assoc groups 
                    group-id 
                    (-> group
                        (assoc-in [:peer-state peer-id :prev-event] new-event))))
           groups))
       groups)))

(defn apply-command [peer-config groups [command group-id arg]]
  (cond (get command-commands command)
        (case command
          :add-peer-group
          (update groups 
                  group-id 
                  (fn [group]
                    (if group
                      group
                      (new-group peer-config)))))

        (get peer-commands command)
        (do
         (let [[group-id peer-id] arg]
           (apply-peer-commands groups group-id peer-id)))

        (get event-commands command)
        (case command
          :submit-job (do ;; Quite stateful
                          (onyx.api/submit-job peer-config (:job arg))
                          groups))

        ;:remove-peer-group
        ; (do (if-let [group (get groups group-id)]
        ;         (onyx.api/shutdown-peer-group group))
        ;       (dissoc group)
        ;       (update groups 
        ;               group-id 
        ;               (fn [group]
        ;                 (if group))))

        (get group-commands command)
        ;; Default case is that this will be a group command
        (if-let [group (get groups group-id)]
          (assoc groups group-id (apply-group-command group [command arg]))   
          groups)

        :else 
        (throw (Exception. (str "cond case not defined: " command)))))

(defn model-commands [commands]
  (reduce (fn [model [gen-cmd g arg]]
            (case gen-cmd
              :add-peer-group 
              (update model :groups conj g)
              :group-command 
              (if (get (:groups model) g)
                (let [[grp-cmd & args] arg] 
                  (case grp-cmd
                    :add-peer
                    (update model :peers conj (first args))
                    :remove-peer
                    (update model :peers disj (first args))
                    model))
                model)
              model))
          {:groups #{}
           :peers #{}}
          commands))

(defn play-commands [commands]
  (let [log-entries (atom nil)
        store (atom {})] 
    (with-redefs [;; Group overrides
                  onyx.log.zookeeper/zookeeper (partial onyx.mocked.zookeeper/fake-zookeeper log-entries store) 
                  onyx.peer.communicator/outbox-loop (fn [_ _ _])
                  pm/peer-group-manager-loop (fn [state])
                  onyx.log.failure-detector/failure-detector onyx.mocked.failure-detector/failure-detector
                  onyx.log.commands.common/start-task! (fn [lifecycle]
                                                         (component/start lifecycle))
                  onyx.log.commands.common/build-stop-task-fn (fn [_ component]
                                                                (fn [scheduler-event] 
                                                                  (component/stop 
                                                                   (assoc-in component 
                                                                             [:task-lifecycle :scheduler-event] 
                                                                             scheduler-event))))
                  ;; Task overrides
                  tl/backoff-until-task-start! (fn [_])
                  tl/backoff-until-covered! (fn [_])
                  tl/backoff-when-drained! (fn [_])
                  tl/start-task-lifecycle! (fn [_ _])]
      (let [_ (reset! log-entries [])
            onyx-id (java.util.UUID/randomUUID)
            config (load-config)
            env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
            peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
            groups {}]
        (try
         (let [final-groups (reduce (partial apply-command peer-config)
                                    groups
                                    commands)
               final-replica (reduce #(extensions/apply-log-entry %2 %1) 
                                     (onyx.log.replica/starting-replica peer-config)
                                     @log-entries)]
            {:replica final-replica 
             :groups final-groups}))))) )

;; Job generator code
; (def gen-task-name (gen/fmap #(keyword (str "t" %)) gen/s-pos-int))

; (defn task->type [graph task]
;   (cond (empty? (dep/immediate-dependents graph task))
;         :output
;         (empty? (dep/immediate-dependencies graph task))
;         :input
;         :else
;         :function))

; (defn to-dependency-graph-safe [workflow]
;   (reduce (fn [[g wf] edge]
;             (try 
;               [(apply dep/depend g (reverse edge))
;                (conj wf edge)]
;               (catch Throwable t
;                 [g wf])))
;           [(dep/graph) []] 
;           workflow))

; (def build-workflow-gen
;   (gen/fmap (fn [workflow] 
;               (let [[g wf] (to-dependency-graph-safe workflow)]
;                 {:workflow wf
;                  :task->type (->> wf 
;                                   (reduce into [])
;                                   (map (fn [t] [t (task->type g t)])))})) 
;             (gen/such-that (complement empty?) 
;                            (gen/vector (gen/such-that #(not= (first %) (second %)) 
;                                                       (gen/tuple gen-task-name gen-task-name))))))


