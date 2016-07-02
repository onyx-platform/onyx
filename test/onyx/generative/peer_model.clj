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
            [onyx.log.replica :refer [base-replica]]
            [onyx.extensions :as extensions]
            [onyx.system :as system]
            [onyx.peer.peer-group-manager :as pm]
            [clojure.test.check.generators :as gen]))

(def peer-group-num-gen
  (gen/fmap (fn [oid]
              (keyword (str "g" oid)))
            (gen/resize 5 gen/pos-int)))

(def peer-num-gen
  (gen/fmap (fn [oid]
              (keyword (str "p" oid)))
            (gen/resize 20 gen/pos-int)))

(def add-peer-gen
  (gen/fmap (fn [[g p]] [:group-command g [:add-peer [g p]]]) 
            (gen/tuple peer-group-num-gen 
                       peer-num-gen)))

(def remove-peer-gen
  (gen/fmap (fn [[g p]] [:group-command g [:remove-peer [g p]]]) 
            (gen/tuple peer-group-num-gen
                       peer-num-gen)))

(def write-entries
  (gen/fmap (fn [[g n]] [:write-entries g n])
            (gen/tuple peer-group-num-gen
                       (gen/resize 20 gen/pos-int))))

(def play-group-commands
  (gen/fmap (fn [[g n]] [:play-group-commands g n])
            (gen/tuple peer-group-num-gen
                       (gen/resize 20 gen/pos-int))))

(def apply-log-entries
  (gen/fmap (fn [[g n]] [:apply-log-entries g n])
            (gen/tuple peer-group-num-gen
                       (gen/resize 20 gen/pos-int))))

(def add-peer-group-gen
  (gen/fmap (fn [g] [:add-peer-group g])
            peer-group-num-gen))

(def remove-peer-group-gen
  (gen/fmap (fn [g] [:remove-peer-group g])
            peer-group-num-gen))

(def restart-peer-group-gen 
  (gen/fmap (fn [g] [:group-command g [:restart-peer-group]])
            peer-group-num-gen))

(defn apply-group-command [state [command arg]]
  (case command 
    :write-entries
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
    (pm/action state arg)))

(defn apply-command [peer-config groups [command group-id arg]]
  (case command
    :add-peer-group
    (update groups 
            group-id 
            (fn [group]
              (if group
                group
                (-> (onyx.api/start-peer-group peer-config)
                    :peer-group-manager 
                    :initial-state 
                    (pm/action [:start-peer-group])))))
    ;:remove-peer-group
    ; (do (if-let [group (get groups group-id)]
    ;         (onyx.api/shutdown-peer-group group))
    ;       (dissoc group)
    ;       (update groups 
    ;               group-id 
    ;               (fn [group]
    ;                 (if group))))

    (if-let [group (get groups group-id)]
      (assoc groups group-id (apply-group-command group [command arg]))   
      groups)))

(defn play-commands [commands]
  (let [log-entries (atom nil)] 
    (with-redefs [;; Group overrides
                  onyx.log.zookeeper/zookeeper (partial onyx.mocked.zookeeper/fake-zookeeper log-entries) 
                  onyx.peer.communicator/outbox-loop (fn [_ _ _])
                  pm/peer-group-manager-loop (fn [state])
                  onyx.log.failure-detector/failure-detector onyx.mocked.failure-detector/failure-detector
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
                                     base-replica
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


