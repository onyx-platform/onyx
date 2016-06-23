(ns onyx.peer.peer-group-manager-pure-gen-test
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
            [onyx.mocked.failure-detector]
            [onyx.log.replica :refer [base-replica]]
            [onyx.extensions :as extensions]
            [onyx.system :as system]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.peer.peer-group-manager :as pm]
            [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

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

(deftest peer-group-gen-test
  (checking
    "Checking peer group manager operation"
    (times 20)
    [n-commands (gen/fmap #(+ 40 %) gen/pos-int)
     commands (gen/vector (gen/one-of [add-peer-gen remove-peer-gen 
                                       add-peer-group-gen ;remove-peer-group-gen
                                       #_restart-peer-group-gen apply-log-entries 
                                       write-entries play-group-commands]) 
                          n-commands)]
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
            (let [model (reduce (fn [model [gen-cmd g arg]]
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
                                commands)
                  model-groups (:groups model)
                  finish-commands (take (* 50 (count model-groups)) 
                                        (cycle 
                                          (mapcat 
                                            (fn [g] 
                                              [[:play-group-commands g 10]
                                               [:write-entries g 10]
                                               [:apply-log-entries g 10]])
                                            model-groups)))
                  all-commands (into (vec commands) finish-commands)
                  final-state (reduce (partial apply-command peer-config)
                                      groups
                                      all-commands)
                  final-replica (reduce #(extensions/apply-log-entry %2 %1) 
                                        base-replica
                                        @log-entries)]
              ;(println "final replica " model-groups " fir" final-replica @log-entries all-commands)
              (println (count (:groups model)) (count (:groups final-replica))
                      (count (:peers model)) (count (:peers final-replica)) 
                       )
              (is (= (count (:groups model)) (count (:groups final-replica))) "groups check")
              (is (= (count (:peers model)) (count (:peers final-replica))) "peers"))))))))
