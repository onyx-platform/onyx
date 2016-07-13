(ns onyx.peer.deterministic-peer-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-boilerplate :refer [build-job run-test-job]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]

            [clojure.data :refer [diff]]
            [onyx.plugin.seq]
            [onyx.plugin.null]
            [clojure.core.async]
            [com.stuartsierra.dependency :as dep]
            [onyx.messaging.messenger :as m]
            [onyx.log.commands.common :refer [peer->allocated-job]]
            [onyx.log.replica :refer [base-replica]]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.extensions :as extensions]

            [onyx.messaging.immutable-messenger :as im]

            [com.stuartsierra.component :as component]
            [onyx.static.planning :as planning]
            [onyx.peer.task-lifecycle :as tl]

            [onyx.generative.peer-model :as g]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking for-all]]

            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.api]))

;;;;;;;;;
;; Job code

(defn add-path [job-id task-id {:keys [n] :as segment}]
  (update segment :path conj [job-id task-id]))

(def add-paths-calls
  {:lifecycle/before-task-start 
   (fn [event lifecycle]
     {:params [(:job-id event) (:task event)]})})

(defn add-paths-lifecycles [job]
  (update job 
          :lifecycles 
          conj 
          {:lifecycle/task :all
           :lifecycle/calls ::add-paths-calls}))

(defn submit-job-gen [n-jobs job-ids]
  (gen/fmap (fn [job-spec]
              {:type :event
               :command :submit-job
               :job-spec job-spec}) 
            (gen/elements 
             (map (fn [job-id]
                    (let [n-messages 20
                          task-opts {:onyx/batch-size 2}
                          inputs (map (fn [n] {:n n :path []}) (range n-messages))
                          job (-> (build-job [[:in1 :inc] [:in2 :inc] [:inc :out]] 
                                             [{:name :in1
                                               :type :seq 
                                               ;;:task-opts (assoc task-opts :onyx/fn ::add-path :onyx/max-peers 2)
                                               ;; n-peers must be set for input task or else slots won't be stable
                                               :task-opts (assoc task-opts 
                                                                 :onyx/fn ::add-path 
                                                                 :onyx/n-peers 1)
                                               :input inputs}
                                              {:name :in2
                                               :type :seq 
                                               ;;:task-opts (assoc task-opts :onyx/fn ::add-path :onyx/max-peers 2)
                                               ;; n-peers must be set for input task or else slots won't be stable
                                               :task-opts (assoc task-opts 
                                                                 :onyx/fn ::add-path 
                                                                 :onyx/n-peers 1)
                                               :input inputs}
                                              {:name :inc
                                               :type :fn 
                                               :task-opts (assoc task-opts :onyx/fn ::add-path :onyx/max-peers 1)}
                                              {:name :out
                                               :type :null-out
                                               :task-opts (assoc task-opts :onyx/fn ::add-path :onyx/max-peers 2)}]
                                             :onyx.task-scheduler/balanced)
                                  (add-paths-lifecycles)
                                  (assoc-in [:metadata :job-id] job-id))]
                      {:job job
                       :job-id job-id
                       :inputs inputs
                       :min-peers (reduce + (map :min-peers (onyx.test-helper/job->min-peers-per-task job)))}))
                  job-ids))))

;;;;;;;;;
;; Runner code

(defn print-messenger-diff [old new]
  (let [[prev-diff next-diff _] (diff old new)]
    (debug "DIFF PREV MSGER:" prev-diff)
    (debug "DIFF NEXT MSGER:" next-diff)))

(defn print-event-diff [old new]
  (let [[prev-diff next-diff _] (diff old new)]
    (debug "DIFF PREV EVENT:" prev-diff)
    (debug "DIFF NEXT EVENT:" next-diff)))

(defn build-path [g start-node path]
  (let [new-path (conj path start-node)
        deps (dep/immediate-dependents g start-node)]
    (if (empty? deps)
      [new-path]
      (mapcat #(build-path g % new-path) deps))))

(defn build-paths [job]
  (let [g (planning/to-dependency-graph (:workflow job))
        inputs (->> (:catalog job)
                    (filter (fn [task] (= :input (:onyx/type task))))
                    (map :onyx/name))]
    (mapcat #(build-path g % []) inputs)))

(defn inputs->outputs [{:keys [job job-id inputs]}]
  (mapcat (fn [path] 
            (map (fn [seg] 
                   (reduce (fn [segment task] 
                             (add-path job-id task segment)) 
                           seg
                           path)) 
                 inputs))
          (build-paths job)))

(defn check-outputs-in-order! 
  "Check that all messages arrive in order, absent rewinds
   This is only true if there is only one peer per task
   at any given time"
  [peer-outputs]
  (doseq [peer-output peer-outputs]
    (doseq [run (partition-by #(= [:reset-messenger] %) peer-output)]
      (when (not= (set run) #{[:reset-messenger]})
        (let [segments (apply concat run)
              ;; We only care about ordering of segments coming from the same input
              group-by-input (group-by #(first (:path %)) segments)]
          (doseq [[input-task segments] group-by-input]
            (is (apply < (map :n segments))
                (str (mapv :n segments)))))))))

(defn job-completion-cmds 
  "Generates a series of commands that should allow any submitted jobs to finish.
   This consists of enough task lifecycle events, and enough exhaust-input outputs to finish."
  [groups jobs]
  (mapcat (fn [_]
            (let [finish-iterations (take (* 50 (count groups)) 
                                          (cycle 
                                           (mapcat 
                                            (fn [g] 
                                              [{:type :peer
                                                :command :task-iteration
                                                ;; Should be one for each known peer in the group, once it's
                                                ;; not one peer per group
                                                :group-id g
                                                :peer-owner-id [g :p0]
                                                :iterations 1}])
                                            groups)))
                  emit-exhaust-input [{:type :drain-commands}]]
              (concat finish-iterations emit-exhaust-input)))
          jobs))

(defn add-enough-peer-cmds 
  "Add at least the minimum number of peers required to run the job"
  [n-required-peers]
  (->> (range n-required-peers)
       (map (fn [i] 
              (let [g-id (keyword (str "g" i))]
                ;; FIXME: hard coded single peer
                [g-id [g-id :p0]])))
       (mapcat (fn [[g p]]
                 [{:type :orchestration
                   :command :add-peer-group
                   :group-id g}
                  {:type :group
                   :command :write-group-command
                   :group-id g
                   :args [:add-peer p]}])))) 

;; TODO:
;; Allow peer groups to leave
;; Start on storing overall state and recovering it

;; Store / recover state checkpoints in same way. Should be amongst the inputs (so need to look at more than just the input tasks)

;; For state you can just not reset state-events
;; The problem is that how do you know which sections of the log checkpoints to
;; restore, since some will be thrown away, while others will actually be
;; restored. When you do a restore, clean up the bits you didn't use? This can
;; be done by destroying any records that are later than the ones that you
;; want. This is a good somewhat temporary solution

;; The peer that recovers the slot is responsible for the cleanup

;; Peer leave - fix event switching
;; Peer re-add

;; State checkpointing can happen in emit-barriers
(defspec deterministic-abs-test {;:seed X
                                 :num-tests (times 500)}
  (for-all [uuid-seed (gen/no-shrink gen/int)
            n-jobs (gen/resize 4 gen/s-pos-int) ;(gen/return 1)
            job-ids (gen/vector gen/uuid n-jobs)
            gen-cmds ;gen/no-shrink (gen/return failing)
            ;gen/no-shrink 
            
            (gen/scale #(* 5 %) ; scale to larger command sets quicker
                       (gen/vector 
                        (gen/frequency [[300 g/task-iteration-gen]
                                        [50 g/add-peer-group-gen]
                                        [50 g/add-peer-gen]
                                        [10 g/remove-peer-gen]
                                        [50 g/play-group-commands-gen]
                                        [50 g/write-outbox-entries-gen]
                                        [50 g/apply-log-entries-gen]
                                        [50 (submit-job-gen n-jobs job-ids)]])))]
           (let [job-commands (set (filter #(= (:command %) :submit-job) gen-cmds)) 
                 jobs (map :job-spec job-commands)
                 n-required-peers (if (empty? jobs) 0 (apply max (map :min-peers jobs)))
                 final-add-peer-cmds (add-enough-peer-cmds n-required-peers)
                 unique-groups (set (keep :group-id (concat gen-cmds final-add-peer-cmds)))
                 all-cmds (concat 
                           final-add-peer-cmds 
                           [{:type :drain-commands}]
                           gen-cmds 
                           ;; Ensure all the peer joining activities have finished
                           [{:type :drain-commands}]
                           ;; Then re-add enough peers to completely the job
                           final-add-peer-cmds 
                           ;; Ensure they've fully joined
                           [{:type :drain-commands}]
                           ;; Complete the job
                           (job-completion-cmds unique-groups jobs)
                           [{:type :drain-commands}])
                 model (g/model-commands all-cmds)
                 _ (println "Start run" (count gen-cmds))
                 {:keys [replica groups]} (g/play-commands all-cmds uuid-seed)
                 n-peers (count (:peers replica))
                 expected-completed-jobs (filterv (fn [job] (<= (:min-peers job) n-peers)) jobs)
                 expected-outputs (mapcat inputs->outputs expected-completed-jobs)
                 peers-state (into {} (mapcat :peer-state (vals groups)))
                 peer-outputs (map (comp :written val) 
                                   (mapcat val peers-state))
                 actual-outputs (remove keyword? (reduce into [] (reduce into [] peer-outputs)))]
             ;(println  "jobs " jobs "comp" (:completed-jobs replica))
             ;(println "gen-cmds" gen-cmds)
             (is (>= n-peers n-required-peers) "not enough peers")
             (is (= (count jobs) (count (:completed-jobs replica))))
             (is (= (count (:groups model)) (count (:groups replica))) "groups check")
             (is (= (count (:peers model)) (count (:peers replica))) "peers")
             (is (= (set expected-outputs) (set actual-outputs)))
             ;(println "replcia " replica)
             ;(println "actual " actual-outputs)
             ;(println replica)
             (check-outputs-in-order! peer-outputs)
             ;(println "Expected: " expected-outputs)
             ;(println "Outputs:" actual-outputs)
             )))
