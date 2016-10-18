(ns onyx.peer.deterministic-peer-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-boilerplate :refer [build-job run-test-job]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]

            ;; FIXME REMOVE ME
            [onyx.messaging.aeron]

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
            [onyx.generative.manual-shrink]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.immutable-messenger :as im]
            [com.stuartsierra.component :as component]
            [onyx.static.planning :as planning]
            [onyx.peer.task-lifecycle :as tl]
            [onyx.peer.peer-group-manager :as pm]
            [onyx.generative.peer-model :as g]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking for-all]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.api])
  (:import [java.text SimpleDateFormat]))


;;;;;;;;;
;; Job code

(defn add-path [peer-id job-id task+slot {:keys [n] :as segment}]
  (-> segment
      ;; Track the path that the segment took through the tasks for graph analysis
      (update :path conj [job-id task+slot])
      ;; Track the peer path that the segments took
      (update :peer-path into peer-id)))

(def add-paths-calls
  {:lifecycle/before-task-start 
   (fn [event lifecycle]
     (if (= :input (:onyx/type (:task-map event))) 
       ;; include slot combinations with input
       {:params [[(:id event)] (:job-id event) [(:task event) (:slot-id event)]]}
       {:params [[(:id event)] (:job-id event) [(:task event)]]}))})

(defn add-paths-lifecycles [job]
  (update job 
          :lifecycles 
          conj 
          {:lifecycle/task :all
           :lifecycle/calls ::add-paths-calls}))

;; Get rid of this atom
(def state-atom (atom :replace))
(def number (atom 0))
(def key-slot-tracker (atom {}))

(defn update-state-atom! [{:keys [messenger] :as event} window trigger state-event extent-state]
  (assert messenger)
  ;; FIXME, needs to re-offer / pause state
  ; (when-not (= :job-completed (:event-type state-event)) 
  ;   (let [{:keys [job-id id egress-tasks]} event 
  ;         destinations (doall 
  ;                        (map (fn [route] 
  ;                               {:src-peer-id id
  ;                                ;; TODO: need better api that fills in site and slot-id for dests
  ;                                :slot-id -1
  ;                                :dst-task-id [job-id route]}) 
  ;                             egress-tasks))]
  ;     (m/offer-segments (:messenger (:state event)) 
  ;                      [{:state-output? true 
  ;                        :send-number (swap! number inc)
  ;                        :path [] 
  ;                        :extent-state extent-state}] 
  ;                      destinations)))

  ;; Triggering on job completed is buggy 
  ;; as peer may die while writing :job-completed
  ;; (when (= :job-completed (:event-type state-event)) 
  (when (or (= :recovered (:event-type state-event))
            (= :new-segment (:event-type state-event))) 
    ;(println "extent state is " (:group-key state-event) extent-state)
    (let [replica-version (m/replica-version messenger)
          value [(java.util.Date.) extent-state]
          dupes (filter (fn [[k v]] (> 1 (count v))) (group-by identity extent-state))] 

      ; (when (= 195 (:group-key state-event)) 
      ;   (println "Triggering" 
      ;            (:id event)
      ;            "messenger"
      ;            replica-version
      ;            (m/epoch (:messenger (:state event)))
      ;            (:event-type state-event) (:group-key state-event) extent-state))
      (assert (empty? dupes) dupes)
      (assert (= (count extent-state) (count (set extent-state))))
      (swap! key-slot-tracker (fn [m]
                                (if-let [slot (get m (:group-key state-event))]
                                  (do 
                                   (assert (= slot (:slot-id event)) {:slot1 slot :slot2 (:slot-id event)})
                                   m)
                                  (assoc m (:group-key state-event) (:slot-id event)))))
      (swap! state-atom 
             update-in 
             [(:job-id event) (:group-key state-event)] 
             ;; CRT type thing to make sure peers that have left and are on old triggers fail to trigger
             ;; Property tests failed because old peers wouldn't be caught up with replica, would still be triggering
             ;; While the new allocated peer was triggering
             (fn [[stored-replica-version stored-extent-state]]
               (if (or (nil? stored-replica-version) (>= replica-version stored-replica-version))
                 [replica-version extent-state]
                 [stored-replica-version stored-extent-state]))))))

(defn simple-job-def [job-id n-input-slots]
  (let [n-messages 200
        task-opts {:onyx/batch-size 2}
        inputs (mapv (fn [n] {:n n :path [] :peer-path []}) (range n-messages))
        job (-> (build-job [[:in1 :inc] 
                            [:in2 :inc] 
                            [:inc :out]] 
                           [{:name :in1
                             :type :seq 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/n-peers n-input-slots)
                             :input inputs}
                            {:name :in2
                             :type :seq 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/n-peers 1)
                             :input inputs}
                            {:name :inc
                             :type :windowed 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/group-by-key :n
                                               :onyx/flux-policy :recover
                                               ;; FIXME AT ONE TO TEST
                                               ;:onyx/min-peers 1
                                               ;:onyx/max-peers 1
                                               :onyx/min-peers 2
                                               :onyx/max-peers 2
                                               )
                             :args [::update-state-atom!]}
                            {:name :out
                             :type :null-out
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               ;; FIXME: needs enough room for trigger output
                                               :onyx/batch-size 4
                                               :onyx/max-peers 1)}]
                           :onyx.task-scheduler/balanced)
                (add-paths-lifecycles)
                (assoc-in [:metadata :job-id] job-id))]
    {:type :event
     :command :submit-job
     :job-spec {:job job
                :job-id job-id
                :inputs inputs
                :min-peers (->> job
                               (onyx.test-helper/job->min-peers-per-task)
                               (map :min-peers)
                               (reduce +))}}))


(defn submit-job-gen [n-jobs job-ids n-input-slots]
  (gen/elements 
    (map (fn [job-id]
           )
         job-ids)))

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
        deps (dep/immediate-dependents g (first start-node))]
    (if (empty? deps)
      [new-path]
      (mapcat #(build-path g [%] new-path) deps))))

(defn build-paths [job]
  (let [g (planning/to-dependency-graph (:workflow job))
        inputs (->> (:catalog job)
                    ;; Grab input tasks
                    (filter (fn [task] (= :input (:onyx/type task))))
                    ;; Generate all the input task slot-id combinations
                    (mapcat (fn [task]
                              (map (fn [slot-id]
                                     [(:onyx/name task) slot-id]) 
                                   (range (:onyx/n-peers task))))))]
    (mapcat #(build-path g % []) inputs)))

(defn inputs->outputs [{:keys [job job-id inputs]}]
  (mapcat (fn [path] 
            (map (fn [seg] 
                   (reduce (fn [segment task] 
                             (add-path [] job-id task segment)) 
                           seg
                           path)) 
                 inputs))
          (build-paths job)))

(defn prop-is [pass? & messages]
  (println "WHEN NOT" pass?)
  (when-not pass? 
    (throw (Exception. (pr-str messages)))))

(defn check-outputs-in-order! 
  "Check that all messages arrive in order, absent rewinds
   This is only true if there is only one peer per task
   at any given time"
  [peer-outputs]
  (doseq [peer-output peer-outputs]
    (doseq [run (->> peer-output
                     (partition-by #(= [:reset-messenger] %))
                     (remove (fn [run] (= #{[:reset-messenger]} (set run)))))]
      (let [segments (apply concat run)
            group-by-input (->> segments 
                                ;; Don't include messaged state
                                (remove :state-output?)
                                ;; We only care about ordering of segments that followed the same path through peers
                                (group-by :peer-path))]
        (doseq [[input-task segments] group-by-input]
          (prop-is (apply < (map :n segments))
                   (str (mapv :n segments)) 
                   (str "outputs not in order " input-task " " segments)))))))


(defn job-completion-cmds 
  "Generates a series of commands that should allow any submitted jobs to finish.
   This consists of enough task lifecycle events, and enough exhaust-input outputs to finish."
  [groups jobs n-cycles-per-group]
  (mapcat (fn [_]
            (let [finish-iterations (reduce into [] 
                                            (take (* n-cycles-per-group (count groups)) 
                                                  (cycle 
                                                   (concat ;; Iterate on the peers for two cycles
                                                           (apply concat
                                                                  ;; 5 full task iterations
                                                                  (repeat 5 
                                                                          (map 
                                                                           (fn [g] 
                                                                             [{:type :peer
                                                                               :command :task-iteration
                                                                               ;; Should be one for each known peer in the group, once it's
                                                                               ;; not one peer per group
                                                                               :group-id g
                                                                               :peer-owner-id [g :p0]
                                                                               ;; Two iterations between barriers to improve completion odds?
                                                                               :iterations 1}])
                                                                           groups)))
                                                           ;; Then a periodic barrier and a couple offer barriers
                                                           (map 
                                                            (fn [g] 
                                                              ;; Emit a barrier on a coordinator
                                                              [{:type :peer
                                                                :command :periodic-barrier
                                                                ;; Should be one for each known peer in the group, once it's
                                                                ;; not one peer per group
                                                                :group-id g
                                                                :peer-owner-id [g :p0]
                                                                :iterations 1}
                                                               {:type :peer
                                                                :command :offer-barriers
                                                                :group-id g
                                                                :peer-owner-id [g :p0]
                                                                :iterations 1}])
                                                            groups)
                                                           (map 
                                                            (fn [g] 
                                                              ;; Emit a barrier on a coordinator
                                                              [{:type :peer
                                                                :command :offer-barriers
                                                                :group-id g
                                                                :peer-owner-id [g :p0]
                                                                :iterations 1}])
                                                            groups)
                                                           (map 
                                                            (fn [g] 
                                                              ;; Emit a barrier on a coordinator
                                                              [{:type :peer
                                                                :command :offer-barriers
                                                                :group-id g
                                                                :peer-owner-id [g :p0]
                                                                :iterations 1}])
                                                            groups)
                                                           ))))
                  ;; Allows emitting exhaust-inputs and thus job completion
                  drain-commands [{:type :drain-commands}]]
              (into finish-iterations drain-commands)))
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
                   :args [:add-peer p]}
                  {:type :group 
                   :command :write-outbox-entries
                   :group-id g
                   :iterations 10}
                  {:type :group
                   :command :apply-log-entries
                   :group-id g
                   :iterations 10}]))))

(defn reset-peer-path [segment]
  (assoc segment :peer-path []))

(defn normalize-state [state]
  (set (map (juxt key 
                  (comp set 
                        (partial map reset-peer-path) 
                        val)) 
            state)))

(defn job-state-properties [expected-state job-id state]
  ;(println "EXPECTED" expected-state)
  ;(println "STATE" state)
  ;let [state-values (reduce into [] state)]
  ;(prop-is (= (count (set state)) (count state-values)) "not enough state values")
  (let [v1 (normalize-state expected-state)
        v2 (normalize-state (into {} (map (juxt key (comp second val)) state)))]
    (prop-is (= v1 v2) 
             "job-state-properties: trigger state is incorrect"
             (vec (take 2 (clojure.data/diff v1 v2))))))

(defn state-properties [expected-state state]
  (let [_ (assert (#{0 1} (count state)) ["only zero or one job is supported for state check for now" (keys state)])
        [job-id job-state] (first state)]
    (job-state-properties expected-state job-id job-state)))

(defn run-test [{:keys [phases] :as generated}]
  (let [_ (reset! state-atom {})
        _ (reset! key-slot-tracker {})
        all-gen-cmds (apply concat phases)
        job-commands (set (filter #(= (:command %) :submit-job) all-gen-cmds)) 
        jobs (map :job-spec job-commands)
        ;; currently tested with one job. Remove once multi job support is confirmed
        _ (assert (= (count jobs) 1))
        n-required-peers (if (empty? jobs) 0 (apply max (map :min-peers jobs)))
        final-add-peer-cmds (add-enough-peer-cmds n-required-peers)
        unique-groups (set (keep :group-id (concat all-gen-cmds final-add-peer-cmds)))
        _ (assert (= 2 (count phases)))
        all-cmds (concat 
                   (first phases)
                   [{:type :drain-commands}]
                   ;; Start with enough peers to finish the job, 
                   ;; just to get a nice mix of task iterations 
                   ;; This probably should be removed sometimes
                   final-add-peer-cmds 
                   ;; Ensure all the peer joining activities have finished
                   [{:type :drain-commands}]
                   (second phases)
                   ;; Then add enough peers to complete the job
                   final-add-peer-cmds 
                   ;; Ensure they've fully joined
                   [{:type :drain-commands}]
                   ;; Complete the job
                   ;; FIXME: not sure why so many iterations are required when using grouping
                   (job-completion-cmds unique-groups jobs 3000)
                   [{:type :drain-commands}])
        model (g/model-commands all-cmds)
        ;_ (println "Start run" (count gen-cmds))
        _ (assert (:media-driver-type generated))
        {:keys [replica groups exception]} (g/play-events (assoc generated :events all-cmds))
        _ (when exception (throw exception))
        n-peers (count (:peers replica))
        _ (println "final peers" n-peers)
        expected-completed-jobs (filterv (fn [job] (<= (:min-peers job) n-peers)) jobs)
        expected-outputs (mapcat inputs->outputs expected-completed-jobs)
        peers-state (into {} (mapcat :peer-state (vals groups)))
        ;; Flattening all the outputs here loses some information
        ;; We actually care about what each peer wrote last
        peer-outputs (map (comp :written val) 
                          (mapcat val peers-state))
        flattened-outputs (->> peer-outputs
                               (reduce into [])
                               (reduce into []))
        flow-outputs (->> flattened-outputs
                          (remove keyword?)
                          (remove :state-output?))
        messaged-state-outputs (->> flattened-outputs
                                    (remove keyword?)
                                    (filter :state-output?))
        ;; TODO: group-by job-id so we can separate out the diff job states?
        expected-state (->> expected-outputs
                            (map (fn [v] 
                                   (update v :path (comp vec butlast))))
                            (group-by :n))]
    (prop-is (>= n-peers n-required-peers) "not enough peers")
    (prop-is (= (count (:groups model)) (count (:groups replica))) ["groups" (:groups model) (:groups replica)])
    (prop-is (= (count (apply concat (vals (:peers model)))) 
                (count (:peers replica))) ["peers" (:peers model) (:peers replica)])
    (println "Replica is " replica)
    (prop-is (= (count jobs) (count (:completed-jobs replica))) "jobs not completed")
    (state-properties expected-state @state-atom)
    ;; FIXME requires fix to how tasks can be blocked. See above trigger
    ;(prop-is (= (set expected-outputs) (set (map reset-peer-path flow-outputs))) "messenger flow values incorrect")
    (check-outputs-in-order! peer-outputs)))

(def n-input-peers-gen
  (gen/elements [1 2]))

(def job-gen
  (gen/fmap (fn [[job-id n-input-peers]]
              (simple-job-def job-id n-input-peers))
            (gen/tuple gen/uuid n-input-peers-gen)))

(defspec deterministic-abs-test {;:seed X 
                                 :num-tests (times 1000)}
  (for-all [uuid-seed (gen/no-shrink gen/int)
            drain-seed (gen/no-shrink gen/int)
            media-driver-type (gen/elements [:shared #_:shared-network #_:dedicated])
            n-jobs (gen/return 1) ;(gen/resize 4 gen/s-pos-int) 
            ;; Number of peers on each input task
            initial-submit? (gen/no-shrink (gen/elements [1 0]))
            jobs (gen/no-shrink (gen/vector job-gen n-jobs))
            phases (gen/no-shrink 
                    (gen/tuple
                     (gen/vector (gen/return jobs) initial-submit?) 
                     (gen/scale #(* 50 %) ; scale to larger command sets quicker
                                (gen/vector 
                                 (gen/frequency [[1000 g/task-iteration-gen]
                                                 [500 g/periodic-barrier]
                                                 [500 g/offer-barriers]
                                                 ;; These should be infrequent
                                                 [5 g/add-peer-group-gen]
                                                 [5 g/remove-peer-group-gen]
                                                 [5 g/add-peer-gen]
                                                 [5 g/remove-peer-gen]
                                                 [5 g/full-remove-peer-gen]
                                                 [5 (gen/elements jobs)]
                                                 ;; These need to be pretty likely, even though most will be no-ops
                                                 ;; We need them to add peers, remove peers, etc
                                                 [500 g/play-group-commands-gen]
                                                 [500 g/write-outbox-entries-gen]
                                                 [500 g/apply-log-entries-gen]])
                                 10000))))]
           (println "Phases" (map count phases))
           (let [generated {:phases phases 
                            :messenger-type :aeron
                            :media-driver-type media-driver-type
                            :drain-seed drain-seed
                            :uuid-seed uuid-seed}]

             ;; Write all the time
             (let [date-str (.format (SimpleDateFormat. "yyyy_dd_MM_HH-mm-ss") (java.util.Date.))
                   filename (str "target/test_check_output/testcase." date-str "-tttt.edn")] 
               (println "Run written to " filename)
               (.mkdir (java.io.File. "target/test_check_output"))
               (spit filename (pr-str generated)))

             (try (run-test generated)
                  (Thread/sleep 5000)
                  (catch Throwable t
                    (let [date-str (.format (SimpleDateFormat. "yyyy_dd_MM_HH-mm-ss") (java.util.Date.))
                          filename (str "target/test_check_output/testcase." date-str ".edn")] 
                      (.mkdir (java.io.File. "target/test_check_output"))
                      (spit filename (pr-str generated))
                      (println "FAILED RUN WRITTEN TO" filename t)
                      (throw t)))))))


(defn successful-run? [generated]
  (assert (:drain-seed generated))
  (try (run-test generated)
       (println "SUCCESSFUL RUN")
       true
       (catch Throwable t
         (println "FAILED RUN" t)
         false)))

(defn shrink-written 
  "Reads the dumped test case from /tmp and iteratively shrinks the events by random selection"
  [filename]
  (onyx.generative.manual-shrink/shrink-annealing 
   successful-run? 
   (read-string (slurp filename)) 
   100))

(defn run-dumped [filename]
  (successful-run? (read-string (slurp filename))))

(defn -main [filename]
  (run-dumped filename))
