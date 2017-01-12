(ns onyx.peer.deterministic-peer-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-boilerplate :refer [build-job run-test-job]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]

            ;; FIXME REMOVE ME
            [onyx.messaging.aeron.messenger]
            
            [onyx.peer.visualization :as viz]

            [clojure.data :refer [diff]]
            [onyx.plugin.seq]
            [onyx.plugin.null]
            [clojure.core.async]
            [com.stuartsierra.dependency :as dep]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.log.commands.common :refer [peer->allocated-job]]
            [onyx.log.replica :refer [base-replica]]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.extensions :as extensions]
            [onyx.generative.manual-shrink]
            [onyx.messaging.protocols.messenger :as m]
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
   (fn [{:keys [onyx.core/task-map onyx.core/id 
                onyx.core/job-id onyx.core/task 
                onyx.core/slot-id] :as event} 
        lifecycle]
     (if (= :input (:onyx/type task-map)) 
       ;; include slot combinations with input
       {:onyx.core/params [[id] job-id [task slot-id]]}
       {:onyx.core/params [[id] job-id [task]]}))})

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

(defn update-state-atom! [{:keys [onyx.core/slot-id onyx.core/job-id]} window trigger 
                          {:keys [messenger event-type group-key] :as state-event} extent-state]
  ;; FIXME FIXME FIXME: I think we need a way to call messenger to inject into the lifecycle the need to message things
  ;; This could look like data and could go through similar offer thing

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
  (when (or (= :recovered event-type)
            (= :new-segment event-type)) 
    ;(println "extent state is " (:group-key state-event) extent-state)
    (let [replica-version (m/replica-version messenger)
          value [(java.util.Date.) extent-state]
          dupes (filter (fn [[k v]] (> 1 (count v))) (group-by identity extent-state))]
      (assert (empty? dupes) dupes)
      (assert (= (count extent-state) (count (set extent-state))))
      (swap! key-slot-tracker (fn [m]
                                (if-let [slot (get m group-key)]
                                  (do 
                                   (assert (= slot slot-id) {:slot1 slot :slot2 slot-id})
                                   m)
                                  (assoc m group-key slot-id))))
      
      (swap! state-atom 
             update-in 
             [job-id group-key] 
             ;; vector clock type thing to make sure peers that have left and are on old triggers fail to trigger
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
        job (-> (build-job #_[[:in1 :identity1] 
                            [:in2 :identity1] 
                            [:identity1 :identity2]
                            [:identity2 :identity3]
                            [:identity3 :inc]
                            [:inc :out]] 
                           [[:in1 :inc] 
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
                            {:name :identity1
                             :type :fn 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/n-peers 2)
                             :args []}
                            {:name :identity2
                             :type :fn 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/n-peers 2)
                             :args []}
                            {:name :identity3
                             :type :fn 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/n-peers 1)
                             :args []}
                            {:name :inc
                             :type :windowed 
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               :onyx/group-by-key :n
                                               :onyx/flux-policy :recover
                                               :onyx/min-peers 2
                                               :onyx/max-peers 2)
                             :args [::update-state-atom!]}
                            {:name :out
                             :type :null-out
                             :task-opts (assoc task-opts 
                                               :onyx/fn ::add-path 
                                               ;; FIXME: needs enough room for trigger output
                                               :onyx/batch-size 4
                                               :onyx/max-peers 2)}]
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

(comment
 (defn build-path-this [g start-node path]
   (let [new-path (conj path start-node)
         deps (dep/immediate-dependents g start-node)]
     (reduce (fn [paths dep]
               (concat paths (build-path-this g dep new-path)))
             [new-path]
             deps)))

 (let [wf [[:a :b] [:b :c] [:b :d] [:d :e] [:d :f] [:f :g]]
       g (planning/to-dependency-graph wf)] 
   (->> wf
        (reduce into [])
        (mapcat (fn [n] (build-path-this g n [])))
        (map count)
        (apply max))))

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
  [groups jobs n-cycles]
  ;; Clean this up please
  (mapcat (fn [_]
            (let [peer-groups (for [g groups
                                    p (mapv (fn [oid]
                                              (keyword (str "p" oid)))
                                            (range g/n-max-peers))]
                                [g p])]
              (->> (concat (mapcat 
                            (fn [[g p]] 
                              [{:type :peer
                                :command :task-iteration
                                ;; Should be one for each known peer in the group, once it's
                                ;; not one peer per group
                                :group-id g
                                :peer-owner-id [g p]
                                ;; Two iterations between barriers to improve completion odds?
                                :iterations 1}])
                            peer-groups) 
                           ;; Then a periodic barrier and a couple offer barriers
                           ;; Not all of these will be coordinators
                           (mapcat 
                            (fn [[g p]] 
                              ;; Emit a barrier on a coordinator
                              [{:type :coordinator
                                :command :periodic-barrier
                                ;; Should be one for each known peer in the group, 
                                ;; once it's not one peer per group
                                :group-id g
                                :peer-owner-id [g p]
                                :iterations 1}
                               {:type :coordinator
                                :command :offer-heartbeats
                                ;; Should be one for each known peer in the group, 
                                ;; once it's not one peer per group
                                :group-id g
                                :peer-owner-id [g p]
                                :iterations 1}
                               {:type :coordinator
                                :command :offer-barriers
                                :group-id g
                                :peer-owner-id [g p]
                                :iterations 1}])
                            peer-groups)
                           ;; drain after each cycle so peers can reboot if necessary
                           [{:type :drain-commands}])
                   (repeat n-cycles)
                   (reduce into []))))
          jobs)) 

(defn partition-peers-over-groups [n-groups n-peers]
  (mapcat (fn [group peers]
            (map (fn [peer] 
                   [(keyword (str "g" group)) 
                    (keyword (str "p" peer))])
                 (range (count peers))))
          (range n-groups)
          (partition-all (int (Math/ceil (/ n-peers n-groups)))
                         (range n-peers))))

(defn add-enough-peer-cmds 
  "Add at least the minimum number of peers required to run the job"
  [n-groups n-required-peers]
  (let [peers (partition-peers-over-groups n-groups n-required-peers)
        groups (set (map first peers))]
    (concat (map (fn [g]
                   {:type :orchestration
                    :command :add-peer-group
                    :group-id g})
                 groups)
            (map (fn [[g p]]
                   {:type :group
                    :command :write-group-command
                    :group-id g
                    :args [:add-peer [g p]]})
                 peers)
            (mapcat (fn [g]
                      [{:type :group 
                        :command :write-outbox-entries
                        :group-id g
                        :iterations 10}
                       {:type :group
                        :command :apply-log-entries
                        :group-id g
                        :iterations 10}])
                    groups))))

(defn reset-peer-path [segment]
  (assoc segment :peer-path []))

(def normalize-state-segment 
  reset-peer-path)

(defn normalize-state [state]
  (->> state
       (map (fn [[k window-state]]
              [k (set (map normalize-state-segment window-state))]))
       set))

(defn job-state-properties [expected-state job-id state]
  (let [v1 (normalize-state expected-state)
        v2 (normalize-state (into {} (map (juxt key (comp second val)) state)))]
    (prop-is (= v1 v2) 
             "job-state-properties: trigger state is incorrect"
             (vec (take 2 (clojure.data/diff v1 v2))))))

(defn state-properties [expected-state state]
  (let [_ (assert (#{0 1} (count state)) ["only zero or one job is supported for state check for now" (keys state)])
        [job-id job-state] (first state)]
    (job-state-properties expected-state job-id job-state)))

(defn normalize-output-segment [segment]
  (-> segment 
      (dissoc :replica-version)
      (reset-peer-path)))

(defn run-test [{:keys [phases] :as generated}]
  (let [_ (reset! state-atom {})
        _ (reset! key-slot-tracker {})
        all-gen-cmds (apply concat phases)
        job-commands (set (filter #(= (:command %) :submit-job) all-gen-cmds)) 
        jobs (map :job-spec job-commands)
        ;; currently tested with one job. Remove once multi job support is confirmed
        _ (assert (= (count jobs) 1) (vec jobs))
        n-required-peers (if (empty? jobs) 0 (apply max (map :min-peers jobs)))
        final-add-peer-cmds (add-enough-peer-cmds 
                             ;; TODO, make this generated
                             g/n-max-groups 
                             n-required-peers)
        unique-groups (set (keep :group-id (concat all-gen-cmds final-add-peer-cmds)))
        all-cmds (concat final-add-peer-cmds
                         (apply concat
                                (interpose ;; Start with enough peers to finish the job, 
                                           ;; just to get a nice mix of task iterations 
                                           ;; This probably should be removed sometimes
                                           [{:type :drain-commands}]
                                           phases))
                         final-add-peer-cmds
                         [{:type :drain-commands}]
                         ;; FIXME: not sure why so many iterations are required when using grouping
                         (job-completion-cmds unique-groups jobs 3000))
        model (g/model-commands all-cmds)
        {:keys [replica groups exception]} (g/play-events (assoc generated :events all-cmds))
        _ (when exception (throw exception))
        n-peers (count (:peers replica))
        _ (println "final peers" n-peers "vs required" n-required-peers)
        expected-completed-jobs (filterv (fn [job] (<= (:min-peers job) n-peers)) jobs)
        expected-outputs (mapcat inputs->outputs expected-completed-jobs)
        peers-state (into {} (mapcat :peer-state (vals groups)))
        ;; Flattening all the outputs here loses some information
        ;; We actually care about what each peer wrote last
        peer-outputs (map (comp :written val) 
                          (mapcat val peers-state))
        flattened-outputs (->> peer-outputs
                               (reduce into [])
                               (reduce into [])
                               (remove keyword?)
                               (mapv normalize-output-segment))
        ; flow-outputs (->> flattened-outputs
        ;                   (remove keyword?)
        ;                   (remove :state-output?))
        ; messaged-state-outputs (->> flattened-outputs
        ;                             (remove keyword?)
        ;                             (filter :state-output?))
        ;; TODO: group-by job-id so we can separate out the diff job states?
        expected-state (->> expected-outputs
                            (map (fn [v] 
                                   ;; state tasks are before output tasks, 
                                   ;; so they won't have the output path yet
                                   (update v :path (comp vec butlast))))
                            (group-by :n))]
    (prop-is (>= n-peers n-required-peers) "not enough peers")
    (prop-is (= (count (:groups model)) (count (:groups replica))) ["groups" (:groups model) (:groups replica)])
    (prop-is (= (count (apply concat (vals (:peers model)))) 
                (count (:peers replica))) ["peers" (:peers model) (:peers replica)])
    (println "Final replica:" replica)
    (prop-is (= (count jobs) (count (:completed-jobs replica))) "jobs not completed")
    (prop-is (= (set flattened-outputs) (set expected-outputs)) 
             ["all output written" 
              (count (set flattened-outputs)) 
              (count (set expected-outputs))
              (clojure.set/difference (set expected-outputs) 
                                      (set flattened-outputs))
              (clojure.set/difference (set flattened-outputs) 
                                      (set expected-outputs))])
    ;; FIXME requires fix to how tasks can be blocked. See above trigger
    ;(prop-is (= (set expected-outputs) (set (map reset-peer-path flow-outputs))) 
    ;         "messenger flow values incorrect")
    (check-outputs-in-order! peer-outputs)
    (state-properties expected-state @state-atom)))

(def n-input-peers-gen
  (gen/elements [1 2]))

(def job-gen
  (gen/fmap (fn [[job-id n-input-peers]]
              (simple-job-def job-id n-input-peers))
            (gen/tuple gen/uuid n-input-peers-gen)))

(def scale-factor 100)

;; Test cases to look into further
;;
;; ....
#_(defspec deterministic-abs-test {;:seed X 
                                 :num-tests (times 2)}
  (for-all [uuid-seed (gen/no-shrink gen/int)
            drain-seed (gen/no-shrink gen/int)
            media-driver-type (gen/elements [:shared] #_[:shared :shared-network :dedicated])
            n-jobs (gen/return 1)
            ;; Number of peers on each input task
            jobs (gen/no-shrink (gen/vector job-gen n-jobs))
            phases (gen/no-shrink 
                    (gen/vector 
                     (gen/one-of [;; operate normally without any peer / peer group losses
                                  (gen/scale #(* scale-factor %)
                                             (gen/vector 
                                              (gen/frequency [[2000 g/task-iteration-gen]
                                                              [2000 g/periodic-coordinator-barrier]
                                                              [4000 g/offer-barriers]])))

                                  ;; Also allow some peer log entries to be written out and played
                                  (gen/scale #(* scale-factor %)
                                             (gen/vector 
                                              (gen/frequency [[2000 g/task-iteration-gen]
                                                              [2000 g/periodic-coordinator-barrier]
                                                              [4000 g/offer-barriers]
                                                              [5000 g/play-group-commands-gen]
                                                              [5000 g/write-outbox-entries-gen]
                                                              [5000 g/apply-log-entries-gen]])))  
                                  ;; join and leave - failures
                                  (gen/scale #(* scale-factor %) 
                                             (gen/vector 
                                              (gen/frequency [[500 g/add-peer-group-gen]
                                                              [500 g/add-peer-gen]
                                                              [50 g/remove-peer-group-gen]
                                                              [50 g/remove-peer-gen]
                                                              [50 g/full-remove-peer-gen]
                                                              [5000 g/play-group-commands-gen]
                                                              [5000 g/write-outbox-entries-gen]
                                                              [5000 g/apply-log-entries-gen]])))
                                  ;; anything can happen
                                  (gen/scale #(* scale-factor %)
                                             (gen/vector 
                                              (gen/frequency [[2000 g/task-iteration-gen]
                                                              [2000 g/periodic-coordinator-barrier]
                                                              [4000 g/offer-barriers]
                                                              ;; These should be infrequent
                                                              [50 g/add-peer-group-gen]
                                                              [50 g/remove-peer-group-gen]
                                                              [50 g/add-peer-gen]
                                                              [50 g/remove-peer-gen]
                                                              [5 g/full-remove-peer-gen]
                                                              [5 (gen/elements jobs)]
                                                              ;; These need to be pretty likely, even though most will be no-ops
                                                              ;; We need them to add peers, remove peers, etc
                                                              [5000 g/play-group-commands-gen]
                                                              [5000 g/write-outbox-entries-gen]
                                                              [5000 g/apply-log-entries-gen]])))
                                  ;; submit job
                                  (gen/return jobs)])))]
           (info "Phases" (mapv count phases))
           (let [generated {:phases (conj phases jobs)
                            :messenger-type :aeron
                            :media-driver-type media-driver-type
                            :drain-seed drain-seed
                            :uuid-seed uuid-seed}]
             ;; Write all the time
             (let [date-str (.format (SimpleDateFormat. "yyyy_dd_MM_HH-mm-ss") (java.util.Date.))
                   filename (str "target/test_check_output/testcase." date-str "-tttt.edn")] 
               (println "Run written to " filename)
               (.mkdir (java.io.File. "target/test_check_output"))
               (spit filename (pr-str generated))
               (try (run-test generated)
                    ;; Double up graph writing
                    (viz/build-graph (str filename ".svg"))
                    (Thread/sleep 2000)
                    (catch Throwable t
                      (let [date-str (.format (SimpleDateFormat. "yyyy_dd_MM_HH-mm-ss") (java.util.Date.))
                            filename (str "target/test_check_output/testcase." date-str ".edn")] 
                        (.mkdir (java.io.File. "target/test_check_output"))
                        (viz/build-graph (str filename ".svg"))
                        (spit filename (pr-str generated))
                        (println "FAILED RUN WRITTEN TO" filename t)
                        (throw t))))))))


(defn successful-run? [generated]
  (try (run-test generated)
       (println "SUCCESSFUL RUN")
       true
       (catch Throwable t
         (viz/build-graph (str "rerun.svg"))
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
