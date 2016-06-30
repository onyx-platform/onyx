(ns onyx.peer.deterministic-peer-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-boilerplate :refer [build-job run-test-job]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]

            [clojure.data :refer [diff]]
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

            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking for-all]]

            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.api]))

;; Job generator code
(def gen-task-name (gen/fmap #(keyword (str "t" %)) gen/s-pos-int))

(defn task->type [graph task]
  (cond (empty? (dep/immediate-dependents graph task))
        :output
        (empty? (dep/immediate-dependencies graph task))
        :input
        :else
        :function))

(defn to-dependency-graph-safe [workflow]
  (reduce (fn [[g wf] edge]
            (try 
              [(apply dep/depend g (reverse edge))
               (conj wf edge)]
              (catch Throwable t
                [g wf])))
          [(dep/graph) []] 
          workflow))

(def build-workflow-gen
  (gen/fmap (fn [workflow] 
              (let [[g wf] (to-dependency-graph-safe workflow)]
                {:workflow wf
                 :task->type (->> wf 
                                  (reduce into [])
                                  (map (fn [t] [t (task->type g t)])))})) 
            (gen/such-that (complement empty?) 
                           (gen/vector (gen/such-that #(not= (first %) (second %)) 
                                                      (gen/tuple gen-task-name gen-task-name))))))

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

;;;;;;;;;
;; Runner code
(comment 
(defn print-messenger-diff [old new]
  (let [[prev-diff next-diff _] (diff old new)]
    (debug "DIFF PREV MSGER:" prev-diff)
    (debug "DIFF NEXT MSGER:" next-diff)))

(defn print-event-diff [old new]
  (let [[prev-diff next-diff _] (diff old new)]
    (debug "DIFF PREV EVENT:" prev-diff)
    (debug "DIFF NEXT EVENT:" next-diff)))

(defn task-iteration 
  [gen-state written-to-ch [peer-id n-iterations]]
  (reduce (fn [{:keys [messenger peer-states log checkpoints] :as gstate} _] 
            (let [{:keys [task prev-replica replica] :as ps} (get peer-states peer-id)]
              (if-not task
                gstate
                (let [event (:event task)
                      peer-messenger (assoc messenger :peer-id peer-id) 
                      new-event (tl/event-iteration 
                                  (assoc event :checkpoints checkpoints) 
                                  prev-replica 
                                  replica 
                                  peer-messenger
                                  (:pipeline event) 
                                  (:barriers event))
                      output-segments (if (= (:task-type new-event) :output)
                                        (map :message (mapcat :leaves (:tree (:results new-event))))
                                        [])
                      new-ps (-> ps 
                                 (add-to-outbox! written-to-ch)
                                 (update :written into output-segments)
                                 (assoc-in [:task :event] new-event)
                                 (assoc :prev-replica replica))
                      next-messenger (:messenger new-event)]
                  (debug "TASK ITERATION:" peer-id task)
                  (debug "Allocations: " (:allocations replica))
                  (print-messenger-diff messenger next-messenger)
                  (print-event-diff (dissoc event :messenger) (dissoc event :messenger))
                  (assoc gstate
                         :messenger next-messenger 
                         :peer-states (assoc peer-states peer-id new-ps))))))
          
          gen-state
          (range n-iterations)))

(defn generate-peer-ids
  ([n]
   (generate-peer-ids 1 n))
  ([n m]
   (map #(keyword (str "p" %))
        (range n (+ n m)))))

(defn all-jobs-completed? 
  "Checks if one of the peers has a replica where all jobs are completed"
  [gen-state]
  (some #(= (:job-ids gen-state) %)
        (map (comp set :completed-jobs :replica) 
             (vals (:peer-states gen-state)))))

(defn setup-peer-states [{:keys [initial-replica peer-group peer-config peer-ids] :as gen-state}]
  (reduce (fn [m peer-id]
            (assoc m 
                   peer-id 
                   {:prev-replica initial-replica
                    :replica initial-replica
                    :written []
                    :state {:peer-group {:messaging-group peer-group}
                            :id peer-id
                            :opts peer-config}
                    :id peer-id
                    :outbox [(build-join-entry peer-id peer-group)]
                    :log-index -1
                    :task nil}))
          {}
          peer-ids))

(defn play-run 
  [{:keys [written-to-ch] :as gen-state} commands]
  (reset! written-to-ch {})
  (reduce (fn [gen-state [command & rst]]
            ;(println "command " command rst)
            (let [new-state (case command
                              :task-iteration (task-iteration gen-state written-to-ch rst)
                              :log-apply (log-apply gen-state rst)
                              :event-queues (write-log-event gen-state rst)
                              :write-log (write-log gen-state rst))]
              (if (all-jobs-completed? gen-state)
                (reduced (assoc new-state :completed? true))
                new-state)))
          (assoc gen-state :peer-states (setup-peer-states gen-state))
          commands))

(defn gen-task-ids [job-id nodes]
  (into {}
        (map (fn [node]
               [node (keyword (str (name job-id) "_t-" (name node)))])
             nodes)))

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

(defn inputs->outputs [job job-id inputs]
  (mapcat (fn [path] 
            (map (fn [seg] 
                   (reduce (fn [segment task] 
                             (add-path job-id task segment)) 
                           seg
                           path)) 
                 inputs))
          (build-paths job)))

(defn complete-job-actions [{:keys [event-queues peer-ids]}]
  (concat (mapcat (fn [[k v]]
                    (repeat (count v) [:event-queues k]))
                  event-queues)
          (mapcat (fn [peer-id]
                    [[:write-log peer-id 5]
                     [:log-apply peer-id 5]])
                  (apply concat (repeat 80 peer-ids)))
          (map (fn [peer-id]
                 [:task-iteration peer-id 1])
               (apply concat (repeat 80 peer-ids)))
          (mapcat (fn [peer-id]
                    [[:write-log peer-id 5]
                     [:log-apply peer-id 5]])
                  (apply concat (repeat 20 peer-ids)))))

(defn job-id->queue-name [job-id]
  (keyword "submit-job" (str job-id)))

(defn build-jobs []
  (let [n-messages 20
        task-opts {:onyx/batch-size 2}
        inputs (map (fn [n] {:n n :path []}) (range n-messages))
        job (add-paths-lifecycles 
              (build-job [[:in :inc] [:inc :out]] 
                         [{:name :in
                           :type :seq 
                           :task-opts (assoc task-opts :onyx/fn ::add-path :onyx/max-peers 2)
                           :input inputs}
                          {:name :inc
                           :type :fn 
                           :task-opts (assoc task-opts :onyx/fn ::add-path)}
                          {:name :out
                           :type :null-out
                           :task-opts (assoc task-opts 
                                             :onyx/fn ::add-path 
                                             :onyx/max-peers 5)}]
                         :onyx.task-scheduler/balanced))]
    {:j1 {:job job 
          :inputs inputs
          :min-peers (reduce + (map :min-peers (onyx.test-helper/job->min-peers-per-task job)))}}))

(defn add-jobs [gen-state jobs]
  (reduce (fn [{:keys [peer-config] :as gen-state} [job-id {:keys [job inputs]}]]
            (let [outputs (inputs->outputs job job-id inputs)
                  discovered (planning/discover-tasks (:catalog job) (:workflow job) #(gen-task-ids job-id %))
                  submit-entry (onyx.api/create-submit-job-entry job-id peer-config job discovered)]
              (-> gen-state
                  (update :expected-outputs into outputs)
                  (update :job-ids conj job-id)
                  (assoc-in [:datastore job-id] {:job job})
                  (assoc-in [:event-queues (job-id->queue-name job-id)] [submit-entry])))) 
          gen-state
          jobs))

(timbre/merge-config!
          {:appenders
           {:println
            {:min-level :error
             :enabled? true}}})

(defn write-log-gen [n-peers]
  (gen/tuple (gen/return :write-log) 
             (gen/elements (generate-peer-ids n-peers))
             (gen/return 1)))

(defn log-apply-gen [n-peers]
  (gen/tuple (gen/return :log-apply) 
             (gen/elements (generate-peer-ids n-peers))
             (gen/resize 5 gen/s-pos-int)))

(defn task-iteration-gen [n-peers]
  (gen/tuple (gen/return :task-iteration) 
             (gen/elements (generate-peer-ids n-peers))
             (gen/return 1)))

(defspec deterministic-abs-test {;:seed 1463496950840 
                                 :num-tests (times 500)}
  (let [written-to-ch (atom nil)
        checkpoint-store (atom {})
        onyx-id "property-testing"
        peer-config {:onyx/tenancy-id onyx-id
                     :zookeeper/address "127.0.0.1" 
                     :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                     :onyx.messaging/bind-addr "127.0.0.1"
                     :onyx.messaging/impl :immutable-messaging
                     :onyx.peer/try-join-once? false}
        pg (component/start (im/immutable-peer-group {}))
        messenger (im/immutable-messenger pg) 
        initial-replica (assoc base-replica 
                               :job-scheduler (:onyx.peer/job-scheduler peer-config)
                               :messaging {:onyx.messaging/impl (:onyx.messaging/impl peer-config)})
        jobs (build-jobs)
        queue-keys (map job-id->queue-name (keys jobs))
        job-min-peers (apply max (map :min-peers (vals jobs)))
        max-peers 10
        n-iteration-gen (gen/resize 5 gen/s-pos-int)
        ;; generates between job-min-peers and max-peers
        n-peer-gen (gen/fmap #(+ job-min-peers %) 
                             (gen/resize (- max-peers job-min-peers) 
                                         gen/pos-int))]
    (for-all [n-peers n-peer-gen
              commands (gen/scale #(* 500 %) ; scale to larger command sets quicker
                                  (gen/vector 
                                    (gen/frequency [[500 (task-iteration-gen n-peers)]
                                                    [50 (write-log-gen n-peers)]
                                                    [50 (log-apply-gen n-peers)]
                                                    [5 (gen/tuple (gen/return :event-queues)
                                                                  (gen/elements queue-keys))]])))]
             (with-redefs [clojure.core.async/>!! (fn [ch v] (swap! written-to-ch update ch (fnil conj []) v))
                           onyx.extensions/write-chunk (fn [_ t e job-id] (throw e))
                           tl/backoff-until-task-start! (fn [_])
                           tl/backoff-until-covered! (fn [_])
                           tl/backoff-when-drained! (fn [_])
                           tl/start-task-lifecycle! (fn [_ _])]
               (let [_ (reset! checkpoint-store {})
                     gen-state (-> {:messenger messenger
                                    :peer-group pg
                                    :initial-replica initial-replica
                                    :peer-config peer-config
                                    ;; must be reset on every command
                                    :written-to-ch written-to-ch
                                    :checkpoints checkpoint-store
                                    :peer-ids (generate-peer-ids n-peers)
                                    :expected-outputs #{}
                                    :job-ids #{}
                                    :datastore {}
                                    :event-queues {}
                                    :log []}
                                   (add-jobs jobs))
                     completion-commands (complete-job-actions gen-state)
                     final-state (play-run gen-state (concat commands completion-commands))] 
                 (println "Final checkpoint is " @checkpoint-store)
                 (is (:completed? final-state))
                 (is (= (:expected-outputs final-state)
                        (set (remove keyword? (mapcat :written (vals (:peer-states final-state)))))))))))))
