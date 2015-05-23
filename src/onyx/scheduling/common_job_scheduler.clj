(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre :refer [info]]))

(defn job-upper-bound [replica job]
  ;; We need to handle a special case here when figuring out the upper saturation limit.
  ;; If this is a job with a grouped task that has already been allocated,
  ;; we can't allocate to the grouped task anymore, even if it's saturation
  ;; level is Infinity.
  (let [tasks (get-in replica [:tasks job])
        grouped-tasks (filter (fn [task] (get-in replica [:flux-policies job task])) tasks)]
    (if (seq (filter (fn [task] (seq (get-in replica [:allocations job task]))) grouped-tasks))
      (apply + (map (fn [task]
                      (if (some #{task} grouped-tasks)
                        ;; Cannot allocate anymore, you have what you have.
                        (count (get-in replica [:allocations job task]))
                        ;; Allocate as much the original task saturation allows since it hasn't
                        ;; been allocated yet.
                        (get-in replica [:task-saturation job task])))
                    tasks))
      (get-in replica [:saturation job] Double/POSITIVE_INFINITY))))

(defn job-lower-bound [replica job]
  ;; Again, we handle the special case of a grouped task that has already
  ;; begun.
  (let [tasks (get-in replica [:tasks job])
        grouped-tasks (filter (fn [task] (get-in replica [:flux-policies job task])) tasks)]
    (if (seq (filter (fn [task] (seq (get-in replica [:allocations job task]))) grouped-tasks))
      (apply + (map (fn [task]
                      (if (some #{task} grouped-tasks)
                        ;; Cannot allocate anymore, you have what you have.
                        (count (get-in replica [:allocations job task]))
                        ;; Grab the absolute minimum for this task, no constraints.
                        (get-in replica [:min-required-peers job task])))
                    tasks))
      (apply + (vals (get-in replica [:min-required-peers job]))))))

(defn job-coverable? [replica job n]
  (>= n (job-lower-bound replica job)))

(defmulti job-offer-n-peers
  (fn [replica]
    (:job-scheduler replica)))

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(defmulti sort-job-priority
  (fn [replica jobs]
    (:job-scheduler replica)))

(defmethod job-offer-n-peers :default
  [replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod claim-spare-peers :default
  [replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod sort-job-priority :default
  [replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defn current-job-allocations [replica]
  (into {}
        (map (fn [j]
               {j (count (remove nil? (apply concat (vals (get-in replica [:allocations j])))))})
             (:jobs replica))))

(defn current-task-allocations [replica]
  (into
   {}
   (map (fn [j]
          {j (into {}
                   (map (fn [t]
                          {t (count (filter identity (get-in replica [:allocations j t])))})
                        (get-in replica [:tasks j])))})
        (:jobs replica))))

(defn job-claim-peers [replica job-offers]
  (reduce-kv
   (fn [all j n]
     (if (job-coverable? replica j n)
       (let [sat (job-upper-bound replica j)]
         (assoc all j (min sat n)))
       (assoc all j 0)))
   {}
   job-offers))

(defn reallocate-peers [origin-replica displaced-peers max-utilization]
  (loop [peer-pool displaced-peers
         replica origin-replica]
    (let [candidate-jobs (remove
                          nil?
                          (mapcat
                           (fn [job]
                             (let [current (get (current-task-allocations replica) job)
                                   desired (cts/task-distribute-peer-count origin-replica job (get max-utilization job))
                                   tasks (get-in replica [:tasks job])]
                               (map (fn [t]
                                      (when (< (or (get current t) 0) (get desired t))
                                        [job t]))
                                    tasks)))
                           (sort-job-priority replica (:jobs replica))))]
      (if (and (seq peer-pool) (seq candidate-jobs))
        (recur (rest peer-pool)
               (let [removed (common/remove-peers replica (first peer-pool))
                     reset-state (assoc-in removed [:peer-state (first peer-pool)] :warming-up)]
                 (-> reset-state
                     (update-in [:allocations
                                 (ffirst candidate-jobs)
                                 (second (first candidate-jobs))]
                                conj (first peer-pool))
                     (update-in [:allocations
                                 (ffirst candidate-jobs)
                                 (second (first candidate-jobs))]
                                vec))))
        replica))))

(defn find-unused-peers [replica]
  (let [used-peers (apply concat (mapcat vals (vals (get-in replica [:allocations]))))]
    (clojure.set/difference (set (:peers replica)) (set used-peers))))

(defn find-displaced-peers [replica current-allocations max-util]
  (clojure.set/union
   (find-unused-peers replica)
   (remove
    nil?
    (mapcat
     (fn [job]
       (let [overflow (- (get current-allocations job) (get max-util job))]
         (when (pos? overflow)
           (cts/drop-peers replica job overflow))))
     (:jobs replica)))))

(defn exempt-from-acker? [replica job task]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn find-physically-colocated-peers
  "Takes replica and a peer. Returns a set of peers, exluding this peer,
   that reside on the same physical machine."
  [replica peer]
  (let [peers (remove (fn [p] (= p peer)) (:peers replica))
        peer-site (extensions/get-peer-site replica peer)]
    (filter
     (fn [p]
       (= (extensions/get-peer-site replica p) peer-site))
     peers)))

(defn sort-acker-candidates
  "We try to be smart about which ackers we pick. If we can avoid
   colocating an acker and any peers executing an exempt task,
   we try to. It's a best effort, though, so if it's not possible
   we proceed anyway."
  [replica peers]
  (sort-by
   (fn [peer]
     (let [colocated-peers (find-physically-colocated-peers replica peer)
           statuses (map
                     #(let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) %)]
                        (exempt-from-acker? replica job task))
                     colocated-peers)]
       (some #{true} statuses)))
   peers))

(defn choose-acker-candidates [replica peers]
  (sort-acker-candidates
   replica
   (remove
    (fn [p]
      (let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) p)]
        (exempt-from-acker? replica job task)))
    peers)))

(defn choose-ackers [replica]
  (reduce
   (fn [result job]
     (let [peers (apply concat (vals (get-in result [:allocations job])))
           pct (or (get-in result [:acker-percentage job]) 10)
           n (int (Math/ceil (* 0.01 pct (count peers))))
           candidates (choose-acker-candidates result peers)]
       (assoc-in result [:ackers job] (vec (take n candidates)))))
   replica
   (:jobs replica)))

(defn deallocate-starved-jobs
  "Strips out allocations from jobs that no longer meet the minimum number
   of peers. This can happen if a peer leaves from a running job."
  [replica]
  (reduce
   (fn [result job]
     (if (< (apply + (map count (vals (get-in result [:allocations job]))))
            (apply + (vals (get-in result [:min-required-peers job]))))
       (update-in result [:allocations] dissoc job)
       result))
   replica
   (:jobs replica)))

(defn reconfigure-cluster-workload [replica]
  (let [job-offers (job-offer-n-peers replica)
        job-claims (job-claim-peers replica job-offers)
        spare-peers (apply + (vals (merge-with - job-offers job-claims)))
        max-utilization (claim-spare-peers replica job-claims spare-peers)
        current-allocations (current-job-allocations replica)
        peers-to-displace (find-displaced-peers replica current-allocations max-utilization)
        deallocated (deallocate-starved-jobs replica)]
    (choose-ackers (reallocate-peers deallocated peers-to-displace max-utilization))))

(clojure.pprint/pprint (:allocations (reconfigure-cluster-workload replica)))

(def replica
  {:exempt-tasks
   {:j3 [],
    :j2 [],
    :j1 []},
   :peer-sites
   {:p11 {:port 1, :address 1},
    :p2 {:port 1, :address 1},
    :p4 {:port 1, :address 1},
    :p3 {:port 1, :address 1},
    :p5 {:port 1, :address 1},
    :p8 {:port 1, :address 1},
    :p9 {:port 1, :address 1},
    :p12 {:port 1, :address 1},
    :p1 {:port 1, :address 1},
    :p7 {:port 1, :address 1},
    :p14 {:port 1, :address 1},
    :p15 {:port 1, :address 1},
    :p13 {:port 1, :address 1},
    :p10 {:port 1, :address 1},
    :p16 {:port 1, :address 1},
    :p6 {:port 1, :address 1}},
   :output-tasks
   {:j3
    [:t9],
    :j2
    [:t6],
    :j1
    [:t3]},
   :job-scheduler :onyx.job-scheduler/balanced,
   :ackers
   {:j2
    [:p5],
    :j1 [:p13]},
   :saturation
   {:j3 42000,
    :j2 42000,
    :j1 42000},
   :task-percentages
   {:j3
    {:t7 25,
     :t8 25,
     :t9 50},
    :j2
    {:t4 25,
     :t5 25,
     :t6 50},
    :j1
    {:t1 25,
     :t2 37.5,
     :t3 37.5}},
   :peers
   [:p11
    :p2
    :p4
    :p3
    :p5
    :p8
    :p9
    :p12
    :p1
    :p7
    :p14
    :p15
    :p13
    :p10
    :p16
    :p6],
   :acker-exclude-outputs
   {:j3 false,
    :j2 false,
    :j1 false},
   :min-required-peers
   {:j3
    {:t7 1,
     :t8 1,
     :t9 1},
    :j2
    {:t4 1,
     :t5 1,
     :t6 1},
    :j1
    {:t1 1,
     :t2 1,
     :t3 1}},
   :accepted {},
   :jobs
   [:j1
    :j2],
   :tasks
   {:j3 [:t7 :t8 :t9],
    :j2 [:t4 :t5 :t6],
    :j1 [:t1 :t2 :t3]},
   :pairs
   {:p11 :p6,
    :p2 :p3,
    :p4 :p8,
    :p3 :p5,
    :p5 :p11,
    :p8 :p9,
    :p9 :p1,
    :p12 :p2,
    :p1 :p16,
    :p7 :p12,
    :p14 :p7,
    :p15 :p14,
    :p13 :p15,
    :p10 :p13,
    :p16 :p10,
    :p6 :p4},
   :flux-policies
   {:j3 {},
    :j2 {},
    :j1 {}},
   :messaging {:onyx.messaging/impl :dummy-messenger},
   :allocations
   {:j2
    {:t6 [:p5 :p8 :p10 :p6],
     :t5
     [:p7 :p9],
     :t4 [:p15 :p4]},
    :j1
    {:t3 [:p13 :p11 :p12],
     :t2 [:p16 :p1 :p3],
     :t1 [:p14 :p2]}},
   :killed-jobs [:j3],
   :prepared {},
   :input-tasks
   {:j3
    [:t7],
    :j2
    [:t4],
    :j1
    [:t1]},
   :acker-percentage
   {:j3 1,
    :j2 1,
    :j1 1},
   :peer-state
   {:p6 :warming-up,
    :p9 :warming-up,
    :p4 :warming-up,
    :p3 :warming-up,
    :p12 :warming-up,
    :p2 :warming-up},
   :acker-exclude-inputs {:j3 false,:j2 false,:j1 false},
   :task-schedulers
   {:j3
    :onyx.task-scheduler/percentage,
    :j2
    :onyx.task-scheduler/percentage,
    :j1
    :onyx.task-scheduler/percentage},
   :task-saturation
   {:j3 {:t7 42000,:t8 42000,:t9 42000},
    :j2 {:t4 42000,:t5 42000,:t6 42000},
    :j1 {:t1 1,:t2 42000,:t3 42000}}})