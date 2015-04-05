(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre]))

(defn unbounded-jobs [replica balanced]
  (filter
   (fn [[job _]]
     (= (get-in replica [:saturation job]) Double/POSITIVE_INFINITY))
   balanced))

(defn adjust-with-overflow [replica balanced]
  (reduce
   (fn [result [job n]]
     (let [sat (or (get-in replica [:saturation job]))
           extra (- n sat)]
       (if (pos? extra)
         (-> result
             (update-in [:overflow] + extra)
             (update-in [:jobs] conj [job sat]))
         (-> result
             (update-in [:jobs] conj [job n])))))
   {:overflow 0 :jobs []}
   balanced))

(defn at-least-one-active? [replica peers]
  (->> peers
       (map #(get-in replica [:peer-state %]))
       (filter (partial = :active))
       (seq)))

(defn job-covered? [replica job]
  (let [tasks (get-in replica [:tasks job])
        active? (partial at-least-one-active? replica)]
    (every? identity (map #(active? (get-in replica [:allocations job %])) tasks))))

(defn current-job-allocations [replica]
  (into {}
        (map (fn [j]
               {j (apply + (map count (vals (get-in replica [:allocations j]))))})
             (:jobs replica))))

(defn current-task-allocations [replica]
  (into
   {}
   (map (fn [j]
          {j (into {} (map (fn [[t a]] {t (count a)}) (get-in replica [:allocations j])))})
        (:jobs replica))))

(defn job->task-claims [replica job-offers]
  (reduce-kv
   (fn [all j claim]
     (assoc all j (cts/task-claim-n-peers replica j claim)))
   {}
   job-offers))

(defn reallocate-peers [origin-replica displaced-peers max-utilization]
  (loop [peer-pool displaced-peers
         replica origin-replica]
    (let [candidate-jobs (filter identity
                                 (mapcat
                                  (fn [job]
                                    (let [current (get (current-task-allocations replica) job)
                                          desired (cts/task-distribute-peer-count replica job (get max-utilization job))
                                          tasks (get-in replica [:tasks job])]
                                      (map
                                       (fn [t]
                                         (when (< (get current t) (get desired t))
                                           [job t]))
                                       tasks)))
                                  (:jobs replica)))]
      (if (and (seq peer-pool) (seq candidate-jobs))
        (recur (rest peer-pool)
               (update-in replica [:allocations
                                   (ffirst candidate-jobs)
                                   (second (first candidate-jobs))]
                          conj (first peer-pool)))
        replica))))

(defn find-displaced-peers [replica max-util]
  (mapcat
   (fn [job]
     (let [overflow (- (get current-allocations-c job) (get max-util job))]
       (when (pos? overflow)
         (cts/drop-peers replica job overflow))))
   (:jobs replica)))

(defmulti select-job
  (fn [{:keys [args]} replica]
    (:job-scheduler replica)))

(defn exempt-from-acker? [replica job task args]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn offer-acker [replica job task args]
  (let [peers (count (apply concat (vals (get-in replica [:allocations job]))))
        ackers (count (get-in replica [:ackers job]))
        pct (get-in replica [:acker-percentage job])
        current-pct (int (Math/ceil (* 10 (double (/ ackers peers)))))]
    (if (and (< current-pct pct) (not (exempt-from-acker? replica job task args)))
      (-> replica
          (update-in [:ackers job] conj (:id args))
          (update-in [:ackers job] vec))
      replica)))

(defmethod select-job :default
  [_ replica]
  (throw (ex-info 
           (format "Job scheduler %s not recognized. Check that you have not supplied a task scheduler instead." 
                   (:job-scheduler replica))
           {:replica replica})))

(defmulti volunteer-via-new-job?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-killed-job?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-sealed-output?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-accept?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-leave?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti reallocate-from-job?
  (fn [scheduler old new state]
    scheduler))

(defmulti job-offer-n-peers :job-scheduler)

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(def replica-g {:job-scheduler :onyx.job-scheduler/greedy
              :jobs [:j1 :j2]
              :tasks {:j1 [:t1 :t2 :t3]
                      :j2 [:t4 :t5]}
              :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                :j2 :onyx.task-scheduler/balanced}
              :saturation {:j1 3}
              :peers [:p1 :p2 :p3 :p4 :p5]})

(def job-offers-g (job-offer-n-peers replica-g))

(def job-claims-g
  (reduce-kv
   (fn [all j claim]
     (assoc all j (cts/task-claim-n-peers replica-g j claim)))
   {}
   job-offers-g))

(def spare-peers-g (apply + (vals (merge-with - job-offers-g job-claims-g))))

(def max-utilization-g (claim-spare-peers replica-g job-claims-g spare-peers-g))

(def task-distribution-g (cts/task-distribute-peer-count replica-g :j1 (:j1 max-utilization-g)))


(def replica-b {:job-scheduler :onyx.job-scheduler/balanced
                :jobs [:j1 :j2]
                :tasks {:j1 [:t1 :t2 :t3 :g :h :i :j :k :l :m :n :o]
                        :j2 [:t4 :t5 :t6]}
                :saturation {:j1 3 :j2 Double/POSITIVE_INFINITY}
                :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                  :j2 :onyx.task-scheduler/balanced}
                :task-saturation {:j2 {:t4 2}}
                :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7]})

(def job-offers-b (job-offer-n-peers replica-b))

(def job-claims-b
  (reduce-kv
   (fn [all j claim]
     (assoc all j (cts/task-claim-n-peers replica-b j claim)))
   {}
   job-offers-b))

(def spare-peers-b (apply + (vals (merge-with - job-offers-b job-claims-b))))

(def max-utilization-b (claim-spare-peers replica-b job-claims-b spare-peers-b))

(def task-distribution-b (cts/task-distribute-peer-count replica-b :j2 (:j2 max-utilization-b)))




(def replica-p {:job-scheduler :onyx.job-scheduler/percentage
                :jobs [:j1 :j2]
                :tasks {:j1 [:t1 :t2 :t3]
                        :j2 [:t4 :t5 :t6]}
                :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                  :j2 :onyx.task-scheduler/balanced}
                :saturation {:j1 3 :j2 Double/POSITIVE_INFINITY}
                :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10]
                :percentages {:j1 40 :j2 60}})

(def job-offers-p (job-offer-n-peers replica-p))

(def job-claims-p
  (reduce-kv
   (fn [all j claim]
     (assoc all j (cts/task-claim-n-peers replica-p j claim)))
   {}
   job-offers-p))

(def spare-peers-p (apply + (vals (merge-with - job-offers-p job-claims-p))))

(def max-utilization-p (claim-spare-peers replica-p job-claims-p spare-peers-p))

(def task-distribution-p (cts/task-distribute-peer-count replica-p :j2 (:j2 max-utilization-p)))



(def replica-pt {:job-scheduler :onyx.job-scheduler/percentage
                 :jobs [:j1 :j2]
                 :tasks {:j1 [:t1 :t2 :t3]
                         :j2 [:t4 :t5 :t6]}
                 :task-schedulers {:j1 :onyx.task-scheduler/percentage
                                   :j2 :onyx.task-scheduler/percentage}
                 :saturation {:j1 0 :j2 Double/POSITIVE_INFINITY}
                 :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10]
                 :percentages {:j1 40 :j2 60}
                 :task-percentages {:j1 {:t1 20 :t2 30 :t3 50}
                                    :j2 {:t4 50 :t5 20 :t6 30}}})

(def job-offers-pt (job-offer-n-peers replica-pt))

(def job-claims-pt
  (reduce-kv
   (fn [all j claim]
     (assoc all j (cts/task-claim-n-peers replica-pt j claim)))
   {}
   job-offers-pt))

(def spare-peers-pt (apply + (vals (merge-with - job-offers-pt job-claims-pt))))

(def max-utilization-pt (claim-spare-peers replica-pt job-claims-pt spare-peers-pt))

(def task-distribution-pt (cts/task-distribute-peer-count replica-pt :j2 (:j2 max-utilization-pt)))


(def replica-c {:job-scheduler :onyx.job-scheduler/percentage
                :jobs [:j1 :j2]
                :tasks {:j1 [:t1 :t2 :t3]
                        :j2 [:t4 :t5 :t6]}
                :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                  :j2 :onyx.task-scheduler/balanced}
                :saturation {:j1 3 :j2 Double/POSITIVE_INFINITY}
                :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10]
                :percentages {:j1 40 :j2 60}
                :allocations {:j1 {:t1 [] :t2 [] :t3 []}
                              :j2 {:t4 [:p1 :p2 :p3 :p4]
                                   :t5 [:p5 :p6 :p7]
                                   :t6 [:p8 :p9 :p10]}}})

(def job-offers-c (job-offer-n-peers replica-c))

(def job-claims-c (job->task-claims replica-c job-offers-c))

(def spare-peers-c (apply + (vals (merge-with - job-offers-c job-claims-c))))

(def max-utilization-c (claim-spare-peers replica-c job-claims-c spare-peers-c))

(def task-distribution-c (cts/task-distribute-peer-count replica-c :j2 (:j2 max-utilization-c)))

(def current-job-allocations-c (current-job-allocations replica-c))

(def current-task-allocations-c (current-task-allocations replica-c))

(def peers-to-displace (find-displaced-peers replica-c max-utilization-c))

