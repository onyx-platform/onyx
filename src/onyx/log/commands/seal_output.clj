(ns onyx.log.commands.seal-output
  (:require [clojure.set :refer [union]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.extensions :as extensions]))

(defn all-outputs-sealed? [replica job]
  (let [all (get-in replica [:output-tasks job])
        sealed (get-in replica [:sealed-outputs job])]
    (= (into #{} all) (into #{} sealed))))

(defn remove-job-attrs [replica args]
  (-> replica
      (update-in [:exhausted-inputs] dissoc (:job args))
      (update-in [:sealed-outputs] dissoc (:job args))))

(defn complete-tasks [replica args tasks]
  (reduce
   (fn [new task]
     (let [peers (get-in new [:allocations (:job args) task])]
       (-> new
           ;; Scheduler TODO: Move from :jobs to :completed-jobs
           (update-in [:completions (:job args)] conj task)
           (update-in [:completions (:job args)] vec)
           (update-in [:allocations (:job args)] dissoc task)
           (update-in [:peer-state] merge (into {} (map (fn [p] {p :idle}) peers))))))
   replica
   tasks))

(defmethod extensions/apply-log-entry :seal-output
  [{:keys [args]} replica]
  (let [new (update-in replica [:sealed-outputs (:job args)] union #{(:task args)})]
    (if (all-outputs-sealed? new (:job args))
      (let [tasks (get-in replica [:tasks (:job args)])]
        (-> new
            (remove-job-attrs args)
            (complete-tasks args tasks)))
      new)))

(defmethod extensions/replica-diff :seal-output
  [{:keys [args]} old new]
  {:job-completed? (not= (get-in old [:allocations (:job args)])
                         (get-in new [:allocations (:job args)]))
   :job (:job args)})

(defmethod extensions/reactions :seal-output
  [{:keys [args]} old new diff state]
  (when (cjs/volunteer-via-sealed-output? old new diff state)
    (do ;; SCHEDULER TODO: << Removed volunteer >>
      nil)))

(defmethod extensions/fire-side-effects! :seal-output
  [{:keys [args]} old new diff state]
  (let [{:keys [job]} (common/peer->allocated-job (:allocations old) (:id state))]
    (if (and (:job-completed? diff) (= (:job diff) job))
      (do (when-let [lc (:lifecycle state)]
            (component/stop @lc)
            (assoc state :lifecycle nil)))
      state)))

