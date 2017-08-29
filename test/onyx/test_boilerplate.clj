(ns onyx.test-boilerplate
  (:require [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.plugin.null]
            [onyx.job :refer [add-task]]
            [onyx.tasks.core-async]
            [onyx.tasks.seq]
            [onyx.tasks.null]
            [onyx.tasks.function]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [schema.core :as s]))

(s/defn windowed-task
  [task-name :- s/Keyword opts sync-fn]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/type :function
                             :onyx/fn :clojure.core/identity
                             :onyx/max-peers 1
                             :onyx/doc "Basic windowed task"}
                            opts)
           :windows [{:window/id :collect-segments
                      :window/task task-name
                      :window/type :global
                      :window/aggregation :onyx.windowing.aggregation/conj
                      ;; FIXME, do not allow window-key to be set for global?
                      ;:window/window-key :event-time
                      }]
           :triggers [{:trigger/window-id :collect-segments
                       
                       :trigger/fire-all-extents? true
                       :trigger/on :onyx.triggers/segment
                       :trigger/threshold [1 :elements]
                       :trigger/sync sync-fn}]}})

(defn build-job [workflow compact-job task-scheduler]
  (reduce (fn [job {:keys [name task-opts type args] :as task}]
            (case type
              :seq (add-task job (apply onyx.tasks.seq/input-serialized name task-opts (:input task) args))
              :windowed (add-task job (apply windowed-task name task-opts args))
              :fn (add-task job (apply onyx.tasks.function/function name task-opts args))
              :null-out (add-task job (apply onyx.tasks.null/output name task-opts args))
              :async-out (add-task job (apply onyx.tasks.core-async/output name task-opts (:chan-size task) args))))
          {:workflow workflow
           :catalog []
           :lifecycles []
           :triggers []
           :windows []
           :task-scheduler task-scheduler}
          compact-job))

(defn run-test-job [job n-peers]
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]
    (with-test-env [test-env [n-peers env-config peer-config]]
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config job)
            _ (assert job-id "Job was not successfully submitted")
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            out-channels (onyx.plugin.core-async/get-core-async-channels job)]
        (into {} 
              (map (fn [[k chan]]
                     [k (take-segments! chan 50)])
                   out-channels))))))

