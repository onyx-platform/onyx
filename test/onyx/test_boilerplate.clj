(ns onyx.test-boilerplate
  (:require [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.plugin.null]
            [onyx.job :refer [add-task]]
            [onyx.tasks.core-async]
            [onyx.tasks.seq]
            [onyx.tasks.null]
            [onyx.tasks.function]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]))

(defn build-job [workflow compact-job task-scheduler]
  (reduce (fn [job {:keys [name task-opts type] :as task}]
            (case type
              :seq (add-task job (onyx.tasks.seq/input-serialized name task-opts (:input task)))
              :fn (add-task job (onyx.tasks.function/function name task-opts))
              :null-out (add-task job (onyx.tasks.null/output name task-opts))
              :async-out (add-task job (onyx.tasks.core-async/output name task-opts (:chan-size task)))))
          {:workflow workflow
           :catalog []
           :lifecycles []
           :task-scheduler task-scheduler}
          compact-job))

(defn run-test-job [job n-peers]
  (let [id (java.util.UUID/randomUUID)
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
                     [k (take-segments! chan)])
                   out-channels))))))

