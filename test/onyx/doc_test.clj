(ns onyx.doc-test
  (:require [taoensso.timbre :refer [info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.schema :as schema]
            [schema.core :as s]
            [onyx.information-model :refer [model]]))

(deftest missing-documentation-test
  (is (empty? (remove (apply merge (map :model (vals model))) 
                      (keys defaults)))))

(defn schema-keys [sc]
  (mapv (fn [k]
          (if (= (type k) schema.core.OptionalKey)
            (:k k)
            k)) 
        (remove #{s/Keyword s/Any} 
                (keys sc))))

(deftest catalog-test
  (is (= (set (keys (:model (:catalog-entry model)))) 
         (set (concat (schema-keys schema/base-task-map)
                      (schema-keys schema/partial-grouping-task)
                      (schema-keys schema/partial-input-task)
                      (schema-keys schema/partial-output-task)
                      (schema-keys schema/partial-fn-task))))))

(deftest peer-config-test
  (is (= (set (keys (:model (:peer-config model)))) 
         (set (schema-keys schema/PeerConfig)))))

(deftest lifecycle-call-test
  (is (= (set (keys (:model (:lifecycle-calls model)))) 
         (set (schema-keys schema/LifecycleCall)))))

(deftest flow-conditions-test
  (is (= (set (keys (:model (:flow-conditions-entry model)))) 
         (set (schema-keys schema/FlowCondition)))))

(deftest window-test
  (is (= (set (keys (:model (:window-entry model)))) 
         (set (schema-keys schema/Window)))))

(deftest trigger-test
  (is (= (set (keys (:model (:trigger-entry model)))) 
         (set (schema-keys schema/Trigger)))))

(deftest env-test
  (is (= (set (keys (:model (:env-config model)))) 
         (set (schema-keys schema/EnvConfig)))))

(deftest state-aggregation-test
  (is (= (set (keys (:model (:state-aggregation model)))) 
         (set (schema-keys schema/StateAggregationCall)))))
