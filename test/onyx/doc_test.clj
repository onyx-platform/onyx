(ns onyx.doc-test
  (:require [taoensso.timbre :refer [info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.schema :as schema]
            [schema.core :as s]
            [onyx.information-model :refer [model model-display-order]]
            [onyx.api]))

(deftest missing-documentation-test
  (is (empty? (remove (apply merge (map :model (vals model))) 
                      (keys defaults)))))

(deftest different-defaults-test
  (let [flattened (apply merge (map :model (vals model)))] 
    (is (= [] 
           (remove (fn [[k v]] 
                     (= (defaults k) 
                        (:default (flattened k))))
                   defaults)))))

(def non-doc-keys 
  #{s/Keyword s/Any 
    schema/UnsupportedTaskMapKey schema/UnsupportedWindowKey 
    schema/UnsupportedFlowKey schema/UnsupportedTriggerKey})

(defn schema-keys [sc]
  (mapv (fn [k]
          (if (= (type k) schema.core.OptionalKey)
            (:k k)
            k)) 
        (remove non-doc-keys 
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
         (set (schema-keys schema/WindowBase)))))

(deftest trigger-test
  (is (= (set (keys (:model (:trigger-entry model)))) 
         (set (schema-keys schema/Trigger)))))

(deftest env-test
  (is (= (set (keys (:model (:env-config model)))) 
         (set (schema-keys schema/EnvConfig)))))

(deftest state-aggregation-test
  (is (= (set (keys (:model (:state-aggregation model)))) 
         (set (schema-keys schema/StateAggregationCall)))))

(deftest check-model-display-order
  (testing "Checks whether all keys in information model are accounted for in ordering used in cheat sheet"
    (is (= (map (fn [k]
                  (set (keys (:model (model k)))))
                (keys model))
           (map (fn [k]
                  (set (model-display-order k)))
                (keys model))))))
