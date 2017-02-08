(ns onyx.log.full-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.test-helper :refer [with-test-env load-config]]
            [schema.test]
            [onyx.static.uuid :refer [random-uuid]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.log.curator :as zk]))

(use-fixtures :once schema.test/validate-schemas)

(deftest ^:smoke log-full-test
  (testing "groups all join and watch each other"
    (let [config (load-config)
          onyx-id (random-uuid)
          env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
          peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
          n-groups 20]
      (with-test-env [test-env [0 env-config peer-config]]
        (let [added-group-cfg
              (-> peer-config
                  (assoc-in [:onyx.messaging.aeron/embedded-driver?] false))
              groups (doall (map
                             (fn [n]
                               (onyx.api/start-peer-group added-group-cfg))
                             ;; 1 group starts up during with-test-env, so only start
                             ;; n - 1.
                             (range (dec n-groups))))
              ch (chan n-groups)
              replica (loop [replica (extensions/subscribe-to-log (:log (:env test-env)) ch)]
                        (let [entry (<!! ch)
                              new-replica (extensions/apply-log-entry entry replica)]
                          (if (< (count (:pairs new-replica)) n-groups)
                            (recur new-replica)
                            new-replica)))]
          (is (= {} (:prepared replica)))
          (is (= {} (:accepted replica)))
          (is (= (set (keys (:pairs replica))) (set (vals (:pairs replica)))))
          (is (= n-groups (count (:groups replica))))
          (doseq [g groups]
            (onyx.api/shutdown-peer-group g)))))))
