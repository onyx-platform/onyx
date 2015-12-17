(ns onyx.generative.flow-conditions-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.peer.task-lifecycle :as t]
            [onyx.peer.task-compile :as c]
            [onyx.api]))

(def true-pred (constantly true))

(def false-pred (constantly false))

(def predicates
  {true ::true-red
   false ::false-pred})

(defn flow-condition-gen [from-task]
  (gen/hash-map
   :flow/from (gen/return from-task)
   :flow/exclude-keys (gen/vector gen/keyword)))

(defn flow-to-all-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/to (gen/return :all))]
       (gen/fmap #(merge v %) g)))))

(defn flow-to-none-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/to (gen/return :none))]
       (gen/fmap #(merge v %) g)))))

(defn flow-to-tasks-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/to (gen/vector gen/keyword))]
       (gen/fmap #(merge v %) g)))))

(defn true-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/return ::true-pred)
           true-gen (gen/hash-map :flow/predicate g)]
       (gen/fmap #(merge v %) true-gen)))))

(defn false-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/return ::false-pred)
           false-gen (gen/hash-map :flow/predicate g)]
       (gen/fmap #(merge v %) false-gen)))))

(defn short-circuit-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/short-circuit? (gen/return true))]
       (gen/fmap #(merge v %) g)))))

(defn exception-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map
              :flow/thrown-exception? (gen/return true)
              :flow/short-circuit? (gen/return true)
              :flow/post-transform (gen/one-of
                                    [(gen/return nil)
                                     (gen/return ::post-transform)]))]
       (gen/fmap #(merge v %) g)))))

(defn retry-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/action (gen/return :retry))]
       (gen/fmap #(merge v %) g)))))

(deftest nil-flow-conditions
  "No flow condition routes to all downstream tasks"
  (let [downstream [:a :b]
        route (t/route-data nil nil nil nil downstream)]
    (is (= downstream (:flow route)))
    (is (nil? (:action route)))))

(deftest nil-flow-conditions-exception
  "No flow conditions with an exception rethrows the exception"
  (let [e (ex-info "hih" {})
        wrapped-e (ex-info "" {:exception e})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (t/route-data nil nil wrapped-e nil [:a :b])))))

(deftest conj-downstream-tasks-together
  (checking
   "It joins the :flow/to tasks together and limits selection"
   (times 50)
   [flow-conditions (gen/vector (->> :a
                         (flow-condition-gen)
                         (flow-to-tasks-gen)
                         (true-flow-condition-gen)))
    other-downstream-tasks (gen/vector gen/keyword)]
   (let [compiled (c/compile-fc-norms flow-conditions :a)
         target-tasks (mapcat :flow/to flow-conditions)
         downstream (into other-downstream-tasks target-tasks)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (t/route-data event nil nil flow-conditions downstream)]
     (is (= (into #{} target-tasks) (:flow results)))
     (is (nil? (:action results))))))

(deftest no-false-predicate-picks
  (checking
   "It doesn't pick any downstream tasks with false predicates"
   (times 50)
   [true-fcs (gen/vector (->> :a
                              (flow-condition-gen)
                              (flow-to-tasks-gen)
                              (true-flow-condition-gen)))
    false-fcs (gen/vector (->> :a
                               (flow-condition-gen)
                               (flow-to-tasks-gen)
                               (false-flow-condition-gen)))]
   (let [flow-conditions (into true-fcs false-fcs)
         downstream (mapcat :flow/to true-fcs false-fcs)
         compiled (c/compile-fc-norms flow-conditions :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (t/route-data event nil nil flow-conditions downstream)]
     (is (= (into #{} (mapcat :flow/to true-fcs)) (into #{} (:flow results))))
     (is (nil? (:action results))))))
