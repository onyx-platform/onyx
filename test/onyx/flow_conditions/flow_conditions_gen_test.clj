(ns onyx.flow-conditions.flow-conditions-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.flow-conditions.fc-routing :as r]
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
           true-gen (gen/hash-map :flow/predicate g
                                  :pred/val (gen/return true))]
       (gen/fmap #(merge v %) true-gen)))))

(defn false-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/return ::false-pred)
           false-gen (gen/hash-map :flow/predicate g
                                   :pred/val (gen/return false))]
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

(defn post-transformation-flow-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/transform (gen/return ::post-transform))]
       (gen/fmap #(merge v %) g)))))

(defn retry-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/hash-map :flow/action (gen/return :retry)
                           :flow/short-circuit? (gen/return true)
                           :flow/to (gen/return nil))]
       (gen/fmap #(merge v %) g)))))

(defn maybe-predicate [g]
  (gen/frequency
   [[1 (true-flow-condition-gen g)]
    [1 (false-flow-condition-gen g)]]))

(deftest nil-flow-conditions
  "No flow condition routes to all downstream tasks"
  (let [downstream [:a :b]
        route (r/route-data nil nil nil nil downstream)]
    (is (= downstream (:flow route)))
    (is (nil? (:action route)))))

(deftest nil-flow-conditions-exception
  "No flow conditions with an exception rethrows the exception"
  (let [e (ex-info "hih" {})
        wrapped-e (ex-info "" {:exception e})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (r/route-data nil nil wrapped-e nil [:a :b])))))

(deftest conj-downstream-tasks-together
  (checking
   "It joins the :flow/to tasks together and limits selection"
   (times 30)
   [flow-conditions
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-tasks-gen)
                      (true-flow-condition-gen))))
    other-downstream-tasks (gen/vector gen/keyword)]
   (let [compiled (c/compile-fc-norms flow-conditions :a)
         target-tasks (mapcat :flow/to flow-conditions)
         downstream (into other-downstream-tasks target-tasks)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil flow-conditions downstream)]
     (is (= (into #{} target-tasks) (into #{} (:flow results))))
     (is (nil? (:action results))))))

(deftest no-false-predicate-picks
  (checking
   "It doesn't pick any downstream tasks with false predicates"
   (times 30)
   [true-fcs
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (true-flow-condition-gen)))
    false-fcs
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (false-flow-condition-gen)))]
   (let [flow-conditions (into true-fcs false-fcs)
         downstream (mapcat :flow/to true-fcs false-fcs)
         compiled (c/compile-fc-norms flow-conditions :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil flow-conditions downstream)]
     (is (= (into #{} (mapcat :flow/to true-fcs)) (into #{} (:flow results))))
     (is (nil? (:action results))))))

(deftest short-circuit
  (checking
   "It stops searching when it finds a short circuit true pred"
   (times 30)
   [false-1
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (false-flow-condition-gen)
                     (short-circuit-flow-condition-gen)))
    true-1
    (gen/not-empty (gen/vector (->> :a
                                    (flow-condition-gen)
                                    (flow-to-tasks-gen)
                                    (true-flow-condition-gen)
                                    (short-circuit-flow-condition-gen))))
    false-2
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (false-flow-condition-gen)
                     (short-circuit-flow-condition-gen)))
    mixed-1
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (maybe-predicate)))]
   (let [flow-conditions (into (into (into false-1 true-1) false-1) mixed-1)
         downstream (mapcat :flow/to flow-conditions)
         compiled (c/compile-fc-norms flow-conditions :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil flow-conditions downstream)]
     (is (= (into #{} (:flow/to (first true-1))) (into #{} (:flow results))))
     (is (nil? (:action results))))))

(deftest retry-action
  (checking
   "Using a retry action with a true predicate flows to nil"
   (times 30)
   [retry-false
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (false-flow-condition-gen)
                     (retry-flow-condition-gen)))
    retry-true
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (true-flow-condition-gen)
                      (retry-flow-condition-gen))))
    retry-mixed
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (retry-flow-condition-gen)
                     (maybe-predicate)))
    ss
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (short-circuit-flow-condition-gen)
                     (maybe-predicate)))
    exceptions
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (exception-flow-condition-gen)
                     (maybe-predicate)))
    mixed
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (maybe-predicate)))]
   (let [all [retry-false retry-true retry-mixed ss exceptions mixed]
         flow-conditions (apply concat all)
         downstream (mapcat :flow/to flow-conditions)
         compiled (c/compile-fc-norms flow-conditions :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil flow-conditions downstream)]
     (is (not (seq (:flow results))))
     (is (= :retry (:action results))))))

(deftest key-exclusion
  (checking
   "Matched predicates excluded keys are conj'ed together"
   (times 30)
   [mixed
    (gen/vector
     (gen/one-of [(->> :a
                       (flow-condition-gen)
                       (flow-to-tasks-gen)
                       (true-flow-condition-gen))
                  (->> :a
                       (flow-condition-gen)
                       (flow-to-tasks-gen)
                       (false-flow-condition-gen))]))]
   (let [downstream (mapcat :flow/to mixed)
         compiled (c/compile-fc-norms mixed :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil mixed downstream)
         matches (filter :pred/val mixed)
         excluded-keys (mapcat :flow/exclude-keys matches)]
     (is (into #{} excluded-keys) (:exclusions results))
     (is (nil? (:action results))))))

(deftest matching-all
  (checking
   "A :flow/to of :all that passes its predicate matches everything"
   (times 30)
   [false-all
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-all-gen)
                     (false-flow-condition-gen)))
    true-all
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-all-gen)
                      (true-flow-condition-gen))))
    mixed-all
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-all-gen)
                     (maybe-predicate)))
    mixed-none
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-none-gen)
                     (maybe-predicate)))]
   (let [fcs [false-all true-all mixed-all mixed-none]
         downstream (mapcat :flow/to fcs)
         compiled (c/compile-fc-norms fcs :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil fcs downstream)]
     (is (= (into #{} downstream) (into #{} (:flow/to results))))
     (is (nil? (:action results))))))

(deftest matching-none
  (checking
   "A :flow/to of :none that passes its pred matches nothing"
   (times 30)
   [false-none
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-none-gen)
                     (false-flow-condition-gen)))
    true-none
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-none-gen)
                      (true-flow-condition-gen))))
    mixed-none
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-none-gen)
                     (maybe-predicate)))
    mixed
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (maybe-predicate)))]
   (let [fcs [false-none true-none mixed-none mixed]
         downstream (mapcat :flow/to fcs)
         compiled (c/compile-fc-norms fcs :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil fcs downstream)]
     (is (not (seq (:flow/to results))))
     (is (nil? (:action results))))))

(deftest post-transformation
  (checking
   "A :flow/post-transform is returned for exception predicates that define it"
   (times 30)
   [false-xform
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (exception-flow-condition-gen)
                     (post-transformation-flow-gen)
                     (false-flow-condition-gen)))
    true-xform
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-tasks-gen)
                      (exception-flow-condition-gen)
                      (post-transformation-flow-gen)
                      (true-flow-condition-gen))))
    mixed
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (maybe-predicate)))]
   (let [fcs [false-xform true-xform mixed]
         downstream (mapcat :flow/to fcs)
         compiled (c/compile-fc-norms fcs :a)
         event {:onyx.core/compiled-norm-fcs compiled}
         results (r/route-data event nil nil fcs downstream)
         xform (:flow/post-transformation (first true-xform))]
     (is (= xform (:post-transformation results)))
     (is (nil? (:action results))))))
