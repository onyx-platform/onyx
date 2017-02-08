(ns onyx.flow-conditions.flow-conditions-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.static.validation :as v]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.peer.task-compile :as c]
            [onyx.api]))

(def min-vec-length 0)

(def max-vec-length 15)

(def true-pred (constantly true))

(def false-pred (constantly false))

(def predicates
  {true ::true-red
   false ::false-pred})

(def post-transformed-segment {:transformed-segment 42})

(defn post-transform [event segment e]
  post-transformed-segment)

(defn gen-sized-kw []
  (gen/resize 10 gen/keyword))

(defn gen-sized-vector []
  (gen/vector (gen-sized-kw) min-vec-length max-vec-length))

(defn flow-condition-gen [from-task]
  (gen/hash-map
   :flow/from (gen/return from-task)
   :flow/exclude-keys (gen-sized-vector)))

(defn merge-gen-maps [gen1 gen2]
  (gen/bind gen1 (fn [m] (gen/fmap #(merge m %) gen2))))

(defn flow-to-all-gen [base-gen]
  (merge-gen-maps base-gen (gen/return {:flow/to :all})))

(defn flow-to-none-gen [base-gen]
  (merge-gen-maps base-gen (gen/return {:flow/to :none})))

(defn flow-to-tasks-gen [base-gen]
  (merge-gen-maps base-gen (gen/hash-map :flow/to (gen-sized-vector))))

(defn true-flow-condition-gen [base-gen]
  (merge-gen-maps base-gen
                  (gen/return {:flow/predicate ::true-pred
                               :pred/val true})))

(defn false-flow-condition-gen [base-gen]
  (merge-gen-maps base-gen
                  (gen/return {:flow/predicate ::false-pred
                               :pred/val false})))

(defn flow-short-circuit-condition-gen [base-gen]
  (merge-gen-maps base-gen (gen/return {:flow/short-circuit? true})))

(defn exception-flow-condition-gen [base-gen]
  (merge-gen-maps base-gen
                  (gen/hash-map
                   :flow/thrown-exception? (gen/return true)
                   :flow/short-circuit? (gen/return true)
                   :flow/post-transform (gen/one-of
                                         [(gen/return nil)
                                          (gen/return ::post-transform)]))))

(defn post-transformation-flow-gen [base-gen]
  (merge-gen-maps base-gen (gen/return {:flow/post-transform ::post-transform})))

(defn retry-flow-condition-gen [base-gen]
  (merge-gen-maps base-gen
                  (gen/hash-map :flow/action (gen/return :retry)
                                :flow/short-circuit? (gen/return true)
                                :flow/to (gen/return nil))))

(defn maybe-predicate [g]
  (gen/frequency
   [[1 (true-flow-condition-gen g)]
    [1 (false-flow-condition-gen g)]]))

(deftest ^:broken nil-flow-conditions
  "No flow condition routes to all downstream tasks"
  (let [downstream [:a :b]
        route (r/route-data {} {:flow-conditions nil :egress-tasks downstream} nil nil)]
    (is (= downstream (:flow route)))
    (is (nil? (:action route)))))

(deftest ^:broken nil-flow-conditions-exception
  "No flow conditions with an exception rethrows the exception"
  (let [e (ex-info "hih" {})
        wrapped-e (ex-info "" {:exception e})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (r/route-data {} 
                               {:compiled-handle-exception-fn (constantly :restart)
                                :flow-conditions nil :egress-tasks [:a :b]} nil wrapped-e)))))

(deftest ^:broken conj-downstream-tasks-together
  (checking
   "It joins the :flow/to tasks together and limits selection"
   (times 5)
   [flow-conditions
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-tasks-gen)
                      (true-flow-condition-gen))))
    other-downstream-tasks (gen-sized-vector)]
   (let [target-tasks (mapcat :flow/to flow-conditions)
         downstream (into other-downstream-tasks target-tasks)
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions flow-conditions})
         results (r/route-data event (:compiled event) nil nil)]
     (is (= (into #{} target-tasks) (into #{} (:flow results))))
     (is (nil? (:action results))))))

(deftest ^:broken no-false-predicate-picks
  (checking
   "It doesn't pick any downstream tasks with false predicates"
   (times 5)
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
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions flow-conditions})
         results (r/route-data event (:compiled event) nil nil)]
     (is (= (into #{} (mapcat :flow/to true-fcs)) (into #{} (:flow results))))
     (is (nil? (:action results))))))

(deftest ^:broken short-circuit
  (checking
   "It stops searching when it finds a short circuit true pred"
   (times 5)
   [false-1
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (false-flow-condition-gen)
                     (flow-short-circuit-condition-gen)))
    true-1
    (gen/not-empty (gen/vector (->> :a
                                    (flow-condition-gen)
                                    (flow-to-tasks-gen)
                                    (true-flow-condition-gen)
                                    (flow-short-circuit-condition-gen))))
    false-2
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (false-flow-condition-gen)
                     (flow-short-circuit-condition-gen)))
    mixed-1
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (maybe-predicate)))]
   (let [flow-conditions (into (into (into false-1 true-1) false-1) mixed-1)
         downstream (mapcat :flow/to flow-conditions)
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions flow-conditions})
         results (r/route-data event (:compiled event) nil nil)]
     (is (= (into #{} (:flow/to (first true-1))) (into #{} (:flow results))))
     (is (nil? (:action results))))))

(deftest ^:broken retry-action
  (checking
   "Using a retry action with a true predicate flows to nil"
   (times 5)
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
                     (flow-short-circuit-condition-gen)
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
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions flow-conditions})
         results (r/route-data event (:compiled event) nil nil)]
     (is (not (seq (:flow results))))
     (is (= :retry (:action results))))))

(deftest ^:broken key-exclusion
  (checking
   "Matched predicates excluded keys are conj'ed together"
   (times 5)
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
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions mixed})
         results (r/route-data event (:compiled event) nil nil)
         matches (filter :pred/val mixed)
         excluded-keys (mapcat :flow/exclude-keys matches)]
     (is (into #{} excluded-keys) (:exclusions results))
     (is (nil? (:action results))))))

(deftest ^:broken matching-all
  (checking
   "A :flow/to of :all that passes its predicate matches everything"
   (times 5)
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
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions fcs})
         results (r/route-data event (:compiled event) nil nil)]
     (is (= (into #{} downstream) (into #{} (:flow/to results))))
     (is (nil? (:action results))))))

(deftest ^:broken matching-none
  (checking
   "A :flow/to of :none that passes its pred matches nothing"
   (times 5)
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
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions fcs})
         results (r/route-data event (:compiled event) nil nil)]
     (is (not (seq (:flow/to results))))
     (is (nil? (:action results))))))

(deftest ^:broken post-transformation
  (checking
   "A :flow/post-transform is returned for exception predicates that define it"
   (times 5)
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
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions fcs})
         results (r/route-data event (:compiled event) nil nil)
         xform (:flow/post-transformation (first true-xform))]
     (is (= xform (:post-transformation results)))
     (is (nil? (:action results))))))

(deftest ^:broken post-transformation-no-invocation
  (checking
   "Post-transformations are not invoked when they are not matched"
   (times 5)
   [fcs
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-tasks-gen)
                      (exception-flow-condition-gen)
                      (post-transformation-flow-gen)
                      (false-flow-condition-gen))))]
   (let [inner-error (ex-info "original error" {})
         message (ex-info "exception" {:exception inner-error})
         downstream (mapcat :flow/to fcs)
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions fcs})
         routes (r/route-data event (:compiled event) nil message)]
     (is (= message (r/flow-conditions-transform message routes fcs event))))))

(deftest ^:broken post-transformation-invocation
  (checking
   "Post-transformations are invoked when they are matched"
   (times 5)
   [fcs
    (gen/not-empty
     (gen/vector (->> :a
                      (flow-condition-gen)
                      (flow-to-tasks-gen)
                      (exception-flow-condition-gen)
                      (post-transformation-flow-gen)
                      (true-flow-condition-gen))))]
   (let [inner-error (ex-info "original error" {})
         message (ex-info "exception" {:exception inner-error})
         downstream (mapcat :flow/to fcs)
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions fcs})
         compiled (:compiled event)
         routes (r/route-data event (:compiled event) nil message)
         transformed (reduce dissoc post-transformed-segment (:exclusions routes))]
     (is (= transformed (r/flow-conditions-transform message routes event compiled))))))

(deftest ^:broken post-transformation-exclusions
  (checking
   "Key exclusions are applied during post transformation"
   (times 5)
   [fcs
    (gen/vector (->> :a
                     (flow-condition-gen)
                     (flow-to-tasks-gen)
                     (maybe-predicate)))]
   (let [segment (zipmap (mapcat :flow/exclude-keys fcs) (repeat 0))
         downstream (mapcat :flow/to fcs)
         event (c/flow-conditions->event-map {:serialized-task {:egress-tasks downstream}
                                              :task :a
                                              :flow-conditions fcs})
         compiled (:compiled event)
         routes (r/route-data event compiled nil segment)
         exclusions (mapcat :flow/exclude-keys (filter :pred/val fcs))
         filtered-segment (reduce dissoc segment exclusions)]
     (is (= filtered-segment (r/flow-conditions-transform segment routes event compiled))))))

(deftest ^:broken none-placed-after-all
  (checking
    ":flow/to :none placed after :flow/to :all"
    (times 5)
    [fcs-all (gen/not-empty
               (gen/vector (->> :a
                                (flow-condition-gen)
                                (flow-to-all-gen))))
     fcs-none (gen/not-empty
                (gen/vector (->> :a
                                 (flow-condition-gen)
                                 (flow-to-none-gen))))]
    (let [fcs (into [] (concat fcs-none fcs-all))]
      (is (thrown? Exception (v/validate-flow-conditions fcs []))))))

(deftest ^:broken none-placed-before-other
  (checking
    ":flow/to :none placed first if there is no :flow/to :all"
    (times 5)
    [fcs-other (gen/not-empty
               (gen/vector (->> :a
                                (flow-condition-gen)
                                (flow-to-tasks-gen))))
     fcs-none (gen/not-empty
                (gen/vector (->> :a
                                 (flow-condition-gen)
                                 (flow-to-none-gen))))]
    (let [fcs (into [] (concat fcs-other fcs-none))]
      (is (thrown? Exception (v/validate-flow-conditions fcs []))))))


(deftest ^:broken short-circuit-placed-before-other
  (checking
    ":flow/short-circuit? true should be placed before other conditions"
    (times 5)
    [fcs-other (gen/not-empty
                 (gen/vector (->> :a
                                  (flow-condition-gen))))
     fcs-short-circuit (gen/not-empty
                        (gen/vector (->> :a
                                         (flow-condition-gen)
                                         (flow-short-circuit-condition-gen))))]
    (let [fcs (into [] (concat fcs-other fcs-short-circuit))]
      (is (thrown? Exception (v/validate-flow-conditions fcs []))))))
