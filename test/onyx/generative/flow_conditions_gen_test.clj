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

(def gen-tasks-and-subset
  "Returns a vector of two elements.
   The first represents a seq of downstream
   tasks, and the second represents a subset
   of that seq."
  (gen/bind
   (gen/vector gen/keyword)
   (fn [downstream]
     (gen/bind
      (gen/choose 0 (count downstream))
      (fn [size]
        (gen/bind
         (gen/shuffle (take size downstream))
         (fn [flow-to]
           (gen/return [downstream flow-to]))))))))

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

(deftest limit-downstream-results
  (checking
   "The matching flow condition limits the downstream selection"
   (times 50)
   [[downstream flow-to] gen-tasks-and-subset]
   (let [task :a
         fcs [{:flow/from task
               :flow/to flow-to
               :flow/predicate ::true-pred}]
         compiled (c/compile-fc-norms fcs task)
         results
         (t/route-data
          {:onyx.core/compiled-norm-fcs compiled} nil nil fcs downstream)]
     (is (= (into #{} flow-to) (:flow results)))
     (is (nil? (:action results))))))

(deftest conj-downstream-tasks-together
  (checking
   "It joins the :flow/to tasks together"
   (times 50)
   [downstream (gen/not-empty (gen/vector gen/keyword))
    size (gen/choose 0 (count downstream))
    flow-conditions (gen/not-empty
                     (gen/vector
                      (gen/hash-map
                       :flow/from (gen/return :a)
                       :flow/to (gen/fmap #(vec (take size %)) (gen/shuffle downstream))
                       :flow/predicate (gen/return ::true-pred))))]
   (let [compiled (c/compile-fc-norms flow-conditions :a)
         results
         (t/route-data
          {:onyx.core/compiled-norm-fcs compiled} nil nil flow-conditions downstream)]
     (is (= (into #{} (mapcat :flow/to flow-conditions)) (:flow results)))
     (is (nil? (:action results))))))

(def predicates
  {true ::true-red
   false ::false-pred})

(defn flow-condition-gen [from-task]
  (gen/hash-map
   :flow/from (gen/return from-task)
   :flow/to (gen/one-of [(gen/return :all) (gen/return :none) (gen/vector gen/keyword)])
   :flow/exclude-keys (gen/vector gen/keyword)))

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
           true-gen (gen/hash-map :flow/predicate g)]
       (gen/fmap #(merge v %) true-gen)))))

(defn short-circuit-flow-condition-gen [base-gen]
  (gen/bind
   base-gen
   (fn [v]
     (let [g (gen/return true)
           true-gen (gen/hash-map :flow/short-circuit? g)]
       (gen/fmap #(merge v %) true-gen)))))

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


(clojure.pprint/pprint
 (gen/sample
  (->> :A
       (flow-condition-gen)
       (true-flow-condition-gen)
       (exception-flow-condition-gen)
       (retry-flow-condition-gen))))





(deftest no-false-predicate-picks
  (checking
   "It doesn't pick any downstream tasks with false predicates"
   (times 50)
   [true-downstream (gen/fmap (fn [xs] (map (comp keyword str) xs)) (gen/not-empty (gen/vector gen/uuid)))
    false-downstream (gen/fmap (fn [xs] (map (comp keyword str) xs)) (gen/not-empty (gen/vector gen/uuid)))
    size (gen/choose 0 (count true-downstream))
    true-flow-conditions (gen/not-empty
                          (gen/vector
                           (gen/hash-map
                            :flow/from (gen/return :a)
                            :flow/to (gen/fmap #(vec (take size %)) (gen/shuffle true-downstream))
                            :flow/predicate (gen/return ::true-pred))))
    false-flow-conditions (gen/not-empty
                           (gen/vector
                            (gen/hash-map
                             :flow/from (gen/return :a)
                             :flow/to (gen/fmap #(vec (take size %)) (gen/shuffle false-downstream))
                             :flow/predicate (gen/return ::false-pred))))]
   (let [flow-conditions (into true-flow-conditions false-flow-conditions)
         downstream (into true-downstream false-downstream)
         compiled (c/compile-fc-norms flow-conditions :a)
         results
         (t/route-data
          {:onyx.core/compiled-norm-fcs compiled} nil nil flow-conditions downstream)]
     (is (= (into #{} (mapcat :flow/to true-flow-conditions)) (:flow results)))
     (is (nil? (some (into #{} (mapcat :flow/to false-flow-conditions)) (:flow results))))
     (is (nil? (:action results))))))
