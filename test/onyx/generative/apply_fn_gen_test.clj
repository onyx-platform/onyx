(ns onyx.generative.apply-fn-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.peer.task-lifecycle :as t]
            [onyx.api]))

(deftest singe-segment-return
  (checking
   "A single returned segment attaches to its root"
   (times 30)
   [input (gen/hash-map :message (gen/map gen/any gen/any))
    output (gen/map gen/any gen/any)]
   (let [f (fn [segment] output)
         event {:onyx.core/batch [input]}
         rets (t/apply-fn f false event)
         tree (:tree (t/persistent-results! (:onyx.core/results rets)))]
     (is (= input (:root (first tree))))
     (is (= output (:message (first (:leaves (first tree)))))))))

(deftest multi-segment-return
  (checking
   "Multiple segments can be returned, attached to their root"
   (times 30)
   [input (gen/hash-map :message (gen/map gen/any gen/any))
    output (gen/vector (gen/map gen/any gen/any))]
   (let [f (fn [segment] output)
         event {:onyx.core/batch [input]}
         rets (t/apply-fn f false event)
         tree (:tree (t/persistent-results! (:onyx.core/results rets)))]
     (is (= input (:root (first tree))))
     (is (= output (map :message (:leaves (first tree))))))))

(deftest multi-batch-size-single-rets
  (checking
   "It generalizes with a bigger batch size for single returns"
   (times 20)
   [input (gen/vector (gen/hash-map :message (gen/not-empty (gen/map gen/uuid gen/any))))
    ;; Uses a UUID as the key to ensure input -> output
    ;; mapping is unique for all inputs
    output (gen/vector (gen/map gen/any gen/any)
                       (count input))]
   (let [mapping (zipmap (map :message input) output)
         f (fn [segment] (get mapping segment))
         event {:onyx.core/batch input}
         rets (t/apply-fn f false event)
         tree (:tree (t/persistent-results! (:onyx.core/results rets)))]
     (is (= input (map :root tree)))
     (is (= output (map :message (mapcat :leaves tree)))))))

(deftest parameterized-functions
  (checking
   "Functions can be parameterized via the event map"
   (times 30)
   [input (gen/vector (gen/hash-map :message (gen/map gen/any gen/any)))
    params (gen/vector gen/any)]
   (let [f (fn [& args] {:result (or (butlast args) [])})
         event {:onyx.core/batch input :onyx.core/params params}
         rets (t/apply-fn f false event)
         tree (:tree (t/persistent-results! (:onyx.core/results rets)))]
     (is (every? (partial = params) (map :result (map :message (mapcat :leaves tree))))))))

(deftest throws-exception
  (checking
   "Functions that throw exceptions pass the exception object back"
   (times 30)
   [input (gen/vector (gen/hash-map :message (gen/map gen/any gen/any)))]
   (let [f (fn [segment] (throw (ex-info "exception" {:val 42})))
         event {:onyx.core/batch input}
         rets (t/apply-fn f false event)
         tree (:tree (t/persistent-results! (:onyx.core/results rets)))
         messages (map :message (:leaves (first tree)))]
     (is (= input (map :root tree)))
     (is (every? #(= clojure.lang.ExceptionInfo (type %)) messages))
     (is (every? #(= {:val 42} (ex-data (:exception %))) (map ex-data messages))))))
