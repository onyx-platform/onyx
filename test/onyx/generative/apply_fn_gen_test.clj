(ns onyx.generative.apply-fn-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.peer.transform :as t]
            [onyx.peer.task-lifecycle :refer [persistent-results!]]
            [onyx.api])
  (:import [java.util UUID]))

(defn segment-map []
  (gen/map
   (gen/resize 5 gen/any)
   (gen/resize 5 gen/any)))

(defn gen-segment []
  (gen/hash-map :message (segment-map)))

(deftest singe-segment-return
  (checking
   "A single returned segment attaches to its root"
   (times 30)
   [input (gen-segment)
    output (segment-map)]
   (let [f (fn [segment] output)
         event {:onyx.core/batch [input]}
         rets (t/apply-fn f false event)
         tree (:tree (persistent-results! (:onyx.core/results rets)))]
     (is (= input (:root (first tree))))
     (is (= output (:message (first (:leaves (first tree)))))))))

(deftest multi-segment-return
  (checking
   "Multiple segments can be returned, attached to their root"
   (times 30)
   [input (gen-segment)
    output (gen/vector (segment-map))]
   (let [f (fn [segment] output)
         event {:onyx.core/batch [input]}
         rets (t/apply-fn f false event)
         tree (:tree (persistent-results! (:onyx.core/results rets)))]
     (is (= input (:root (first tree))))
     (is (= output (map :message (:leaves (first tree))))))))

(deftest multi-batch-size-single-rets
  (checking
   "It generalizes with a bigger batch size for single returns"
   (times 30)
   ;; Uses a unique mapping of keys to values to look up an
   ;; input for an output with no collisions.
   [input (gen/vector (gen/hash-map
                       :message
                       (gen/not-empty
                        (gen/map
                         (gen/fmap (fn [v] (UUID/randomUUID)) (gen/tuple))
                         (gen/resize 10 gen/any)))))
    output (gen/vector (segment-map)
                       (count input))]
   (let [mapping (zipmap (map :message input) output)
         f (fn [segment] (get mapping segment))
         event {:onyx.core/batch input}
         rets (t/apply-fn f false event)
         tree (:tree (persistent-results! (:onyx.core/results rets)))]
     (is (= input (map :root tree)))
     (is (= output (map :message (mapcat :leaves tree)))))))

(deftest parameterized-functions
  (checking
   "Functions can be parameterized via the event map"
   (times 30)
   [input (gen/vector (gen-segment))
    params (gen/vector (gen/resize 10 gen/any))]
   (let [f (fn [& args] {:result (or (butlast args) [])})
         event {:onyx.core/batch input :onyx.core/params params}
         rets (t/apply-fn f false event)
         tree (:tree (persistent-results! (:onyx.core/results rets)))]
     (is (every? (partial = params) (map :result (map :message (mapcat :leaves tree))))))))

(deftest throws-exception
  (checking
   "Functions that throw exceptions pass the exception object back"
   (times 30)
   [input (gen/vector (gen-segment))]
   (let [f (fn [segment] (throw (ex-info "exception" {:val 42})))
         event {:onyx.core/batch input}
         rets (t/apply-fn f false event)
         tree (:tree (persistent-results! (:onyx.core/results rets)))
         messages (map :message (:leaves (first tree)))]
     (is (= input (map :root tree)))
     (is (every? #(= clojure.lang.ExceptionInfo (type %)) messages))
     (is (every? #(= {:val 42} (ex-data (:exception %))) (map ex-data messages))))))

(deftest bulk-functions
  (checking
   "Bulk functions call the function, but always return their inputs"
   (times 30)
   [input (gen/not-empty (gen/vector (gen-segment)))
    output (gen/resize 10 gen/any)]
   (let [called? (atom false)
         f (fn [segment] (reset! called? true) output)
         event {:onyx.core/batch input}
         rets (t/apply-fn f true event)
         tree (:tree (persistent-results! (:onyx.core/results rets)))]
     (is (= input (map :root tree)))
     (is (= (map :message input) (map :message (mapcat :leaves tree))))
     (is @called?))))
