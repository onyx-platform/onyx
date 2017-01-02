(ns onyx.generative.apply-fn-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.peer.transform :as t]
            [onyx.peer.task-compile :as c]
            [onyx.api])
  (:import [java.util UUID]))

(def segment-map 
  (gen/map
   (gen/resize 5 gen/any)
   (gen/resize 5 gen/any)))

(deftest ^:broken single-segment-return
  (checking
   "A single returned segment attaches to its root"
   (times 15)
   [input segment-map
    output segment-map]
   (let [f (fn [segment] output)
         event {:onyx.core/batch [input]}
         rets (t/apply-fn {:onyx.core/fn f :onyx.core/bulk? false} event)
         tree (:tree (:onyx.core/results rets))]
     (is (= input (:root (first tree))))
     (is (= output (first (:leaves (first tree))))))))

(deftest ^:broken multi-segment-return
  (checking
   "Multiple segments can be returned, attached to their root"
   (times 15)
   [input segment-map
    output (gen/vector segment-map)]
   (let [f (fn [segment] output)
         event {:onyx.core/batch [input]}
         rets (t/apply-fn {:onyx.core/fn f :onyx.core/bulk? false} event)
         tree (:tree (:onyx.core/results rets))]
     (is (= input (:root (first tree))))
     (is (= output (:leaves (first tree)))))))

(deftest ^:broken multi-batch-size-single-rets
  (checking
   "It generalizes with a bigger batch size for single returns"
   (times 15)
   ;; Uses a unique mapping of keys to values to look up an
   ;; input for an output with no collisions.
   [input (gen/vector 
           (gen/not-empty 
            (gen/map
             (gen/fmap (fn [v] (UUID/randomUUID)) (gen/tuple))
             (gen/resize 10 gen/any))))
    output (gen/vector segment-map
                       (count input))]
   (let [mapping (zipmap input output)
         f (fn [segment] (get mapping segment))
         event {:onyx.core/batch input}
         rets (t/apply-fn {:onyx.core/fn f :onyx.core/bulk? false} event)
         tree (:tree (:onyx.core/results rets))]
     (is (= input (map :root tree)))
     (is (= output (mapcat :leaves tree))))))

(deftest ^:broken parameterized-functions
  (checking
   "Functions can be parameterized via the event map"
   (times 15)
   [input (gen/vector segment-map)
    params (gen/vector (gen/resize 10 gen/any))]
   (let [f (fn [& args] {:onyx.core/result (or (butlast args) [])})
         event {:onyx.core/batch input :onyx.core/params params}
         rets (t/apply-fn {:onyx.core/fn f :onyx.core/bulk? false} event)
         tree (:tree (:onyx.core/results rets))]
     (is (every? (partial = params) (map :result (mapcat :leaves tree)))))))

(deftest ^:broken throws-exception
  (checking
   "Functions that throw exceptions pass the exception object back"
   (times 15)
   [input (gen/vector segment-map)]
   (let [f (fn [segment] (throw (ex-info "exception" {:val 42})))
         event {:onyx.core/batch input}
         rets (t/apply-fn {:onyx.core/fn f :onyx.core/bulk? false} event)
         tree (:tree (:onyx.core/results rets))
         messages (:leaves (first tree))]
     (is (= input (map :root tree)))
     (is (every? #(= clojure.lang.ExceptionInfo (type %)) messages))
     (is (every? #(= {:val 42} (ex-data (:exception %))) (map ex-data messages))))))

(deftest ^:broken bulk-functions
  (checking
   "Bulk functions call the function, but always return their inputs"
   (times 15)
   [input (gen/not-empty (gen/vector segment-map))
    output (gen/resize 10 gen/any)]
   (let [called? (atom false)
         f (fn [segment] (reset! called? true) output)
         event {:onyx.core/batch input}
         rets (t/apply-fn {:onyx.core/fn f :onyx.core/bulk? true} event)
         tree (:tree (:onyx.core/results rets))]
     (is (= input (map :root tree)))
     (is (= input (mapcat :leaves tree)))
     (is @called?))))

(deftest ^:broken supplied-params
  (checking
   "Supplied parameters concat in the right order"
   (times 15)
   [fn-params (gen/vector (gen/resize 10 gen/any))
    catalog-params (gen/hash-map gen/keyword (gen/resize 10 gen/any))]
   (let [task-name :a
         catalog-param-keys (keys catalog-params)
         catalog-param-vals (map #(get catalog-params %) catalog-param-keys)
         peer-config {:onyx.peer/fn-params {task-name fn-params}}
         task-map (merge {:onyx/name task-name
                          :onyx/params catalog-param-keys}
                         catalog-params)
         event (c/task-params->event-map {:onyx.core/peer-opts peer-config 
                                          :onyx.core/task-map task-map})]
     (is (= (into fn-params catalog-param-vals)
            (:onyx.core/params event))))))
