(ns onyx.generative.lifecycles-gen-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.lifecycles.lifecycle-compile :as lc]))

(def task :my-task)

(def irrelevant-task :not-my-task)

(defn handle-exception-defer [event lifecycle phase e]
  :defer)

(defn handle-exception-restart [event lifecycle phase e]
  :restart)

(defn handle-exception-kill [event lifecycle phase e]
  :kill)

(def defer-calls
  {:lifecycle/handle-exception handle-exception-defer})

(def restart-calls
  {:lifecycle/handle-exception handle-exception-restart})

(def kill-calls
  {:lifecycle/handle-exception handle-exception-kill})

(def defer-lifecycle
  {:lifecycle/task task
   :lifecycle/calls ::defer-calls})

(def restart-lifecycle
  {:lifecycle/task task
   :lifecycle/calls ::restart-calls})

(def kill-lifecycle
  {:lifecycle/task task
   :lifecycle/calls ::kill-calls})

(def restart-irrelevant-lifecycle
  {:lifecycle/task irrelevant-task
   :lifecycle/calls ::restart-calls})

(def lifecycles [defer-lifecycle restart-lifecycle kill-lifecycle])

(deftest restart-call-first
  (checking
   "Putting a restart call at the top of the lifecycles always wins"
   (times 50)
   [v (gen/fmap #(into [kill-lifecycle] %) (gen/vector (gen/elements lifecycles)))]
   (let [event {:onyx.core/lifecycles v
                :onyx.core/task task}
         res ((lc/compile-lifecycle-handle-exception-functions event) nil nil nil)]
     (is (= :kill res)))))

(deftest kill-call-first
  (checking
   "Putting a kill call at the top of the lifecycles always wins"
   (times 50)
   [v (gen/fmap #(into [kill-lifecycle] %) (gen/vector (gen/elements lifecycles)))]
   (let [event {:onyx.core/lifecycles v
                :onyx.core/task task}
         res ((lc/compile-lifecycle-handle-exception-functions event) nil nil nil)]
     (is (= :kill res)))))

(deftest all-defer
  (checking
   "All defer values defaults to kill"
   (times 50)
   [v (gen/vector (gen/return defer-lifecycle))]
   (let [event {:onyx.core/lifecycles v
                :onyx.core/task task}
         res ((lc/compile-lifecycle-handle-exception-functions event) nil nil nil)]
     (is (= :kill res)))))

(deftest defer-falls-through-to-restart
  (checking
   "Putting restart afrer defer means restart wins"
   (times 50)
   [defers (gen/not-empty (gen/vector (gen/return defer-lifecycle)))
    trailing (gen/vector (gen/elements lifecycles))]
   (let [v (into (conj defers restart-lifecycle) trailing)
         event {:onyx.core/lifecycles v
                :onyx.core/task task}
         res ((lc/compile-lifecycle-handle-exception-functions event) nil nil nil)]
     (is (= :restart res)))))

(deftest defer-falls-through-to-kill
  (checking
   "Putting kill after defer means kill wins"
   (times 50)
   [defers (gen/not-empty (gen/vector (gen/return defer-lifecycle)))
    trailing (gen/vector (gen/elements lifecycles))]
   (let [v (into (conj defers kill-lifecycle) trailing)
         event {:onyx.core/lifecycles v
                :onyx.core/task task}
         res ((lc/compile-lifecycle-handle-exception-functions event) nil nil nil)]
     (is (= :kill res)))))

(deftest filter-irrelevant-lifecycles
  (checking
   "It doesn't use lifecycles that aren't in my task"
   (times 50)
   [v (gen/vector (gen/elements [kill-lifecycle restart-irrelevant-lifecycle]))]
   (let [event {:onyx.core/lifecycles v
                :onyx.core/task task}
         res ((lc/compile-lifecycle-handle-exception-functions event) nil nil nil)]
     (is (= :kill res)))))
