(ns onyx.prepare-join-cluster-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]))

(def entry (onyx.log.entry/create-log-entry :prepare-join-cluster {:joiner :d}))

(def f (extensions/apply-log-entry (:fn entry) (:args entry)))

(def state {:replica {:pairs {:a :b :b :c :c :a}
                      :peers [:a :b :c]}
            :local-state {}})

(fact (:prepared (f state 0)) => {:d :a})

(let [state (assoc-in state [:replica :prepared :e] :a)]
  (fact (:prepared (f state 0)) => {:e :a :d :b}))

(let [state (-> state
                (assoc-in [:replica :prepared :e] :a)
                (assoc-in [:replica :prepared :f] :b))]
  (fact (:prepared (f state 0)) => {:e :a :f :b :d :c}))

(let [state (-> state
                (assoc-in [:replica :prepared :e] :a)
                (assoc-in [:replica :prepared :f] :b)
                (assoc-in [:replica :prepared :g] :c))]
  (fact (:prepared (f state 0)) => {:e :a :f :b :g :c}))

