(ns onyx.prepare-join-cluster-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]))

(def entry (onyx.log.entry/create-log-entry :prepare-join-cluster {:joiner :d}))

(def f (extensions/apply-log-entry (:fn entry) (:args entry)))

(def old-replica {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]})

(let [new-replica (f old-replica 0)
      diff (extensions/replica-diff :prepare-join-cluster old-replica new-replica)]
  (fact (:prepared new-replica) => {:d :a})
  (fact diff => {:d :a}))

(let [old-replica (assoc-in old-replica [:prepared :e] :a)
      new-replica (f old-replica 0)
      diff (extensions/replica-diff :prepare-join-cluster old-replica new-replica)]
  (fact (:prepared new-replica) => {:e :a :d :b})
  (fact diff => {:d :b}))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :e] :a)
                      (assoc-in [:prepared :f] :b))
      new-replica (f old-replica 0)
      diff (extensions/replica-diff :prepare-join-cluster old-replica new-replica)]
  (fact (:prepared new-replica) => {:e :a :f :b :d :c})
  (fact diff => {:d :c}))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :e] :a)
                      (assoc-in [:prepared :f] :b)
                      (assoc-in [:prepared :g] :c))
      new-replica (f old-replica 0)
      diff (extensions/replica-diff :prepare-join-cluster old-replica new-replica)]
  (fact (:prepared new-replica) => {:e :a :f :b :g :c})
  (fact diff => nil))

