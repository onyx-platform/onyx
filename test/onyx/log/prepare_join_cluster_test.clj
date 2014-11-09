(ns onyx.prepare-join-cluster-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :prepare-join-cluster {:joiner :d}))

(def f (extensions/apply-log-entry (:fn entry) (:args entry)))

(def rep-diff (partial extensions/replica-diff :prepare-join-cluster))

(def rep-reactions (partial extensions/reactions :prepare-join-cluster))

(def old-replica {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]})

(let [new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:d :a})
  (fact diff => {:watching :d :watched :a})
  (fact reactions => [{:f :notify-watchers :args {:watcher :c :to-watch :d}}]))

(let [old-replica (assoc-in old-replica [:prepared :e] :a)
      new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:e :a :d :b})
  (fact diff => {:watching :d :watched :b})
  (fact reactions => [{:f :notify-watchers :args {:watcher :a :to-watch :d}}]))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :e] :a)
                      (assoc-in [:prepared :f] :b))
      new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:e :a :f :b :d :c})
  (fact diff => {:watching :d :watched :c})
  (fact reactions => [{:f :notify-watchers :args {:watcher :b :to-watch :d}}]))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :e] :a)
                      (assoc-in [:prepared :f] :b)
                      (assoc-in [:prepared :g] :c))
      new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:e :a :f :b :g :c})
  (fact diff => nil)
  (fact reactions => nil))

