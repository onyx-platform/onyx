(ns onyx.log.abort-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [->DummyMessenger]]
            [onyx.system]
            [midje.sweet :refer :all]))

(let [peer-state {:id :d :messenger (->DummyMessenger)}
      entry (create-log-entry :abort-join-cluster {:id :d})
      f (partial extensions/apply-log-entry entry)
      rep-diff (partial extensions/replica-diff entry)
      rep-reactions (partial extensions/reactions entry)

      old-replica {:messaging {:onyx.messaging/impl :dummy-messaging}
                   :pairs {:a :b :b :c :c :a} :prepared {:a :d} :peers [:a :b :c]}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff peer-state)]
  (fact (:pairs new-replica) => {:a :b :b :c :c :a})
  (fact (:peers new-replica) => [:a :b :c])
  (fact diff => {:aborted :d})
  ;; Test messaging layer never tries to rejoin after abort.
  (fact reactions => nil?))

