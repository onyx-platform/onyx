(ns onyx.log.notify-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.aeron :as aeron]
            [com.stuartsierra.component :as component]
            [midje.sweet :refer :all]))


(def peer-config 
  (:peer-config (read-string (slurp (clojure.java.io/resource "test-config.edn")))))

(def messaging 
  (aeron/aeron {:opts peer-config}))

(component/start messaging)
(try
  (let [entry (create-log-entry :notify-join-cluster {:observer :d :subject :a})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica {:pairs {:a :b :b :c :c :a} :prepared {:a :d} :peers [:a :b :c]}
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id :d :messenger messaging})]
    (fact diff => {:observer :d :subject :b :accepted-joiner :d :accepted-observer :a})
    (fact reactions => [{:fn :accept-join-cluster :args diff :immediate? true :site-resources {:aeron/port 40201}}])
    (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil)
    (fact (rep-reactions old-replica new-replica diff {:id :b}) => nil)
    (fact (rep-reactions old-replica new-replica diff {:id :c}) => nil))

  (finally
    (component/stop messaging)))
