(ns onyx.peer.dag-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group 
  (onyx.api/start-peer-group peer-config))

(def n-messages 15000)

(def batch-size 40)

(def a-chan (chan (inc n-messages)))

(def b-chan (chan (inc n-messages)))

(def c-chan (chan (inc n-messages)))

(def j-chan (chan 500000 #_(sliding-buffer (* 2 (inc n-messages)))))

(def k-chan (chan 500000 #_(sliding-buffer (* 2 (inc n-messages)))))

(def l-chan (chan 500000 #_(sliding-buffer (* 3 (inc n-messages)))))

(def a-segments
  (map (fn [n] {:n n})
       (range n-messages)))

(def b-segments
  (map (fn [n] {:n n})
       (range n-messages (* 2 n-messages))))

(def c-segments
  (map (fn [n] {:n n})
       (range (* 2 n-messages) (* 3 n-messages))))

(doseq [x a-segments]
  (>!! a-chan x))

(doseq [x b-segments]
  (>!! b-chan x))

(doseq [x c-segments]
  (>!! c-chan x))

(>!! a-chan :done)
(>!! b-chan :done)
(>!! c-chan :done)

(def d identity)

(def e identity)

(def f identity)

(def g identity)

(def h identity)

(def i identity)

(def catalog
  [{:onyx/name :A
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :B
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :C
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :D
    :onyx/fn :onyx.peer.dag-test/d
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :E
    :onyx/fn :onyx.peer.dag-test/e
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :F
    :onyx/fn :onyx.peer.dag-test/f
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :G
    :onyx/fn :onyx.peer.dag-test/g
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :H
    :onyx/fn :onyx.peer.dag-test/h
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :I
    :onyx/fn :onyx.peer.dag-test/i
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :J
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :K
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :L
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

;;; A    B       C
;;;  \  /        |
;;;   D- >       E
;;;   |  \     / | \
;;;   F   \-> G  H  I
;;;  / \       \ | /
;;; J   K        L

(def workflow
  [[:A :D]
   [:B :D]
   [:D :F]
   [:F :J]
   [:F :K]
   [:C :E]
   [:E :G]
   [:E :H]
   [:E :I]
   [:G :L]
   [:H :L]
   [:I :L]
   [:D :G]])

(defn inject-a-ch [event lifecycle]
  {:core.async/chan a-chan})

(defn inject-b-ch [event lifecycle]
  {:core.async/chan b-chan})

(defn inject-c-ch [event lifecycle]
  {:core.async/chan c-chan})

(defn inject-j-ch [event lifecycle]
  {:core.async/chan j-chan})

(defn inject-k-ch [event lifecycle]
  {:core.async/chan k-chan})

(defn inject-l-ch [event lifecycle]
  {:core.async/chan l-chan})

(def a-calls
  {:lifecycle/before-task :onyx.peer.dag-test/inject-a-ch})

(def b-calls
  {:lifecycle/before-task :onyx.peer.dag-test/inject-b-ch})

(def c-calls
  {:lifecycle/before-task :onyx.peer.dag-test/inject-c-ch})

(def j-calls
  {:lifecycle/before-task :onyx.peer.dag-test/inject-j-ch})

(def k-calls
  {:lifecycle/before-task :onyx.peer.dag-test/inject-k-ch})

(def l-calls
  {:lifecycle/before-task :onyx.peer.dag-test/inject-l-ch})

(def lifecycles
  [{:lifecycle/task :A
    :lifecycle/calls :onyx.peer.dag-test/a-calls}
   {:lifecycle/task :A
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :B
    :lifecycle/calls :onyx.peer.dag-test/b-calls}
   {:lifecycle/task :B
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :C
    :lifecycle/calls :onyx.peer.dag-test/c-calls}
   {:lifecycle/task :C
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :J
    :lifecycle/calls :onyx.peer.dag-test/j-calls}
   {:lifecycle/task :J
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :K
    :lifecycle/calls :onyx.peer.dag-test/k-calls}
   {:lifecycle/task :K
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :L
    :lifecycle/calls :onyx.peer.dag-test/l-calls}
   {:lifecycle/task :L
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 12 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def j-results (take-segments! j-chan))

(def k-results (take-segments! k-chan))

(def l-results (take-segments! l-chan))

(fact (last j-results) => :done)

(fact (last k-results) => :done)

(fact (last l-results) => :done)

(fact (into #{} (butlast j-results))
      => (into #{} (concat a-segments b-segments)))

(fact (into #{} (butlast k-results))
      => (into #{} (concat a-segments b-segments)))

(fact (into #{} (butlast l-results))
      => (into #{} (concat a-segments b-segments c-segments)))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

