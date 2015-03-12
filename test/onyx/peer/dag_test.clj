(ns onyx.peer.dag-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def n-messages 15000)

(def batch-size 40)

(def a-chan (chan (inc n-messages)))

(def b-chan (chan (inc n-messages)))

(def c-chan (chan (inc n-messages)))

(def j-chan (chan (sliding-buffer (inc n-messages))))

(def k-chan (chan (sliding-buffer (inc n-messages))))

(def l-chan (chan (sliding-buffer (inc n-messages))))

(defmethod l-ext/inject-lifecycle-resources :A
  [_ _] {:core.async/chan a-chan})

(defmethod l-ext/inject-lifecycle-resources :B
  [_ _] {:core.async/chan b-chan})

(defmethod l-ext/inject-lifecycle-resources :C
  [_ _] {:core.async/chan c-chan})

(defmethod l-ext/inject-lifecycle-resources :J
  [_ _] {:core.async/chan j-chan})

(defmethod l-ext/inject-lifecycle-resources :K
  [_ _] {:core.async/chan k-chan})

(defmethod l-ext/inject-lifecycle-resources :L
  [_ _] {:core.async/chan l-chan})

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

(def v-peers (onyx.api/start-peers 12 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def j-results (take-segments! j-chan))

(def k-results (take-segments! k-chan))

(def l-results (take-segments! l-chan))

(fact (last j-results) => :done)

(fact (last k-results) => :done)

(fact (last l-results) => :done)

(fact (into #{} (concat a-segments b-segments))
      => (into #{} (butlast j-results)))

(fact (into #{} (concat a-segments b-segments))
      => (into #{} (butlast k-results)))

(fact (into #{} (concat a-segments b-segments c-segments))
      => (into #{} (butlast l-results)))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

