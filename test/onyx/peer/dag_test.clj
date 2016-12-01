(ns onyx.peer.dag-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def n-messages 15000)

(def a-chan (atom nil))

(def b-chan (atom nil))

(def c-chan (atom nil))

(def j-chan (atom nil))

(def k-chan (atom nil))

(def l-chan (atom nil))

(def d identity)

(def e identity)

(def f identity)

(def g identity)

(def h identity)

(def i identity)

(defn inject-a-ch [event lifecycle]
  {:core.async/chan @a-chan})

(defn inject-b-ch [event lifecycle]
  {:core.async/chan @b-chan})

(defn inject-c-ch [event lifecycle]
  {:core.async/chan @c-chan})

(defn inject-j-ch [event lifecycle]
  {:core.async/chan @j-chan})

(defn inject-k-ch [event lifecycle]
  {:core.async/chan @k-chan})

(defn inject-l-ch [event lifecycle]
  {:core.async/chan @l-chan})

(def a-calls
  {:lifecycle/before-task-start inject-a-ch})

(def b-calls
  {:lifecycle/before-task-start inject-b-ch})

(def c-calls
  {:lifecycle/before-task-start inject-c-ch})

(def j-calls
  {:lifecycle/before-task-start inject-j-ch})

(def k-calls
  {:lifecycle/before-task-start inject-k-ch})

(def l-calls
  {:lifecycle/before-task-start inject-l-ch})

(deftest dag-workflow
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 40

        a-segments (map (fn [n] {:n n}) (range n-messages))
        b-segments (map (fn [n] {:n n}) (range n-messages (* 2 n-messages)))
        c-segments (map (fn [n] {:n n}) (range (* 2 n-messages) (* 3 n-messages)))

        catalog [{:onyx/name :A
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :B
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :C
                  :onyx/plugin :onyx.plugin.core-async/input
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
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :K
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}

                 {:onyx/name :L
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        ;;; A    B       C
        ;;;  \  /        |
        ;;;   D- >       E
        ;;;   |  \     / | \
        ;;;   F   \-> G  H  I
        ;;;  / \       \ | /
        ;;; J   K        L

        workflow [[:A :D]
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
                  [:D :G]]

        lifecycles [{:lifecycle/task :A
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
                     :lifecycle/calls :onyx.plugin.core-async/writer-calls}]]

    (reset! a-chan (chan (inc n-messages)))
    (reset! b-chan (chan (inc n-messages)))
    (reset! c-chan (chan (inc n-messages)))

    (reset! j-chan (chan 5000000))
    (reset! k-chan (chan 5000000))
    (reset! l-chan (chan 5000000))

    (with-test-env [test-env [12 env-config peer-config]]
      (doseq [x a-segments]
        (>!! @a-chan x))

      (doseq [x b-segments]
        (>!! @b-chan x))

      (doseq [x c-segments]
        (>!! @c-chan x))

      (>!! @a-chan :done)
      (>!! @b-chan :done)
      (>!! @c-chan :done)

      (onyx.api/submit-job peer-config
                           {:catalog catalog :workflow workflow
                            :lifecycles lifecycles
                            :task-scheduler :onyx.task-scheduler/balanced})

      (let [j-results (take-segments! @j-chan)
            k-results (take-segments! @k-chan)
            l-results (take-segments! @l-chan)]
        (is (= :done (last j-results)))
        (is (= :done (last k-results)))
        (is (= :done (last l-results)))

        (is (= (into #{} (concat a-segments b-segments))
               (into #{} (butlast j-results))))

        (is (= (into #{} (concat a-segments b-segments))
               (into #{} (butlast k-results))))

        (is (= (into #{} (concat a-segments b-segments c-segments))
               (into #{} (butlast l-results))))))))
