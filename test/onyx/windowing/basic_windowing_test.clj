(ns onyx.windowing.basic-windowing-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def batch-size 20)

(def input
  [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :age 3 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:id 9  :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def workflow [[:in :identity] [:identity :out]])

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/max-peers 1
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def windows
  [{:window/id :collect-segments
    :window/task :identity
    :window/type :fixed
    :window/aggregation :count
    :window/window-key :event-time
    :window/range [5 :minutes]
;;    :window/slide [5 :minutes]
    :window/doc "Collects segments on a 30 minute window sliding every 5 minutes"}])

(defn trigger-pred [event window-id upper lower segment]
  (:trigger? segment))

(def triggers
  [#_{:trigger/window-id :collect-segments
    :trigger/refinement :accumulating
    :trigger/on :timer
    :trigger/period [5 :seconds]
    :trigger/sync ::write-to-stdout}

   #_{:trigger/window-id :collect-segments
    :trigger/refinement :accumulating
    :trigger/on :segment
    :trigger/threshold [5 :elements]
    :trigger/sync ::write-to-stdout}

   #_{:trigger/window-id :collect-segments
    :trigger/refinement :discarding
    :trigger/on :punctuation
    :trigger/pred ::trigger-pred
    :trigger/sync ::write-to-stdout}

   {:trigger/window-id :collect-segments
    :trigger/refinement :discarding
    :trigger/on :percentile-watermark
    :trigger/watermark-percentage 0.50
    :trigger/sync ::write-to-stdout}])

(defn write-to-stdout [event window-id lower-bound upper-bound state]
  (println window-id (java.util.Date. lower-bound) (java.util.Date. upper-bound) state))

(def in-chan (chan (inc (count input))))

(def out-chan (chan (sliding-buffer (inc (count input)))))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :windows windows
  :triggers triggers
  :task-scheduler :onyx.task-scheduler/balanced})

(doseq [i input]
  (>!! in-chan i))

(>!! in-chan {:id 2 :age 12 :event-time #inst "2015-09-13T03:02:30.829-00:00"})

(>!! in-chan :done)
(close! in-chan)

(def results (take-segments! out-chan))

(do
  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env))

