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
  [{:id 1  :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:id 9  :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:id 10 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:id 11 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:id 12 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:id 13 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:id 14 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:id 15 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

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
    :window/type :sliding
    :window/window-key :event-time
    :window/range [30 :minutes]
    :window/slide [5 :minutes]
    :window/doc "Collects segments on a 30 minute window sliding every 5 minutes"}])

(def in-chan (chan (inc n-messages)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

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

(doseq [i input]
  (>!! in-chan input))

(>!! in-chan :done)
(close! in-chan)

(def v-peers (onyx.api/start-peers 3 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :windows windows
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
