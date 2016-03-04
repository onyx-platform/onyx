(ns onyx.plugin.core-async-tasks
  (:require [clojure.core.async :refer [chan]]
            [clojure.set :refer [join]]
            [onyx.schema :as os]
            [schema.core :as s]))

(def channels (atom {}))

(def default-channel-size 1000)

(defn get-channel
  ([id] (get-channel id default-channel-size))
  ([id size]
   (if-let [id (get @channels id)]
     id
     (do (swap! channels assoc id (chan size))
         (get-channel id)))))

(defn inject-in-ch
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle))})
(defn inject-out-ch
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle))})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn get-core-async-channels
  [{:keys [catalog lifecycles]}]
  (let [lifecycle-catalog-join (join catalog lifecycles {:onyx/name :lifecycle/task})]
    (reduce (fn [acc item]
              (assoc acc
                     (:onyx/name item)
                     (get-channel (:core.async/id item)))) {} (filter :core.async/id lifecycle-catalog-join))))

(s/defn input-task
  ([task-name :- s/Keyword opts]
   (input-task task-name opts default-channel-size))
  ([task-name :- s/Keyword opts chan-size]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.core-async/input
                             :onyx/type :input
                             :onyx/medium :core.async
                             :onyx/max-peers 1
                             :onyx/doc "Reads segments from a core.async channel"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :core.async/id (java.util.UUID/randomUUID)
                         :core.async/size chan-size
                         :lifecycle/calls ::in-calls}
                        {:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.core-async/reader-calls}]}
    :schema {:task-map os/TaskMap
             :lifecycles [os/Lifecycle]}}))

(s/defn output-task
  ([task-name :- s/Keyword opts]
   (output-task task-name opts default-channel-size))
  ([task-name :- s/Keyword opts chan-size]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.core-async/output
                             :onyx/type :output
                             :onyx/medium :core.async
                             :onyx/max-peers 1
                             :onyx/doc "Writes segments to a core.async channel"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :core.async/id (java.util.UUID/randomUUID)
                         :core.async/size (inc chan-size)
                         :lifecycle/calls ::out-calls}
                        {:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.core-async/writer-calls}]}
    :schema {:task-map os/TaskMap
             :lifecycles [os/Lifecycle]}}))
