(ns onyx.tasks.seq
  (:require [onyx.schema :as os]
            [onyx.plugin.seq]
            [schema.core :as s])
  (:import [java.io FileReader BufferedReader]))

(s/defn input-serialized
  [task-name :- s/Keyword opts sequential]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.seq/input
                             :onyx/type :input
                             :onyx/medium :seq
                             :onyx/n-peers 1
                             :onyx/doc "Reads segments from a seq"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :seq/sequential sequential
                         :lifecycle/calls :onyx.plugin.seq/inject-seq-via-lifecycle}]}})

(s/defn input-materialized
  [task-name :- s/Keyword opts lifecycle]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.seq/input
                             :onyx/type :input
                             :onyx/medium :seq
                             :onyx/n-peers 1
                             :onyx/doc "Reads segments from a seq"}
                            opts)
           :lifecycles [lifecycle]}})


(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. ^String (:buffered-reader/filename lifecycle))] 
    {:seq/rdr rdr
     :seq/seq (map (fn [line] {:line line}) 
                   (line-seq (BufferedReader. rdr)))}))

(defn close-reader [event lifecycle]
  (.close ^FileReader (:seq/rdr event)))

(def in-file-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})

(s/defn input-file
  [task-name :- s/Keyword opts filename]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.seq/input
                             :onyx/type :input
                             :onyx/medium :seq
                             :onyx/n-peers 1
                             :onyx/doc "Reads segments from a seq"}
                            opts)
           :lifecycles [{:lifecycle/task :in
                         :buffered-reader/filename filename
                         :lifecycle/calls ::in-file-calls}]}})
