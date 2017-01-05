(ns onyx.plugin.seq
  (:require [clojure.core.async :refer [poll! timeout chan alts!! >!! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

(defrecord AbsSeqReader [event sequential rst completed? offset]
  p/Plugin

  (start [this event]
    this)

  (stop [this event] 
    this)

  i/Input

  (checkpoint [this]
    @offset)

  (recover! [this _ checkpoint]
    (if (nil? checkpoint) 
      (do (reset! rst sequential)
          (reset! offset -1))
      (do
       (println "RECOVER dropping:" checkpoint (take (inc checkpoint) sequential))
       (reset! rst (drop (inc checkpoint) sequential))
       (reset! offset checkpoint))))

  (checkpointed! [this epoch])

  (synced? [this epoch]
    true)

  (poll! [this _]
    (if-let [seg (first @rst)]
      (do (swap! rst rest)
          (swap! offset inc)
          seg)
      (do (reset! completed? true)
          nil)))

  (completed? [this]
    @completed?))

(defn input [event]
  (map->AbsSeqReader {:event event :sequential (:seq/seq event) 
                      :rst (atom nil) :completed? (atom false) :offset (atom nil)}))

(defn inject-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def in-calls
  {:lifecycle/before-task-start inject-seq})
