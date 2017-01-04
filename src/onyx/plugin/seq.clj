(ns onyx.plugin.seq
  (:require [clojure.core.async :refer [poll! timeout chan alts!! >!! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

(defrecord AbsSeqReader [event sequential rst segment offset]
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

  (segment [this]
    @segment)
  
  (checkpointed! [this epoch])

  (synced? [this epoch]
    true)

  (next-state [this _]
    (if-let [seg (first @rst)]
      (do (reset! segment seg)
          (reset! rst (rest @rst))
          (swap! offset inc))
      (reset! segment nil))
    true)

  (completed? [this]
    (empty? @rst)))

(defn input [event]
  (map->AbsSeqReader {:event event :sequential (:seq/seq event) 
                      :rst (atom nil) :segment (atom nil) :offset (atom nil)}))

(defn inject-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def in-calls
  {:lifecycle/before-task-start inject-seq})
