(ns onyx.plugin.seq
  (:require [clojure.core.async :refer [poll! timeout chan alts!! >!! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [onyx.plugin.protocols :as p]
            [taoensso.timbre :refer [fatal info debug] :as timbre]))

(defrecord AbsSeqReader [event sequential rst completed? offset]
  p/Plugin

  (start [this event]
    this)

  (stop [this event] 
    this)

  p/Checkpointed
  (checkpoint [this]
    @offset)

  (recover! [this _ checkpoint]
    (vreset! completed? false)
    (if (nil? checkpoint) 
      (do 
       (vreset! rst sequential)
       (vreset! offset 0))
      (do
       (info "ABS plugin, recover dropping:" checkpoint (take checkpoint sequential))
       (vreset! rst (drop checkpoint sequential))
       (vreset! offset checkpoint))))

  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    @completed?)

  p/Input
  (poll! [this _ _]
    (if-let [seg (first @rst)]
      (do (vswap! rst rest)
          (vswap! offset inc)
          seg)
      (do (vreset! completed? true)
          nil))))

(defn input [event]
  (map->AbsSeqReader {:event event 
                      :sequential (:seq/seq event) 
                      :rst (volatile! nil) 
                      :completed? (volatile! false) 
                      :offset (volatile! nil)}))

(def reader-calls
  {})

(defn inject-lifecycle-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def inject-seq-via-lifecycle
  {:lifecycle/before-task-start inject-lifecycle-seq})
