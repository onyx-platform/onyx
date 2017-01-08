(ns onyx.plugin.seq
  (:require [clojure.core.async :refer [poll! timeout chan alts!! >!! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]
            [taoensso.timbre :refer [fatal info debug] :as timbre]))

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

  (synced? [this epoch]
    true)

  (poll! [this _]
    (if-let [seg (first @rst)]
      (do (vswap! rst rest)
          (vswap! offset inc)
          seg)
      (do (vreset! completed? true)
          nil)))

  (completed? [this]
    @completed?))

(defn input [event]
  (map->AbsSeqReader {:event event :sequential (:seq/seq event) 
                      :rst (volatile! nil) 
                      :completed? (volatile! false) 
                      :offset (volatile! nil)}))

(defn inject-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def in-calls
  {:lifecycle/before-task-start inject-seq})
