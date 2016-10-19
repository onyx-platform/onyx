(ns onyx.plugin.seq
  (:require [clojure.core.async :refer [poll! timeout chan alts!! >!! close!]]
            [clojure.core.async.impl.protocols]
            [clojure.set :refer [join]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.plugin.onyx-input :as i]
            [onyx.plugin.onyx-output :as o]
            [onyx.plugin.onyx-plugin :as p]))

(defrecord AbsSeqReader [event sequential rst segment offset]
  p/OnyxPlugin

  (start [this]
    (let [sequential (:seq/seq event)] 
      (assoc this :rst sequential :sequential sequential :offset -1)))

  (stop [this event] 
    (assoc this :rst nil :sequential nil))

  i/OnyxInput

  (checkpoint [this]
    offset)

  (recover [this checkpoint]
    (if (nil? checkpoint) 
      (assoc this 
             :rst sequential 
             :offset -1)
      (do
       (println "RECOVER dropping " checkpoint (take (inc checkpoint) sequential))
       (assoc this 
             :rst (drop (inc checkpoint) sequential)
             :offset checkpoint))))

  (offset-id [this]
    offset)

  (segment [this]
    segment)

  (next-state [this event]
    (let [segment (first rst)
          remaining (rest rst)]
      (assoc this
             :segment segment
             :rst remaining
             :offset (if segment (inc offset) offset))))

  (segment-complete! [this segment])

  (completed? [this]
    (empty? rst)))

(defn input [event]
  (map->AbsSeqReader {:event event}))

(defn inject-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def in-calls
  {:lifecycle/before-task-start inject-seq})
