(ns onyx.monitoring.measurements
  (:require [onyx.types :as t]
            [onyx.extensions :as extensions]))

(defn measure-latency
  "Calls f, and passing the amount of time it took
   to call and return f in milliseconds to g.
   Returns the value that calling f returned."
  [f g]
  (let [start (System/currentTimeMillis)
        rets (f)
        end (- (System/currentTimeMillis) start)]
    (g end)
    rets))

(defn emit-latency [event-type monitoring f]
  (if (extensions/registered? monitoring event-type)
    (let [start (System/currentTimeMillis)
          rets (f)
          elapsed (- (System/currentTimeMillis) start)]
      (extensions/emit monitoring (t/->MonitorEventLatency event-type elapsed))
      rets)
    (f)))
