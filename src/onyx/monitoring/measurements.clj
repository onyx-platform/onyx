(ns onyx.monitoring.measurements)

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
