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

(defmacro emit-latency-2 
  "Start a test env in a way that shuts down after body is completed. 
   Useful for running tests that can be killed, and re-run without bouncing the repl."
  [monitoring event-name & body]
  `(let [start# (System/currentTimeMillis)
         rets# ~@body
         elapsed# (- (System/currentTimeMillis) start#)]
     (extensions/emit ~monitoring (t/->MonitorEventLatency ~event-name elapsed#))
     rets#))

(defn emit-latency [event-type monitoring f]
  (if (extensions/registered? monitoring event-type)
    (let [start (System/currentTimeMillis)
          rets (f)
          elapsed (- (System/currentTimeMillis) start)]
      (extensions/emit monitoring (t/->MonitorEventLatency event-type elapsed))
      rets)
    (f)))

(defn emit-latency-value [event-type monitoring elapsed]
  (when (extensions/registered? monitoring event-type)
    (extensions/emit monitoring (t/->MonitorEventLatency event-type elapsed))))

(defn emit-count [event-type monitoring cnt]
  (when (extensions/registered? monitoring event-type)
    (extensions/emit monitoring (t/->MonitorTaskEventCount event-type cnt))))
