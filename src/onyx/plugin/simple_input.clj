(ns onyx.plugin.simple-input
  (:require [onyx.extensions]
            [onyx.types]))

(defprotocol SimpleInput
  (start [this]
    "Initialize the plugin, generally assoc'ing any initial state.")

  (stop [this]
    "Shutdown the input and close any resources that needs to be closed.
     This can also be done using lifecycles.")

  (checkpoint [this]
    "Pure function that returns the full checkpoint state,
     that allows the plugin state to be recovered. This checkpoint value
     will be passed in to recover when restarting the task and recovering
     the plugin's state.")

  (offset-id [this]
    "Returns the offset id for the value last read from the input medium.
     Return nil if no segment is available.")

  (segment [this]
    "Returns the segment last read from the input medium.
     Return nil if no segment is available.")

  (next-state [this event]
    "Moves reader to the next state. Returns the reader in the updated state.")

  (recover [this checkpoint]
    "Recover the state of the plugin from the supplied checkpoint.
     Returns a new reader.")
  
  (ack-barrier [this offset]
    "Acknowledges the barrier has been fully processed. Returns a new reader.")

  (segment-complete! [this segment]
    "Perform any side-effects that you might want to perform as a
     result of a segment being completed."))

(defrecord SegmentOffset [segment offset])

;;; Default implementation of Input protocol for function and output tasks.
(extend-type Object
  SimpleInput

  (start [this] this)

  (stop [this] this))
