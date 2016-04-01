(ns onyx.plugin.onyx-input
  (:require [onyx.extensions]
            [onyx.types]))

(defprotocol OnyxInput
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

  (set-epoch [this epoch]
    "Moves the reader to the epoch, signaling which offset comes before it.
     Returns a new reader.")

  (recover [this checkpoint]
    "Recover the state of the plugin from the supplied checkpoint.
     Returns a new reader.")
  
  (ack-barrier [this offset]
    "Acknowledges the barrier has been fully processed. Returns a new reader.")

  (segment-complete! [this segment]
    "Perform any side-effects that you might want to perform as a
     result of a segment being completed.")

  (completed? [this]
    "Returns true if this all of this input's data has been processed.
     Streaming inputs should always return false."))

(defrecord SegmentOffset [segment offset])
