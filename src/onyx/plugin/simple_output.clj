(ns onyx.plugin.simple-output)

(defprotocol SimpleOutput
  (start [this])

  (stop [this])

  (write-batch [this event])

  (seal-resource [this event]))

;;; Default implementation of Output protocol for input and function tasks.
(extend-type Object
  SimpleOutput

  (start [this] this)

  (stop [this] this)

  (write-batch [this event]
    {})

  (seal-batch [this event]
    {}))
