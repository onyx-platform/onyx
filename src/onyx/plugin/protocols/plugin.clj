(ns onyx.plugin.protocols.plugin)

(defprotocol Plugin
  (start [this event]
    "Initialize the plugin, generally assoc'ing any initial state.")

  (stop [this event]
    "Shutdown the input and close any resources that needs to be closed.
     This can also be done using lifecycles."))
