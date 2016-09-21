(ns onyx.plugin.onyx-plugin)

(defprotocol OnyxPlugin
  (start [this]
    "Initialize the plugin, generally assoc'ing any initial state.")

  (stop [this event]
    "Shutdown the input and close any resources that needs to be closed.
     This can also be done using lifecycles."))
