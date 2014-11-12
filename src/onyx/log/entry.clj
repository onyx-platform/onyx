(ns onyx.log.entry
  (:require [onyx.log.commands.prepare-join-cluster]
            [onyx.log.commands.accept-join-cluster]
            [onyx.log.commands.notify-watchers]
            [onyx.log.commands.leave-cluster]))

(defn create-log-entry [kw args]
  {:fn kw :args args})

