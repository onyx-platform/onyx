(ns onyx.log.entry
  (:require [onyx.log.entries.prepare-join-cluster]
            [onyx.log.entries.accept-join-cluster]
            [onyx.log.entries.notify-watchers]))

(defn create-log-entry [kw args]
  {:fn kw :args args})

