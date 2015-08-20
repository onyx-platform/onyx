(ns onyx.log.entry)

(defn create-log-entry [kw args]
  {:fn kw :args args})
