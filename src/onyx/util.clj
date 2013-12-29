(ns onyx.util)

(defn config []
  (let [resource (clojure.java.io/resource "config.edn")]
    (read-string (slurp resource))))

