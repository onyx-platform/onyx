(ns onyx.static.uuid)

(defn random-uuid []
  (let [local-random (java.util.concurrent.ThreadLocalRandom/current)] 
    (java.util.UUID. (.nextLong local-random)
                     (.nextLong local-random))))
