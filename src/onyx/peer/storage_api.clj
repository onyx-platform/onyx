(ns onyx.peer.storage-api)

(defn storage-dispatch [task event]
  (select-keys task [:onyx/type :onyx/medium :onyx/direction]))

(defmulti read-batch storage-dispatch)

(defmulti decompress-segment storage-dispatch)

(defmulti compress-segment storage-dispatch)

(defmulti write-batch storage-dispatch)

