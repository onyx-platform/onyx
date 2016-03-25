(ns onyx.interop
  (:gen-class :name onyx.interop
              :methods [^:static [write_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
                        ^:static [read_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]])
  (:require [onyx.information-model :refer [model]]))

(defn -write_batch
  [event]
  ((resolve 'onyx.peer.function/write-batch) event))

(defn -read_batch
  [event]
  ((resolve 'onyx.peer.function/read-batch) event))

(gen-interface
  :name onyx.IPipeline
  :methods [[writeBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]])

(def casts
  {:boolean (fn [x] x)
   :integer (fn [x] x)
   :string (fn [x] x)
   :any (fn [x] x)
   :keyword (fn [x] (keyword x))
   :vector (fn [x] (vec x))})

(defn cast-types [section m]
  (let [section* (keyword section)]
    (reduce-kv
     (fn [m* k v]
       (let [k* (keyword k)
             type (get-in model [section* :model k* :type])
             v* ((get casts type identity) v)]
         (assoc m* k* v*)))
     {}
     m)))

(defn coerce-workflow [workflow]
  (mapv #(mapv (fn [v] (keyword v)) %) workflow))

(defn coerce-catalog [catalog]
  (mapv #(cast-types :catalog-entry %) catalog))

(defn coerce-lifecycles [lifecycles]
  (mapv #(cast-types :lifecycle-entry %) lifecycles))

(defn coerce-flow-conditions [fcs]
  (mapv #(cast-types :flow-conditions-entry %) fcs))

(defn coerce-windows [windows]
  (mapv #(cast-types :window-entry %) windows))

(defn coerce-trigger [trigger]
  (mapv #(cast-types :trigger-entry %) trigger))
