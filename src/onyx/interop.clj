(ns onyx.interop
  (:gen-class :name onyx.interop
              :methods [^:static [write_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
                        ^:static [read_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]])
  (:require [onyx.peer.pipeline-extensions :refer [PipelineInput Pipeline]]
            [onyx.information-model :refer [model]]))

(defn -write_batch
  [event]
  ((resolve 'onyx.peer.function/write-batch) event))

(defn -read_batch
  [event]
  ((resolve 'onyx.peer.function/read-batch) event))

(gen-interface
  :name onyx.IPipelineInput
  :methods [[ackSegment [clojure.lang.IPersistentMap java.util.UUID] void]
            [retrySegment [clojure.lang.IPersistentMap java.util.UUID] Object]
            [isPending [clojure.lang.IPersistentMap java.util.UUID] Object]
            [isDrained [clojure.lang.IPersistentMap] boolean]])

(gen-interface
  :name onyx.IPipeline
  :methods [[readBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [writeBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [sealResource [clojure.lang.IPersistentMap] Object]])

(extend-protocol PipelineInput
  onyx.IPipelineInput
  (ack-segment [this event message-id]
    (.ackSegment this event message-id))
  (retry-segment [this event message-id]
    (.retrySegment this event message-id))
  (pending? [this event message-id]
    (.isPending this event message-id))
  (drained? [this event]
    (.isDrained this event)))

(extend-protocol Pipeline
  onyx.IPipeline
  (read-batch [this event]
    (.readBatch this event))
  (write-batch [this event]
    (.writeBatch this event))
  (seal-resource [this event]
    (.sealResource this event)))

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
