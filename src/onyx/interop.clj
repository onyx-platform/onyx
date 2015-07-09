(ns onyx.interop
  (:gen-class :name onyx.interop
              :methods [^:static [write_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
                        ^:static [read_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]])
  (:require [onyx.peer.function :refer [write-batch read-batch]]
            [onyx.peer.pipeline-extensions :refer [PipelineInput Pipeline]]))

(defn -write_batch 
  [event]
  (write-batch event))

(defn -read_batch 
  [event]
  (read-batch event))

(gen-interface
  :name onyx.IPipelineInput
  :methods [[ackMessage [clojure.lang.IPersistentMap java.util.UUID] void]
            [retryMessage [clojure.lang.IPersistentMap java.util.UUID] Object]
            [isPending [clojure.lang.IPersistentMap java.util.UUID] Object]
            [isDrained [clojure.lang.IPersistentMap] boolean]])

(gen-interface
  :name onyx.IPipeline
  :methods [[readBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [writeBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [sealResource [clojure.lang.IPersistentMap] Object]])

(extend-protocol PipelineInput
  onyx.IPipelineInput
  (ack-message [this event message-id]
    (.ackMessage this event message-id))
  (retry-message [this event message-id]
    (.retryMessage this event message-id))
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
