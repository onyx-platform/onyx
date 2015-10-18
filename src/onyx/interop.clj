(ns onyx.interop
  (:gen-class :name onyx.interop
              :methods [^:static [write_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
                        ^:static [read_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]])
  (:require [onyx.peer.pipeline-extensions :refer [PipelineInput Pipeline]]))

(defn -write_batch
  [event]
  (require 'onyx.peer.function)
  ((resolve 'onyx.peer.function/write-batch) event))

(defn -read_batch
  [event]
  (require 'onyx.peer.function)
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
    (.ackMessage this event message-id))
  (retry-segment [this event message-id]
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
