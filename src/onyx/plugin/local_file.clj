(ns onyx.plugin.local-file
  (:require [clojure.java.io :as io]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :as timbre])
  (:import [java.io LineNumberReader FileReader]))

(defrecord LocalFile [location]
  p-ext/PipelineBatchInput
  (n-partitions [this]
    (let [lnr (LineNumberReader. (FileReader. (io/file location)))
          _ (.skip lnr Long/MAX_VALUE)
          lines (inc (.getLineNumber lnr))
          partition-size 20]
      (.close lnr)
      (int (Math/ceil (/ 100 partition-size)))))
  
  p-ext/PipelineBatchOutput
  (write-content [this content]
    (spit location content)))
