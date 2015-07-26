(ns onyx.plugin.local-file
  (:require [clojure.java.io :as io]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :as timbre])
  (:import [java.io LineNumberReader FileReader]))

(def partition-size 20)

(defrecord LocalFile [location]
  p-ext/PipelineBatchInput
  (n-partitions [this]
    (let [lnr (LineNumberReader. (FileReader. (io/file location)))
          _ (.skip lnr Long/MAX_VALUE)
          lines (inc (.getLineNumber lnr))]
      (.close lnr)
      (int (Math/ceil (/ 100 partition-size)))))

  (read-partition [this part]
    (with-open [rdr (clojure.java.io/reader location)]
      (->> (line-seq rdr)
           (drop (* part partition-size))
           (take partition-size)
           (map read-string)
           (doall))))
  
  p-ext/PipelineBatchOutput
  (write-content [this content]
    (spit location content)))

(defn input [pipeline-data]
  (->LocalFile (:file/path (:onyx.core/task-map pipeline-data))))

(defn output [pipeline-data]
  (->LocalFile (:file/path (:onyx.core/task-map pipeline-data))))
