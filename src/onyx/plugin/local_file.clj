(ns onyx.plugin.local-file
  (:require [clojure.java.io :as io]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :as timbre])
  (:import [java.io LineNumberReader FileReader]))

(def partition-size 20)

(defrecord LocalFile [location extension]
  p-ext/PipelineBatchInput
  (n-partitions [this]
    (let [lnr (LineNumberReader. (FileReader. (io/file (str location extension))))
          _ (.skip lnr Long/MAX_VALUE)
          lines (inc (.getLineNumber lnr))]
      (.close lnr)
      (int (Math/ceil (/ 100 partition-size)))))

  (read-partition [this part]
    (with-open [rdr (clojure.java.io/reader (str location extension))]
      (->> (line-seq rdr)
           (drop (* part partition-size))
           (take partition-size)
           (map read-string)
           (doall))))
  
  p-ext/PipelineBatchOutput
  (write-partition [this part content]
    (doseq [segment content]
      (spit (str location "-part-" part extension) (str (pr-str segment) "\n") :append true))))

(defn input [pipeline-data]
  (->LocalFile (:file/path (:onyx.core/task-map pipeline-data))
               (:file/extension (:onyx.core/task-map pipeline-data))))

(defn output [pipeline-data]
  (->LocalFile (:file/path (:onyx.core/task-map pipeline-data))
               (:file/extension (:onyx.core/task-map pipeline-data))))
