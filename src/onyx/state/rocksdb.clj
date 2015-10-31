(ns onyx.state.rocksdb
  (:import [org.rocksdb CompressionType]))

(defn compression-option->type [option]
  (case option
    :bzip2 CompressionType/BZLIB2_COMPRESSION
    :lz4 CompressionType/LZ4_COMPRESSION
    :lz4hc CompressionType/LZ4HC_COMPRESSION
    :none CompressionType/NO_COMPRESSION
    :snappy CompressionType/SNAPPY_COMPRESSION
    :zlib CompressionType/ZLIB_COMPRESSION))
