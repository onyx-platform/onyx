(ns onyx.storage.s3
  (:require [onyx.checkpoint :as checkpoint])
  (:import [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [com.amazonaws.handlers AsyncHandler]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.event ProgressListener$ExceptionReporter]
           [com.amazonaws.services.s3.transfer TransferManager Upload Transfer$TransferState]
           [com.amazonaws.services.s3 AmazonS3Client S3ClientOptions]
           [com.amazonaws.services.s3.model S3ObjectSummary S3ObjectInputStream PutObjectRequest GetObjectRequest ObjectMetadata]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.event ProgressEventType]
           [java.io ByteArrayInputStream InputStreamReader BufferedReader]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.codec.binary Base64]))

(defn new-client ^AmazonS3Client []
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (AmazonS3Client. credentials)))

(defn accelerate-client [^AmazonS3Client client]
  (doto client 
    (.setS3ClientOptions (.build (.setAccelerateModeEnabled (S3ClientOptions/builder) true)))))

(defn set-endpoint [^AmazonS3Client client ^String endpoint]
  (doto client
    (.setEndpoint endpoint)))

(defn set-region [^AmazonS3Client client region]
  (doto client
    (.setRegion (RegionUtils/getRegion region))))

(defn transfer-manager ^TransferManager [^AmazonS3Client client]
  (TransferManager. client))

(defn upload [^TransferManager transfer-manager ^String bucket ^String key 
              ^bytes serialized ^String content-type encryption]
  (let [size (alength serialized)
        md5 (String. (Base64/encodeBase64 (DigestUtils/md5 serialized)))
        encryption-setting (case encryption 
                             :aes256 
                             (ObjectMetadata/AES_256_SERVER_SIDE_ENCRYPTION)
                             :none nil
                             (throw (ex-info "Unsupported encryption type." 
                                             {:encryption encryption})))
        metadata (doto (ObjectMetadata.)
                  (.setContentLength size)
                  (.setContentMD5 md5))
        _ (some->> content-type
                   (.setContentType metadata))
        _ (some->> encryption-setting 
                   (.setSSEAlgorithm metadata))
        put-request (PutObjectRequest. bucket
                                       key
                                       (ByteArrayInputStream. serialized)
                                       metadata)
        upload ^Upload (.upload transfer-manager put-request)]
    upload))

(defn upload-synchronous [^AmazonS3Client client ^String bucket ^String k ^bytes serialized]
  (let [size (alength serialized)
        md5 (String. (Base64/encodeBase64 (DigestUtils/md5 serialized)))
        metadata (doto (ObjectMetadata.)
                  (.setContentMD5 md5)
                  (.setContentLength size))]
    (.putObject client
                bucket
                k
                (ByteArrayInputStream. serialized)
                metadata)))

(defn s3-object-input-stream ^S3ObjectInputStream
  [^AmazonS3Client client ^String bucket ^String k & [start-range]]
  (let [object-request (GetObjectRequest. bucket k)
        object (.getObject client object-request)]
    (.getObjectContent object)))

(defn checkpointed-bytes [^AmazonS3Client client ^String bucket ^String k]
  (let [object-request (GetObjectRequest. bucket k)
        object (.getObject client object-request)
        nbytes (.getContentLength (.getObjectMetadata object))
        bs (byte-array nbytes)
        n-read (.read (.getObjectContent object) bs)]
    (when-not (= nbytes n-read)
      (throw (ex-info "Didn't read entire checkpoint."
                      {:bytes-read n-read
                       :size nbytes})))
    bs))

(defn list-keys [^AmazonS3Client client ^String bucket ^String prefix]
  (loop [listing (.listObjects client bucket prefix) ks []]
    (let [new-ks (into ks 
                       (map (fn [^S3ObjectSummary s] (.getKey s)) 
                            (.getObjectSummaries listing)))]
      (if (.isTruncated listing)
        (recur (.listObjects client bucket prefix) new-ks)
        new-ks))))

(defrecord CheckpointManager [transfer-manager upload start-time])

(defmethod onyx.checkpoint/storage :s3 [peer-config]
  ;; TODO, siet region via peer-config
  ;; TODO, set accelerate client via peer config
  ;; TODO, set bucket via peer config
  ;;
  ;; use correct region endpoint, set via config
  ; (set-region "us-west-2")
  ; (accelerate-client)


  (->CheckpointManager (-> (new-client)
                           ;; use correct region endpoint, set via config
                           (set-region "us-west-2")
                           ;(accelerate-client)
                           (transfer-manager))
                       (atom nil)
                       (atom nil))) 

(defn checkpoint-task-key [tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  ;; We need to prefix the checkpoint key in a random way to partition keys for
  ;; maximum write performance.
  (let [prefix-hash (hash [tenancy-id job-id replica-version epoch task-id slot-id])]
    (str prefix-hash "_" tenancy-id "/" job-id "/" replica-version "-" epoch "/" (name task-id)
         "/" slot-id "/" (name checkpoint-type))))

(def bucket "onyx-s3-testing")

(defmethod checkpoint/write-checkpoint onyx.storage.s3.CheckpointManager
  [{:keys [transfer-manager upload start-time] :as storage} tenancy-id job-id replica-version epoch 
   task-id slot-id checkpoint-type checkpoint-bytes]
  ;(.setMultipartCopyPartSize (.getConfiguration transfer-manager) 1000000)
  ;(.setMultipartUploadThreshold (.getConfiguration transfer-manager) 1000000)
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id
                               slot-id checkpoint-type)
        up ^Upload (onyx.storage.s3/upload ^TransferManager transfer-manager 
                                           bucket
                                           k 
                                           checkpoint-bytes
                                           "application/octet-stream"
                                           :none)]
    (reset! upload up)
    (reset! start-time (System/nanoTime))
    storage))

(defmethod checkpoint/write-complete? onyx.storage.s3.CheckpointManager
  [{:keys [upload start-time]}]
  (if-not @upload
    true
    (let [state (.getState ^Upload @upload)] 
      (cond (= (Transfer$TransferState/Failed) state)
            (throw (.waitForException ^Upload @upload))

            (= (Transfer$TransferState/Completed) state)
            (do
             (println "TOOK" (float (/ (- (System/nanoTime) @start-time) 1000000)))
             (reset! start-time nil)
             (reset! upload nil)
             true)

            :else
            false))))

(defmethod checkpoint/cancel! onyx.storage.s3.CheckpointManager
  [{:keys [upload]}]
  (when-let [u @upload]
    (reset! upload nil)
    (.abort ^Upload u)))

(defmethod checkpoint/stop onyx.storage.s3.CheckpointManager
  [{:keys [transfer-manager] :as storage}]
  (checkpoint/cancel! storage)
  (.shutdownNow ^TransferManager transfer-manager))

(defmethod checkpoint/read-checkpoint onyx.storage.s3.CheckpointManager
  [{:keys [transfer-manager]} tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id
                               slot-id checkpoint-type)]
    (-> (.getAmazonS3Client ^TransferManager transfer-manager)
      (checkpointed-bytes bucket k))))
