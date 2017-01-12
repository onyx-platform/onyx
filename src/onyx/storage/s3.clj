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
        _ (when content-type
            (.setContentType metadata content-type))
        _  (when encryption-setting 
             (.setSSEAlgorithm metadata encryption-setting))
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

(defmethod onyx.checkpoint/storage :s3 [peer-config]
  (-> (new-client)
      ;; use correct region endpoint, set via config
      (set-region "us-west-2")
      ;(accelerate-client)
      (transfer-manager))) 

(defn checkpoint-task-key [tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (str tenancy-id "/" job-id "/" replica-version "-" epoch "/" (name task-id)
       "/" slot-id "/" (name checkpoint-type)))

(def bucket "onyx-s3-testing")

(defmethod checkpoint/write-checkpoint com.amazonaws.services.s3.transfer.TransferManager
  [transfer-manager tenancy-id job-id replica-version epoch 
   task-id slot-id checkpoint-type checkpoint-bytes]
  ;(.setMultipartCopyPartSize (.getConfiguration transfer-manager) 1000000)
  ;(.setMultipartUploadThreshold (.getConfiguration transfer-manager) 1000000)
  (let [up ^Upload (upload transfer-manager 
                   bucket
                   (checkpoint-task-key tenancy-id job-id replica-version epoch task-id
                                        slot-id checkpoint-type)
                   checkpoint-bytes
                   "nippy"
                   :none)]
    (time 
     (loop [state (.getState up)]
       (cond (= (Transfer$TransferState/Failed) state)
             (throw (.waitForException ^Upload up))

             (= (Transfer$TransferState/Completed) state)
             nil

             :else (do
                    ;(println "Still uploading" state)
                    ;; TODO, return back to state machine, and don't advance
                    (Thread/sleep 1)
                    (recur (.getState up))))))))

(defmethod checkpoint/read-checkpoint com.amazonaws.services.s3.transfer.TransferManager
  [^TransferManager transfer-manager tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (-> transfer-manager
      (.getAmazonS3Client)
      (checkpointed-bytes bucket
                          (checkpoint-task-key tenancy-id job-id replica-version epoch task-id
                                               slot-id checkpoint-type))))
