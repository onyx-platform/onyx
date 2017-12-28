(ns onyx.storage.s3
  (:require [onyx.checkpoint :as checkpoint]
            [onyx.monitoring.metrics-monitoring :as m]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns ns->ms]]
            [taoensso.timbre :refer [info error warn trace fatal debug] :as timbre])
  (:import [com.amazonaws.auth DefaultAWSCredentialsProviderChain BasicAWSCredentials]
           [com.amazonaws.handlers AsyncHandler]
           [com.amazonaws ClientConfiguration]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.event ProgressListener$ExceptionReporter]
           [com.amazonaws.services.s3.transfer TransferManager TransferManagerConfiguration Upload Transfer$TransferState]
           [com.amazonaws.services.s3 AmazonS3Client S3ClientOptions]
           [com.amazonaws.services.s3.model S3ObjectSummary S3ObjectInputStream PutObjectRequest GetObjectRequest ObjectMetadata AmazonS3Exception]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.event ProgressEventType]
           [java.util.concurrent.locks LockSupport]
           [java.io ByteArrayInputStream InputStreamReader BufferedReader]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.codec.binary Base64]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent.atomic AtomicLong]))

(defn new-client ^AmazonS3Client [peer-config]
   (case (arg-or-default :onyx.peer/storage.s3.auth-type peer-config)
     :provider-chain (let [credentials (DefaultAWSCredentialsProviderChain.)]
                 (AmazonS3Client. credentials))
     :config (let [access-key (:onyx.peer/storage.s3.auth.access-key peer-config) 
                   secret-key (:onyx.peer/storage.s3.auth.secret-key peer-config) 
                   _ (when-not (and access-key secret-key)
                       (throw (ex-info "When :onyx.peer/storage.s3.auth-type is set to :config, both :onyx.peer/storage.s3.auth.access-key and :onyx.peer/storage.s3.auth.secret-key must be defined." {:access-key access-key :secret-key secret-key})))
                   credentials (BasicAWSCredentials. access-key secret-key)]
               (AmazonS3Client. credentials))))

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
        _ (some->> content-type (.setContentType metadata))
        _ (some->> encryption-setting (.setSSEAlgorithm metadata))
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

(defn read-checkpointed-bytes [^AmazonS3Client client ^String bucket ^String k]
  (let [object-request (GetObjectRequest. bucket k)
        object (.getObject client object-request)
        nbytes (.getContentLength (.getObjectMetadata object))
        bs (byte-array nbytes)
        n-read (loop [offset 0]
                 (let [n-read (.read (.getObjectContent object) bs offset (- nbytes offset))]
                   (if-not (= n-read -1)
                     (recur (+ offset n-read))
                     offset)))]
    (.close object)
    (when-not (= nbytes n-read)
      (throw (ex-info "Didn't read entire checkpoint."
                      {:key k
                       :bytes-read n-read
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

(defrecord CheckpointManager [id monitoring client transfer-manager bucket encryption transfers timeout-ns])

(defmethod onyx.checkpoint/storage :s3 [peer-config monitoring]
  (let [id (java.util.UUID/randomUUID)
        region (:onyx.peer/storage.s3.region peer-config)
        endpoint (:onyx.peer/storage.s3.endpoint peer-config)
        accelerate? (:onyx.peer/storage.s3.accelerate? peer-config)
        encryption (arg-or-default :onyx.peer/storage.s3.encryption peer-config)
        timeout-ns (ms->ns (arg-or-default :onyx.peer/storage.timeout peer-config))
        bucket (or (:onyx.peer/storage.s3.bucket peer-config)
                   (throw (Exception. ":onyx.peer/storage.s3.bucket must be supplied via peer-config when using :onyx.peer/storage = :s3.")))
        client (new-client peer-config)
        transfer-manager (cond-> client
                           endpoint (set-endpoint endpoint)
                           region (set-region region)
                           accelerate? (accelerate-client)
                           true (transfer-manager))
        configuration ^TransferManagerConfiguration (.getConfiguration ^TransferManager transfer-manager)]
    (when-let [v (:onyx.peer/storage.s3.multipart-copy-part-size peer-config)]
      (.setMultipartCopyPartSize configuration (long v)))
    (when-let [v (:onyx.peer/storage.s3.multipart-upload-threshold peer-config)]
      (.setMultipartUploadThreshold configuration (long v)))
    (->CheckpointManager id monitoring client transfer-manager bucket encryption (atom []) timeout-ns)))

(defn checkpoint-task-key [tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  ;; We need to prefix the checkpoint key in a random way to partition keys for
  ;; maximum write performance.
  (let [prefix-hash (mod (hash [tenancy-id job-id replica-version epoch task-id slot-id]) 100000)]
    (str prefix-hash "_" tenancy-id "/"
         job-id "/"
         replica-version "-" epoch "/"
         (namespace task-id) 
         (if (namespace task-id) "-") (name task-id)
         "/" slot-id "/"
         (name checkpoint-type))))

(defmethod checkpoint/write-checkpoint onyx.storage.s3.CheckpointManager
  [{:keys [transfer-manager transfers bucket encryption] :as storage} tenancy-id job-id replica-version epoch
   task-id slot-id checkpoint-type ^bytes checkpoint-bytes]
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id
                               slot-id checkpoint-type)
        _ (debug "Starting checkpoint to s3 under key" k)
        up ^Upload (onyx.storage.s3/upload ^TransferManager transfer-manager
                                           bucket
                                           k
                                           checkpoint-bytes
                                           "application/octet-stream"
                                           encryption)]
    (swap! transfers conj {:key k
                           :upload up
                           :size-bytes (alength checkpoint-bytes)
                           :start-time (System/nanoTime)})
    storage))

(defmethod checkpoint/complete? onyx.storage.s3.CheckpointManager
  [{:keys [transfers monitoring timeout-ns]}]
  (empty? 
   (swap! transfers
          (fn [tfers]
            (doall
             (keep (fn [transfer]
                     (let [{:keys [key upload size-bytes start-time]} transfer
                           state (.getState ^Upload upload)
                           elapsed (- (System/nanoTime) start-time)]
                       (cond (> elapsed timeout-ns)
                             (throw (ex-info "S3 upload forcefully timed out by storage interface."
                                             {:timeout-ms (ns->ms timeout-ns)
                                              :elapsed-ms (ns->ms elapsed)}))

                             (= (Transfer$TransferState/Failed) state)
                             (throw (.waitForException ^Upload upload))

                             (= (Transfer$TransferState/Canceled) state)
                             (throw (ex-info "S3 checkpoint was cancelled. This should never happen." {}))

                             (= (Transfer$TransferState/Completed) state)
                             (let [{:keys [checkpoint-store-latency 
                                           checkpoint-written-bytes]} monitoring]
                               (debug "Completed checkpoint to s3 under key" key)
                               (m/update-timer-ns! checkpoint-store-latency elapsed)
                               (.addAndGet ^AtomicLong checkpoint-written-bytes size-bytes)
                               nil)

                             :else
                             transfer)))
                   tfers))))))

(defmethod checkpoint/cancel! onyx.storage.s3.CheckpointManager
  [{:keys [transfers]}]
  (run! #(.abort ^Upload (:upload %)) @transfers)
  (reset! transfers nil))

(defmethod checkpoint/stop onyx.storage.s3.CheckpointManager
  [{:keys [client transfer-manager id] :as storage}]
  (checkpoint/cancel! storage)
  (.shutdownNow ^TransferManager transfer-manager true))

(def max-read-checkpoint-retries 5)

(defmethod checkpoint/read-checkpoint onyx.storage.s3.CheckpointManager
  [{:keys [transfer-manager bucket id monitoring] :as storage} 
   tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type)]
    (loop [n-retries max-read-checkpoint-retries]
      (let [result (try
                     (-> (.getAmazonS3Client ^TransferManager transfer-manager)
                         (read-checkpointed-bytes bucket k))
                     (catch AmazonS3Exception es3 es3))]
        (if (= (type result) com.amazonaws.services.s3.model.AmazonS3Exception)
          (if (and (pos? n-retries)
                   (= "NoSuchKey"
                      (.getErrorCode ^AmazonS3Exception result)))
            (do
              (info (format "Unable to read S3 checkpoint as the key, %s, does not exist yet. Retrying up to %s more times."
                            k n-retries))
              (LockSupport/parkNanos (* 1000 1000000))
              (recur (dec n-retries)))
            (throw result))
          (do
           (.addAndGet ^AtomicLong (:checkpoint-read-bytes monitoring) (alength ^bytes result))
           result))))))

(defmethod checkpoint/gc-checkpoint! onyx.storage.s3.CheckpointManager
  [{:keys [client bucket monitoring] :as storage} 
   tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  ;; TODO: add monitoring.
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type)]
    (info "Garbage collecting key:" k)
    (.deleteObject ^AmazonS3Client client bucket k)))
