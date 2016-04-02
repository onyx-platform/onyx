(ns ^:no-doc onyx.messaging.aeron.publication-manager
  (:require [taoensso.timbre :refer [error fatal info warn] :as timbre]
            [clojure.core.async :refer [chan >!! <!! put! close! sliding-buffer thread]])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription]
           [org.agrona ErrorHandler]
           [io.aeron.exceptions DriverTimeoutException]
           [org.agrona.concurrent IdleStrategy]))

(defprotocol PPublicationManager
  (write [this buf start end])
  (connect [_])
  (start [_])
  (stop [_]))

(defrecord Message [buf start end])

(defn write-to-pub [^Publication pub ^IdleStrategy send-idle-strategy msg]
  (loop [result ^long (.offer ^Publication pub (:buf msg) (:start msg) (:end msg))]
    (when (neg? result)
      (if (= result Publication/CLOSED)
        (throw (IllegalStateException. "Publication has been closed"))
        (.idle send-idle-strategy 0))
      (recur ^long (.offer ^Publication pub (:buf msg) (:start msg) (:end msg))))))

(defn write-from-buffer [publication-manager pending-ch send-idle-strategy]
  (try 
    (let [pub (:publication publication-manager)] 
      (loop [] 
        (when-let [msg (<!! pending-ch)]
          (write-to-pub pub send-idle-strategy msg)
          (recur))))
    (catch InterruptedException e)
    (catch DriverTimeoutException e
      (fatal "Aeron write failure due to driver time out." e)
      (stop publication-manager))
    (catch IllegalStateException e
      (fatal "Writing to closed publication." e)
      (stop publication-manager))
    (catch Throwable t
      (fatal "Aeron write from buffer error: " t)
      (stop publication-manager))))

(extend-protocol PPublicationManager nil 
  (start [this]
    (warn "Started nil publication manager, likely due to timeout on creation."))
  (write [this _ _ _]
    (warn "Writing nil publication manager, likely due to timeout on creation."))
  (stop [this]
    (warn "Stopping nil publication manager, likely due to timeout on creation."))
  (connect [this]
    (warn "Connecting to nil publication manager, likely due to timeout on creation.")))

(defrecord PublicationManager [channel stream-id send-idle-strategy connection publication pending-ch write-fut cleanup-fn]
  PPublicationManager
  (start [this]
    (assoc this :write-fut (future (write-from-buffer this pending-ch send-idle-strategy))))

  (write [this buf start end]
    ;; use core.async offer! here and call monitoring when blocked
    ;; will only be possible when core.async releases master again
    (>!! pending-ch (->Message buf start end)))

  (stop [this]
    (info (format "Stopped publication manager at: %s, stream-id: %s" channel stream-id))
    (cleanup-fn)
    (future-cancel (:write-fut this))
    (close! pending-ch)
    (try (.close ^Publication publication)
         (catch Throwable t 
           (info "Could not close publication:" t)))
    (try (.close ^Aeron connection)
         (catch Throwable t 
           (info "Could not close connection" t)))
    this)

  (connect [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (taoensso.timbre/warn "Aeron messaging publication error:" x)))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler))
          conn (Aeron/connect ctx)
          pub (.addPublication conn channel stream-id)]
      (info (format "Created publication at: %s, stream-id: %s" channel stream-id))
      (assoc this :publication pub :connection conn))))

(defn new-publication-manager [channel stream-id send-idle-strategy write-buffer-size cleanup-fn]
  (->PublicationManager channel stream-id send-idle-strategy 
                        nil nil (chan write-buffer-size) nil cleanup-fn))


