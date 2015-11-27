(ns ^:no-doc onyx.messaging.aeron.publication-manager
  (:require [taoensso.timbre :refer [error fatal info] :as timbre]
            [clojure.core.async :refer [chan >!! <!! put! close! sliding-buffer thread]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription AvailableImageHandler]
           [uk.co.real_logic.agrona ErrorHandler CloseHelper]
           [uk.co.real_logic.aeron.exceptions DriverTimeoutException]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]))

(defprotocol PPublicationManager
  (write [this buf start end])
  (connect [_])
  (start [_])
  (stop [_]))

(defrecord Message [buf start end])

(defn write-from-buffer [publication-manager pending-ch send-idle-strategy]
  (try 
    (let [pub (:publication publication-manager)] 
      (loop [] 
        (when-let [msg (<!! pending-ch)]
          (while (let [result ^long (.offer ^Publication pub (:buf msg) (:start msg) (:end msg))]
                   (or (= result Publication/BACK_PRESSURED)
                       (= result Publication/NOT_CONNECTED)))
            ;; idle for different amounts of time depending on whether backpressuring or not?
            (.idle ^IdleStrategy send-idle-strategy 0))
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


