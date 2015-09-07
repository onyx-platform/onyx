(ns onyx.messaging.aeron.publication-manager
  (:require [taoensso.timbre :refer [error fatal info] :as timbre]
            [clojure.core.async :refer [chan >!! <!! put! close! sliding-buffer thread]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription AvailableImageHandler]
           [uk.co.real_logic.agrona ErrorHandler CloseHelper]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]))

(defprotocol PPublicationManager
  (write [this buf start end])
  (connect [_])
  (disconnect [_])
  (reset-connection [_])
  (start [_])
  (stop [_]))

(defrecord Message [buf start end])

(defn write-from-buffer [pending-ch publication send-idle-strategy]
  (try 
    (loop [] 
      (when-let [msg (<!! pending-ch)]
        (while (if-let [pub @publication]
                 (let [result ^long (.offer pub (:buf msg) (:start msg) (:end msg))]
                   (or (= result Publication/BACK_PRESSURED)
                       (= result Publication/NOT_CONNECTED))))
          ;; idle for different amounts of time depending on whether backpressuring or not?
          (.idle ^IdleStrategy send-idle-strategy 0))
        (recur)))
    (catch InterruptedException e)
    (catch Throwable t
      ;; TODO, handle this outcome better. Should reset connection?
      (fatal "Aeron write from buffer error: " t))))

(defrecord PublicationManager [channel stream-id send-idle-strategy connection publication pending-ch write-fut]
  PPublicationManager
  (start [this]
    (assoc this :write-fut (future (write-from-buffer pending-ch publication send-idle-strategy))))

  (reset-connection [this]
    (connect (disconnect this)))

  (write [this buf start end]
    ;; use core.async offer! here and call monitoring when blocked
    ;; will only be possible when core.async releases master again
    (>!! pending-ch (->Message buf start end)))

  (disconnect [this]
    (.close ^Publication @publication)
    (.close ^Aeron @connection)
    (reset! publication nil)
    (reset! connection nil)
    this)

  (stop [this]
    (disconnect this)
    (future-cancel (:write-fut this))
    (close! pending-ch)
    this)

  (connect [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (taoensso.timbre/warn "Aeron messaging publication error:" x)
                            #_(taoensso.timbre/warn "Resetting Aeron publication.")
                            #_(reset-connection this)))
          conn (Aeron/connect (.errorHandler (Aeron$Context.) error-handler))
          pub (.addPublication conn channel stream-id)]
      (reset! publication pub)
      (reset! connection conn) 
      this)))

(defn new-publication-manager [channel stream-id send-idle-strategy write-buffer-size]
  (->PublicationManager channel stream-id send-idle-strategy 
                        (atom nil) (atom nil) (chan write-buffer-size) nil))
