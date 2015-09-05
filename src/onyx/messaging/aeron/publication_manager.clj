(ns onyx.messaging.aeron.publication-manager
  (:require [taoensso.timbre :refer [error fatal info] :as timbre]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer thread]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription AvailableImageHandler]
           [uk.co.real_logic.agrona ErrorHandler CloseHelper]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]))

(def ^:const backpressured (long -2))
(def ^:const not-connected (long -1))

(defprotocol PPublicationManager
  (write [this buf start end])
  (connect [_])
  (disconnect [_])
  (reset-connection [_])
  (start [_])
  (stop [_]))

(def write-buffer-size 1000)

(defrecord Message [buf start end])

(defn write-from-buffer [pending-ch publication send-idle-strategy]
  (try 
    (loop [] 
      (when-let [msg (<!! pending-ch)]
        (while (if-let [pub @publication]
                 (let [result ^long (.offer pub (:buf msg) (:start msg) (:end msg))]
                   (or (= result backpressured)
                       (= result not-connected))))
          ;; idle for different amounts of time depending on whether backpressuring or not?
          (.idle ^IdleStrategy send-idle-strategy 0))
        (recur)))
    (catch InterruptedException e)
    (catch Throwable t
      (error "Aeron write from buffer error: " t))))

(defrecord PublicationManager [send-idle-strategy channel stream-id connection publication pending-ch write-fut]
  PPublicationManager
  (start [this]
    (assoc this :write-fut (future (write-from-buffer pending-ch publication send-idle-strategy))))

  (reset-connection [this]
    (connect (disconnect this)))

  (write [this buf start end]
    ;; should possibly use core-async offer here and print out message if going to block
    (when-not (>!! pending-ch (->Message buf start end))))

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

(defn new-publication-manager [send-idle-strategy channel stream-id]
  (->PublicationManager send-idle-strategy channel stream-id 
                        (atom nil) (atom nil) (chan write-buffer-size) nil))
