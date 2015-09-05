(ns onyx.messaging.aeron.publication-manager
  (:require [taoensso.timbre :refer [fatal info] :as timbre])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription AvailableImageHandler]
           [uk.co.real_logic.agrona ErrorHandler CloseHelper]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]))

(def no-op-error-handler
  (reify ErrorHandler
    (onError [this x] (taoensso.timbre/warn x))))

(def ^:const publication-backpressured (long -2))
(def ^:const publication-not-connected (long -1))

(defprotocol PPublicationManager
  (connect [_ channel stream-id])
  (send-pub [this buf start end])
  (close [_])
  (reset [_]))

(defrecord PublicationManager [messenger connection publication pending-ch]
  PPublicationManager
  (reset [this]) 

  (send-pub [this buf start end]
    (let [pub ^Publication @publication
          offer-f (fn [] (.offer pub buf start end))]
      (while (let [result ^long (offer-f)]
               (or (= result publication-backpressured)
                   (= result publication-not-connected)))
        (.idle ^IdleStrategy (:send-idle-strategy messenger) 0))))

  (close [this]
    (.close ^Publication @publication)
    (.close ^Aeron @connection)
    (reset! publication nil)
    (reset! connection nil)
    this)

  (connect [this channel stream-id]
    (let [conn (Aeron/connect (.errorHandler (Aeron$Context.) no-op-error-handler))
          pub (.addPublication conn channel stream-id)]
      (reset! publication pub)
      (reset! connection conn)
      this)))

(defn new-publication-manager [messenger]
  (->PublicationManager messenger (atom nil) (atom nil) nil #_(chan 1000)))
