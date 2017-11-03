(ns onyx.messaging.aeron.utils
  (:require [onyx.messaging.common :refer [bind-addr bind-port]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [io.aeron.logbuffer ControlledFragmentHandler$Action]
           [io.aeron.driver Configuration]
           [io.aeron Subscription Image Publication Aeron]))

(defn action->kw [action]
  (cond (= action ControlledFragmentHandler$Action/CONTINUE)
        :CONTINUE
        (= action ControlledFragmentHandler$Action/BREAK)
        :BREAK
        (= action ControlledFragmentHandler$Action/ABORT)
        :ABORT
        (= action ControlledFragmentHandler$Action/COMMIT)
        :COMMIT
        :else
        (throw (Exception. (str "Invalid action " action)))))

(def heartbeat-stream-id 0)

(defn stream-id [task-id slot-id site]
  (hash [task-id slot-id site]))

(defn channel 
  ([addr port]
   (channel addr port (* 8 524288)))
  ([addr port term-length]
   (format "aeron:udp?endpoint=%s:%s|term-length=%s" addr port term-length))
  ([peer-config]
   (channel (bind-addr peer-config) (bind-port peer-config))))

(defn short-circuit? [peer-config site]
  (boolean 
   (and (arg-or-default :onyx.messaging/allow-short-circuit? peer-config)
        (= (channel peer-config)
           (channel (:address site) (:port site))))))

(defn image->map [^Image image]
  {:pos (.position image) 
   :term-id (.initialTermId image)
   :session-id (.sessionId image) 
   :closed? (.isClosed image) 
   :correlation-id (.correlationId image) 
   :source-id (.sourceIdentity image)})

(defn max-aeron-message-length [peer-config]
  (/ (max (arg-or-default :onyx.messaging/term-buffer-size.segment-short-circuit peer-config) 
          (arg-or-default :onyx.messaging/term-buffer-size.segment peer-config))
     8))

(defn try-close-subscription [^Subscription subscription]
  (try
   (.close subscription)
   (catch io.aeron.exceptions.DriverTimeoutException dte
     (info "Driver timeout exception stopping publisher."))
   (catch io.aeron.exceptions.RegistrationException re
     (info "Error stopping subscriber's subscription." re))))

(defn try-close-conn [^Aeron conn]
 (try
  (.close conn)
  (catch io.aeron.exceptions.DriverTimeoutException dte
    (info "Driver timeout exception stopping subscription"))
  (catch io.aeron.exceptions.RegistrationException re
    (info "Error stopping subscription." re))))

(defn try-close-publication [^Publication publication]
 (try
  (.close publication)
  (catch io.aeron.exceptions.DriverTimeoutException dte
    (info "Driver timeout exception stopping publication"))
  (catch io.aeron.exceptions.RegistrationException re
    (info "Error stopping publication" re))))
