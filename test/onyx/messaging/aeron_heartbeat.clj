(ns onyx.messaging.aeron-heartbeat
  (:require [onyx.messaging.aeron :as am]
            [onyx.messaging.messenger :as m]

            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [clojure.test :refer [deftest is testing]]
            )
  
  )

;; Setup an upstream task
;; Setup two downstream peers on a dst-task-id with same task-id
;; Publications will now need to know downstream peers as well as dst-task-id so they know what heartbeats to expect.
;; Subscriptions will need to know upstream peer, but this is already known in src-peer-id

;; Upstream task starts offering barriers. 
   ;; Iterate through all the publications 
        ;; Offer a barrier in each iteration to peers that haven't replied with a ready
        ;; Send a heartbeat in each iteration to peers that haven't sent a heartbeat. This should include replica-version
   ;; Poll heartbeat channel, if you receive a heartbeat, set the publication to ready. Upon receiving either a heartbeat or a ready [replica epoch], reset heartbeat count.
   ;; For 


;; Extra peer group subscriber that can look at all the heartbeats and make an overall decision? Would also allow this to be exposed to a health endpoint more easily

(deftest aa-test
  (let [peer-config {:onyx.messaging.aeron/embedded-driver? true
                     :onyx.messaging.aeron/embedded-media-driver-threading :shared
                     :onyx.messaging/peer-port 42000
                     :onyx.messaging/bind-addr "127.0.0.1"
                     :onyx.messaging/impl :aeron}
        media-driver (component/start (am/->EmbeddedMediaDriver peer-config))]
    (try
     (let [peer-group (component/start (m/build-messenger-group peer-config))]
       (try
        (let [upstream1 (component/start (m/build-messenger peer-config peer-group :upstream1))]
          (try
           (let [downstream1 (component/start (m/build-messenger peer-config peer-group :downstream1))
                 site {:address (:onyx.messaging/bind-addr peer-config)
                       :port (:onyx.messaging/peer-port peer-config)}]
             (try 
              (-> upstream1
                  (m/add-publication {:src-peer-id :upstream1 
                                      :dst-task-id :downstream-1 
                                      :site site 
                                      :slot-id -1})
                  (m/set-replica-version! 1))
              (-> downstream1
                  (m/add-subscription {:src-peer-id :upstream1 
                                       :dst-task-id :downstream-1 
                                       :site site 
                                       :slot-id -1})
                  (m/set-replica-version! 1))
              ;; Put on availableimage to check correlation id and registration id for both
              (while (= -1 (m/offer-barrier upstream1 (first (m/publications upstream1)))))
              (Thread/sleep 1000)
              (println "Pub reg" 
                       (.sessionId (:publication (first (m/publications upstream1))))
                       (mapv (fn [i] (.sessionId i)) (.images (:subscription (first (m/subscriptions downstream1))))))
              (-> upstream1
                  (m/remove-publication {:src-peer-id :upstream1 
                                         :dst-task-id :downstream-1 
                                         :site site 
                                         :slot-id -1})
                  )
              (Thread/sleep 1000)
              (Thread/sleep 40000)
              (println "Images now"
                       (mapv (fn [i] (.sessionId i)) (.images (:subscription (first (m/subscriptions downstream1))))))

              (-> upstream1
                  (m/add-publication {:src-peer-id :upstream1 
                                      :dst-task-id :downstream-1 
                                      :site site 
                                      :slot-id -1}))

              (while (= -1 (m/offer-barrier upstream1 (first (m/publications upstream1)))))
              (Thread/sleep 1000)
              (println "Pub reg" 
                       (.sessionId (:publication (first (m/publications upstream1))))
                       (mapv (fn [i] (.sessionId i)) (.images (:subscription (first (m/subscriptions downstream1))))))

              (finally
               (component/stop downstream1))))
           (finally (component/stop upstream1))))
        (finally
         (component/stop peer-group)))) 
     (finally
      (component/stop media-driver)))))

