(ns onyx.coordinator.concurrent-task-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.coordinator :refer [onyx-coordinator]]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(facts
 "plan one job with four peers concurrently"
 (with-system
   (fn [coordinator sync log]
     (let [n 4
           peers (take n (repeatedly (fn [] (extensions/create sync :peer))))
           pulses (take n (repeatedly (fn [] (extensions/create sync :pulse))))
           shutdowns (take n (repeatedly (fn [] (extensions/create sync :shutdown))))
           payloads (take n (repeatedly (fn [] (extensions/create sync :payload))))
           sync-spies (take n (repeatedly (fn [] (chan 1))))
           
           status-spy (chan (* n 5))
           offer-ch-spy (chan (* n 5))
           ack-ch-spy (chan (* n 5))

           catalog [{:onyx/name :in
                     :onyx/direction :input
                     :onyx/type :queue
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "in-queue"}
                    {:onyx/name :inc
                     :onyx/type :transformer
                     :onyx/consumption :concurrent}
                    {:onyx/name :out
                     :onyx/direction :output
                     :onyx/type :queue
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "out-queue"}]
           workflow {:in {:inc :out}}]

       (tap (:offer-mult coordinator) offer-ch-spy)
       (tap (:ack-mult coordinator) ack-ch-spy)
       
       (doseq [[peer pulse shutdown payload sync-spy]
               (map vector peers pulses shutdowns payloads sync-spies)]
         (extensions/write-place sync peer {:pulse pulse
                                            :shutdown shutdown
                                            :payload payload})
         (extensions/on-change sync payload #(>!! sync-spy %)))

       (doseq [peer peers]
         (>!! (:born-peer-ch-head coordinator) peer))

       (doseq [_ (range n)]
         (<!! offer-ch-spy))

       (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})
       (<!! offer-ch-spy)

       (doseq [_ (range n)]
         (alts!! sync-spies))

       (let [db (d/db (:conn log))]
         
         (facts "Four peers are :acking"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/status :acking]]
                      result (ffirst (d/q query db))]
                  (fact result => 4)))

         (facts "No peers are idle"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/status :idle]]
                      result (or (ffirst (d/q query db)) 0)]
                  (fact result => zero?)))

         (facts ":inc has two active peers"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/task ?task]
                              [?task :task/name :inc]]
                      result (ffirst (d/q query db))]
                  (fact result => 2))))))
   {:revoke-delay 50000}))

