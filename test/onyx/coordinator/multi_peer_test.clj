(ns onyx.coordinator.multi-peer-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(facts
 "plan one job with two peers"
 (with-system
   (fn [coordinator sync log]
     (let [peer-node-a (extensions/create sync :peer)
           peer-node-b (extensions/create sync :peer)

           pulse-node-a (extensions/create sync :pulse)
           pulse-node-b (extensions/create sync :pulse)

           shutdown-node-a (extensions/create sync :shutdown)
           shutdown-node-b (extensions/create sync :shutdown)

           payload-node-a-1 (extensions/create sync :payload)
           payload-node-b-1 (extensions/create sync :payload)

           payload-node-a-2 (extensions/create sync :payload)
           payload-node-b-2 (extensions/create sync :payload)

           sync-spy-a (chan 1)
           sync-spy-b (chan 1)
           ack-ch-spy (chan 2)
           offer-ch-spy (chan 10)
           status-spy (chan 2)
           
           catalog [{:onyx/name :in
                     :onyx/direction :input
                     :onyx/type :queue
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "in-queue"}
                    {:onyx/name :inc
                     :onyx/type :transformer
                     :onyx/consumption :sequential}
                    {:onyx/name :out
                     :onyx/direction :output
                     :onyx/type :queue
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "out-queue"}]
           workflow {:in {:inc :out}}]

       (tap (:ack-mult coordinator) ack-ch-spy)
       (tap (:offer-mult coordinator) offer-ch-spy)

       (extensions/write-place sync peer-node-a {:pulse pulse-node-a
                                                 :shutdown shutdown-node-a
                                                 :payload payload-node-a-1})
       (extensions/on-change sync payload-node-a-1 #(>!! sync-spy-a %))

       (extensions/write-place sync peer-node-b {:pulse pulse-node-b
                                                 :shutdown shutdown-node-b
                                                 :payload payload-node-b-1})
       (extensions/on-change sync payload-node-b-1 #(>!! sync-spy-b %))

       (>!! (:born-peer-ch-head coordinator) peer-node-a)
       (>!! (:born-peer-ch-head coordinator) peer-node-b)

       (<!! offer-ch-spy)
       (<!! offer-ch-spy)

       (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})

       (<!! offer-ch-spy)
       (<!! sync-spy-a)
       (<!! sync-spy-b)

       (facts "Both payloads are received"
              (let [payload-a (extensions/read-place sync payload-node-a-1)
                    payload-b (extensions/read-place sync payload-node-b-1)]
                (fact payload-a => (comp not nil?))
                (fact payload-b => (comp not nil?))
                (fact (not= payload-a payload-b) => true)

                (extensions/on-change sync (:status (:nodes payload-a)) #(>!! status-spy %))
                (extensions/on-change sync (:status (:nodes payload-b)) #(>!! status-spy %))

                (extensions/touch-place sync (:ack (:nodes payload-a)))
                (extensions/touch-place sync (:ack (:nodes payload-b)))

                (<!! ack-ch-spy)
                (<!! ack-ch-spy)

                (<!! status-spy)
                (<!! status-spy)

                (extensions/write-place sync peer-node-a {:pulse pulse-node-a
                                                          :shutdown shutdown-node-a
                                                          :payload payload-node-a-2})
                (extensions/on-change sync payload-node-a-2 #(>!! sync-spy-a %))

                (extensions/write-place sync peer-node-b {:pulse pulse-node-b
                                                          :shutdown shutdown-node-b
                                                          :payload payload-node-b-2})
                (extensions/on-change sync payload-node-b-2 #(>!! sync-spy-b %))

                (extensions/touch-place sync (:completion (:nodes payload-a)))
                (extensions/touch-place sync (:completion (:nodes payload-b)))

                (<!! offer-ch-spy)
                (<!! offer-ch-spy)

                (let [[v ch] (alts!! [sync-spy-a sync-spy-b])
                      nodes (:nodes (extensions/read-place sync (:path v)))]
                  (extensions/on-change sync (:status nodes) #(>!! status-spy %))
                  (extensions/touch-place sync (:ack nodes))

                  (<!! ack-ch-spy)
                  (<!! status-spy)

                  (extensions/touch-place sync (:completion nodes))
                  (<!! offer-ch-spy))

                (let [db (d/db (:conn log))]
                  
                  (facts "All tasks are complete"
                         (let [query '[:find (count ?task) :where
                                       [?task :task/complete? true]]
                               result (ffirst (d/q query db))]
                           (fact result => 3)))

                  (facts "All peers are idle"
                         (let [query '[:find (count ?peer) :where
                                       [?peer :peer/status :idle]]
                               result (ffirst (d/q query db))]
                           (fact result => 2))))))))
   
   {:revoke-delay 50000}))

(facts
 "plan one job with four peers"
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
                     :onyx/consumption :sequential}
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

       (alts!! sync-spies)
       (alts!! sync-spies)
       (alts!! sync-spies)

       (let [db (d/db (:conn log))]
         
         (facts "Three peers are :acking"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/status :acking]]
                      result (ffirst (d/q query db))]
                  (fact result => 3)))

         (facts "One peer is idle"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/status :idle]]
                      result (ffirst (d/q query db))]
                  (fact result => 1)))

         (let [query '[:find ?node :where
                       [?peer :peer/status :acking]
                       [?peer :node/payload ?node]]
               payload-nodes (map first (d/q query db))]
           
           (doseq [payload-node payload-nodes]
             (let [payload (extensions/read-place sync payload-node)]
               (extensions/on-change sync (:status (:nodes payload)) #(>!! status-spy %))
               (extensions/touch-place sync (:ack (:nodes payload)))))

           (doseq [_ (range 3)]
             (<!! ack-ch-spy)
             (<!! status-spy))

           (doseq [payload-node payload-nodes]
             (let [payload (extensions/read-place sync payload-node)]
               (extensions/touch-place sync (:completion (:nodes payload)))
               (<!! offer-ch-spy)))))

       (let [db (d/db (:conn log))]
         
         (facts "Four peers are :idle"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/status :idle]]
                      result (ffirst (d/q query db))]
                  (fact result => 4)))

         (facts "All three tasks are complete"
                (let [query '[:find (count ?task) :where
                              [?task :task/complete? true]]
                      result (ffirst (d/q query db))]
                  (fact result => 3))))))
   {:revoke-delay 50000}))

