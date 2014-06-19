(ns onyx.coordinator.multi-job-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [onyx.coordinator.async :as async]
            [onyx.extensions :as extensions]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(facts
 "plan two jobs with two peers"
 (with-system
   (fn [coordinator sync]
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
           
           catalog-a [{:onyx/name :in-a
                       :onyx/type :input
                       :onyx/medium :hornetq
                       :onyx/consumption :sequential
                       :hornetq/queue-name "in-queue"}
                      {:onyx/name :inc-a
                       :onyx/type :transformer
                       :onyx/consumption :sequential}
                      {:onyx/name :out-a
                       :onyx/type :output
                       :onyx/medium :hornetq
                       :onyx/consumption :sequential
                       :hornetq/queue-name "out-queue"}]
           
           catalog-b [{:onyx/name :in-b
                       :onyx/type :input
                       :onyx/medium :hornetq
                       :onyx/consumption :sequential
                       :hornetq/queue-name "in-queue"}
                      {:onyx/name :inc-b
                       :onyx/type :transformer
                       :onyx/consumption :sequential}
                      {:onyx/name :out-b
                       :onyx/type :output
                       :onyx/medium :hornetq
                       :onyx/consumption :sequential
                       :hornetq/queue-name "out-queue"}]
           
           workflow-a {:in-a {:inc-a :out-a}}
           workflow-b {:in-b {:inc-b :out-b}}]

       (tap (:ack-mult coordinator) ack-ch-spy)
       (tap (:offer-mult coordinator) offer-ch-spy)

       (extensions/write-place sync (:node peer-node-a)
                               {:id (:uuid peer-node-a)
                                :peer-node (:node peer-node-a)
                                :payload-node (:node payload-node-a-1)
                                :pulse-node (:node pulse-node-a)
                                :shutdown-node (:node shutdown-node-a)})
       (extensions/on-change sync (:node payload-node-a-1) #(>!! sync-spy-a %))

       (extensions/write-place sync (:node peer-node-b)
                               {:id (:uuid peer-node-b)
                                :peer-node (:node peer-node-b)
                                :payload-node (:node payload-node-b-1)
                                :pulse-node (:node pulse-node-b)
                                :shutdown-node (:node shutdown-node-b)})
       (extensions/on-change sync (:node payload-node-b-1) #(>!! sync-spy-b %))

       (>!! (:born-peer-ch-head coordinator) (:node peer-node-a))
       (<!! offer-ch-spy)

       (>!! (:planning-ch-head coordinator) {:catalog catalog-a :workflow workflow-a})
       (<!! offer-ch-spy)
       
       (>!! (:born-peer-ch-head coordinator) (:node peer-node-b))
       (<!! offer-ch-spy)

       (<!! sync-spy-a)
       (<!! sync-spy-b)

       (let [payload-a (extensions/read-place sync (:node payload-node-a-1))
             payload-b (extensions/read-place sync (:node payload-node-b-1))]

         (facts
          "Payload A is for job A"
          (extensions/read-place sync (:node/catalog (:nodes payload-a))) => catalog-a
          (extensions/read-place sync (:node/workflow (:nodes payload-a))) => workflow-a)

         (facts
          "Payload B is for job A"
          (extensions/read-place sync (:node/catalog (:nodes payload-b))) => catalog-a
          (extensions/read-place sync (:node/workflow (:nodes payload-b))) => workflow-a)

         (extensions/on-change sync (:node/status (:nodes payload-a)) #(>!! status-spy %))
         (extensions/on-change sync (:node/status (:nodes payload-b)) #(>!! status-spy %))

         (extensions/touch-place sync (:node/ack (:nodes payload-a)))
         (extensions/touch-place sync (:node/ack (:nodes payload-b)))

         (<!! ack-ch-spy)
         (<!! ack-ch-spy)

         (<!! status-spy)
         (<!! status-spy)

         (>!! (:planning-ch-head coordinator) {:catalog catalog-b :workflow workflow-b})
         (<!! offer-ch-spy)

         (extensions/write-place sync (:node peer-node-a)
                                 {:id (:uuid peer-node-a)
                                  :peer-node (:node peer-node-a)
                                  :pulse-node (:node pulse-node-a)
                                  :payload-node (:node payload-node-a-2)
                                  :shutdown-node (:node shutdown-node-a)})
         
         (extensions/on-change sync (:node payload-node-a-2) #(>!! sync-spy-a %))
         (extensions/touch-place sync (:node/completion (:nodes payload-a))))

       (<!! offer-ch-spy)
       (prn "1")
       (<!! sync-spy-a)
       (prn "2")

       #_(let [payload-a (extensions/read-place sync payload-node-a-2)]
         (facts "Payload A is for job B"
                (let [db (d/db (:conn log))
                      query '[:find ?job :in $ ?task ?catalog :where
                              [?job :job/task ?task]
                              [?job :job/catalog ?catalog]]
                      result (d/q query db (:db/id (:task payload-a)) (pr-str catalog-b))
                      jobs (map first result)]
                  (fact (count jobs) => 1)))

         (extensions/on-change sync (:status (:nodes payload-a)) #(>!! status-spy %))
         (extensions/touch-place sync (:ack (:nodes payload-a)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-b (extensions/read-place sync payload-node-b-1)]
         (extensions/write-place sync peer-node-b {:pulse pulse-node-b
                                                   :payload payload-node-b-2
                                                   :shutdown shutdown-node-b})
         (extensions/on-change sync payload-node-b-2 #(>!! sync-spy-b %))
         (extensions/touch-place sync (:completion (:nodes payload-b)))

         (<!! offer-ch-spy)
         (<!! sync-spy-b))

       #_(let [payload-b (extensions/read-place sync payload-node-b-2)]
         (facts "Payload B is for job A"
                (let [db (d/db (:conn log))
                      query '[:find ?job :in $ ?task ?catalog :where
                              [?job :job/task ?task]
                              [?job :job/catalog ?catalog]]
                      result (d/q query db (:db/id (:task payload-b)) (pr-str catalog-a))
                      jobs (map first result)]
                  (fact (count jobs) => 1)))
         
         (extensions/on-change sync (:status (:nodes payload-b)) #(>!! status-spy %))
         (extensions/touch-place sync (:ack (:nodes payload-b)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-a (extensions/read-place sync payload-node-a-2)]
         (extensions/write-place sync peer-node-a {:pulse pulse-node-a
                                                   :shutdown shutdown-node-a
                                                   :payload payload-node-a-1})
         (extensions/on-change sync payload-node-a-1 #(>!! sync-spy-a %))
         (extensions/touch-place sync (:completion (:nodes payload-a)))

         (<!! offer-ch-spy)
         (<!! sync-spy-a))

       #_(let [payload-a (extensions/read-place sync payload-node-a-1)]
         (facts "Payload A is for job B"
                (let [db (d/db (:conn log))
                      query '[:find ?job :in $ ?task ?catalog :where
                              [?job :job/task ?task]
                              [?job :job/catalog ?catalog]]
                      result (d/q query db (:db/id (:task payload-a)) (pr-str catalog-b))
                      jobs (map first result)]
                  (fact (count jobs) => 1)))

         (extensions/on-change sync (:status (:nodes payload-a)) #(>!! status-spy %))
         (extensions/touch-place sync (:ack (:nodes payload-a)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-b (extensions/read-place sync payload-node-b-2)]
         (extensions/write-place sync peer-node-b {:pulse pulse-node-b
                                                   :shutdown shutdown-node-b
                                                   :payload payload-node-b-1})
         (extensions/on-change sync payload-node-b-1 #(>!! sync-spy-b %))
         (extensions/touch-place sync (:completion (:nodes payload-b)))

         (<!! offer-ch-spy)
         (<!! sync-spy-b))

       #_(let [payload-b (extensions/read-place sync payload-node-b-1)]
         (facts "Payload B is for job B"
                (let [db (d/db (:conn log))
                      query '[:find ?job :in $ ?task ?catalog :where
                              [?job :job/task ?task]
                              [?job :job/catalog ?catalog]]
                      result (d/q query db (:db/id (:task payload-b)) (pr-str catalog-b))
                      jobs (map first result)]
                  (fact (count jobs) => 1)))

         (extensions/on-change sync (:status (:nodes payload-b)) #(>!! status-spy %))
         (extensions/touch-place sync (:ack (:nodes payload-b)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-a (extensions/read-place sync payload-node-a-1)
             payload-b (extensions/read-place sync payload-node-b-1)]
         (extensions/touch-place sync (:completion (:nodes payload-a)))
         (extensions/touch-place sync (:completion (:nodes payload-b)))

         (<!! offer-ch-spy)
         (<!! offer-ch-spy))

       #_(let [db (d/db (:conn log))]
           (facts "All tasks are complete"
                  (let [query '[:find (count ?task) :where
                                [?task :task/complete? true]]
                        result (ffirst (d/q query db))]
                    (fact result => 6)))

           (facts "All peers are idle"
                  (let [query '[:find (count ?peer) :where
                                [?peer :peer/status :idle]]
                        result (ffirst (d/q query db))]
                    (fact result => 2))))

       (facts "Peer death succeeds"
              (extensions/delete sync pulse-node-a)
              (extensions/delete sync pulse-node-b)
              (<!! offer-ch-spy)
              (<!! offer-ch-spy))))
   
   {:revoke-delay 50000}))

