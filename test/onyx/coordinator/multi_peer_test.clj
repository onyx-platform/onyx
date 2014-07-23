(ns onyx.coordinator.multi-peer-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [zookeeper :as zk]
            [onyx.coordinator.async :as async]
            [onyx.extensions :as extensions]
            [onyx.sync.zookeeper :as onyx-zk]
            [onyx.coordinator.impl :as impl]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(facts
 "plan one job with two peers"
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
           job-ch (chan 1)
           
           catalog [{:onyx/name :in
                     :onyx/type :input
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "in-queue"}
                    {:onyx/name :inc
                     :onyx/type :transformer
                     :onyx/consumption :sequential}
                    {:onyx/name :out
                     :onyx/type :output
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "out-queue"}]
           workflow {:in {:inc :out}}]

       (tap (:ack-mult coordinator) ack-ch-spy)
       (tap (:offer-mult coordinator) offer-ch-spy)

       (extensions/write-node sync (:node peer-node-a)
                               {:id (:uuid peer-node-a)
                                :peer-node (:node peer-node-a)
                                :pulse-node (:node pulse-node-a)
                                :shutdown-node (:node shutdown-node-a)
                                :payload-node (:node payload-node-a-1)})
       (extensions/on-change sync (:node payload-node-a-1) #(>!! sync-spy-a %))

       (extensions/write-node sync (:node peer-node-b)
                               {:id (:uuid peer-node-b)
                                :peer-node (:node peer-node-b)
                                :pulse-node (:node pulse-node-b)
                                :shutdown-node (:node shutdown-node-b)
                                :payload-node (:node payload-node-b-1)})
       (extensions/on-change sync (:node payload-node-b-1) #(>!! sync-spy-b %))

       (>!! (:born-peer-ch-head coordinator) (:node peer-node-a))
       (>!! (:born-peer-ch-head coordinator) (:node peer-node-b))

       (<!! offer-ch-spy)
       (<!! offer-ch-spy)

       (>!! (:planning-ch-head coordinator)
            [{:catalog catalog :workflow workflow} job-ch])

       (<!! offer-ch-spy)
       (<!! sync-spy-a)
       (<!! sync-spy-b)

       (facts "Both payloads are received"
              (let [payload-a (extensions/read-node sync (:node payload-node-a-1))
                    payload-b (extensions/read-node sync (:node payload-node-b-1))]
                (fact payload-a => (comp not nil?))
                (fact payload-b => (comp not nil?))
                (fact (not= payload-a payload-b) => true)

                (extensions/on-change sync (:node/status (:nodes payload-a)) #(>!! status-spy %))
                (extensions/on-change sync (:node/status (:nodes payload-b)) #(>!! status-spy %))

                (extensions/touch-node sync (:node/ack (:nodes payload-a)))
                (extensions/touch-node sync (:node/ack (:nodes payload-b)))

                (<!! ack-ch-spy)
                (<!! ack-ch-spy)

                (<!! status-spy)
                (<!! status-spy)

                (extensions/write-node sync (:node peer-node-a)
                                        {:id (:uuid peer-node-a)
                                         :peer-node (:node peer-node-a)
                                         :pulse-node (:node pulse-node-a)
                                         :shutdown-node (:node shutdown-node-a)
                                         :payload-node (:node payload-node-a-2)})
                (extensions/on-change sync (:node payload-node-a-2) #(>!! sync-spy-a %))

                (extensions/write-node sync (:node peer-node-b)
                                        {:id (:uuid peer-node-b)
                                         :peer-node (:node peer-node-b)
                                         :pulse-node (:node pulse-node-b)
                                         :shutdown-node (:node shutdown-node-b)
                                         :payload-node (:node payload-node-b-2)})
                (extensions/on-change sync (:node payload-node-b-2) #(>!! sync-spy-b %))

                (extensions/touch-node sync (:node/completion (:nodes payload-a)))
                (extensions/touch-node sync (:node/completion (:nodes payload-b)))

                (<!! offer-ch-spy)
                (<!! offer-ch-spy)

                (let [[v ch] (alts!! [sync-spy-a sync-spy-b])
                      nodes (:nodes (extensions/read-node sync (:path v)))]
                  (extensions/on-change sync (:node/status nodes) #(>!! status-spy %))
                  (extensions/touch-node sync (:node/ack nodes))

                  (<!! ack-ch-spy)
                  (<!! status-spy)

                  (extensions/touch-node sync (:node/completion nodes))
                  (<!! offer-ch-spy))

                (facts "All tasks are complete"
                       (let [task-path (extensions/resolve-node sync :task (str (<!! job-ch)))]
                         (doseq [task-node (extensions/children sync task-path)]
                           (when-not (impl/completed-task? task-node)
                             (fact (impl/task-complete? sync task-node) => true)))))

                (facts "All peers are idle"
                       (doseq [state-path (extensions/bucket sync :peer-state)]
                         (let [state (extensions/dereference sync state-path)]
                           (fact (:state (:content state)) => :idle))))))))
   {:onyx.coordinator/revoke-delay 50000}))

(facts
 "plan one job with four peers"
 (with-system
   (fn [coordinator sync]
     (let [n 4
           peers (take n (repeatedly (fn [] (extensions/create sync :peer))))
           pulses (take n (repeatedly (fn [] (extensions/create sync :pulse))))
           shutdowns (take n (repeatedly (fn [] (extensions/create sync :shutdown))))
           payloads (take n (repeatedly (fn [] (extensions/create sync :payload))))
           sync-spies (take n (repeatedly (fn [] (chan 1))))
           
           status-spy (chan (* n 5))
           offer-ch-spy (chan (* n 5))
           ack-ch-spy (chan (* n 5))
           job-ch (chan 1)

           catalog [{:onyx/name :in
                     :onyx/type :input
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "in-queue"}
                    {:onyx/name :inc
                     :onyx/type :transformer
                     :onyx/consumption :sequential}
                    {:onyx/name :out
                     :onyx/type :output
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "out-queue"}]
           workflow {:in {:inc :out}}]

       (tap (:offer-mult coordinator) offer-ch-spy)
       (tap (:ack-mult coordinator) ack-ch-spy)
       
       (doseq [[peer pulse shutdown payload sync-spy]
               (map vector peers pulses shutdowns payloads sync-spies)]
         (extensions/write-node sync (:node peer)
                                 {:id (:uuid peer)
                                  :peer-node (:node peer)
                                  :pulse-node (:node pulse)
                                  :shutdown-node (:node shutdown)
                                  :payload-node (:node payload)})
         (extensions/on-change sync (:node payload) #(>!! sync-spy %)))

       (doseq [peer peers]
         (>!! (:born-peer-ch-head coordinator) (:node peer)))

       (doseq [_ (range n)]
         (<!! offer-ch-spy))

       (>!! (:planning-ch-head coordinator)
            [{:catalog catalog :workflow workflow} job-ch])
       (<!! offer-ch-spy)

       (alts!! sync-spies)
       (alts!! sync-spies)
       (alts!! sync-spies)

       (let [states (->> (onyx-zk/peer-state-path (:onyx/id (:opts sync)))
                         (zk/children (:conn sync))
                         (map (partial extensions/resolve-node sync :peer-state))
                         (map (partial extensions/dereference sync))
                         (map :content))]
         
         (fact "Three peers are acking"
               (count (filter (partial = :acking) (map :state states))) => 3)

         (fact "One peer is idle"
               (count (filter (partial = :idle) (map :state states))) => 1)

         (let [ackers (filter #(= (:state %) :acking) states)
               payload-nodes (map (comp :node/payload :nodes) ackers)]

           (doseq [payload-node payload-nodes]
             (let [payload (extensions/read-node sync payload-node)]
               (extensions/on-change sync (:node/status (:nodes payload)) #(>!! status-spy %))
               (extensions/touch-node sync (:node/ack (:nodes payload)))))

           (doseq [_ (range 3)]
             (<!! ack-ch-spy)
             (<!! status-spy))

           (doseq [payload-node payload-nodes]
             (let [payload (extensions/read-node sync payload-node)]
               (extensions/touch-node sync (:node/completion (:nodes payload)))
               (<!! offer-ch-spy)))))

       (let [states (->> (onyx-zk/peer-state-path (:onyx/id (:opts sync)))
                         (zk/children (:conn sync))
                         (map (partial extensions/resolve-node sync :peer-state))
                         (map (partial extensions/dereference sync))
                         (map :content))]
         
         (facts "Four peers are :idle"
                (count (filter (partial = :idle) (map :state states))) => 4)

         (facts "All three tasks are complete"
                (let [task-path (extensions/resolve-node sync :task (str (<!! job-ch)))]
                  (doseq [task-node (extensions/children sync task-path)]
                    (when-not (impl/completed-task? task-node)
                      (fact (impl/task-complete? sync task-node) => true))))))))
   {:onyx.coordinator/revoke-delay 50000}))

