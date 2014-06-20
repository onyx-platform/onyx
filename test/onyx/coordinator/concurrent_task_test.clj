(ns onyx.coordinator.concurrent-task-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [zookeeper :as zk]
            [onyx.coordinator.async :as async]
            [onyx.sync.zookeeper :as onyx-zk]
            [onyx.extensions :as extensions]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(facts
 "plan one job with four peers concurrently"
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

           catalog [{:onyx/name :in
                     :onyx/type :input
                     :onyx/medium :hornetq
                     :onyx/consumption :sequential
                     :hornetq/queue-name "in-queue"}
                    {:onyx/name :inc
                     :onyx/type :transformer
                     :onyx/consumption :concurrent}
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
         (extensions/write-place sync (:node peer)
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
            [{:catalog catalog :workflow workflow} (chan 1)])
       (<!! offer-ch-spy)

       (doseq [_ (range n)]
         (alts!! sync-spies))

       (let [states (->> (onyx-zk/peer-state-path (:onyx-id sync))
                         (zk/children (:conn sync))
                         (map (partial extensions/resolve-node sync :peer-state))
                         (map (partial extensions/dereference sync))
                         (map :content))]
         
         (facts "Four peers are :acking"
                (count (filter (partial = :acking) (map :state states))) => 4)

         (facts "No peers are idle"
                (count (filter (partial = :idle) (map :state states))) => 0)

         (facts ":inc has two active peers"
                (->> states
                     (map :task-node)
                     (map (partial extensions/read-place sync))
                     (map :task/name)
                     (filter (partial = :inc))
                     (count))
                => 2))))
   {:revoke-delay 50000}))

