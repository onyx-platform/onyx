(ns onyx.coordinator.single-peer-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap >!! <!!]]
            [com.stuartsierra.component :as component]
            [zookeeper :as zk]
            [onyx.extensions :as extensions]
            [onyx.coordinator.async :as async]
            [onyx.sync.zookeeper :as onyx-zk]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(facts
 "new peer"
 (with-system
   (fn [coordinator sync]
     (let [peer (extensions/create sync :peer)
           pulse (extensions/create sync :pulse)
           shutdown (extensions/create sync :shutdown)
           offer-ch-spy (chan 1)
           failure-ch-spy (chan 1)]

       (extensions/write-place sync (:node peer)
                               {:id (:uuid peer)
                                :peer-node (:node peer)
                                :pulse-node (:node pulse)
                                :shutdown-node (:node shutdown)})
             
       (tap (:offer-mult coordinator) offer-ch-spy)
       (tap (:failure-mult coordinator) failure-ch-spy)
             
       (>!! (:born-peer-ch-head coordinator) (:node peer))
       (<!! offer-ch-spy)

       (facts "There is one peer"
              (let [peers (zk/children (:conn sync) (onyx-zk/peer-path (:onyx-id sync)))
                    peer-path (str (onyx-zk/peer-path (:onyx-id sync)) "/" (first peers))]
                (fact (count peers) => 1)
                (fact (:id (extensions/read-place sync peer-path)) => (:uuid peer))))))))

(facts
 "peer joins and dies"
 (with-system
   (fn [coordinator sync]
     (let [peer (extensions/create sync :peer)
           pulse (extensions/create sync :pulse)
           shutdown (extensions/create sync :shutdown)
           offer-ch-spy (chan 1)
           evict-ch-spy (chan 1)
           shutdown-ch-spy (chan 1)
           failure-ch-spy (chan 1)]

       (extensions/write-place sync (:node peer)
                               {:id (:uuid peer)
                                :peer-node (:node peer)
                                :pulse-node (:node pulse)
                                :shutdown-node (:node shutdown)})
             
       (tap (:offer-mult coordinator) offer-ch-spy)
       (tap (:evict-mult coordinator) evict-ch-spy)
       (tap (:shutdown-mult coordinator) shutdown-ch-spy)
       (tap (:failure-mult coordinator) failure-ch-spy)
             
       (>!! (:born-peer-ch-head coordinator) (:node peer))
       (<!! offer-ch-spy)
       (extensions/delete sync (:node pulse))
       (<!! evict-ch-spy)
       (<!! shutdown-ch-spy)

       (fact "There are no peers"
             (extensions/place-exists? sync (:node pulse)) => false)

       (fact "The only peer is marked as dead"
             (let [peers (zk/children (:conn sync) (onyx-zk/peer-path (:onyx-id sync)))
                   peer-path (str (onyx-zk/peer-path (:onyx-id sync)) "/" (first peers))
                   peer-id (:id (extensions/read-place sync peer-path))
                   state-path (extensions/resolve-node sync :peer-state peer-id)
                   state (extensions/dereference sync state-path)]
               (:state state) => :dead))))))

(facts
 "planning one job with no peers"
 (with-system
   (fn [coordinator sync]
     (let [catalog [{:onyx/name :in
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
           workflow {:in {:inc :out}}
           offer-ch-spy (chan 1)]

       (tap (:offer-mult coordinator) offer-ch-spy)
             
       (>!! (:planning-ch-head coordinator)
            {:catalog catalog :workflow workflow})

       (let [job-id (<!! offer-ch-spy)]

         (facts "There is one job"
                (let [jobs (extensions/bucket sync :job)]
                  (fact (count jobs) => 1)))

         (facts "There are three tasks"
                (let [task-nodes (extensions/bucket-at sync :task job-id)
                      tasks (map #(extensions/read-place sync %) task-nodes)]
                  (fact (count task-nodes) => 3)
                  (fact (into #{} (map :task/name tasks)) => #{:in :inc :out})

                  (facts ":in has an ingress queue"
                         (let [task (first (filter #(= (:task/name %) :in) tasks))
                               in-queues (:task/ingress-queues task)]
                           (fact in-queues =not=> nil?)))

                  (facts ":inc's ingress queue is :in's egress queues"
                         (let [in (first (filter #(= (:task/name %) :in) tasks))
                               inc (first (filter #(= (:task/name %) :inc) tasks))]
                           (fact (:task/ingress-queues inc) =>
                                 (first (:task/egress-queues in)))))

                  (facts ":out's ingess queue is :inc's egress queue"
                         (let [inc (first (filter #(= (:task/name %) :inc) tasks))
                               out (first (filter #(= (:task/name %) :out) tasks))]
                           (fact (:task/ingress-queues out) =>
                                 (first (:task/egress-queues inc)))))

                  (facts ":out's egress queue is generated"
                         (let [task (first (filter #(= (:task/name %) :out) tasks))
                               out-queues (:task/egress-queues task)]
                           (fact out-queues =not=> empty?))))))))))

(defn test-task-life-cycle
  [{:keys [id sync sync-spy ack-ch-spy seal-ch-spy completion-ch-spy offer-ch-spy
           status-spy seal-node-spy peer-node payload-node next-payload-node task-name
           pulse-node shutdown-node]}]
  (prn "a")
  (facts "The payload node is populated"
         (let [event (<!! sync-spy)]
           (fact (:path event) => payload-node)))

  (let [task (:task (extensions/read-place sync payload-node))
        state-path (extensions/resolve-node sync :peer-state (:id (extensions/read-place sync peer-node)))
        state (extensions/dereference sync state-path)]

    (fact "It receives the task"
          (extensions/read-place sync (:task-node state)) => task)

    (fact "The peer's state is :acking the task"
          (:state state) => :acking)


    (fact "The payload node contains the other node paths"
          (fact (into #{} (keys (:nodes state)))
                => #{:node/payload :node/ack :node/completion
                     :node/status :node/catalog :node/workflow
                     :node/peer :node/exhaust :node/seal}))

    (prn "b")
    (extensions/on-change sync (:node/status (:nodes state)) #(>!! status-spy %))
    (extensions/on-change sync (:node/seal (:nodes state)) #(>!! seal-node-spy %))

    (facts "Touching the ack node triggers the callback"
           (extensions/touch-place sync (:node/ack (:nodes state)))
           (let [event (<!! ack-ch-spy)]
             (fact (:path event) => (:node/ack (:nodes state))))))

  (prn "C")

  (extensions/write-place sync peer-node {:id id
                                          :peer-node peer-node
                                          :pulse-node pulse-node
                                          :shutdown-node shutdown-node
                                          :payload-node next-payload-node})
  
  (extensions/on-change sync next-payload-node #(>!! sync-spy %))

  (<!! status-spy)

  (facts "Touching the exhaustion node triggers the callback"
         (let [nodes (:nodes (extensions/read-place sync payload-node))]
           (extensions/touch-place sync (:node/exhaust nodes))
           (let [event (<!! seal-ch-spy)]
             (fact (:seal? event) => true)
             (fact (:seal-node event) => (:node/seal nodes)))))

  (<!! seal-node-spy)

  (facts "The resource should be sealed"
         (let [nodes (:nodes (extensions/read-place sync payload-node))]
           (fact (extensions/read-place sync (:node/seal nodes)) => true)))

  (facts "Touching the completion node triggers the callback"
         (let [nodes (:nodes (extensions/read-place sync payload-node))]
           (extensions/touch-place sync (:node/completion nodes))
           (let [event (<!! completion-ch-spy)]
             (fact (:path event) => (:node/completion nodes)))))

  (<!! offer-ch-spy))

(facts
 "planning one job with one peer"
 (with-system
   (fn [coordinator sync]
     (let [peer (extensions/create sync :peer)
           pulse (extensions/create sync :pulse)
           shutdown (extensions/create sync :shutdown)

           in-payload (extensions/create sync :payload)
           inc-payload (extensions/create sync :payload)
           out-payload (extensions/create sync :payload)
           future-payload (extensions/create sync :payload)
                 
           sync-spy (chan 1)
           ack-ch-spy (chan 1)
           status-spy (chan 1)
           offer-ch-spy (chan 1)
           seal-ch-spy (chan 1)
           seal-node-spy (chan 1)
           completion-ch-spy (chan 1)
                 
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
       (tap (:seal-mult coordinator) seal-ch-spy)
       (tap (:completion-mult coordinator) completion-ch-spy)

       (extensions/write-place sync (:node peer)
                               {:id (:uuid peer)
                                :peer-node (:node peer)
                                :pulse-node (:node pulse)
                                :shutdown-node (:node shutdown)
                                :payload-node (:node in-payload)})

       (extensions/on-change sync (:node in-payload) #(>!! sync-spy %))

       (>!! (:born-peer-ch-head coordinator) (:node peer))
       (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})

       (<!! offer-ch-spy)
       (<!! offer-ch-spy)

       (let [base-cycle {:id (:uuid peer)
                         :sync sync
                         :sync-spy sync-spy
                         :ack-ch-spy ack-ch-spy
                         :offer-ch-spy offer-ch-spy
                         :status-spy status-spy
                         :seal-ch-spy seal-ch-spy
                         :seal-node-spy seal-node-spy
                         :completion-ch-spy completion-ch-spy
                         :peer-node (:node peer)
                         :pulse-node (:node pulse)}]
         (prn "1")
         (test-task-life-cycle
          (assoc base-cycle
            :task-name :in
            :payload-node (:node in-payload)
            :next-payload-node (:node inc-payload)))

         (prn "2")

         (test-task-life-cycle
          (assoc base-cycle
            :task-name :inc
            :payload-node (:node inc-payload)
            :next-payload-node (:node out-payload)))

         (prn "3")

         (test-task-life-cycle
          (assoc base-cycle
            :task-name :out
            :payload-node (:node out-payload)
            :next-payload-node (:node future-payload))))))
   {:revoke-delay 500000}))

(facts
 "evicting one peer"
 (with-system
   (fn [coordinator sync]
     (let [catalog [{:onyx/name :in
                     :onyx/consumption :sequential
                     :onyx/type :input
                     :onyx/medium :hornetq
                     :hornetq/queue-name "in-queue"}
                    {:onyx/name :inc
                     :onyx/type :transformer
                     :onyx/consumption :sequential}
                    {:onyx/name :out
                     :onyx/consumption :sequential
                     :onyx/type :output
                     :onyx/medium :hornetq
                     :hornetq/queue-name "out-queue"}]
           workflow {:in {:inc :out}}

           peer-node (extensions/create sync :peer)
           pulse-node (extensions/create sync :pulse)
           shutdown-node (extensions/create sync :shutdown)
           payload-node (extensions/create sync :payload)
                 
           sync-spy (chan 1)
           offer-ch-spy (chan 3)]
             
       (tap (:offer-mult coordinator) offer-ch-spy)

       (extensions/write-place sync peer-node {:pulse pulse-node :payload payload-node})
       (extensions/on-change sync payload-node #(>!! sync-spy %))

       (>!! (:born-peer-ch-head coordinator) peer-node)
       (<!! offer-ch-spy)
       (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})
       (<!! offer-ch-spy)
       (<!! sync-spy)

       ;; Instant revoke.
       (<!! offer-ch-spy)

       (facts "The peer gets deleted after eviction"
              (let [db (d/db (:conn log))
                    query '[:find ?p :in $ ?peer-node :where
                            [?p :node/peer ?peer-node]]]
                (fact (count (d/q query db peer-node)) => zero?)))

       (facts "The status node gets deleted on sync storage"
              (let [db (d/history (d/db (:conn log)))
                    query '[:find ?status :in $ ?peer-node :where
                            [?p :node/peer ?peer-node]
                            [?p :node/status ?status]]
                    status-node (ffirst (d/q query db peer-node))]
                (fact (extensions/read-place sync status-node) => (throws Exception))))))
   {:revoke-delay 0}))

(facts
 "error cases"
 (with-system
   (fn [coordinator sync]
     (let [peer (extensions/create sync :peer)
           pulse (extensions/create sync :pulse)
           shutdown (extensions/create sync :shutdown)
           offer-ch-spy (chan 5)
           ack-ch-spy (chan 5)
           evict-ch-spy (chan 5)
           completion-ch-spy (chan 5)
           failure-ch-spy (chan 10)]

       (extensions/write-place sync peer {:pulse pulse :shutdown shutdown})
             
       (tap (:offer-mult coordinator) offer-ch-spy)
       (tap (:ack-mult coordinator) ack-ch-spy)
       (tap (:evict-mult coordinator) evict-ch-spy)
       (tap (:completion-mult coordinator) completion-ch-spy)
       (tap (:failure-mult coordinator) failure-ch-spy)

       (>!! (:born-peer-ch-head coordinator) peer)
       (<!! offer-ch-spy)

       (facts "Adding a duplicate peer fails"
              (>!! (:born-peer-ch-head coordinator) peer)
              (let [failure (<!! failure-ch-spy)]
                (fact (:ch failure) => :peer-birth)))

       (facts "Attempts to delete a non-existent peer fails"
              (extensions/delete sync pulse)
              (<!! evict-ch-spy)

              (facts "A failure is raised for the second callback"
                     (let [failure (<!! failure-ch-spy)]
                       (fact (:ch failure) => :peer-death)))
                    
              (facts "A failure is raised for the second delete"
                     (>!! (:dead-peer-ch-head coordinator) peer)
                     (let [failure (<!! failure-ch-spy)]
                       (fact (:ch failure) => :peer-death))))

       (facts "Acking a non-existent node fails"
              (>!! (:ack-ch-head coordinator) {:path (str (java.util.UUID/randomUUID))})
              (let [failure (<!! failure-ch-spy)]
                (fact (:ch failure) => :ack)))

       (facts "Acking a completed task fails"
              (let [peer-id (d/tempid :onyx/log)
                    task-id (d/tempid :onyx/log)
                    node-path (str (java.util.UUID/randomUUID))
                    tx [{:db/id peer-id
                         :peer/status :acking
                         :node/ack node-path
                         :peer/task {:db/id task-id
                                     :task/complete? true}}]]
                @(d/transact (:conn log) tx)
                      
                (>!! (:ack-ch-head coordinator) {:path node-path})
                (let [failure (<!! failure-ch-spy)]
                  (fact (:ch failure) => :ack))))

       (facts "Acking with a peer who's state isnt :acking fails"
              (let [peer-id (d/tempid :onyx/log)
                    task-id (d/tempid :onyx/log)
                    node-path (str (java.util.UUID/randomUUID))
                    tx [{:db/id peer-id
                         :peer/status :idle
                         :node/ack node-path
                         :peer/task {:db/id task-id
                                     :task/complete? false}}]]
                @(d/transact (:conn log) tx)
                      
                (>!! (:ack-ch-head coordinator) {:path node-path})
                (let [failure (<!! failure-ch-spy)]
                  (fact (:ch failure) => :ack))))

       (facts "Completing a task that doesn't exist fails"
              (>!! (:completion-ch-head coordinator) {:path "dead path"})
              (let [failure (<!! failure-ch-spy)]
                (fact (:ch failure) => :complete)))
             
       (facts "Completing a task that's already been completed fails"
              (let [peer-id (d/tempid :onyx/log)
                    task-id (d/tempid :onyx/log)
                    node-path (str (java.util.UUID/randomUUID))
                    tx [{:db/id peer-id
                         :peer/status :active
                         :node/completion node-path
                         :node/payload (str (java.util.UUID/randomUUID))
                         :node/ack (str (java.util.UUID/randomUUID))
                         :node/status (str (java.util.UUID/randomUUID))
                         :peer/task {:db/id task-id
                                     :task/complete? true}}]]
                @(d/transact (:conn log) tx)
                      
                (>!! (:completion-ch-head coordinator) {:path node-path})
                (let [failure (<!! failure-ch-spy)]
                  (fact (:ch failure) => :complete))))

       (facts "Completing a task from an idle peer fails"
              (let [peer-id (d/tempid :onyx/log)
                    task-id (d/tempid :onyx/log)
                    node-path (str (java.util.UUID/randomUUID))
                    tx [{:db/id peer-id
                         :peer/status :idle
                         :node/completion node-path
                         :node/payload (str (java.util.UUID/randomUUID))
                         :node/ack (str (java.util.UUID/randomUUID))
                         :node/status (str (java.util.UUID/randomUUID))
                         :peer/task {:db/id task-id
                                     :task/complete? false}}]]
                @(d/transact (:conn log) tx)
                      
                (>!! (:completion-ch-head coordinator) {:path node-path})
                (let [failure (<!! failure-ch-spy)]
                  (fact (:ch failure) => :complete))))))
   {:revoke-delay 50000}))

