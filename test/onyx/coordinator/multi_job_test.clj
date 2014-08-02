(ns onyx.coordinator.multi-job-test
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [onyx.coordinator.async :as async]
            [onyx.extensions :as extensions]
            [onyx.coordinator.impl :as impl]
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
       
       (extensions/write-node sync (:node peer-node-a)
                               {:id (:uuid peer-node-a)
                                :peer-node (:node peer-node-a)
                                :payload-node (:node payload-node-a-1)
                                :pulse-node (:node pulse-node-a)
                                :shutdown-node (:node shutdown-node-a)})
       (extensions/on-change sync (:node payload-node-a-1) #(>!! sync-spy-a %))

       (extensions/write-node sync (:node peer-node-b)
                               {:id (:uuid peer-node-b)
                                :peer-node (:node peer-node-b)
                                :payload-node (:node payload-node-b-1)
                                :pulse-node (:node pulse-node-b)
                                :shutdown-node (:node shutdown-node-b)})
       (extensions/on-change sync (:node payload-node-b-1) #(>!! sync-spy-b %))

       (extensions/create sync :born-log (:node peer-node-a))
       (extensions/create sync :born-log (:node peer-node-b))
       
       (>!! (:born-peer-ch-head coordinator) true)
       (<!! offer-ch-spy)

       (extensions/create
        sync :planning-log
        {:job {:workflow workflow-a :catalog catalog-a}
         :node (:node (extensions/create sync :plan))})

       (>!! (:planning-ch-head coordinator) true)
       (<!! offer-ch-spy)
       
       (>!! (:born-peer-ch-head coordinator) true)
       (<!! offer-ch-spy)

       (<!! sync-spy-a)
       (<!! sync-spy-b)

       (let [payload-a (extensions/read-node sync (:node payload-node-a-1))
             payload-b (extensions/read-node sync (:node payload-node-b-1))]

         (facts
          "Payload A is for job A"
          (extensions/read-node sync (:node/catalog (:nodes payload-a))) => catalog-a
          (extensions/read-node sync (:node/workflow (:nodes payload-a))) => workflow-a)

         (facts
          "Payload B is for job A"
          (extensions/read-node sync (:node/catalog (:nodes payload-b))) => catalog-a
          (extensions/read-node sync (:node/workflow (:nodes payload-b))) => workflow-a)

         (extensions/on-change sync (:node/status (:nodes payload-a)) #(>!! status-spy %))
         (extensions/on-change sync (:node/status (:nodes payload-b)) #(>!! status-spy %))

         (extensions/touch-node sync (:node/ack (:nodes payload-a)))
         (extensions/touch-node sync (:node/ack (:nodes payload-b)))

         (<!! ack-ch-spy)
         (<!! ack-ch-spy)

         (<!! status-spy)
         (<!! status-spy)

         (extensions/create
          sync :planning-log
          {:job {:workflow workflow-b :catalog catalog-b}
           :node (:node (extensions/create sync :plan))})
         
         (>!! (:planning-ch-head coordinator) true)
         (<!! offer-ch-spy)

         (extensions/write-node sync (:node peer-node-a)
                                 {:id (:uuid peer-node-a)
                                  :peer-node (:node peer-node-a)
                                  :pulse-node (:node pulse-node-a)
                                  :payload-node (:node payload-node-a-2)
                                  :shutdown-node (:node shutdown-node-a)})
         
         (extensions/on-change sync (:node payload-node-a-2) #(>!! sync-spy-a %))
         (extensions/touch-node sync (:node/completion (:nodes payload-a))))

       (<!! offer-ch-spy)
       (<!! sync-spy-a)

       (let [payload-a (extensions/read-node sync (:node payload-node-a-2))]
         (facts
          "Payload A is for job B"
          (extensions/read-node sync (:node/catalog (:nodes payload-a))) => catalog-b
          (extensions/read-node sync (:node/workflow (:nodes payload-a))) => workflow-b)

         (extensions/on-change sync (:node/status (:nodes payload-a)) #(>!! status-spy %))
         (extensions/touch-node sync (:node/ack (:nodes payload-a)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-b (extensions/read-node sync (:node payload-node-b-1))]
         (extensions/write-node sync (:node peer-node-b)
                                 {:id (:uuid peer-node-b)
                                  :peer-node (:node peer-node-b)
                                  :pulse-node (:node pulse-node-b)
                                  :payload-node (:node payload-node-b-2)
                                  :shutdown-node (:node shutdown-node-b)})
         
         (extensions/on-change sync (:node payload-node-b-2) #(>!! sync-spy-b %))
         (extensions/touch-node sync (:node/completion (:nodes payload-b)))

         (<!! offer-ch-spy)
         (<!! sync-spy-b))

       (let [payload-b (extensions/read-node sync (:node payload-node-b-2))]
         (facts
          "Payload B is for job A"
          (extensions/read-node sync (:node/catalog (:nodes payload-b))) => catalog-a
          (extensions/read-node sync (:node/workflow (:nodes payload-b))) => workflow-a)
         
         (extensions/on-change sync (:node/status (:nodes payload-b)) #(>!! status-spy %))
         (extensions/touch-node sync (:node/ack (:nodes payload-b)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-a (extensions/read-node sync (:node payload-node-a-2))]
         (extensions/write-node sync (:node peer-node-a)
                                 {:id (:uuid peer-node-a)
                                  :peer-node (:node peer-node-a)
                                  :pulse-node (:node pulse-node-a)
                                  :shutdown-node (:node shutdown-node-a)
                                  :payload-node (:node payload-node-a-1)})
         
         (extensions/on-change sync (:node payload-node-a-1) #(>!! sync-spy-a %))
         (extensions/touch-node sync (:node/completion (:nodes payload-a)))

         (<!! offer-ch-spy)
         (<!! sync-spy-a))

       (let [payload-a (extensions/read-node sync (:node payload-node-a-1))]
         (facts
          "Payload A is for job B"
          (extensions/read-node sync (:node/catalog (:nodes payload-a))) => catalog-b
          (extensions/read-node sync (:node/workflow (:nodes payload-a))) => workflow-b)

         (extensions/on-change sync (:node/status (:nodes payload-a)) #(>!! status-spy %))
         (extensions/touch-node sync (:node/ack (:nodes payload-a)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-b (extensions/read-node sync (:node payload-node-b-2))]
         (extensions/write-node sync (:node peer-node-b)
                                 {:id (:uuid peer-node-b)
                                  :peer-node (:node peer-node-b)
                                  :pulse-node (:node pulse-node-b)
                                  :shutdown-node (:node shutdown-node-b)
                                  :payload-node (:node payload-node-b-1)})
         
         (extensions/on-change sync (:node payload-node-b-1) #(>!! sync-spy-b %))
         (extensions/touch-node sync (:node/completion (:nodes payload-b)))

         (<!! offer-ch-spy)
         (<!! sync-spy-b))

       (let [payload-b (extensions/read-node sync (:node payload-node-b-1))]
         (facts
          "Payload B is for job B"
          (extensions/read-node sync (:node/catalog (:nodes payload-b))) => catalog-b
          (extensions/read-node sync (:node/workflow (:nodes payload-b))) => workflow-b)

         (extensions/on-change sync (:node/status (:nodes payload-b)) #(>!! status-spy %))
         (extensions/touch-node sync (:node/ack (:nodes payload-b)))

         (<!! ack-ch-spy)
         (<!! status-spy))

       (let [payload-a (extensions/read-node sync (:node payload-node-a-1))
             payload-b (extensions/read-node sync (:node payload-node-b-1))]
         (extensions/touch-node sync (:node/completion (:nodes payload-a)))
         (extensions/touch-node sync (:node/completion (:nodes payload-b)))

         (<!! offer-ch-spy)
         (<!! offer-ch-spy))

       (facts
        "All tasks are complete"
        (let [job-nodes (extensions/bucket sync :job)
              task-paths (map #(extensions/resolve-node sync :task %) job-nodes)]
          (doseq [task-path task-paths]
            (doseq [task-node (extensions/children sync task-path)]
              (when-not (impl/completed-task? task-node)
                (fact (impl/task-complete? sync task-node) => true))))))

       (facts "All peers are idle"
              (doseq [state-path (extensions/bucket sync :peer-state)]
                (let [state (extensions/dereference sync state-path)]
                  (fact (:state (:content state)) => :idle))))

       (facts "Peer death succeeds"
              (extensions/delete sync (:node pulse-node-a))
              (extensions/delete sync (:node pulse-node-b))
              (<!! offer-ch-spy)
              (<!! offer-ch-spy))))
   
   {:onyx.coordinator/revoke-delay 50000}))

