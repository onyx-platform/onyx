(ns onyx.peer.deterministic-peer-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-boilerplate :refer [build-job run-test-job]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]

            [onyx.log.replica :refer [base-replica]]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.extensions :as extensions]
            [onyx.messaging.immutable-messenger :as im]
            ;; TODO: remove in favour of immutable messenger which needs assign-task-resources impl
            [onyx.messaging.dummy-messenger]

            [com.stuartsierra.component :as component]
            [onyx.static.planning :as planning]
            [onyx.peer.task-lifecycle :as tl]

            [onyx.api]))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn start-task [job peer-config discovered-task job-id task-id peer-id]
  (let [task-information (-> job
                             (assoc :task discovered-task)
                             (assoc :job-id job-id)
                             (assoc :task-id task-id)
                             tl/map->TaskInformation)]
    (component/start
     (tl/map->TaskLifeCycle {:id peer-id
                             :log nil
                             ;; To be assoc'd each run
                             :messenger nil
                             :job-id job-id
                             :task-id task-id
                             ;; To be assoc'd each run
                             :replica nil
                             :opts peer-config

                             :restart-ch nil
                             :kill-ch nil
                             :outbox-ch nil
                             :task-kill-ch nil
                             :scheduler-event :FIXME_USE_LOG_EVENT
                             :task-monitoring (no-op-monitoring-agent)
                             :task-information task-information}))))

(def log
  [{:fn :prepare-join-cluster, :args {:joiner #uuid "9e258df9-973f-4029-b7c8-a4937c006482", :peer-site {}, :tags []}, :message-id 0, :created-at 1463164641131} {:fn :prepare-join-cluster, :args {:joiner #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea", :peer-site {}, :tags []}, :message-id 1, :created-at 1463164641142} {:fn :notify-join-cluster, :args {:observer #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea"}, :peer-parent #uuid "9e258df9-973f-4029-b7c8-a4937c006482", :entry-parent 1, :message-id 2, :created-at 1463164641144} {:fn :accept-join-cluster, :args {:observer #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea", :subject #uuid "9e258df9-973f-4029-b7c8-a4937c006482", :accepted-observer #uuid "9e258df9-973f-4029-b7c8-a4937c006482", :accepted-joiner #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea"}, :peer-parent #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea", :entry-parent 2, :message-id 3, :created-at 1463164641146} {:fn :prepare-join-cluster, :args {:joiner #uuid "c4e2d63a-4e82-4918-97bd-6a0d3b2349b8", :peer-site {}, :tags []}, :message-id 4, :created-at 1463164641152} {:fn :notify-join-cluster, :args {:observer #uuid "c4e2d63a-4e82-4918-97bd-6a0d3b2349b8"}, :peer-parent #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea", :entry-parent 4, :message-id 5, :created-at 1463164641153} {:fn :accept-join-cluster, :args {:observer #uuid "c4e2d63a-4e82-4918-97bd-6a0d3b2349b8", :subject #uuid "9e258df9-973f-4029-b7c8-a4937c006482", :accepted-observer #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea", :accepted-joiner #uuid "c4e2d63a-4e82-4918-97bd-6a0d3b2349b8"}, :peer-parent #uuid "c4e2d63a-4e82-4918-97bd-6a0d3b2349b8", :entry-parent 5, :message-id 6, :created-at 1463164641154} {:fn :submit-job, :args {:exempt-tasks '(), :saturation Double/POSITIVE_INFINITY, :task-name->id {:in #uuid "71590711-b7c5-492f-9492-8877a57123d9", :inc #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b", :out #uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7"}, :acker-exclude-outputs false, :min-required-peers {#uuid "71590711-b7c5-492f-9492-8877a57123d9" 1, #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b" 1, #uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7" 1}, :task-scheduler :onyx.task-scheduler/balanced, :tasks '(#uuid "71590711-b7c5-492f-9492-8877a57123d9" #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b" #uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7"), :inputs '(#uuid "71590711-b7c5-492f-9492-8877a57123d9"), :required-tags {}, :flux-policies {}, :id #uuid "5490b4bf-abcb-49f4-b4ca-1cc75035211d", :outputs '(#uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7"), :acker-percentage 1, :acker-exclude-inputs false, :task-saturation {#uuid "71590711-b7c5-492f-9492-8877a57123d9" 1, #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b" Double/POSITIVE_INFINITY, #uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7" 1}}, :message-id 7, :created-at 1463164641178} {:fn :signal-ready, :args {:id #uuid "8a58727b-5057-46d0-9c82-89d21e45d2ea"}, :message-id 8, :created-at 1463164641207} {:fn :signal-ready, :args {:id #uuid "c4e2d63a-4e82-4918-97bd-6a0d3b2349b8"}, :message-id 9, :created-at 1463164641207} {:fn :signal-ready, :args {:id #uuid "9e258df9-973f-4029-b7c8-a4937c006482"}, :message-id 10, :created-at 1463164641207} 
   
#_{:fn :exhaust-input, :args {:job #uuid "5490b4bf-abcb-49f4-b4ca-1cc75035211d", :task #uuid "71590711-b7c5-492f-9492-8877a57123d9"}, :message-id 11, :created-at 1463164641336}])

(deftest deterministic-abs-test
  (let [complete-job (atom false)]
    (with-redefs [onyx.peer.task-lifecycle/backoff-until-task-start! (fn [_])
                  onyx.peer.task-lifecycle/start-task-lifecycle! (fn [_ _])
                  onyx.peer.task-lifecycle/complete-job (fn [_] (reset! complete-job true))] 
      (let [n-messages 100
            task-opts {:onyx/batch-size 20}
            job (build-job [[:in :inc] [:inc :out]] 
                           [{:name :in
                             :type :seq 
                             :task-opts task-opts 
                             :input (map (fn [n] {:n n}) (range n-messages))}
                            {:name :inc
                             :type :fn 
                             :task-opts (assoc task-opts :onyx/fn ::my-inc)}
                            {:name :out
                             :type :async-out
                             :chan-size 10000000
                             :task-opts task-opts}]
                           :onyx.task-scheduler/balanced)
            onyx-id "property-testing"
            peer-config {:onyx/tenancy-id onyx-id
                         :zookeeper/address "127.0.0.1" 
                         :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                         :onyx.messaging/bind-addr "127.0.0.1"
                         :onyx.messaging/impl :dummy-messenger
                         :onyx.peer/try-join-once? true}
            discovered-tasks #_(planning/discover-tasks (:catalog job) (:workflow job))
            '({:id #uuid "71590711-b7c5-492f-9492-8877a57123d9", :name :in, :ingress-ids [], :egress-ids {:inc #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b"}} {:id #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b", :name :inc, :ingress-ids {:in #uuid "71590711-b7c5-492f-9492-8877a57123d9"}, :egress-ids {:out #uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7"}} {:id #uuid "fa23075f-5279-4a64-84ef-7ea80c7918a7", :name :out, :ingress-ids {:inc #uuid "a6f5a276-25c8-4819-adb1-85b54c5ea27b"}, :egress-ids []})

            ;; TODO, create job submission
            ;submitted-job (onyx.api/create-submit-job-entry job-id peer-config job discovered-tasks)
            pg (component/start (im/immutable-peer-group {}))
        messenger (im/immutable-messenger pg) 

        task-id->discovered-task (zipmap (map :id discovered-tasks) discovered-tasks)

        replica (assoc base-replica 
                       :job-scheduler (:onyx.peer/job-scheduler peer-config)
                       :messaging {:onyx.messaging/impl (:onyx.messaging/impl peer-config)})
        new-replica (reduce (fn [replica entry] 
                              (assoc (extensions/apply-log-entry entry replica) :version (:message-id entry))) 
                            replica
                            log)
        ;; Can this be gotten from the job entry created? Do we even need it, or should we just get rid of the get-in
        job-id (first (:jobs new-replica))
        peer-tasks (reduce (fn [m [task-id peer-ids]]
                                     (let [task (task-id->discovered-task task-id)] 
                                       (into m 
                                             (map (fn [peer-id] 
                                                    [peer-id (start-task job peer-config task job-id task-id peer-id)])
                                                  peer-ids))))
                                    {}
                                    (get-in new-replica [:allocations job-id]))
        peer-replicas (zipmap (keys peer-tasks) (repeat base-replica))]

    ;(println (:peers new-replica) "allocations" (:allocations new-replica))
    ;(println "peer id" (:id event))
    (reduce (fn [[messenger peer-tasks peer-replicas] peer-id]
              ;(reset! complete-job false)
              (let [peer-task (get peer-tasks peer-id)
                    event (:event peer-task)
                    prev-replica (get peer-replicas peer-id)
                    new-event (tl/event-iteration 
                               event 
                               prev-replica 
                               new-replica 
                               (assoc messenger :peer-id peer-id) 
                               (:pipeline event) 
                               (:barriers event))] 
                ;; TODO: if complete-job is detected, should output a exhaust-input message
                ;; alternatively can override things so >!! emits the message which is then played
                ;; This is probably more preferable
                [(:messenger new-event)
                 (assoc-in peer-tasks [peer-id :event] new-event)
                 (assoc peer-replicas peer-id new-replica)]))
            [messenger peer-tasks peer-replicas]
            (concat (keys peer-tasks) 
                    (keys peer-tasks) (keys peer-tasks) (keys peer-tasks) (keys peer-tasks)
                    (keys peer-tasks) (keys peer-tasks) (keys peer-tasks) (keys peer-tasks)
                    (keys peer-tasks) (keys peer-tasks) (keys peer-tasks) (keys peer-tasks)
                    (keys peer-tasks) (keys peer-tasks) (keys peer-tasks) (keys peer-tasks)
                    (keys peer-tasks) (keys peer-tasks) (keys peer-tasks) (keys peer-tasks)
                    (keys peer-tasks)))

    (is @complete-job)))))
