(ns onyx.spec
  (:require #?(:clj [clojure.spec :as s]
               :cljs [cljs.spec :as s :refer-macros [coll-of]])
            #?(:clj [clojure.future :refer [any? boolean? uuid? pos-int?]])
            [onyx.information-model :as i]
            [onyx.refinements :as r]
            [onyx.triggers :as t]))

;; This is an experimental Clojure.spec specification for Onyx.
;; It should not be treated as the canonical specification. Refer
;; to schema.cljc for the current, definitive data specification.

(defn atom? [x]
  (instance? clojure.lang.Atom x))

(defn namespaced-keyword? [x]
  (and (keyword? x) (namespace x)))

(s/def :onyx/name keyword?)

(s/def :job/workflow
  (s/coll-of (s/coll-of :onyx/name :kind vector? :count 2)
             :kind vector?
             :min-count 1))

(s/def :onyx/batch-size pos-int?)

(s/def :onyx/batch-timeout pos-int?)

(s/def :onyx/bulk? boolean?)

(s/def :onyx/batch-fn? boolean?)

(s/def :onyx/doc string?)

(s/def :onyx/flux-policy #{:kill :continue :recover})

(s/def :onyx/fn namespaced-keyword?)

(s/def :onyx/group-by-key any?)

(s/def :onyx/group-by-fn namespaced-keyword?)

(s/def :onyx/language #{:clojure :java})

(s/def :onyx/max-peers pos-int?)

(s/def :onyx/max-pending pos-int?)

(s/def :onyx/medium keyword?)

(s/def :onyx/min-peers pos-int?)

(s/def :onyx/n-peers pos-int?)

(s/def :onyx/params
  (s/coll-of keyword? :kind vector?))

(s/def :onyx/plugin keyword?)

(s/def :onyx/required-tags
  (s/coll-of keyword? :kind vector?))

(s/def :onyx/restart-pred-fn keyword?)

(s/def :onyx/type #{:input :function :output})

(defmulti onyx-type :onyx/type)

(defmethod onyx-type :input
  [_]
  (s/keys :req [:onyx/name
                :onyx/plugin
                :onyx/type
                :onyx/batch-size]
          :opt [:onyx/batch-timeout
                :onyx/batch-fn?
                :onyx/fn
                :onyx/n-peers
                :onyx/min-peers
                :onyx/max-peers
                :onyx/params
                :onyx/required-tags
                :onyx/language
                :onyx/doc]))

(defmethod onyx-type :function
  [_]
  (s/keys :req [:onyx/name :onyx/type :onyx/fn :onyx/batch-size]
          :opt [:onyx/batch-timeout
                :onyx/batch-fn?
                :onyx/n-peers
                :onyx/min-peers
                :onyx/max-peers
                :onyx/group-by-key
                :onyx/group-by-fn
                :onyx/flux-policy
                :onyx/params
                :onyx/required-tags
                :onyx/plugin
                :onyx/language
                :onyx/doc]))

(defmethod onyx-type :output
  [_]
  (s/keys :req [:onyx/name
                :onyx/plugin
                :onyx/type
                :onyx/batch-size]
          :opt [:onyx/batch-timeout
                :onyx/batch-fn?
                :onyx/fn
                :onyx/group-by-key
                :onyx/group-by-fn
                :onyx/flux-policy
                :onyx/n-peers
                :onyx/min-peers
                :onyx/max-peers
                :onyx/params
                :onyx/required-tags
                :onyx/language
                :onyx/doc]))

(s/def :lifecycle/task keyword?)

(s/def :lifecycle/calls namespaced-keyword?)

(s/def :lifecycle/doc string?)

(s/def :flow/action #{:retry})

(s/def :flow/special-tasks #{:all :none})

(s/def :flow/from
  (s/or :task-name keyword?
        :special-task :flow/special-tasks))

(s/def :flow/to
  (s/or :task-name keyword?
        :task-names (s/coll-of keyword? :kind vector?)
        :special-task :flow/special-tasks))

(s/def :flow/predicate
  (s/or :and (s/cat :op #{:and} :exprs (s/+ :flow/predicate))
        :or (s/cat :op #{:or} :exprs (s/+ :flow/predicate))
        :not (s/cat :op #{:not} :exprs (s/cat :pred :flow/predicate))
        :fn keyword?
        :fn-and-args (s/cat :fn (fn [x]
                                  (and (keyword? x)
                                       (not (some #{x} #{:and :or :not}))))
                            :args (s/* keyword?))))

(s/def :flow/post-transform namespaced-keyword?)

(s/def :flow/thrown-exception? boolean?)

(s/def :flow/short-circuit? boolean?)

(s/def :flow/exclude-keys
  (s/coll-of keyword? :kind vector?))

(s/def :flow/doc string?)

(s/def :flow/condition
  (s/keys :req [:flow/from
                :flow/to
                :flow/predicate]
          :opt [:flow/post-transform
                :flow/thrown-exception?
                :flow/action
                :flow/short-circuit]))

(s/def :job/flow-conditions
  (s/coll-of :flow/condition
             :kind vector?))

(s/def :job.lifecycles/lifecycle
  (s/keys :req [:lifecycle/task
                :lifecycle/calls]
          :opt [:lifecycle/doc]))

(s/def :data/unit
  #{:element :elements})

(s/def :time/unit
  #{:day :days
    :hour :hours
    :minute :minutes
    :second :seconds
    :millisecond :milliseconds})

(s/def :measurement/unit
  #{:day :days
    :hour :hours
    :minute :minutes
    :second :seconds
    :millisecond :milliseconds
    :element :elements})

(s/def :window/doc string?)

(s/def :window/session-key any?)

(s/def :window/task keyword?)

(s/def :window/id (s/or :keyword keyword? :uuid uuid?))

(s/def :window/task keyword?)

(s/def :window/init any?)

(s/def :window/min-value number?)

(s/def :window/type
  #{:fixed :sliding :global :session})

(s/def :window/range
  (s/cat :int integer? :unit :measurement/unit))

(s/def :window/slide
  (s/cat :int integer? :unit :measurement/unit))

(s/def :window/timeout-gap
  (s/cat :int integer? :unit :measurement/unit))

(s/def :window/aggregation
  (s/or :keyword keyword?
        :vec (s/cat :keyword keyword?)
        :vec-params (s/cat :keyword keyword? :any any?)))

(s/def :window/window-key any?)

(defmulti window-type :window/type)

(defmethod window-type :fixed
  [_]
  (s/keys :req [:window/id :window/task :window/type
                :window/aggregation :window/window-key :window/range]
          :opt [:window/init :window/doc :window/min-value]))

(defmethod window-type :sliding
  [_]
  (s/keys :req [:window/id :window/task :window/type :window/range
                :window/aggregation :window/window-key :window/slide]
          :opt [:window/init :window/doc :window/min-value]))

(defmethod window-type :global
  [_]
  (s/keys :req [:window/id :window/task :window/type :window/aggregation]
          :opt [:window/init :window/doc]))

(defmethod window-type :session
  [_]
  (s/keys :req [:window/id :window/task :window/type :window/aggregation
                :window/window-key :window/session-key :window/timeout-gap]
          :opt [:window/init :window/doc :window/min-value]))

(s/def :job/windows
  (s/coll-of (s/multi-spec window-type :window/type)
             :kind vector?))

(s/def :trigger/doc string?)

(s/def :trigger/fire-all-extents? boolean?)

(s/def :trigger/on keyword?)

(s/def :trigger/pred keyword?)

(s/def :trigger/sync keyword?)

(s/def :trigger/emit keyword?)

(s/def :trigger/window-id :window/id)

(s/def :trigger/on keyword?)

(s/def :trigger/fire-all-extents? boolean?)

(s/def :trigger/watermark-percentage number?)

(s/def :trigger/refinement
  #{:onyx.refinements/accumulating
    :onyx.refinements/discarding})

(s/def :trigger/period
  (s/cat :int integer? :unit :time/unit))

(s/def :trigger/threshold
  (s/cat :int integer? :unit :data/unit))

(defmulti trigger-on :trigger/on)

(defmethod trigger-on :onyx.triggers/timer
  [_]
  (s/keys :req [:trigger/window-id :trigger/refinement :trigger/on
                :trigger/period :trigger/sync]
          :opt [:trigger/doc]))

(defmethod trigger-on :onyx.triggers/segment
  [_]
  (s/keys :req [:trigger/window-id :trigger/refinement :trigger/on
                :trigger/threshold :trigger/sync]
          :opt [:trigger/fire-all-extents? :trigger/doc]))

(defmethod trigger-on :onyx.triggers/punctuation
  [_]
  (s/keys :req [:trigger/window-id :trigger/refinement :trigger/on
                :trigger/pred :trigger/sync]
          :opt [:trigger/doc]))

(defmethod trigger-on :onyx.triggers/watermark
  [_]
  (s/keys :req [:trigger/window-id :trigger/refinement :trigger/on
                :trigger/sync]
          :opt [:trigger/doc]))

(defmethod trigger-on :onyx.triggers/percentile-watermark
  [_]
  (s/keys :req [:trigger/window-id :trigger/refinement :trigger/on
                :trigger/watermark-percentage :trigger/sync]
          :opt [:trigger/doc]))

(s/def :job/trigger (s/multi-spec trigger-on :trigger/on))

(s/def :job/triggers
  (s/coll-of :job/trigger :kind vector?))

(s/def :jvm/function
  (s/or :fn fn? :var var?))

(s/def :onyx.core/id uuid?)

(s/def :onyx.core/lifecycle-id uuid?)

(s/def :onyx.core/job-id uuid?)

(s/def :onyx.core/task-id keyword?)

(s/def :onyx.core/task keyword?)

(s/def :onyx.core/fn fn?)

(s/def :onyx.core/pipeline any?)

(s/def :onyx.core/compiled any?)

(s/def :onyx.core/log-prefix string?)

(s/def :onyx.core/params?
  (s/coll-of any? :kind vector?))

(s/def :onyx.core/scheduler-event keyword?)

(s/def :onyx.core/drained-back-off integer?)

(s/def :onyx.core/task-kill-ch any?)

(s/def :onyx.core/kill-ch any?)

(s/def :onyx.core/outbox-ch any?)

(s/def :onyx.core/seal-ch any?)

(s/def :onyx.core/group-ch any?)

(s/def :onyx.core/state-ch any?)

(s/def :onyx.core/state-thread-ch any?)

(s/def :onyx.core/messenger-buffer any?)

(s/def :onyx.core/state-log record?)

(s/def :onyx.core/task-information record?)

(s/def :onyx.core/log record?)

(s/def :onyx.core/messenger record?)

(s/def :onyx.core/monitoring record?)

(s/def :onyx.core/results any?)

(s/def :onyx.core/state any?)

(s/def :onyx.core/peer-replica-view any?)

(s/def :onyx.core/replica any?)

(s/def :onyx.core/windows-state any?)

(s/def :onyx.core/filter-state any?)

(s/def :onyx.core/emitted-exhausted? atom?)

(s/def :onyx.core/batch
  (s/coll-of any? :kind vector?))

(s/def :onyx.core/metadata map?)

(s/def :onyx.core/serialized-task any?)

(s/def :onyx.core/workflow :job/workflow)

(s/def :onyx.core/task-map (s/multi-spec onyx-type :onyx/type))

(s/def :onyx.core/catalog
  (s/coll-of (s/multi-spec onyx-type :onyx/type) :kind vector?))

(s/def :onyx.core/flow-conditions :job/flow-conditions)

(s/def :onyx.core/windows :job/windows)

(s/def :onyx.core/triggers :job/triggers)

(s/def :onyx.core/lifecycles
  (s/coll-of :job.lifecycles/lifecycle :kind vector?))

(s/def :onyx.messaging/peer-port integer?)

(s/def :onyx.messaging/external-addr string?)

(s/def :onyx.messaging/inbound-buffer-size integer?)

(s/def :onyx.messaging/completion-buffer-size integer?)

(s/def :onyx.messaging/release-ch-buffer-size integer?)

(s/def :onyx.messaging/retry-ch-buffer-size integer?)

(s/def :onyx.messaging/peer-link-gc-interval integer?)

(s/def :onyx.messaging/peer-link-idle-timeout integer?)

(s/def :onyx.messaging/ack-daemon-timeout integer?)

(s/def :onyx.messaging/ack-daemon-clear-interval integer?)

(s/def :onyx.messaging/decompress-fn fn?)

(s/def :onyx.messaging/compress-fn fn?)

(s/def :onyx.messaging/allow-short-circuit? boolean?)

(s/def :onyx.messaging.aeron/embedded-driver? boolean?)

(s/def :onyx.messaging.aeron/subscriber-count integer?)

(s/def :onyx.messaging.aeron/write-buffer-size integer?)

(s/def :onyx.messaging.aeron/publication-creation-timeout integer?)

(s/def :onyx.messaging.aeron/embedded-media-driver-threading
  #{:dedicated :shared :shared-network})

(s/def :aeron/idle-strategy
  #{:busy-spin :low-restart-latency :high-restart-latency})

(s/def :onyx.messaging.aeron/poll-idle-strategy :aeron/idle-strategy)

(s/def :onyx.messaging.aeron/offer-idle-strategy :aeron/idle-strategy)

(s/def :onyx.bookkeeper/client-timeout pos-int?)

(s/def :onyx.bookkeeper/client-throttle pos-int?)

(s/def :onyx.bookkeeper/ledger-password string?)

(s/def :onyx.bookkeeper/ledger-id-written-back-off pos-int?)

(s/def :onyx.bookkeeper/ledger-ensemble-size pos-int?)

(s/def :onyx.bookkeeper/ledger-quorum-size pos-int?)

(s/def :onyx.bookkeeper/write-batch-size pos-int?)

(s/def :onyx.bookkeeper/write-buffer-size pos-int?)

(s/def :onyx.bookkeeper/write-batch-backoff pos-int?)

(s/def :onyx.bookkeeper/read-batch-size pos-int?)

(s/def :onyx.rocksdb.filter/base-dir string?)

(s/def :onyx.rocksdb.filter/bloom-filter-bits pos-int?)

(s/def :onyx.rocksdb.filter/compression
  #{:bzip2 :lz4 :lz4hc :none :snappy :zlib})

(s/def :onyx.rocksdb.filter/block-size pos-int?)

(s/def :onyx.rocksdb.filter/peer-block-cache-size pos-int?)

(s/def :onyx.rocksdb.filter/num-buckets pos-int?)

(s/def :onyx.rocksdb.filter/num-ids-per-bucket pos-int?)

(s/def :onyx.rocksdb.filter/rotation-check-interval-ms pos-int?)

(s/def :onyx.zookeeper/backoff-base-sleep-time-ms integer?)

(s/def :onyx.zookeeper/backoff-max-sleep-time-ms integer?)

(s/def :onyx.zookeeper/backoff-max-retries integer?)

(s/def :onyx.zookeeper/prepare-failure-detection-interval integer?)

(s/def :onyx.peer/stop-task-timeout-ms integer?)

(s/def :onyx.peer/inbox-capacity integer?)

(s/def :onyx.peer/outbox-capacity integer?)

(s/def :onyx.peer/retry-start-interval integer?)

(s/def :onyx.peer/join-failure-back-off integer?)

(s/def :onyx.peer/drained-back-off integer?)

(s/def :onyx.peer/job-not-ready-back-off integer?)

(s/def :onyx.peer/peer-not-ready-back-off integer?)

(s/def :onyx.peer/fn-params any?)

(s/def :onyx.peer/backpressure-check-interval integer?)

(s/def :onyx.peer/backpressure-low-water-pct integer?)

(s/def :onyx.peer/backpressure-high-water-pct integer?)

(s/def :onyx.peer/state-log-impl #{:bookkeeper :none})

(s/def :onyx.peer/state-filter-impl #{:set :rocksdb})

(s/def :onyx.peer/tags (s/coll-of keyword? :kind vector?))

(s/def :onyx.peer/trigger-timer-resolution pos-int?)

(s/def :onyx.log/config map?)

(s/def :onyx.windowing/min-value integer?)

(s/def :onyx.task-scheduler.colocated/only-send-local? boolean?)

(s/def :onyx.query/server? boolean?)

(s/def :onyx.query.server/ip string?)

(s/def :onyx.query.server/port integer?)

(s/def :zookeeper/address string?)

(s/def :onyx/tenancy-id
  (s/or :uuid uuid? :string string?))

(s/def :onyx.peer/job-scheduler namespaced-keyword?)

(s/def :onyx.messaging/bind-addr string?)

(s/def :onyx/messaging #{:aeron :dummy-messenger})

(s/def :onyx.messaging/impl :onyx/messaging)

(s/def :onyx/peer-config
  (s/keys :req [:onyx/tenancy-id
                :onyx.peer/job-scheduler
                :onyx.messaging/bind-addr
                :onyx.messaging/impl
                :zookeeper/address]
          :opt [:onyx.log/config
                :onyx.windowing/min-value
                :onyx.task-scheduler.colocated/only-send-local?
                :onyx.query/server?
                :onyx.query.server/ip
                :onyx.query.server/port
                :onyx.peer/stop-task-timeout-ms
                :onyx.peer/inbox-capacity
                :onyx.peer/outbox-capacity
                :onyx.peer/retry-start-interval
                :onyx.peer/join-failure-back-off
                :onyx.peer/drained-back-off
                :onyx.peer/job-not-ready-back-off
                :onyx.peer/peer-not-ready-back-off
                :onyx.peer/fn-params
                :onyx.peer/backpressure-check-interval
                :onyx.peer/backpressure-low-water-pct
                :onyx.peer/backpressure-high-water-pct
                :onyx.peer/state-log-impl
                :onyx.peer/state-filter-impl
                :onyx.peer/tags
                :onyx.peer/trigger-timer-resolution
                :onyx.messaging/peer-port
                :onyx.messaging/external-addr
                :onyx.messaging/inbound-buffer-size
                :onyx.messaging/completion-buffer-size
                :onyx.messaging/release-ch-buffer-size
                :onyx.messaging/retry-ch-buffer-size
                :onyx.messaging/peer-link-gc-interval
                :onyx.messaging/peer-link-idle-timeout
                :onyx.messaging/ack-daemon-timeout
                :onyx.messaging/ack-daemon-clear-interval
                :onyx.messaging/decompress-fn
                :onyx.messaging/compress-fn
                :onyx.messaging/allow-short-circuit?
                :onyx.messaging.aeron/embedded-driver?
                :onyx.messaging.aeron/subscriber-count
                :onyx.messaging.aeron/write-buffer-size
                :onyx.messaging.aeron/publication-creation-timeout
                :onyx.messaging.aeron/embedded-media-driver-threading
                :onyx.messaging.aeron/poll-idle-strategy
                :onyx.messaging.aeron/offer-idle-strategy
                :onyx.bookkeeper/client-timeout
                :onyx.bookkeeper/client-throttle
                :onyx.bookkeeper/ledger-password
                :onyx.bookkeeper/ledger-id-written-back-off
                :onyx.bookkeeper/ledger-ensemble-size
                :onyx.bookkeeper/ledger-quorum-size
                :onyx.bookkeeper/write-batch-size
                :onyx.bookkeeper/write-buffer-size
                :onyx.bookkeeper/write-batch-backoff
                :onyx.bookkeeper/read-batch-size
                :onyx.rocksdb.filter/base-dir
                :onyx.rocksdb.filter/bloom-filter-bits
                :onyx.rocksdb.filter/compression
                :onyx.rocksdb.filter/block-size
                :onyx.rocksdb.filter/peer-block-cache-size
                :onyx.rocksdb.filter/num-buckets
                :onyx.rocksdb.filter/num-ids-per-bucket
                :onyx.rocksdb.filter/rotation-check-interval-ms
                :onyx.zookeeper/backoff-base-sleep-time-ms
                :onyx.zookeeper/backoff-max-sleep-time-ms
                :onyx.zookeeper/backoff-max-retries
                :onyx.zookeeper/prepare-failure-detection-interval]))

(s/def :onyx.core/peer-opts :onyx/peer-config)

(s/def :onyx.core/event-map
  (s/keys :req [:onyx.core/id
                :onyx.core/lifecycle-id
                :onyx.core/job-id
                :onyx.core/task-id
                :onyx.core/task
                :onyx.core/fn
                :onyx.core/pipeline
                :onyx.core/compiled
                :onyx.core/log-prefix
                :onyx.core/drained-back-off
                :onyx.core/params
                :onyx.core/task-kill-ch
                :onyx.core/kill-ch
                :onyx.core/outbox-ch
                :onyx.core/seal-ch
                :onyx.core/group-ch
                :onyx.core/task-information
                :onyx.core/log
                :onyx.core/messenger
                :onyx.core/messenger-buffer
                :onyx.core/monitoring
                :onyx.core/state
                :onyx.core/peer-replica-view
                :onyx.core/replica
                :onyx.core/emitted-exhausted?
                :onyx.core/metadata
                :onyx.core/serialized-task
                :onyx.core/task-map
                :onyx.core/workflow
                :onyx.core/catalog
                :onyx.core/flow-conditions
                :onyx.core/windows
                :onyx.core/triggers
                :onyx.core/lifecycles
                :onyx.core/peer-opts]
          :opt [:onyx.core/scheduler-event
                :onyx.core/state-ch
                :onyx.core/state-thread-ch
                :onyx.core/state-log
                :onyx.core/results
                :onyx.core/windows-state
                :onyx.core/filter-state
                :onyx.core/batch]))

(s/def :onyx.state-event/event-type keyword?)

(s/def :onyx.state-event/task-event :onyx.core/event-map)

(s/def :onyx.state-event/segment any?)

(s/def :onyx.state-event/group-key any?)

(s/def :onyx.state-event/window
  (s/multi-spec window-type :window/type))

(s/def :onyx.state-event/grouped? boolean?)

(s/def :onyx.state-event/lower-bound integer?)

(s/def :onyx.state-event/upper-bound integer?)

(s/def :onyx.state-event/log-type #{:trigger :aggregation})

(s/def :onyx.state-event/trigger-update
  (s/coll-of any? :kind vector?))

(s/def :onyx.state-event/aggregation-update
  (s/coll-of any? :kind vector?))

(s/def :onyx.state-event/next-state any?)

(s/def :onyx.core/state-event
  (s/keys :req-un [:onyx.state-event/event-type
                   :onyx.state-event/task-event
                   :onyx.state-event/segment
                   :onyx.state-event/group-key
                   :onyx.state-event/window]
          :opt-un [:onyx.state-event/grouped?
                   :onyx.state-event/lower-bound
                   :onyx.state-event/upper-bound
                   :onyx.state-event/log-type
                   :onyx.state-event/trigger-update
                   :onyx.state-event/aggregation-update
                   :onyx.state-event/next-state]))

(s/def :onyx.trigger-state.timer/fire-time integer?)

(s/def :onyx.trigger-state.timer/fire? boolean?)

(s/def :onyx.trigger-state/timer
  (s/keys :req-un [:onyx.trigger-state.timer/fire-time
                   :onyx.trigger-state.timer/fire?]))

(s/fdef r/discarding-create-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :state-event :onyx.core/state-event))

(s/fdef r/discarding-apply-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :entry any?))

(s/fdef r/accumulating-create-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :state-event :onyx.core/state-event))

(s/fdef r/accumulating-apply-state-update
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :entry any?))

(s/fdef t/next-fire-time
        :args (s/cat :trigger :job/trigger))

(s/fdef t/timer-init-state
        :args (s/cat :trigger :job/trigger))

(s/fdef t/punctuation-init-state
        :args (s/cat :trigger :job/trigger))

(s/fdef t/watermark-init-state
        :args (s/cat :trigger :job/trigger))

(s/fdef t/percentile-watermark-init-state
        :args (s/cat :trigger :job/trigger))

(s/fdef t/segment-next-state
        :args (s/cat :trigger :job/trigger
                     :state integer?
                     :state-event :onyx.core/state-event))

(s/fdef t/timer-next-state
        :args (s/cat :trigger :job/trigger
                     :state :onyx.trigger-state/timer
                     :state-event :onyx.core/state-event))

(s/fdef t/punctuation-next-state
        :args (s/cat :trigger :job/trigger
                     :state map?
                     :state-event :onyx.core/state-event))

(s/fdef t/watermark-next-state
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :state-event :onyx.core/state-event))

(s/fdef t/percentile-watermark-next-state
        :args (s/cat :trigger :job/trigger
                     :state any?
                     :state-event :onyx.core/state-event))

(s/fdef t/segment-fire?
        :args (s/cat :trigger :job/trigger
                     :trigger-state integer?
                     :state-event :onyx.core/state-event))

(s/fdef t/timer-fire?
        :args (s/cat :trigger :job/trigger
                     :trigger-state :onyx.trigger-state/timer
                     :state-event :onyx.core/state-event))

(s/fdef t/punctuation-fire?
        :args (s/cat :trigger :job/trigger
                     :trigger-state any?
                     :state-event :onyx.core/state-event))

(s/fdef t/watermark-fire?
        :args (s/cat :trigger :job/trigger
                     :trigger-state any?
                     :state-event :onyx.core/state-event))

(s/fdef t/percentile-watermark-fire?
        :args (s/cat :trigger :job/trigger
                     :trigger-state any?
                     :state-event :onyx.core/state-event))
