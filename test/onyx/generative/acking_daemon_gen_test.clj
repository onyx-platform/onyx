(ns onyx.generative.acking-daemon-gen-test
  (:require [clojure.core.async :refer [chan dropping-buffer]]
            [clojure.test :refer :all]
            [clojure.test :refer [is]]
            [clojure.test.check.generators :as gen]
            [stateful-check.core :refer [specification-correct?]]
            [onyx.messaging.acking-daemon :as a])
  (:import [java.util UUID]))

(def max-state-size 10000)

(def new-segment-specification
  ;; Disallow the acking daemon state to grow too large by not
  ;; exceeding a certain number of keys.
  {:model/requires
   (fn [state]
     (<= (count (keys (:model-state state))) max-state-size))

   ;; Generates a new message with a deterministically random
   ;; UUID for its identifier.
   :model/args
   (fn [state]
     [(:real-state state)
      (gen/return (chan 1))
      (gen/hash-map
       :id (gen/return (UUID/randomUUID))
       :completion-id (gen/return (UUID/randomUUID))
       :ack-val (gen/return (a/gen-ack-value)))])

   :real/command #'a/ack-segment

   :next-state
   (fn [state [real-state ch {:keys [id completion-id ack-val]}] result]
     (-> state
         (assoc-in [:model-state id :completion-id] completion-id)
         (assoc-in [:model-state id :ack-val] ack-val)))

   ;; Ensure that the ack value that we generated shows up in the
   ;; acking daemon state. Since we generated a new segment ID that
   ;; wasn't in the state before hand, the exact ack value should
   ;; be present, and not XOR'ed against any other values.
   :real/postcondition
   (fn [prev-state next-state [real-state ch {:keys [id ack-val]}] result]
     (= (get-in (:state @(:real-state next-state)) [id :ack-val])
        ack-val))})

(def updated-segment-specification
  {:model/requires
   ;; A segment must exist for us to update it.
   (fn [state]
     (seq (keys (:model-state state))))

   ;; Pick a segment ID that's already present, and choose an
   ;; ack value that is *not* equal to the present one. This
   ;; means the XOR result will never be zero, and the acking
   ;; daemon's state will think the segment is complete.
   :model/args
   (fn [state]
     (let [message-ids (keys (:model-state state))
           id-gen (gen/elements message-ids)]
       [(:real-state state)
        (gen/return (chan (dropping-buffer 1)))
        (gen/hash-map
         :id id-gen
         :ack-val (gen/return (a/gen-ack-value))
         :completion-id
         (gen/bind
          id-gen
          #(gen/return (get-in state [:model-state % :completion-id]))))]))

   ;; Notice this is the same command as the new-segment
   ;; specification. It works for both types of interactions.
   :real/command #'a/ack-segment

   ;; Transition to the next internal state with XOR.
   :next-state
   (fn [state [real-state ch {:keys [id completion-id ack-val]}] result]
     (update-in state [:model-state id :ack-val] bit-xor ack-val))

   ;; The ack value should be different between the two states.
   :real/postcondition
   (fn [prev-state next-state [real-state ch {:keys [id ack-val]}] result]
     (let [real @(:real-state next-state)
           current-ack-val (get-in (:state real) [id :ack-val])
           prev-model-ack-val (get-in prev-state [:model-state id :ack-val])
           next-model-ack-val (get-in next-state [:model-state id :ack-val])]
       (and (= current-ack-val next-model-ack-val)
            (not= prev-model-ack-val current-ack-val)
            (not= prev-model-ack-val next-model-ack-val)
            (not (nil? current-ack-val))
            (not (nil? prev-model-ack-val)))))})

(def complete-existing-segment-specification
  {:model/requires
   ;; A segment must exist for us to complete it over
   ;; more than one action.
   (fn [state]
     (seq (keys (:model-state state))))

   ;; Pick a segment ID that already exists and ack
   ;; its current value. This will result in the XOR
   ;; operation equaling 0, thus completing the segment.
   :model/args
   (fn [state]
     (let [message-ids (keys (:model-state state))
           id-gen (gen/elements message-ids)]
       [(:real-state state)
        (gen/return (chan (dropping-buffer 1)))
        (gen/fmap
         (fn [v]
           (let [av (get-in state [:model-state (:id v) :ack-val])
                 ci (get-in state [:model-state (:id v) :completion-id])]
             (-> v
                 (assoc :ack-val av)
                 (assoc :completion-id ci))))
         (gen/hash-map :id id-gen))]))

   :real/command #'a/ack-segment

   ;; Transition should go to zero, removing this segment from the state.
   :next-state
   (fn [state [real-state ch {:keys [id completion-id ack-val]}] result]
     (update-in state [:model-state] dissoc id))

   ;; Segment should be gone from both the model and the real state.
   :real/postcondition
   (fn [prev-state next-state [real-state ch {:keys [id ack-val]}] result]
     (let [real @(:real-state next-state)
           real-record (get (:state real) id)
           prev-model-record (get-in prev-state [:model-state id])
           next-model-record (get-in next-state [:model-state id])]
       (and (nil? real-record)
            (nil? next-model-record)
            (not (nil? prev-model-record)))))})

(def complete-new-segment-specification
  {:model/requires
   (fn [state]
     (<= (count (keys (:model-state state))) max-state-size))

   ;; Complete a new segment by immediately reported 0 as an ack val.
   ;; This can happen if a flow condition for an input task chooses
   ;; not to route a segment to the next task.
   :model/args
   (fn [state]
     [(:real-state state)
      (gen/return (chan 1))
      (gen/hash-map
       :id (gen/return (UUID/randomUUID))
       :completion-id (gen/return (UUID/randomUUID))
       :ack-val (gen/return 0))])

   :real/command #'a/ack-segment

   ;; Immediately remove this segment from the state.
   :next-state
   (fn [state [real-state ch {:keys [id completion-id ack-val]}] result]
     (update-in state [:model-state] dissoc id))

   ;; Check that it was removed from the state.
   :real/postcondition
   (fn [prev-state next-state [real-state ch {:keys [id ack-val]}] result]
     (let [real @(:real-state next-state)
           real-record (get (:state real) id)
           prev-model-record (get-in prev-state [:model-state id])
           next-model-record (get-in next-state [:model-state id])]
       (and (nil? real-record)
            (nil? next-model-record)
            (nil? prev-model-record))))})

(def acking-daemon-spec
  {:commands {:new #'new-segment-specification
              :update #'updated-segment-specification
              :complete-existing #'complete-existing-segment-specification
              :complete-new #'complete-new-segment-specification}
   :real/setup #'a/init-state
   :initial-state (fn [ack-state] {:real-state ack-state :model-state {}})})

(is (specification-correct? acking-daemon-spec))
