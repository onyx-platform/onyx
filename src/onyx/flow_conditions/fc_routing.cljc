(ns onyx.flow-conditions.fc-routing
  (:require [onyx.static.util :refer [kw->fn exception?] :as u]
            [onyx.types :refer [->Route]]))

(defn join-output-paths [all to-add downstream]
  (cond (= to-add :all) (set downstream)
        (= to-add :none) #{}
        :else (into (set all) to-add)))

(defn maybe-attach-segment [e task-id segment]
  #?(:cljs e
     :clj (u/deserializable-exception e {:offending-task task-id
                                         :offending-segment segment})))

(defn choose-output-paths
  [event compiled-flow-conditions result message downstream]
  (reduce
   (fn [{:keys [flow exclusions] :as all} entry]
     (cond ((:flow/predicate entry) [event (:root result) message (:leaves result)])
           (if (:flow/short-circuit? entry)
             (reduced (->Route (join-output-paths flow (:flow/to entry) downstream)
                               (into (set exclusions) (:flow/exclude-keys entry))
                               (:flow/post-transform entry)
                               (:flow/action entry)))
             (->Route (join-output-paths flow (:flow/to entry) downstream)
                      (into (set exclusions) (:flow/exclude-keys entry))
                      nil
                      nil))

           (= (:flow/action entry) :retry)
           (->Route (join-output-paths flow (:flow/to entry) downstream)
                    (into (set exclusions) (:flow/exclude-keys entry))
                    nil
                    nil)

           :else all))
   (->Route #{} #{} nil nil)
   compiled-flow-conditions))

(defn route-data [{:keys [egress-tasks compiled-ex-fcs compiled-norm-fcs
                          onyx.core/flow-conditions] :as event} result message]
  (let [{:keys [onyx.core/task-id]} event]
    (if (nil? flow-conditions)
      (if (exception? message)
        (let [{:keys [exception segment]} (ex-data message)]
          (throw (maybe-attach-segment exception task-id segment)))
        (->Route egress-tasks nil nil nil))
      (if (exception? message)
        (if (seq compiled-ex-fcs)
          (choose-output-paths event compiled-ex-fcs result (:exception (ex-data message)) egress-tasks)
          (let [{:keys [exception segment]} (ex-data message)]
            (throw (maybe-attach-segment exception task-id segment)))))
      (if (seq compiled-norm-fcs)
        (choose-output-paths event compiled-norm-fcs result message egress-tasks)
        (->Route egress-tasks nil nil nil))))))

(defn apply-post-transformation [message routes event]
  (let [post-transformation (:post-transformation routes)
        msg (if (and (exception? message) post-transformation)
              (let [data (ex-data message)
                    f (kw->fn post-transformation)]
                (f event (:segment data) (:exception data)))
              message)]
    (reduce dissoc msg (:exclusions routes))))

(defn flow-conditions-transform
  [message routes event]
  (if (:onyx.core/flow-conditions event)
    (apply-post-transformation message routes event)
    message))
