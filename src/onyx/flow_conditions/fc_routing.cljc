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
     (let [pred-result
           (try
             {:success? true
              :result ((:flow/predicate entry) [event (:root result) message (:leaves result)])}
             (catch #?(:clj Throwable :cljs js/Error) t
               {:success? false
                :result {:old (:root result)
                         :new message
                         :exception t}}))]
       (cond (not (:success? pred-result))
             (reduced (->Route (join-output-paths flow (:flow/predicate-errors-to entry) downstream)
                               (into (set exclusions) (:flow/exclude-keys entry))
                               (:flow/post-transform entry)
                               (:flow/action entry)
                               (:result pred-result)))

             (and (:success? pred-result) (:result pred-result))
             (if (:flow/short-circuit? entry)
               (reduced (->Route (join-output-paths flow (:flow/to entry) downstream)
                                 (into (set exclusions) (:flow/exclude-keys entry))
                                 (:flow/post-transform entry)
                                 (:flow/action entry)
                                 nil))
               (->Route (join-output-paths flow (:flow/to entry) downstream)
                        (into (set exclusions) (:flow/exclude-keys entry))
                        nil
                        nil
                        nil))

             (= (:flow/action entry) :retry)
             (->Route (join-output-paths flow (:flow/to entry) downstream)
                      (into (set exclusions) (:flow/exclude-keys entry))
                      nil
                      nil
                      nil)

             :else all)))
   (->Route #{} #{} nil nil nil)
   compiled-flow-conditions))

(defn route-data [{:keys [egress-tasks onyx.core/flow-conditions] :as event} result message]
  (if (nil? (:onyx.core/flow-conditions event))
    (if (exception? message)
      (let [{:keys [exception segment]} (ex-data message)]
        (throw (maybe-attach-segment exception (:onyx.core/task-id event) segment)))
      (->Route egress-tasks nil nil nil nil))
    (if (exception? message)
      (if-let [compiled-ex-fcs (seq (:compiled-ex-fcs event))]
        (choose-output-paths event compiled-ex-fcs result (:exception (ex-data message)) egress-tasks)
        (let [{:keys [exception segment]} (ex-data message)]
          (throw (maybe-attach-segment exception (:onyx.core/task-id event) segment))))
      (if-let [compiled-norm-fcs (seq (:compiled-norm-fcs event))]
        (choose-output-paths event compiled-norm-fcs result message egress-tasks)
        (->Route egress-tasks nil nil nil nil)))))

(defn apply-post-transformation [message routes event]
  (let [post-transformation (:post-transformation routes)
        pred-failure (:pred-failure routes)
        msg (cond pred-failure
                  (if post-transformation
                    (let [f (kw->fn post-transformation)
                          message-ks (select-keys pred-failure [:old :new])]
                      (f event message-ks (:exception pred-failure)))
                    (throw (:exception pred-failure)))

                  (and (exception? message) post-transformation)
                  (let [data (ex-data message)
                        f (kw->fn post-transformation)]
                    (f event (:segment data) (:exception data)))

                  :else
                  message)]
    (reduce dissoc msg (:exclusions routes))))

(defn flow-conditions-transform
  [message routes event]
  (if (:onyx.core/flow-conditions event)
    (apply-post-transformation message routes event)
    message))
