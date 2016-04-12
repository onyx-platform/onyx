(ns onyx.flow-conditions.fc-routing
  (:require [onyx.peer.operation :as operation]
            [onyx.lifecycles.lifecycle-invoke :as lc]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.types :refer [->Route]]))

(defn join-output-paths [all to-add downstream]
  (cond (= to-add :all) (set downstream)
        (= to-add :none) #{}
        :else (into (set all) to-add)))

(defn choose-output-paths
  [event compiled-flow-conditions result message downstream]
  (reduce
   (fn [{:keys [flow exclusions] :as all} entry]
     (cond ((:flow/predicate entry) [event (:message (:root result)) message (map :message (:leaves result))])
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

(defn route-data
  [event {:keys [egress-ids compiled-ex-fcs compiled-norm-fcs flow-conditions] :as compiled} result message]
  (if (nil? flow-conditions)
    (if (operation/exception? message)
      (let [e (:exception (ex-data message))]
        (lc/handle-exception
         event :lifecycle/apply-fn e (:compiled-handle-exception-fn compiled)))
      (->Route egress-ids nil nil nil))
    (if (operation/exception? message)
      (if (seq compiled-ex-fcs)
        (choose-output-paths event compiled-ex-fcs result (:exception (ex-data message)) egress-ids)
        (throw (:exception (ex-data message))))
      (if (seq compiled-norm-fcs)
        (choose-output-paths event compiled-norm-fcs result message egress-ids)
        (->Route egress-ids nil nil nil)))))

(defn apply-post-transformation [message routes event]
  (let [post-transformation (:post-transformation routes)
        msg (if (and (operation/exception? message) post-transformation)
              (let [data (ex-data message)
                    f (operation/kw->fn post-transformation)]
                (f event (:segment data) (:exception data)))
              message)]
    (reduce dissoc msg (:exclusions routes))))

(defn flow-conditions-transform
  [message routes event compiled]
  (if (:flow-conditions compiled)
    (apply-post-transformation message routes event)
    message))
