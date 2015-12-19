(ns onyx.flow-conditions.fc-routing
  (:require [onyx.peer.operation :as operation]
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
  [event result message flow-conditions downstream]
  (if (nil? flow-conditions)
    (if (operation/exception? message)
      (throw (:exception (ex-data message)))
      (->Route downstream nil nil nil))
    (let [compiled-ex-fcs (:onyx.core/compiled-ex-fcs event)]
      (if (operation/exception? message)
        (if (seq compiled-ex-fcs)
          (choose-output-paths event compiled-ex-fcs result
                               (:exception (ex-data message)) downstream)
          (throw (:exception (ex-data message))))
        (let [compiled-norm-fcs (:onyx.core/compiled-norm-fcs event)]
          (if (seq compiled-norm-fcs)
            (choose-output-paths event compiled-norm-fcs result message downstream)
            (->Route downstream nil nil nil)))))))
