(ns onyx.log.commands.notify-join-cluster
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info] :as timbre]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defn already-joined? [replica entry]
  (boolean (get (set (:groups replica)) (:observer (:args entry)))))

(s/defmethod extensions/apply-log-entry :notify-join-cluster :- Replica
  [{:keys [args] :as entry} :- LogEntry replica]
  (let [prepared (get (map-invert (:prepared replica)) (:observer args))]
    (assert (not= prepared (:observer args)))
    (if (and prepared (not (already-joined? replica entry)))  
      (-> replica
          (update-in [:accepted] merge {prepared (:observer args)})
          (update-in [:prepared] dissoc prepared))
      replica)))

(s/defmethod extensions/replica-diff :notify-join-cluster :- ReplicaDiff
  [entry old new]
  (let [rets (second (diff (:accepted old) (:accepted new)))]
    (assert (<= (count rets) 1))
    (when (seq rets)
      {:observer (first (vals rets))
       :subject (or (get-in old [:pairs (first (keys rets))]) (first (keys rets)))
       :accepted-observer (first (keys rets))
       :accepted-joiner (first (vals rets))})))

(s/defmethod extensions/reactions [:notify-join-cluster :group] :- Reactions
  [entry old new diff peer-args]
  (let [success? (and (= (vals diff) (remove nil? (vals diff)))
                      (= (:id peer-args) (:observer diff)))] 
    (cond success?
          [{:fn :accept-join-cluster 
            :args diff}]
          (already-joined? old entry)
          []
          (= (:id peer-args) (:observer (:args entry)))
          [{:fn :abort-join-cluster
            :args {:id (:observer (:args entry))}}])))

(s/defmethod extensions/fire-side-effects! [:notify-join-cluster :group] :- State
  [{:keys [args message-id]} old new diff {:keys [monitoring] :as state}]
  (if (= (:id state) (:observer diff))
    (let [ch (chan 1)]
      (extensions/emit monitoring {:event :group-notify-join :id (:id state)})
      (extensions/on-delete (:log state) (:subject diff) ch)
      (go (when (<! ch)
            (extensions/write-log-entry
             (:log state)
             {:fn :group-leave-cluster :args {:id (:subject diff)}
              :entry-parent message-id
              :peer-parent (:id state)}))
          (close! ch))
      (close! (or (:watch-ch state) (chan)))
      (let [result-state (assoc state :watch-ch ch)]
        (if-let [failure-detector (:failure-detector result-state)]
          (do (component/stop failure-detector)
              (dissoc result-state :failure-detector))
          result-state)))
    state))
