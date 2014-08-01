(ns ^:no-doc onyx.coordinator.election
    (:require [clojure.core.async :refer [chan >!! <!!]]
              [onyx.extensions :as extensions]))

(defn block-until-leader! [sync content]
  (try
    (prn "Coordinator announced")
    (let [node (:node (extensions/create sync :election content))]
      (prn "Node is " node)
      (loop []
        (let [ch (chan)
              predecessor (extensions/previous-node sync :election node)]
          (when predecessor
            (extensions/on-delete sync predecessor #(>!! ch (:path %)))

            (prn node " is blocking")
            (<!! ch)
            (prn node " is unblocking")
            (let [children (extensions/bucket sync :election)]
              (let [s? (extensions/smallest? sync :election node)]
                (prn "Am I the leader? " node "::" s?)
                (when-not s?
                    (recur))))))))
    (catch Exception e
      (.printStackTrace e))))

