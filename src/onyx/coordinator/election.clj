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
          (prn "Previous node is " predecessor)
          (when predecessor
            (extensions/on-delete sync predecessor #(>!! ch))

            (prn "Blocking")
            (<!! ch)
            (prn "Unblocking")
            (let [children (extensions/children sync :election)]
              (prn "Children are " children)
              (when-not (extensions/smallest? sync :election node)
                (recur)))))))
    (catch Exception e
      (.printStackTrace e))))

