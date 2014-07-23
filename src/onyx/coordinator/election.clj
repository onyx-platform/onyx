(ns ^:no-doc onyx.coordinator.election
    (:require [clojure.core.async :refer [chan >!! <!!]]
              [onyx.extensions :as extensions]))

(defn block-until-leader! [sync content]
  (let [node (:node (extensions/create sync :election content))]
    (loop []
      (let [ch (chan)
            predecessor (extensions/predecessor sync node)]
        (extensions/on-delete sync predecessor #(>!! ch))
    
        (<!! ch)
        (let [children (extensions/children sync :election)]
          (when-not (extensions/smallest? sync node)
            (recur)))))))

