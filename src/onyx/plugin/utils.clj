(ns onyx.plugin.utils
  (:require [clojure.core.async :refer [poll! alts!! go-loop timeout]]
            [onyx.plugin.simple-input :as i]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]]))

(defn start-commit-loop! [reader shutdown-ch commit-ms log k]
  (go-loop []
           (let [timeout-ch (timeout commit-ms)] 
             (when-let [[_ ch] (alts!! [shutdown-ch timeout-ch] :priority true)]
               (when (= ch timeout-ch)
                 (extensions/force-write-chunk log :chunk (i/checkpoint @reader) k)
                 (recur))))))

(defn process-completed! [reader complete-ch] 
  (loop [r reader input (poll! complete-ch)]
    (if input
      (let [next-r (i/checkpoint-ack r (:offset input))]
        (i/segment-complete! next-r (:message input))
        (recur next-r (poll! complete-ch)))
      r)))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))
