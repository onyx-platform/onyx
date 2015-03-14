(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout go <!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :refer [debug] :as timbre]))

(defmethod l-ext/inject-lifecycle-resources :core.async/read-from-chan
  [_ event]
  {:core.async/pending-messages (atom {})
   :core.async/retry-ch (chan 1000)})

(defmethod p-ext/read-batch [:input :core.async]
  [{:keys [onyx.core/task-map core.async/chan core.async/retry-ch
           core.async/pending-messages] :as event}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 50)
        batch (->> (range max-segments)
                   (map (fn [_] {:id (java.util.UUID/randomUUID)
                                :input :core.async
                                :message (first (alts!! [retry-ch chan (timeout ms)] :priority true))}))
                   (filter (comp not nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (:message m)))
    {:onyx.core/batch batch}))

(defmethod p-ext/decompress-batch [:input :core.async]
  [{:keys [onyx.core/batch]}]
  {:onyx.core/decompressed batch})

(defmethod p-ext/apply-fn [:input :core.async]
  [event segment]
  segment)

(defmethod p-ext/ack-message [:input :core.async]
  [{:keys [core.async/pending-messages]} message-id]
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message [:input :core.async]
  [{:keys [core.async/pending-messages core.async/retry-ch]} message-id]
  (>!! retry-ch (get @pending-messages message-id))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/pending? [:input :core.async]
  [{:keys [core.async/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? [:input :core.async]
  [{:keys [core.async/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (vals x)) :done))))

(defmethod p-ext/apply-fn [:output :core.async]
  [event segment]
  segment)

(defmethod p-ext/compress-batch [:output :core.async]
  [{:keys [onyx.core/results]}]
  {:onyx.core/compressed results})

(defmethod p-ext/write-batch [:output :core.async]
  [{:keys [onyx.core/compressed core.async/chan]}]
  (doseq [segment compressed]
    (>!! chan (:message segment)))
  {})

(defmethod p-ext/seal-resource [:output :core.async]
  [{:keys [core.async/chan]}]
  (>!! chan :done))

(defn take-segments!
  "Takes segments off the channel until :done is found.
   Returns a seq of segments, including :done."
  [ch]
  (loop [x []]
    (let [segment (<!! ch)]
      (let [stack (conj x segment)]
        (if-not (= segment :done)
          (recur stack)
          stack)))))

