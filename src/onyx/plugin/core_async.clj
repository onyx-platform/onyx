(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout go <!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :refer [debug] :as timbre]))

(defmethod l-ext/inject-lifecycle-resources :core.async/read-from-chan
  [_ event]
  {:core.async/pending-messages (atom {})
   :core.async/replay-ch (chan 1000)})

(defmethod p-ext/read-batch [:input :core.async]
  [{:keys [onyx.core/task-map core.async/in-chan core.async/replay-ch
           core.async/pending-messages] :as event}]
  (let [batch-size (:onyx/batch-size task-map)
        ms (or (:onyx/batch-timeout task-map) 1000)
        batch (->> (range batch-size)
                   (map (fn [_] {:id (java.util.UUID/randomUUID)
                                :input :core.async
                                :message (first (alts!! [replay-ch in-chan (timeout ms)] :priority true))}))
                   (filter (comp not nil? :message)))]
    (doseq [m batch]
      (go (try (<! (timeout 5000))
               (when (get @pending-messages (:id m))
                 (p-ext/replay-message event (:id m)))
               (catch Exception e
                 (taoensso.timbre/warn e))))
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

(defmethod p-ext/replay-message [:input :core.async]
  [{:keys [core.async/pending-messages core.async/replay-ch]} message-id]
  (>!! replay-ch (get @pending-messages message-id))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/drained? [:input :core.async]
  [{:keys [core.async/pending-messages]}]
  (let [x @pending-messages]
    (prn x)
    (and (= (count (keys x)) 1)
         (= (first (vals x)) :done))))

(defmethod p-ext/apply-fn [:output :core.async]
  [event segment]
  segment)

(defmethod p-ext/compress-batch [:output :core.async]
  [{:keys [onyx.core/results]}]
  {:onyx.core/compressed results})

(defmethod p-ext/write-batch [:output :core.async]
  [{:keys [onyx.core/compressed core.async/out-chan]}]
  (doseq [segment compressed]
    (>!! out-chan (:message segment)))
  {})

(defmethod p-ext/seal-resource [:output :core.async]
  [{:keys [core.async/out-chan]}]
  (>!! out-chan :done)
  {})

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

