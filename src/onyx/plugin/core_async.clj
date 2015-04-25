(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout go <!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults]]
            [taoensso.timbre :refer [debug] :as timbre]))

(defmethod l-ext/inject-lifecycle-resources :core.async/read-from-chan
  [_ event]
  (assert (:core.async/chan event) ":core.async/chan not found - add it via inject-lifecycle-resources.")
  {:core.async/pending-messages (atom {})
   :core.async/drained? (atom false)
   :core.async/retry-ch (chan 1000)})

(defmethod l-ext/inject-lifecycle-resources :core.async/write-to-chan
  [_ event]
  (assert (:core.async/chan event) ":core.async/chan not found - add it via inject-lifecycle-resources.")
  {})

(defmethod p-ext/read-batch :core.async/read-from-chan
  [{:keys [onyx.core/task-map core.async/chan core.async/retry-ch 
           core.async/pending-messages core.async/drained?] :as event}]
  (let [pending (count @pending-messages)
        max-pending (or (:onyx/max-pending task-map) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        step-ms (/ ms (:onyx/batch-size task-map))
        timeout-ch (timeout ms)
        batch (if (zero? max-segments)
                (<!! timeout-ch)
                (loop [segments [] cnt 0]
                  (if (= cnt batch-size)
                    segments
                    (if-let [message (first (alts!! [retry-ch chan timeout-ch] :priority true))] 
                      (recur (conj segments 
                                   {:id (java.util.UUID/randomUUID)
                                    :input :core.async
                                    :message message})
                             (inc cnt))
                      segments))))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (:message m)))
    (when (and (= 1 (count @pending-messages))
               (= (count batch) 1)
               (= (:message (first batch)) :done))
      (reset! drained? true))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :core.async/read-from-chan
  [{:keys [core.async/pending-messages]} message-id]
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :core.async/read-from-chan
  [{:keys [core.async/pending-messages core.async/retry-ch]} message-id]
  (when-let [msg (get @pending-messages message-id)]
    (>!! retry-ch msg)
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/pending? :core.async/read-from-chan
  [{:keys [core.async/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :core.async/read-from-chan
  [{:keys [core.async/drained? core.async/pending-messages] :as event}]
  @drained?)

(defmethod p-ext/write-batch :core.async/write-to-chan
  [{:keys [onyx.core/results core.async/chan] :as event}]
  (doseq [msg (mapcat :leaves results)]
    (>!! chan (:message msg)))
  {})

(defmethod p-ext/seal-resource :core.async/write-to-chan
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
