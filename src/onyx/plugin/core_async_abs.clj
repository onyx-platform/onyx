(ns onyx.plugin.core-async-abs
  (:require [clojure.core.async :refer [poll!]]
            [onyx.plugin.buffered-reader :as buffered-reader]
            [onyx.plugin.simple-input :as i]))

(defrecord AbsCoreAsyncReader [event channel]
  i/SimpleInput

  (start [this]
    (assoc this :channel channel :processing #{} :segment nil))

  (stop [this]
    (dissoc this :processing :segment))

  (checkpoint [this])

  (recover [this checkpoint]
    this)

  (segment-id [{:keys [segment]}]
    segment)

  (segment [{:keys [segment]}]
    segment)

  (next-state [{:keys [channel processing segment] :as this}
               {:keys [core.async/chan] :as event}]
    (let [segment (poll! chan)]
      (assoc this
             :segment segment
             :processing (if segment (conj processing segment) processing))))

  (checkpoint-ack [{:keys [processing] :as this} segment-id]
    (assoc this :processing (remove #(= segment-id %) processing)))

  (segment-complete! [{:keys [conn]} segment]))

(defn abs-core-async-reader [event]
  (map->AbsCoreAsyncReader {:event event}))

(def abs-core-async-reader-calls
  {:lifecycle/before-task-start (fn [event lifecycle]
                                  (buffered-reader/inject-buffered-reader event lifecycle))
   :lifecycle/after-task-stop (fn [event lifecycle]
                                (buffered-reader/close-buffered-reader event lifecycle))})
