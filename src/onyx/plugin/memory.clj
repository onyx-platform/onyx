(ns onyx.plugin.memory
  (:require [onyx.plugin.onyx-input :as i]
            [onyx.plugin.onyx-plugin :as p]))

(defrecord MemoryReader [event value]
  p/OnyxPlugin

  (start [this]
    (assoc this :checkpoint 0 :offset 0))

  (stop [this event] this)

  i/OnyxInput

  (checkpoint [{:keys [checkpoint]}]
    checkpoint)

  (set-epoch [this epoch]
    (assoc this :epoch epoch))

  (recover [{:keys [value] :as this} checkpoint]
    (assoc this :value (drop checkpoint value)))

  (offset-id [{:keys [offset]}]
    offset)

  (segment [{:keys [segment]}]
    segment)

  (next-state [{:keys [offset] :as this}
               {:keys [memory/value] :as event}]
    (let [segment (first value)]
      (assoc this
             :value (rest value)
             :segment segment
             :offset (if segment (inc offset) offset)
             :closed? (nil? value))))

  (ack-barrier [{:keys [checkpoint] :as this} barrier-epoch]
    (assoc this :checkpoint barrier-epoch))

  (segment-complete! [{:keys [conn]} segment])

  (completed? [{:keys [closed? segment checkpoint epoch]}]
    (and closed? (nil? segment) (= checkpoint epoch))))

(defn input [event]
  (map->MemoryReader {:event event}))
