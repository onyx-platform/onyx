(ns onyx.state.open-checkpoint-db
  (:require [onyx.compression.nippy :refer [checkpoint-compress checkpoint-decompress]]
            [onyx.state.protocol.db :as db]
            [onyx.checkpoint :as checkpoint]
            [onyx.state.serializers.checkpoint :as cpenc]
            [onyx.state.serializers.utils]
            [taoensso.timbre :refer [debug info error warn trace fatal]]))

(defn read-open-checkpoint-db [windows triggers grouped? job-id task-id slot-id storage coordinates]
  (let [{:keys [tenancy-id job-id replica-version epoch]} coordinates
        bs (checkpoint/read-checkpoint storage tenancy-id job-id replica-version epoch task-id slot-id :windows)
        state-serializers (onyx.state.serializers.utils/state-serializers grouped? windows triggers)
        db-name "checkpoint-db"
        peer-opts {:onyx.peer/state-store-impl :memory}
        state-store (db/create-db peer-opts db-name state-serializers)
        decoder (cpenc/new-decoder bs)
        schema-version (cpenc/get-schema-version decoder)
        metadata-bs (cpenc/get-metadata decoder)
        _ (when-not (= schema-version cpenc/current-schema-version)
            (throw (ex-info "Incompatible schema for state checkpoint."
                            {:current cpenc/current-schema-version
                             :retrieved schema-version})))
        metadata (checkpoint-decompress metadata-bs)]
    (db/restore! state-store decoder identity)
    {:db state-store
     :state-indexes (:state-indexes metadata)}))
