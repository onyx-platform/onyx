(ns onyx.windowing.window-extensions-generative-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test :refer [deftest is]]
            [onyx.state.memory]
            [onyx.state.protocol.db :as s]
            [onyx.state.serializers.utils :as u]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.windowing.window-extensions :as we]
            [onyx.api]))

(deftest session-window-gen-test
  (checking "Session windows stores in proper extents"
   (times 2000)
   [timeout-gap-secs (gen/fmap #(Math/abs ^Integer %) (gen/resize 500 gen/int))
    event-times (gen/vector (gen/fmap #(Math/abs ^Integer %) (gen/resize 500000 gen/int)))]

   ;; do the actions
   ;; then maybe get all the state idx, groups and extent combinations, and then get a bunch
   ;; and then also check whether the group etc results are the same
   (let [window-config {:window/type :session 
                        :timeout-gap [timeout-gap-secs :second]}
         gap (* 1000 timeout-gap-secs)
         serializers (u/event->window-serializers {:onyx.core/windows [window-config]
                                                   :onyx.core/task-map {:onyx/group-by :X}})
         wext ((we/windowing-builder window-config) window-config)
         mem-store (onyx.state.memory/create-db {} :state-id-1 serializers)
         state-idx 0
         group-id 33]
     (try
      (doseq [event-time event-times]
        (let [ops (we/extent-operations wext 
                                        (s/group-extents mem-store state-idx group-id) 
                                        nil
                                        event-time)]
          ;(println "OPS" ops)
          (doseq [[op & args] ops]
            (case op
              :update (let [[extent] args] 
                        (->> (or (s/get-extent mem-store state-idx group-id extent) 0)
                             (inc)
                             (s/put-extent! mem-store 
                                            state-idx 
                                            group-id 
                                            extent)))

              :merge-extents (let [[extent-1 extent-2 extent-merged] args]
                               (s/put-extent! mem-store
                                              state-idx
                                              group-id
                                              extent-merged
                                              (+ (s/get-extent mem-store state-idx group-id extent-1)
                                                 (s/get-extent mem-store state-idx group-id extent-2)))
                               (s/delete-extent! mem-store state-idx group-id extent-1)
                               (s/delete-extent! mem-store state-idx group-id extent-2))

              :alter-extents (let [[from-extent to-extent] args]
                               (s/put-extent! mem-store
                                              state-idx
                                              group-id
                                              to-extent
                                              (s/get-extent mem-store state-idx group-id from-extent))
                               (s/delete-extent! mem-store state-idx group-id from-extent))))))
      (let [final-extents (s/group-extents mem-store state-idx group-id)]
        (is (empty? (->> (map (fn [[_ upper] [lower _]] (- lower upper)) 
                              final-extents 
                              (rest final-extents))
                         (remove (partial < timeout-gap-secs))))
            final-extents)
        (is (= (count event-times) 
               (reduce + (map (fn [extent] 
                                (s/get-extent mem-store state-idx group-id extent))
                              final-extents)))))
      (finally 
       (s/drop! mem-store)
       (s/close! mem-store))))))
