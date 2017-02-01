(ns onyx.seq.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.seq/input
    {:summary "An input task to read messages from a sequence."
     :model {:seq/checkpoint?
             {:doc "When true, Onyx will record progress about how much of the sequence has been processed. The seq that is read from must be reproducible on restart."
              :type :boolean}}}}

   :lifecycle-entry
   {:onyx.plugin.seq/input
    {:model
     [{:task.lifecycle/name :read-seq
       :lifecycle/calls :onyx.plugin.seq/reader-calls}]}}

   :display-order
   {:onyx.plugin.seq/input
    [:seq/checkpoint?]}})
