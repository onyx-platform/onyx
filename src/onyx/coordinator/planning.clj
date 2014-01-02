(ns onyx.coordinator.planning)

(defn discover-tasks
  ([catalog workflow] (discover-tasks catalog workflow #{} 1))
  ([catalog workflow tasks phase]
     (cond (= workflow {}) tasks
           (keyword? workflow) (conj tasks {:name workflow :phase phase :complete? false})
           :else
           (let [discovered (map (fn [t-name]
                                   :name t-name
                                   :phase phase
                                   :complete? false)
                                 (keys workflow))]
             (recur catalog (apply merge (vals workflow))
                    (conj tasks discovered) (inc phase))))))

