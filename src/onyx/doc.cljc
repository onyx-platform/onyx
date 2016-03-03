(ns onyx.information-model
  (:require [onyx.information-model]))

(defn lookup-categories [model kw] 
  (->> (keys model)
       (map (fn [k] 
              [(set (keys (get-in model [k :model]))) k]))
       (filter (fn [[s k]]
                 (s kw)))    
       (map second)))

(defn lookup-doc [kw]
  (mapv (fn [cat] (get-in model [cat :model kw])) 
        (lookup-categories model kw)))

