(ns onyx.static.util)

(defn index-of [coll element]
  (ffirst 
   (filter (fn [[index elem]] (= elem element)) 
           (map list (range) coll))))
