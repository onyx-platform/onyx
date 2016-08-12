(ns onyx.generative.manual-shrink)

(defn mutate-bitmap [bitmap pct]
  (map (fn [bit]
         (if (and (< (rand) (/ pct 100)) bit)
           (not bit)
           bit))
       bitmap))

(defn initial-bitmap [cmds]
  (repeat (count cmds) true))

(defn mutate [bitmap pct]
  ;(println "bitmap is " bitmap)
  (update bitmap 
          :phases 
          (fn [phases]
            (map #(mutate-bitmap % pct) phases))))

(defn filter-phase [bitmap phase]
  (vec 
    (keep (fn [[cmd select?]]
            (if select?
              cmd)) 
          (map list phase bitmap))))

(defn filter-generated [generated bitmaps]
  ;(println "Phases " (:phases generated))
  (update generated 
          :phases 
          (fn [phases]
            (map filter-phase (:phases bitmaps) phases))))

(defn phases-count [phases]
  (count (apply concat phases)))

(defn fitness [generated]
  (- (phases-count (:phases generated))))

(defn print-generated-count [{:keys [phases uuid-seed]}]
  (println "cmds cnt " (phases-count phases)))

(defn bitmap-phases-count [phases]
  (count (remove false? (apply concat phases))))

(defn shrink-annealing [test-fn generated iterations]
  (let [initial-bitmap {:phases (map initial-bitmap (:phases generated))}
        initial-count (bitmap-phases-count (:phases initial-bitmap))]
    (loop [bitmap initial-bitmap 
           iteration iterations]
      (if-not (zero? iteration)
        (let [current-count (bitmap-phases-count (:phases bitmap))
              ;; start at 10% and work down to flipping one on average
              pct (max (* 20 (/ iteration iterations))
                       (* 1 (/ initial-count current-count)))
              ;_ (println "pct" pct)
              mutated-bitmap (mutate bitmap pct)
              ;_ (println "Mutated " mutated-bitmap)
              mutate-generated (filter-generated generated mutated-bitmap)] 
          (println "Before mutate count:")
          (println current-count)
          (println "After mutate count:")
          (print-generated-count mutate-generated)
          (println "PCT" (double pct))
          (if (and (> (fitness mutate-generated) (fitness generated))
                   (not (test-fn mutate-generated)))
            (recur mutated-bitmap (dec iteration))
            (recur bitmap (dec iteration))))
        (do
          (println "Final shrunk")
          (println (pr-str (filter-generated generated bitmap))))))))
