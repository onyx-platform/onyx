(ns onyx.static.helpful-job-errors
  (:require [clojure.string :refer [split join]]
            [onyx.information-model :refer [model]]
            [io.aviso.ansi :as a]))

(def structure-names
  {:catalog-entry "catalog"})

(defn bold-backticks [coll]
  (let [{:keys [result raw]}
        (reduce
         (fn [{:keys [result raw]} match]
           (let [i (.indexOf raw match)]
             (let [altered (a/bold (apply str (butlast (rest match))))]
               {:result (conj result (apply str (take i raw)) altered)
                :raw (apply str (drop (+ i (count match)) raw))})))
         {:result [] :raw coll}
         (re-seq #"`.*?`" coll))]
    (str (apply str result) raw)))

(defn show-header [structure-type faulty-key]
  (println "------ Onyx Schema Error -----")
  (println "There was a validation error in your"
           (a/bold (get structure-names structure-type))
           "for key" (a/bold faulty-key))
  (println))

(defn show-footer []
  (println "------")
  (println))

(defn show-structure [context faulty-key error-f]
  (println "{")
  (doseq [[k v] context]
    (if (= k faulty-key)
      (error-f k v)
      (println "  " k v)))
  (println "}")
  (println))

(defn line-wrap-str [xs]
  (let [max-len 80]
    (->> (split xs #"\s+")
         (reduce
          (fn [result word]
            (let [current-len
                  (+ (apply + (map count (last result)))
                     (count (last result))
                     (count word))]
              (if (> current-len max-len)
                (conj result [word])
                (let [pos (if (seq result) (dec (count result)) 0)]
                  (update-in result [pos] (fn [x] (vec (conj x word))))))))
          [])
         (map (partial join " "))
         (join "\n"))))

(defn show-docs [entry faulty-key]
  (println "-- Docs for key" (a/bold faulty-key) "--")
  (println)
  (println (bold-backticks (line-wrap-str (:doc entry))))
  (println)
  (println "Expected type:" (a/bold (:type entry)))
  (println "Choices:" (a/bold (:choices entry)))
  (println "Added in Onyx version:" (a/bold (:added entry))))

(defn print-helpful-invalid-choice-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f
        (fn [k v]
          (prn "   " (a/bold-red (str k " " v)))
          (println (str "    " (a/magenta (str " ^-- " v " is not a valid choice for " k))))
          (println (str "    " (a/magenta (str "     Must be one of " (:choices entry))))))]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defn print-helpful-invalid-type-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str k " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " v " isn't of the expected type."))))
          (println (str "    " (a/magenta (str "     Found " (.getName (.getClass v)) ", requires " (:type entry))))))]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defn print-helpful-invalid-key-error
  [context faulty-key structure-type suggestion]
  (let [error-f
        (fn [k v]
          (do (prn "   " (a/bold-red (str k " " v)))
              (println (str "    " (a/magenta (str " ^-- " k " isn't a valid key."))))))]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f))
  (println "Did you mean:" (a/bold-green suggestion))
  (show-footer))

(defn print-helpful-missing-key-error
  [context faulty-key structure-type]
  (let [error-f (fn [k v])]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f))
  (println "It was missing. Add the key:" (a/bold-green faulty-key))
  (show-footer))
