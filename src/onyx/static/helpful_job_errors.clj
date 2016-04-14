(ns onyx.static.helpful-job-errors
  (:require [onyx.information-model :refer [model]]
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

(defn show-structure [context faulty-key error-f]
  (println "{")
  (doseq [[k v] context]
    (if (= k faulty-key)
      (error-f k v)
      (println "  " k v)))
  (println "}")
  (println))

(defn show-docs [entry faulty-key]
  (println "-- Docs for key" (a/bold faulty-key) "--")
  (println)
  (println (bold-backticks (:doc entry)))
  (println)
  (println "Expected type:" (a/bold (:type entry)))
  (println "Choices:" (a/bold (:choices entry)))
  (println "Added in Onyx version:" (a/bold (:added entry))))

(defn print-helpful-invalid-choice-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f
        (fn [k v]
          (println (str "   " (a/bold-red (str k " " v))))
          (println (str "    " (a/magenta (str " ^-- " v " is not a valid choice for " k))))
          (println (str "    " (a/magenta (str "     Must be one of " (:choices entry))))))]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f)
    (show-docs entry faulty-key)))

(defn print-helpful-invalid-type-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f
        (fn [k v]
          (println (str "   " (a/bold-red (str k " " v))))
          (println (str "    " (a/magenta (str " ^-- " v " isn't of the expected type."))))
          (println (str "    " (a/magenta (str "     Found " (.getName (.getClass v)) ", requires " (:type entry))))))]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f)
    (show-docs entry faulty-key)))

(defn print-helpful-invalid-key-error
  [context faulty-key structure-type suggestion]
  (let [error-f
        (fn [k v]
          (do (println (str "   " (a/bold-red (str k " " v))))
              (println (str "    " (a/magenta (str " ^-- " k " isn't a valid key."))))))]
    (show-header structure-type faulty-key)
    (show-structure context faulty-key error-f))
  (println "Did you mean:" (a/bold-green suggestion)))
