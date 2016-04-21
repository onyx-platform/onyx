(ns onyx.static.helpful-job-errors
  (:require [clojure.string :refer [split join]]
            [onyx.information-model :refer [model]]
            [clj-fuzzy.metrics :refer [levenshtein]]
            [io.aviso.ansi :as a]))

(def structure-names
  {:workflow "workflow"
   :catalog-entry "catalog"
   :lifecycle-entry "lifecycles"})

(defn matches-faulty-key? [k v faulty-key]
  (some #{k v} #{faulty-key}))

(defn maybe-bad-key [faulty-key x display-x]
  (if (= x faulty-key)
    (a/bold-red display-x) display-x))

(defn error-left-padding [faulty-key k]
  (if (= faulty-key k)
    " "
    (apply str (repeat (+ (count (str k)) 2) " "))))

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

(defn closest-match [choices faulty-key]
  (let [faulty-str (name faulty-key)
        distances
        (map
         (fn [k]
           [k (levenshtein faulty-str (name k))])
         choices)]
    (when (seq distances)
      (let [candidate (apply min-key second distances)]
        ;; Don't guess wildly. Make sure it's at least
        ;; a guess within reason.
        (when (<= (second candidate) 5)
          (first candidate))))))

(defn show-header [structure-type faulty-key]
  (println "------ Onyx Schema Error -----")
  (println "There was a validation error in your"
           (a/bold (get structure-names structure-type))
           "for key" (a/bold faulty-key))
  (println))

(defn show-footer []
  (println "------")
  (println))

(defn show-map [context faulty-key error-f]
  (println "{")
  (doseq [[k v] context]
    (if (= k faulty-key)
      (error-f k v)
      (println "  " (pr-str k) (pr-str v))))
  (println "}")
  (println))

(defn show-vector [context faulty-key match-f error-f]
  (println "[")
  (doseq [[k v] context]
    (if (match-f k v faulty-key)
      (error-f k v)
      (println (format "   [%s %s]" (pr-str k) (pr-str v)))))
  (println "]")
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
  (when (:choices entry)
    (println "Choices:" (a/bold (:choices entry))))
  (println "Added in Onyx version:" (a/bold (:added entry))))

(defn print-invalid-choice-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f
        (fn [k v]
          (println "   " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " v " is not a valid choice for " k))))
          (when (:choices entry)
            (println (str "    " (a/magenta (str "     Must be one of " (:choices entry)))))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defn print-missing-required-key-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f (constantly nil)]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (println (a/magenta (str "^-- Missing required key " (a/bold faulty-key))))
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn print-invalid-type-error
  [context faulty-key structure-type]
  (let [entry (get-in model [structure-type :model faulty-key])
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str v) " isn't of the expected type."))))
          (println (str "    " (a/magenta (str "     Found " (.getName (.getClass v)) ", requires " (:type entry))))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defn print-type-error [faulty-key k required]
  (let [padding (error-left-padding faulty-key k)]
    (println "   " (a/magenta (str padding " ^-- " (pr-str faulty-key) " isn't of the expected type.")))
    (println padding (a/magenta (str "     Found " (.getName (.getClass faulty-key)) ", requires " required)))))

(defn print-invalid-task-name [faulty-key k]
  (let [padding (error-left-padding faulty-key k)]
    (println  "   " (a/magenta (str padding " ^-- " (pr-str faulty-key) " is not a valid task name.")))))

(defn print-invalid-workflow-task-name
  [context faulty-key structure-type]
  (let [error-f
        (fn [k v]
          (println (format "   [%s %s]"
                           (maybe-bad-key faulty-key k (pr-str k))
                           (maybe-bad-key faulty-key v (pr-str v))))
          (if (some #{faulty-key} #{:all :none})
            (print-invalid-task-name faulty-key k)
            (print-type-error faulty-key k "clojure.lang.Keyword")))]
    (show-header :workflow faulty-key)
    (show-vector context faulty-key matches-faulty-key? error-f)))

(defn print-invalid-key-error
  [context faulty-key structure-type]
  (let [choices (keys (get-in model [structure-type :model]))
        error-f
        (fn [k v]
          (println "   " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str k) " isn't a valid key.")))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f))
  (when-let [suggestion (closest-match structure-type faulty-key)]
    (println "Did you mean:" (a/bold-green suggestion)))
  (show-footer))

(defn print-invalid-task-name-error
  [context faulty-key faulty-value structure-type tasks]
  (let [error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "   " (a/magenta (str " ^-- " v " isn't a valid task name.")))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (when-let [suggestion (closest-match tasks faulty-value)]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)))

(defn print-workflow-element-error
  [context faulty-key msg-fn]
  (let [error-f
        (fn [k v]
          (println (format "   [%s %s]"
                           (maybe-bad-key faulty-key k k)
                           (maybe-bad-key faulty-key v (pr-str v))))
          (println (str "   "
                        (a/magenta
                         (str (error-left-padding faulty-key k)
                              (str "^-- " (msg-fn faulty-key)))))))]
    (show-header :workflow faulty-key)
    (show-vector context faulty-key matches-faulty-key? error-f)
    (show-footer)))

(defn print-workflow-edge-error
  [context faulty-key msg-fn]
  (let [error-f
        (fn [k v]
          (println (format "   [%s %s]"
                           (a/bold-red (pr-str k))
                           (a/bold-red (pr-str v))))
          (println (str "   ^-- " (a/magenta (msg-fn faulty-key)))))
        match-f
        (fn [k v faulty-key]
          (= [k v] faulty-key))]
    (show-header :workflow faulty-key)
    (show-vector context faulty-key match-f error-f)
    (show-footer)))

(defmulti print-helpful-error
  (fn [data entry structure-type]
    (:type (:error data))))

(defmethod print-helpful-error :invalid-key
  [data entry structure-type]
  (print-invalid-key-error entry (:error-value data) structure-type))

(defmethod print-helpful-error :missing-required-key
  [data entry structure-type]
  (print-missing-required-key-error entry (:key data) structure-type))

(defmethod print-helpful-error :value-predicate-error
  [data entry structure-type]
  (print-invalid-type-error entry (:key data) structure-type))

(defmethod print-helpful-error :value-type-error
  [data entry structure-type]
  (print-invalid-type-error entry (:key data) structure-type))

(defmulti print-helpful-conditional-error
  (fn [data entry structure-type]
    (:conditional (:error data))))

(defmethod print-helpful-conditional-error :onyx-type-conditional
  [data entry structure-type]
  (if (:onyx/type entry)
    (print-invalid-choice-error entry :onyx/type :catalog-entry)
    (print-missing-required-key-error entry :onyx/type :catalog-entry)))

(defmethod print-helpful-conditional-error :matches-some-precondition?
  [data entry structure-type]
  (if (get entry (:key data))
    (print-invalid-type-error entry (:key data) :catalog-entry)
    (print-missing-required-key-error entry (:key data) :catalog-entry)))

(defmethod print-helpful-error :condition-failed
  [data entry structure-type]
  (print-helpful-conditional-error data entry structure-type))
