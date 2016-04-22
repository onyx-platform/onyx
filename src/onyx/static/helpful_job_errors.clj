(ns onyx.static.helpful-job-errors
  (:require [clojure.string :refer [split join]]
            [onyx.information-model :refer [model]]
            [clj-fuzzy.metrics :refer [levenshtein]]
            [io.aviso.ansi :as a]))

(def structure-names
  {:workflow :workflow
   :catalog :catalog-entry
   :lifecycles :lifecycle-entry})

(defn matches-faulty-key? [k v elements faulty-key]
  (some #{k v} #{faulty-key}))

(defn maybe-bad-key [faulty-key x display-x]
  (if (= x faulty-key)
    (a/bold-red display-x) display-x))

(defn wrap-str [x]
  (if x (pr-str x) ""))

(defn wrap-vec [k v]
  (if (and k v)
    (format "   [%s %s]" (wrap-str k) (wrap-str v))
    (format "   [%s%s]" (wrap-str k) (wrap-str v))))

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
  (println "------ Onyx Job Error -----")
  (println "There was a validation error in your"
           (a/bold (name structure-type))
           "for key" (a/bold (pr-str faulty-key)))
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
  (doseq [[k v :as elements] context]
    (if (match-f k v elements faulty-key)
      (error-f k v elements)
      (println (wrap-vec k v))))
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
  (let [entry (get-in model [(structure-names structure-type) :model faulty-key])
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
  (let [entry (get-in model [(structure-names structure-type) :model faulty-key])
        error-f (constantly nil)]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (println (a/magenta (str "^-- Missing required key " (a/bold faulty-key))))
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn print-invalid-type-error
  [context {:keys [error-key expected-type] :as d} structure-type]
  (let [entry (get-in model [(structure-names structure-type) :model error-key])
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str v) " isn't of the expected type."))))
          (println (str "    " (a/magenta (str "     Found " (.getName (.getClass v)) ", requires " (.getName expected-type))))))]
    (show-header structure-type error-key)
    (show-map context error-key error-f)
    (show-docs entry error-key)
    (show-footer)))

(defn print-type-error [faulty-key k required]
  (let [padding (error-left-padding faulty-key k)]
    (println "   " (a/magenta (str padding " ^-- " (pr-str faulty-key) " isn't of the expected type.")))
    (println padding (a/magenta (str "     Found " (.getName (.getClass faulty-key)) ", requires " (.getName (.getClass required)))))))

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
  (let [choices (keys (get-in model [(structure-names structure-type) :model]))
        error-f
        (fn [k v]
          (println "   " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str k) " isn't a valid key.")))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (when-let [suggestion (closest-match choices faulty-key)]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)))

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
        (fn [k v elements]
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
        (fn [k v elements]
          (println (format "   [%s %s]"
                           (a/bold-red (pr-str k))
                           (a/bold-red (pr-str v))))
          (println (str "   ^-- " (a/magenta (msg-fn faulty-key)))))
        match-f
        (fn [k v elements faulty-key]
          (= [k v] faulty-key))]
    (show-header :workflow faulty-key)
    (show-vector context faulty-key match-f error-f)
    (show-footer)))

(defmulti print-helpful-error
  (fn [data entry structure-type]
    (:error-type data)))

(defmethod print-helpful-error :invalid-key
  [data entry structure-type]
  (print-invalid-key-error entry (:error-key data) structure-type))

(defmethod print-helpful-error :missing-required-key
  [data entry structure-type]
  (print-missing-required-key-error entry (:error-key data) structure-type))

(defmethod print-helpful-error :value-predicate-error
  [data entry structure-type]
  (print-invalid-type-error entry data structure-type))

(defmethod print-helpful-error :type-error
  [data entry structure-type]
  (print-invalid-type-error entry data structure-type))

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
  (if (get entry (:error-key data))
    (print-invalid-type-error entry data :catalog-entry)
    (print-missing-required-key-error entry (:error-key data) :catalog-entry)))

(defmethod print-helpful-error :condition-failed
  [data entry structure-type]
  (print-helpful-conditional-error data entry structure-type))





(defmulti print-helpful-job-error
  (fn [job error-data entry structure-type]
    [(first (:path error-data)) (:error-type error-data)]))

(defmulti predicate-error-msg
  (fn [entry error-data]
    (:predicate error-data)))

(defn type-error-msg [err-val req-class]
  [(a/magenta (str "^-- " (pr-str err-val) " isn't of the expected type."))
   (a/magenta (str "     Found " (.getName (.getClass err-val)) ", requires " (.getName req-class)))])

(defn restricted-value-error-msg [err-val]
  [(a/magenta (str "^-- Task name " (pr-str err-val) " is reserved by Onyx and cannot be used."))])

(defmethod predicate-error-msg 'task-name?
  [entry {:keys [error-value]}]
  (cond (not (keyword? error-value))
        (type-error-msg error-value clojure.lang.Keyword)

        (some #{error-value} #{:all :none})
        (restricted-value-error-msg error-value)

        :else
        [(str "^-- Task " (pr-str error-value) " is invalid.")]))

(def predicate-phrases
  {'keyword-namespaced? "a namespaced keyword"
   'keyword? "a keyword"
   'integer? "an integer"})

(defn chain-phrases [phrases]
  (case (count phrases)
    1 (first phrases)
    2 (join " or " phrases)
    (apply str (join ", " (butlast phrases)) ", or " (last phrases))))

(defmethod predicate-error-msg 'keyword-namespaced?
  [entry error-data]
  (if (seq (:predicates error-data))
    (let [chain (->> (:predicates error-data)
                     (select-keys predicate-phrases)
                     (vals)
                     (chain-phrases))]
      [(str "^-- Value " (pr-str (get entry (:error-key error-data))) " must be " chain)])
    [(str "^-- Value " (pr-str (get entry (:error-key error-data))) " must be " (get predicate-phrases (:predicate error-data)))]))

(defmethod predicate-error-msg 'edge-two-nodes?
  [entry {:keys [error-value]}]
  [(str "^-- Workflow vector must have exactly two elements.")])

(defmethod predicate-error-msg 'onyx-input-task-type
  [entry error-data]
  (let [choices  (:onyx/type (get-in model [:catalog-entry :model :onyx/type]))
        error-value (:onyx/type entry)]
    (let [base
          [(str " ^-- " error-value " is not a valid choice for :onyx/type")]]
      (if (seq choices)
        (conj base (str "    " (a/magenta (str "     Must be one of " choices))))
        base))))

(def relevant-key
  {'task-name? :onyx/name
   'onyx-input-task-type ':onyx/type
   'onyx-function-task-type ':onyx/type
   'onyx-output-task-type ':onyx/type})

(defmethod print-helpful-job-error [:workflow :value-predicate-error]
  [job error-data entry structure-type]
  (let [faulty-key (:error-value error-data)
        error-f
        (fn [k v elements]
          (as-> elements t
            (reduce
             (fn [result x]
               (str result (maybe-bad-key faulty-key x (pr-str x)) " ")) 
             "   [" t)
            (butlast t)
            (vec t)
            (conj t "]")
            (apply str t)
            (println t))
          (doseq [m (predicate-error-msg entry error-data)]
            (println (str "   " (a/magenta (str (error-left-padding faulty-key k) m))))))]
    (show-header :workflow faulty-key)
    (show-vector (:workflow job) faulty-key matches-faulty-key? error-f)
    (show-footer)))

(defmethod print-helpful-job-error [:workflow :constraint-violated]
  [job error-data entry structure-type]
  (let [faulty-key (get-in job (:path error-data))
        error-f
        (fn [k v elements]
          (println (format "   %s" (a/bold-red (pr-str elements))))
          (doseq [m (predicate-error-msg entry error-data)]
            (println (str "   " (a/magenta m)))))
        match-f
        (fn [k v elements faulty-key]
          (= elements faulty-key))]
    (show-header :workflow faulty-key)
    (show-vector entry faulty-key match-f error-f)
    (show-footer)))

(defmethod print-helpful-job-error [:catalog :value-predicate-error]
  [job error-data entry structure-type]
  (let [faulty-key (last (:path error-data))
        faulty-val (:error-value error-data)
        error-f
        (fn [k v]
          
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (doseq [m (predicate-error-msg entry error-data)]
            (println (str "   " (a/magenta m)))))]
    (show-header :catalog faulty-key)
    (show-map (get-in job (butlast (:path error-data))) faulty-key error-f)
    (show-footer)))

(defmethod print-helpful-job-error [:catalog :invalid-key]
  [job error-data entry structure-type]
  (let [choices (keys (get-in model [(structure-names structure-type) :model]))
        faulty-key (:error-key error-data)
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str k) " isn't a valid key.")))))]
    (show-header :catalog faulty-key)
    (show-map (get-in job (butlast (:path error-data))) faulty-key error-f)
    (when-let [suggestion (closest-match choices faulty-key)]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)))

(defmethod print-helpful-job-error [:catalog :type-error]
  [job error-data entry structure-type]
  (let [faulty-key (:error-key error-data)
        faulty-val (:error-value error-data)
        expected-type (:expected-type error-data)
        found-type (:found-type error-data)
        context (get-in job (butlast (:path error-data)))
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str v) " isn't of the expected type."))))
          (println (str "    " (a/magenta (str "     Found " (.getName (.getClass v)) ", requires " (.getName expected-type))))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defmethod print-helpful-job-error [:catalog :missing-required-key]
  [job error-data catalog structure-type]
  (let [context (get-in job (butlast (:path error-data)))
        faulty-key (:missing-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])]
    (let [error-f (constantly nil)]
      (show-header structure-type faulty-key)
      (show-map context faulty-key error-f)
      (println (a/magenta (str "^-- Missing required key " (a/bold faulty-key))))
      (println)
      (show-docs entry faulty-key)
      (show-footer))))

(defmethod print-helpful-job-error [:catalog :conditional-failed]
  [job error-data catalog structure-type]
  (let [context (get-in job (:path error-data))
        pred (first (:predicates error-data))
        msg (predicate-error-msg context (assoc error-data :predicate pred))
        faulty-key (relevant-key pred)
        entry (get-in model [(structure-names structure-type) :model faulty-key])]
    (cond (and (map? context) (context faulty-key))
          (let [error-f
                (fn [k v]
                  (println "   " (a/bold-red (str (pr-str k) " " (pr-str v))))
                  (doseq [m msg]
                    (println (str "   " (a/magenta m)))))]
            (show-header structure-type faulty-key)
            (show-map context faulty-key error-f)
            (show-docs entry faulty-key)
            (show-footer))

          (map? context)
          (let [error-f (constantly nil)]
            (show-header structure-type faulty-key)
            (show-map context faulty-key error-f)
            (println (a/magenta (str "^-- Missing required key " (a/bold faulty-key))))
            (println)
            (show-docs entry faulty-key)
            (show-footer))

          :else
          (let [faulty-key (:error-key error-data)
                entry (get-in model [(structure-names structure-type) :model faulty-key])
                context (get-in job (butlast (:path error-data)))
                msg (predicate-error-msg context (assoc error-data :predicate pred))
                error-f
                (fn [k v]
                  (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
                  (doseq [m msg]
                    (println (str "   " (a/magenta m)))))]
            (show-header structure-type faulty-key)
            (show-map context faulty-key error-f)
            (show-docs entry faulty-key)
            (show-footer)))))

(defmethod print-helpful-job-error [:lifecycles :type-error]
  [job error-data entry structure-type]
  (let [faulty-key (:error-key error-data)
        faulty-val (:error-value error-data)
        expected-type (:expected-type error-data)
        found-type (:found-type error-data)
        context (get-in job (butlast (:path error-data)))
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str v) " isn't of the expected type."))))
          (println (str "    " (a/magenta (str "     Found " (.getName (.getClass v)) ", requires " (.getName expected-type))))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defmethod print-helpful-job-error [:lifecycles :missing-required-key]
  [job error-data catalog structure-type]
  (let [context (get-in job (butlast (:path error-data)))
        faulty-key (:missing-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])]
    (let [error-f (constantly nil)]
      (show-header structure-type faulty-key)
      (show-map context faulty-key error-f)
      (println (a/magenta (str "^-- Missing required key " (a/bold faulty-key))))
      (println)
      (show-docs entry faulty-key)
      (show-footer))))

(defmethod print-helpful-job-error [:lifecycles :value-predicate-error]
  [job error-data context structure-type]
  (let [faulty-key (last (:path error-data))
        faulty-val (:error-value error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        error-f
        (fn [k v]
          
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (doseq [m (predicate-error-msg context error-data)]
            (println (str "   " (a/magenta m)))))]
    (show-header :lifecycles faulty-key)
    (show-map (get-in job (butlast (:path error-data))) faulty-key error-f)
    (show-docs entry faulty-key)
    (show-footer)))
