(ns onyx.static.helpful-job-errors
  (:require [clojure.string :refer [split join]]
            [onyx.information-model :refer [model]]
            [clj-fuzzy.metrics :refer [levenshtein]]
            [io.aviso.ansi :as a]))

(def manual-ex (ex-info "Manual validation check failed" {:manual? true}))

(def structure-names
  {:workflow :workflow
   :catalog :catalog-entry
   :lifecycles :lifecycle-entry
   :flow-conditions :flow-conditions-entry
   :windows :window-entry
   :triggers :trigger-entry})

(def vec-of-maps-depth
  {1 1
   2 2
   3 2
   4 3})

(def contextual-depth
  {:workflow
   {1 1
    2 1
    3 1}
   :catalog vec-of-maps-depth
   :lifecycles vec-of-maps-depth
   :flow-conditions vec-of-maps-depth
   :windows vec-of-maps-depth
   :triggers vec-of-maps-depth})

(def semantic-error-msgs
  {:min-peers-gt-max-peers
   [":onyx/min-peers must be less than or equal to :onyx/max-peers."]

   :n-peers-with-min-or-max
   [":onyx/n-peers cannot be used with :onyx/min-peers or :onyx/max-peers."]

   :valid-flux-policy-min-max-n-peers
   ["When using a flux policy, a valid :onyx/min-peers and :onyx/max-peers, or :onyx/n-peers must be set."]

   :range-and-slide-incompatible
   ["Units specified for :window/range and :window/slide are incompatible with each other."]

   :sliding-window-needs-range-and-slide
   ["Sliding windows must define both :window/range and :window/slide."]

   :fixed-windows-dont-define-slide
   ["Fixed windows cannot define a :window/slide value."]

   :global-windows-dont-define-range-or-slide
   ["Global windows cannot define a :window/range or :window/slide value."]

   :session-windows-dont-define-range-or-slide
   ["Session windows cannot define a :window/range or :window/slide value."]

   :session-windows-define-a-timeout
   ["Session windows must define a :window/timeout-gap value."]

   :window-key-required
   ["This window type requires a :window/window-key to be defined."]

   :auto-short-circuit
   [":flow/to :all and :none require :flow/short-circuit? to be true."]})

(def relevant-key
  {'task-name? :onyx/name
   'onyx-type-conditional :onyx/type
   'range-defined-for-fixed-and-sliding? :window/type})

(defn pad [n]
  (apply str (repeat n " ")))

(defn match-map-or-val [error-data]
  (fn
    ([k v] (= k (:error-key error-data)))
    ([x] (= x (:error-value error-data)))))

(defn print-multi-line-error [[msg & more :as all] left]
  (when (seq all)
    (println (a/magenta (str (pad (+ left)) "^-- " msg)))
    (doseq [m more]
      (println (a/magenta (str (pad (+ left)) "    " m))))))

(defn display-err-map-or-val
  ([msgs] (display-err-map-or-val msgs 0))
  ([all extra]
   (fn
     ([left k v]
      (println (str (pad left) (a/bold-red (str (pr-str k) " " (pr-str v)))))
      (print-multi-line-error all left))
     ([left x]
      (println (str (pad left) (a/bold-red (str (pr-str x)))))
      (print-multi-line-error all left)))))

(defn matches-faulty-key? [k v elements faulty-key]
  (some #{k v} #{faulty-key}))

(defn matches-map-key? [k v faulty-key]
  (= k faulty-key))

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
           (let [i (.indexOf ^String raw ^String match)]
             (let [altered (a/bold (apply str (butlast (rest match))))]
               {:result (conj result (apply str (take i raw)) altered)
                :raw (apply str (drop (+ i (count match)) raw))})))
         {:result [] :raw coll}
         (re-seq #"`.*?`" coll))]
    (str (apply str result) raw)))

(defn closest-match [choices faulty-key]
  (when (keyword? faulty-key)
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
            (first candidate)))))))

(defn show-header [structure-type faulty-key]
  (println "------ Onyx Job Error -----")
  (println "There was a validation error in your"
           (a/bold (name structure-type))
           "for key" (a/bold (pr-str faulty-key)))
  (println))

(defn show-footer []
  (println "------"))

(defmulti show-value*
  (fn [value left depth target-depth match-f error-f]
    (type value)))

(defmethod show-value* clojure.lang.PersistentArrayMap
  [value left depth target-depth match-f error-f]
  (if (and (= depth target-depth) (match-f value))
    (error-f left value)
    (do
      (println (str (pad left) "{"))
      (doseq [[k v] value]
        (if (and (= (inc depth) target-depth) (match-f k v))
          (error-f (+ left 4) k v)
          (println (str (pad (+ left 4)) (pr-str k) " " (pr-str v)))))
      (println (str (pad left) "}")))))

(defmethod show-value* clojure.lang.PersistentVector
  [value left depth target-depth match-f error-f]
  (if (and (= depth target-depth) (match-f value))
    (error-f left value)
    (do
      (println "[")
      (doseq [x value]
        (if (vector? x)
          (if (and (= (inc depth) target-depth) (match-f x))
            (error-f (+ left 4) x)
            (println (str (pad (+ left 4)) (pr-str x))))
          (show-value* x (+ left 4) (inc depth) target-depth match-f error-f)))
      (println "]"))))

(defmethod show-value* :default
  [value left depth target-depth match-f error-f]
  (if (and (= depth target-depth) (match-f value))
    (error-f left value)
    (println (str (pad left) (pr-str value)))))

(defn show-value [value target-depth match-f error-f]
  (show-value* value 0 0 target-depth match-f error-f))

(defn show-map [context faulty-key match-f error-f]
  (println "{")
  (doseq [[k v] context]
    (if (match-f k v faulty-key)
      (error-f k v)
      (println "  " (pr-str k) (pr-str v))))
  (println "}")
  (println))

(defn show-vector [context faulty-key match-f error-f]
  (if (coll? context)
    (do
      (println "[")
      (doseq [x context]
        (if (coll? x)
          (let [[k v :as elements] x]
            (if (match-f k v elements faulty-key)
              (error-f k v elements)
              (println (wrap-vec k v))))
          (error-f x nil x)))
      (println "]")
      (println))
    (error-f context nil context)))

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
  (when entry
    (println "-- Docs for key" (a/bold faulty-key) "--")
    (when-let [deprecated-docs (:deprecation-doc entry)]
      (println)
      (println (a/blue (bold-backticks (line-wrap-str deprecated-docs))))
      (println)
      (println "--"))
    (println)
    (println (bold-backticks (line-wrap-str (:doc entry))))
    (println)
    (println "Expected type:" (a/bold (:type entry)))
    (when (:choices entry)
      (println "Choices:" (a/bold (:choices entry))))
    (println "Added in Onyx version:" (a/bold (:added entry)))
    (when-let [ver (:deprecated-version entry)]
      (println "Deprecated in Onyx version:" (a/bold ver)))))

(defn print-type-error [faulty-key k required]
  (let [padding (error-left-padding faulty-key k)]
    (println "   " (a/magenta (str padding " ^-- " (pr-str faulty-key) " isn't of the expected type.")))
    (println padding (a/magenta (str "     Found " (.getName (.getClass ^Object faulty-key)) ", requires " (.getName (.getClass ^Object required)))))))

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
    (show-vector context faulty-key matches-faulty-key? error-f)
    (throw manual-ex)))

(defn print-invalid-key-error
  [context faulty-key structure-type]
  (let [choices (keys (get-in model [(structure-names structure-type) :model]))
        error-f
        (fn [k v]
          (println "   " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str k) " isn't a valid key.")))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key matches-map-key? error-f)
    (when-let [suggestion (closest-match choices faulty-key)]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)
    (throw manual-ex)))

(defn print-invalid-task-name-error
  [context faulty-key faulty-value structure-type tasks]
  (let [error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "   " (a/magenta (str " ^-- " v " isn't a valid task name.")))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key matches-map-key? error-f)
    (when-let [suggestion (closest-match tasks faulty-value)]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)
    (throw manual-ex)))

(defn print-invalid-flow-to-type
  [context faulty-key faulty-value structure-type tasks]
  (let [error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "   " (a/magenta (str " ^-- " v " isn't a valid value.")))))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key matches-map-key? error-f)
    (when (keyword? faulty-value) 
      (println "Did you mean:" (a/bold-green [faulty-value])))
    (show-footer)
    (throw manual-ex)))

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
    (show-footer)
    (throw manual-ex)))

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
    (show-footer)
    (throw manual-ex)))

(defmulti print-helpful-job-error
  (fn [job error-data entry structure-type]
    [(first (:path error-data)) (:error-type error-data)]))

(defmulti predicate-error-msg
  (fn [entry error-data]
    (:predicate error-data)))

(defn type-error-msg [err-val ^Class found-class ^Class req-class]
  [(str "of type " (.getName req-class) " (found " (.getName found-class) ")")])

(defn restricted-value-error-msg [err-val]
  [(str "renamed. " (pr-str err-val) " is reserved by Onyx and cannot be used.")])

(defn chain-phrases [phrases]
  (case (count phrases)
    1 (first phrases)
    2 (join " or " phrases)
    (apply str (join ", " (butlast phrases)) ", or " (last phrases))))

(defmethod predicate-error-msg 'task-name?
  [entry {:keys [error-value] :as data}]
  (cond (not (keyword? error-value))
        (type-error-msg error-value (.getClass ^Object error-value) clojure.lang.Keyword)

        (some #{error-value} #{:all :none})
        (restricted-value-error-msg error-value)

        :else
        [(str "Task " (pr-str error-value) " is invalid.")]))

(defmethod predicate-error-msg 'keyword?
  [entry error-data] ["a keyword"])

(defmethod predicate-error-msg 'keyword-namespaced?
  [entry error-data] ["a namespaced keyword"])

(defmethod predicate-error-msg 'integer?
  [entry error-data] ["an integer"])

(defmethod predicate-error-msg 'anything?
  [entry error-data] ["any value"])

(defmethod predicate-error-msg 'edge-two-nodes?
  [entry {:keys [error-value]}]
  ["Workflow must be a non-empty vector and only contain vectors of exactly two elements."])

(defn invalid-onyx-type [entry]
  (let [choices  (:onyx/type (get-in model [:catalog-entry :model :onyx/type]))
        error-value (:onyx/type entry)]
    (if-not (map? entry)
      ["a map."]
      (let [base
            [(str error-value " is not a valid choice for :onyx/type")]]
        (if (seq choices)
          (conj base (str "    " (a/magenta (str "     Must be one of " choices))))
          base)))))

(defmethod predicate-error-msg 'onyx-type-conditional
  [entry error-data] (invalid-onyx-type entry))

(defmethod predicate-error-msg 'pos?
  [entry error-data]
  [(str (last (:path error-data)) " must be positive.")])

(defmethod predicate-error-msg 'deprecated-key?
  [entry error-data]
  [(str (last (:path error-data)) " has been deprecated and removed from the Onyx API.")])

(defmethod predicate-error-msg 'range-defined-for-fixed-and-sliding?
  [entry error-data]
  [(str (semantic-error-msgs :sliding-window-needs-range-and-slide))])

(defmethod predicate-error-msg 'valid-flux-policy-min-max-n-peers
  [entry error-data]
  [(str (semantic-error-msgs :valid-flux-policy-min-max-n-peers))])

(defn missing-required-key* [job context error-data structure-type]
  (let [faulty-key (:missing-key error-data)
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        match-f (constantly nil)
        error-f (constantly nil)]
    (show-header structure-type faulty-key)
    (show-value context (- path-len n-deep) match-f error-f)
    (println)
    (println (a/magenta (str "^-- Missing required key " (a/bold faulty-key))))
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn type-error* [job error-data structure-type]
  (let [faulty-key (:error-key error-data)
        faulty-val (:error-value error-data)
        expected-type (:expected-type error-data)
        found-type (:found-type error-data)
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))        
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        match-f (match-map-or-val error-data)
        msgs [(join " " (into ["Value must be"] (type-error-msg faulty-val found-type expected-type)))]
        error-f (display-err-map-or-val msgs)]
    (show-header structure-type faulty-key)
    (show-value context (- path-len n-deep) match-f error-f)
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn value-predicate-error* [job error-data structure-type]
  (let [faulty-key (last (:path error-data))
        faulty-val (:error-value error-data)
        error-data (assoc error-data :error-key faulty-key)
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        match-f (match-map-or-val error-data)
        error-pre (if (= 'deprecated-key? (:predicate error-data))
                    "Deprecation error,"
                    "Value must be")
        error-f (display-err-map-or-val [(join " " (into [error-pre] (predicate-error-msg context error-data)))])]
    (show-header (first (:path error-data)) faulty-key)
    (show-value context (- path-len n-deep) match-f error-f)
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn invalid-key* [job error-data structure-type]
  (let [choices (keys (get-in model [(structure-names structure-type) :model]))
        faulty-key (:error-key error-data)
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        match-f (fn [k v] (= k faulty-key))
        error-f
        (fn [left k v]
          (println (str (pad left) (a/bold-red (str (pr-str k) " " (pr-str v)))))
          (println (str (pad left) (a/magenta (str " ^-- " (pr-str k) " isn't a valid key.")))))]
    (show-header (first (:path error-data)) faulty-key)
    (show-value context (- path-len n-deep) match-f error-f)
    (println)
    (when-let [suggestion (closest-match choices faulty-key)]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)))

(defn malformed-value-error* [job error-data structure-type msg]
  (let [faulty-key (:error-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " msg)))))]
    (show-header (first (:path error-data)) faulty-key)
    (show-map (:context error-data) faulty-key matches-map-key? error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defn map-conditional-failed*
  [job error-data structure-type faulty-key pred]
  (let [err-key (relevant-key pred)
        error-data (assoc error-data :error-key err-key)
        error-data (assoc error-data :error-value (get-in job (conj (:path error-data) err-key)))
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        msgs (predicate-error-msg context (assoc error-data :predicate pred))
        match-f (match-map-or-val error-data)
        error-f (display-err-map-or-val msgs)]
    (show-header structure-type faulty-key)
    (show-value context (inc (- path-len n-deep)) match-f error-f)
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn chain-predicates
  ([job error-data preds]
   (chain-predicates job error-data preds []))
  ([job error-data preds result]
   (cond (not (seq preds))
         (reduce into #{} result)

         (set? (first preds))
         (chain-predicates
          job error-data (rest preds)
          (conj result [(first preds)]))
         
         (coll? (first preds))
         (chain-predicates
          job error-data (rest preds)
          (conj result (chain-predicates job error-data (first preds) [])))

         :else
         (chain-predicates
          job error-data (rest preds)
          (conj result
                (predicate-error-msg (get-in job (butlast (:path error-data)))
                                     (assoc error-data :predicate (first preds))))))))

(defn value-conditional-failed* [job error-data structure-type pred]
  (let [faulty-key (:error-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        msg (chain-phrases
             (reduce
              (fn [r s]
                (if (coll? s)
                  (conj r (str "a vector with elements that are typed as " (chain-phrases s)))
                  (conj r s)))
              []
              (chain-predicates job error-data (:predicates error-data))))
        match-f (match-map-or-val error-data)
        error-f (display-err-map-or-val [(str "Value must be " msg)])]
    (show-header structure-type faulty-key)
    (show-value context (- path-len n-deep) match-f error-f)
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn conditional-failed* [job error-data structure-type]
  (let [context (get-in job (:path error-data))
        pred (first (:predicates error-data))
        faulty-key (relevant-key pred)]
    (cond (and (map? context) (context faulty-key))
          (map-conditional-failed* job error-data structure-type faulty-key pred)

          (map? context)
          (missing-required-key* job
                                 (get-in job (:path error-data))
                                 (assoc error-data :missing-key faulty-key) structure-type)

          :else
          (value-conditional-failed* job error-data structure-type pred))))

(defn value-choice-error* [job error-data structure-type]
  (let [faulty-key (:error-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        choices (:choices entry)
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (println (str "    " (a/magenta (str " ^-- " (pr-str v) " isn't a valid choice, must be one of " (:choices error-data))))))]
    (show-header (first (:path error-data)) faulty-key)
    (show-map (get-in job (butlast (:path error-data))) faulty-key matches-map-key? error-f)
    (show-docs entry faulty-key)
    (println)
    (when-let [suggestion (closest-match choices (:error-value error-data))]
      (println "Did you mean:" (a/bold-green suggestion)))
    (show-footer)))

(defn duplicate-entry-error* [context error-data structure-type]
  (let [faulty-key (:error-key error-data)
        faulty-val (:error-value error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        match-f
        (fn [k v faulty-key]
          (and (= k faulty-key) (= v faulty-val)))
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v)))))]
    (show-header structure-type faulty-key)
    (doseq [c context]
      (show-map c faulty-key match-f error-f))
    (println (a/magenta (str "^-- Key " (a/bold faulty-key) (a/magenta " must be unique across all entries, duplicates were detected."))))
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn entry-ordering-error* [context error-data structure-type msg]
  (let [faulty-key (:error-key error-data)
        faulty-val (:error-value error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        match-f
        (fn [k v faulty-key]
          (and (= k faulty-key) (= v faulty-val)))
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v)))))]
    (show-header structure-type faulty-key)
    (doseq [c context]
      (show-map c faulty-key match-f error-f))
    (println (a/magenta (str "^-- " msg)))
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn multi-key-semantic-error* [context error-data structure-type]
  (let [faulty-keys (:error-keys error-data)
        faulty-key (:error-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        match-f
        (fn [k v faulty-key]
          (some #{k} faulty-keys))
        error-f
        (fn [k v]
          (println "  " (a/bold-red (str (pr-str k) " " (pr-str v))))
          (when (= k faulty-key)
            (print-multi-line-error (semantic-error-msgs (:semantic-error error-data)) 4)))]
    (show-header structure-type faulty-key)
    (show-map context faulty-key match-f error-f)
    (show-docs entry faulty-key)
    (show-footer)))

(defn mutually-exclusive-error* [job error-data structure-type]
  (let [faulty-keys (:error-keys error-data)
        faulty-key (:error-key error-data)
        entry (get-in model [(structure-names structure-type) :model faulty-key])
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        match-f (fn [k v] (some #{k} (:error-keys error-data)))
        error-f (display-err-map-or-val [])]
    (show-header structure-type faulty-key)
    (show-value context (inc (- path-len n-deep)) match-f error-f)
    (println)
    (println (a/magenta (str "^-- " (join " " (semantic-error-msgs (:semantic-error error-data))))))
    (println)
    (show-docs entry faulty-key)
    (show-footer)))

(defn contextual-missing-key-error*
  [context {:keys [present-key absent-key] :as error-data} structure-type]
  (let [path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        entry (get-in model [(structure-names structure-type) :model absent-key])
        match-f (match-map-or-val (assoc error-data :error-key (:present-key error-data)))
        msgs (semantic-error-msgs (:semantic-error error-data))
        error-f (display-err-map-or-val msgs)]
    (show-header structure-type absent-key)
    (show-value context (- (inc path-len) n-deep) match-f error-f)
    (println)
    (show-docs entry absent-key)
    (show-footer)))

(defmethod print-helpful-job-error [:workflow :value-predicate-error]
  [job error-data entry structure-type]
  (let [bad-link (get-in job (take 2 (:path error-data)))
        error-data (assoc error-data :error-value bad-link)
        faulty-key (:error-value error-data)
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        match-f (match-map-or-val error-data)
        msgs (predicate-error-msg entry (assoc error-data :error-value (get-in job (:path error-data))))
        error-f (display-err-map-or-val [(join " " (into ["Workflow element must be"] msgs))])]
    (show-header :workflow faulty-key)
    (show-value context (dec (- path-len n-deep)) match-f error-f)
    (println)
    (show-footer)))

(defmethod print-helpful-job-error [:workflow :constraint-violated]
  [job error-data entry structure-type]
  (let [faulty-val (get-in job (:path error-data))
        error-data (assoc error-data :error-value faulty-val)
        path-len (count (:path error-data))
        n-deep (get-in contextual-depth [structure-type path-len])
        context (get-in job (take n-deep (:path error-data)))
        match-f (match-map-or-val error-data)
        msgs (predicate-error-msg entry error-data)
        error-f (display-err-map-or-val msgs)]
    (show-header :workflow faulty-val)
    (show-value context (- path-len n-deep) match-f error-f)
    (println)
    (show-footer)))

(defmethod print-helpful-job-error [:workflow :type-error]
  [job error-data context structure-type]
  (type-error* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :value-predicate-error]
  [job error-data context structure-type]
  (value-predicate-error* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :invalid-key]
  [job error-data context structure-type]
  (invalid-key* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :type-error]
  [job error-data context structure-type]
  (type-error* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :missing-required-key]
  [job error-data context structure-type]
  (missing-required-key* job context error-data structure-type))

(defmethod print-helpful-job-error [:catalog :conditional-failed]
  [job error-data context structure-type]
  (conditional-failed* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :value-choice-error]
  [job error-data context structure-type]
  (value-choice-error* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :duplicate-entry-error]
  [job error-data context structure-type]
  (duplicate-entry-error* context error-data structure-type))

(defmethod print-helpful-job-error [:catalog :multi-key-semantic-error]
  [job error-data context structure-type]
  (multi-key-semantic-error* context error-data structure-type))

(defmethod print-helpful-job-error [:catalog :constraint-violated]
  [job error-data context structure-type]
  (value-predicate-error* job error-data structure-type))

(defmethod print-helpful-job-error [:catalog :mutually-exclusive-error]
  [job error-data context structure-type]
  (mutually-exclusive-error* job error-data structure-type))

(defmethod print-helpful-job-error [:lifecycles :type-error]
  [job error-data context structure-type]
  (type-error* job error-data structure-type))

(defmethod print-helpful-job-error [:lifecycles :missing-required-key]
  [job error-data context structure-type]
  (missing-required-key* job context error-data structure-type))

(defmethod print-helpful-job-error [:lifecycles :value-predicate-error]
  [job error-data context structure-type]
  (value-predicate-error* job error-data structure-type))

(defmethod print-helpful-job-error [:lifecycles :invalid-key]
  [job error-data context structure-type]
  (invalid-key* job error-data structure-type))

(defmethod print-helpful-job-error [:flow-conditions :conditional-failed]
  [job error-data context structure-type]
  (conditional-failed* job error-data structure-type))

(defmethod print-helpful-job-error [:flow-conditions :type-error]
  [job error-data context structure-type]
  (type-error* job error-data structure-type))

(defmethod print-helpful-job-error [:flow-conditions :value-choice-error]
  [job error-data context structure-type]
  (value-choice-error* job error-data structure-type))

(defmethod print-helpful-job-error [:flow-conditions :missing-required-key]
  [job error-data context structure-type]
  (missing-required-key* job context error-data structure-type))

(defmethod print-helpful-job-error [:flow-conditions :invalid-key]
  [job error-data context structure-type]
  (invalid-key* job error-data structure-type))

(defmethod print-helpful-job-error [:flow-conditions :multi-key-semantic-error]
  [job error-data context structure-type]
  (multi-key-semantic-error* context error-data structure-type))

(defmethod print-helpful-job-error [:windows :missing-required-key]
  [job error-data context structure-type]
  (missing-required-key* job context error-data structure-type))

(defmethod print-helpful-job-error [:windows :type-error]
  [job error-data context structure-type]
  (type-error* job error-data structure-type))

(defmethod print-helpful-job-error [:windows :invalid-key]
  [job error-data context structure-type]
  (invalid-key* job error-data structure-type))

(defmethod print-helpful-job-error [:windows :value-choice-error]
  [job error-data context structure-type]
  (value-choice-error* job error-data structure-type))

(defmethod print-helpful-job-error [:windows :conditional-failed]
  [job error-data context structure-type]
  (conditional-failed* job error-data structure-type))

(defmethod print-helpful-job-error [:windows :duplicate-entry-error]
  [job error-data context structure-type]
  (duplicate-entry-error* context error-data structure-type))

(defmethod print-helpful-job-error [:windows :multi-key-semantic-error]
  [job error-data context structure-type]
  (multi-key-semantic-error* context error-data structure-type))

(defmethod print-helpful-job-error [:windows :contextual-missing-key-error]
  [job error-data context structure-type]
  (contextual-missing-key-error* context error-data structure-type))

(defmethod print-helpful-job-error [:triggers :missing-required-key]
  [job error-data context structure-type]
  (missing-required-key* job context error-data structure-type))

(defmethod print-helpful-job-error [:triggers :type-error]
  [job error-data context structure-type]
  (type-error* job error-data structure-type))

(defmethod print-helpful-job-error [:triggers :invalid-key]
  [job error-data context structure-type]
  (invalid-key* job error-data structure-type))

(defmethod print-helpful-job-error [:triggers :value-choice-error]
  [job error-data context structure-type]
  (value-choice-error* job error-data structure-type))

(defmethod print-helpful-job-error [:triggers :conditional-failed]
  [job error-data context structure-type]
  (conditional-failed* job error-data structure-type))

(defmethod print-helpful-job-error [:triggers :value-predicate-error]
  [job error-data context structure-type]
  (value-predicate-error* job error-data structure-type))

(defmethod print-helpful-job-error [:triggers :duplicate-entry-error]
  [job error-data context structure-type]
  (duplicate-entry-error* context error-data structure-type))

(defmethod print-helpful-job-error :default
  [_ error-data _ _]
  (let [s "Determining a helpful exception failed."]
    (throw (ex-info s {:helpful-failed? true
                       :e (:e error-data)}))))

(defn print-helpful-job-error-and-throw [job error-data context structure-type]
  (print-helpful-job-error job error-data context structure-type)
  (throw manual-ex))
