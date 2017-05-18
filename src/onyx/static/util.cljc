(ns onyx.static.util)

(defn index-of [coll element]
  (ffirst 
   (filter (fn [[index elem]] (= elem element)) 
           (map list (range) coll))))

#?(:cljs
   (defn munge-fn-name [kw]
     (str (munge-str (str (namespace kw)))
          "."
          (munge-str (str (name kw))))))

#?(:cljs
   (defn resolve-dynamic [kw]
           (js/eval (munge-fn-name kw))))

(defn kw->fn [kw]
  #?(:clj
     (try
       (let [user-ns (symbol (namespace kw))
             user-fn (symbol (name kw))]
         (or (ns-resolve user-ns user-fn)
             (throw (Exception.))))
       (catch Throwable e
         (throw (ex-info (str "Could not resolve symbol on the classpath, did you require the file that contains the symbol " kw "?") {:kw kw})))))
  #?(:cljs (resolve-dynamic kw)))

(defn exception? [e]
  #?(:clj (instance? java.lang.Throwable e))
  #?(:cljs (instance? js/Error e)))

(defn now []
  #?(:clj (System/currentTimeMillis))
  #?(:cljs (.now js/Date)))

(defn ms->ns [ms]
  (* 1000000 ms))

(defn ns->ms [ms]
  (double (/ ms 1000000)))

#?(:clj
   (defn deserializable-exception [^Throwable throwable more-context]
     (let [{:keys [data trace]} (Throwable->map throwable)
           this-ex-type (keyword (.getName (.getClass throwable)))
           data (-> data
                    (assoc :original-exception (:original-exception data this-ex-type))
                    (merge more-context))]
       ;; First element may either be a StackTraceElement or a vector
       ;; of 4 elements, those of which construct a STE.
       (if (sequential? (first trace))
         (let [ste (map #(StackTraceElement.
                          (str (nth % 0))
                          (str (nth % 1))
                          (nth % 2)
                          (nth % 3))
                        trace)]
           (doto ^Throwable (ex-info (.getMessage throwable) data)
             (.setStackTrace (into-array StackTraceElement ste))))
         (doto ^Throwable (ex-info (.getMessage throwable) data)
           (.setStackTrace (into-array StackTraceElement trace)))))))
