(ns ^:no-doc onyx.lifecycles.lifecycle-compile
  (:require #?(:clj [onyx.static.validation :as validation])
            [taoensso.timbre :refer [error]]
            [onyx.static.util :refer [kw->fn]]))

(defn resolve-lifecycle-calls [calls]
  #?(:clj
     (let [calls-map (var-get (kw->fn calls))]
       (try
         (validation/validate-lifecycle-calls calls-map)
         (catch Throwable t
           (let [e (ex-info (str "Error validating lifecycle map. " (.getCause t)) calls-map)]
             (error e)
             (throw e))))
       calls-map))
  #?(:cljs (onyx.static.util/resolve-dynamic calls)))

(defn select-applicable-lifecycles [lifecycles task-name]
  (filter #(or (= (:lifecycle/task %) :all)
               (= (:lifecycle/task %) task-name)) lifecycles))

(defn resolve-lifecycle-functions [lifecycles phase invoker]
  (remove
   nil?
   (map
    (fn [lifecycle]
      (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
        (when-let [g (get calls-map phase)]
          (invoker lifecycle g))))
    lifecycles)))

(defn compile-start-task-functions [{:keys [onyx.core/lifecycles onyx.core/task]}]
  (let [matched (select-applicable-lifecycles lifecycles task)
        fs (resolve-lifecycle-functions matched
                                        :lifecycle/start-task?
                                        (fn [lifecycle f]
                                          (fn [event]
                                            (f event lifecycle))))]
    (fn [event]
      (if (seq fs)
        (every? true? ((apply juxt fs) event))
        true))))

(defn compile-lifecycle-handle-exception-functions [{:keys [onyx.core/lifecycles onyx.core/task]}]
  (let [matched (select-applicable-lifecycles lifecycles task)
        fs (resolve-lifecycle-functions matched
                                        :lifecycle/handle-exception
                                        (fn [lifecycle f]
                                          (fn [event phase e]
                                            (f event lifecycle phase e))))]
    (fn [event phase e]
      (if (seq fs)
        (let [results ((apply juxt fs) event phase e)]
          (or (first (filter (partial not= :defer) results)) :kill))
        :kill))))

(defn compile-lifecycle-functions [{:keys [onyx.core/lifecycles onyx.core/task]} kw]
  (let [matched (select-applicable-lifecycles lifecycles task)]
    (if-not (empty? matched)
      (reduce
       (fn [f lifecycle]
         (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
           (if-let [g (get calls-map kw)]
             (comp (fn [x] (merge x (g x lifecycle))) f)
             f)))
       identity
       matched))))
