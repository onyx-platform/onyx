(ns ^:no-doc onyx.lifecycles.lifecycle-compile
  (:require [onyx.static.validation :as validation]
            [onyx.peer.operation :refer [kw->fn]]
            [taoensso.timbre :refer [error]]))

(defn resolve-lifecycle-calls [calls]
  (let [calls-map (var-get (kw->fn calls))]
    (try
      (validation/validate-lifecycle-calls calls-map)
      (catch Throwable t
        (let [e (ex-info (str "Error validating lifecycle map. " (.getCause t)) calls-map )]
          (error e)
          (throw e))))
    calls-map))

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

(defn compile-start-task-functions [lifecycles task-name]
  (let [matched (select-applicable-lifecycles lifecycles task-name)
        fs (resolve-lifecycle-functions matched
                                        :lifecycle/start-task?
                                        (fn [lifecycle f]
                                          (fn [event]
                                            (f event lifecycle))))]
    (fn [event]
      (if (seq fs)
        (every? true? ((apply juxt fs) event))
        true))))

(defn compile-lifecycle-handle-exception-functions [lifecycles task-name]
  (let [matched (select-applicable-lifecycles lifecycles task-name)
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

(defn compile-lifecycle-functions [lifecycles task-name kw]
  (let [matched (select-applicable-lifecycles lifecycles task-name)]
    (reduce
     (fn [f lifecycle]
       (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
         (if-let [g (get calls-map kw)]
           (comp (fn [x] (merge x (g x lifecycle))) f)
           f)))
     identity
     matched)))

(defn compile-ack-retry-lifecycle-functions [lifecycles task-name kw]
  (let [matched (select-applicable-lifecycles lifecycles task-name)
        fns (keep (fn [lifecycle]
                    (let [calls-map (resolve-lifecycle-calls (:lifecycle/calls lifecycle))]
                      (if-let [g (get calls-map kw)]
                        (vector lifecycle g))))
                  matched)]
    (reduce (fn [g [lifecycle f]]
              (fn [event message-id rets]
                (g event message-id rets)
                (f event message-id rets lifecycle)))
            (fn [event message-id rets])
            fns)))

(defn compile-before-task-start-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-task-start))

(defn compile-before-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/before-batch))

(defn compile-after-read-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-read-batch))

(defn compile-after-batch-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-batch))

(defn compile-after-task-functions [lifecycles task-name]
  (compile-lifecycle-functions lifecycles task-name :lifecycle/after-task-stop))

(defn compile-after-retry-segment-functions [lifecycles task-name]
  (compile-ack-retry-lifecycle-functions lifecycles task-name :lifecycle/after-retry-segment))

(defn compile-handle-exception-functions [lifecycles task-name]
  (compile-lifecycle-handle-exception-functions lifecycles task-name))
