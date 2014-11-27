(ns onyx.api
  (:require [clojure.string :refer [split]]
            [clojure.core.async :refer [chan alts!! >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [com.stuartsierra.dependency :as dep]
            [clj-http.client :refer [post]]
            [taoensso.timbre :refer [warn]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.validation :as validator]))

(defn topological-sort [workflow]
  (dep/topo-sort
   (reduce (fn [all [to from]]
             (dep/depend all from to))
           (dep/graph)
           workflow)))

(defn submit-job [log job]
  (let [id (java.util.UUID/randomUUID)
        tasks (topological-sort (:workflow job))
        scheduler (:task-scheduler job)
        args {:id id :tasks tasks :task-scheduler scheduler}
        entry (create-log-entry :submit-job args)]
    (extensions/write-chunk log :catalog (:catalog job) id)
    (extensions/write-chunk log :workflow (:workflow job) id)
    (extensions/write-log-entry log entry)
    id))

(defn await-job-completion* [sync job-id]
  ;; TODO: re-implement me
  )

(defn unpack-workflow
  ([workflow] (vec (unpack-workflow workflow [])))
  ([workflow result]
     (let [roots (keys workflow)]
       (if roots
         (concat result
                 (mapcat
                  (fn [k]
                    (let [child (get workflow k)]
                      (if (map? child)
                        (concat (map (fn [x] [k x]) (keys child))
                                (unpack-workflow child result))
                        [[k child]])))
                  roots))
         result))))

(defn start-peers!
  "Launches n virtual peers. Each peer may be stopped
   by invoking the fn returned by :shutdown-fn."
  [onyx-id n config opts]
  (doall
   (map
    (fn [_]
      (let [stop-ch (chan (clojure.core.async/sliding-buffer 1))
            v-peer (system/onyx-peer onyx-id config opts)]
        {:runner (future
                   (let [live (component/start v-peer)]
                     (let [ack-ch (<!! stop-ch)]
                       (component/stop live)
                       (>!! ack-ch true)
                       (close! ack-ch))))
         :shutdown-fn (fn []
                        (let [ack-ch (chan)]
                          (>!! stop-ch ack-ch)
                          (<!! ack-ch)))}))
    (range n))))

