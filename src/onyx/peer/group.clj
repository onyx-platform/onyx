(ns ^:no-doc onyx.peer.group
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.transform :as transform]
            [taoensso.timbre :refer [info]]
            [dire.core :refer [with-post-hook!]])
  (:import [java.security MessageDigest]))

(def md5 (MessageDigest/getInstance "MD5"))

(defn hash-segment [segment]
  (apply str (.digest md5 (.getBytes (pr-str segment) "UTF-8"))))

(defn apply-fn-shim [event]
  (let [groups (:results (transform/apply-fn-shim event))]
    {:results
     (map (fn [segment group]
            (with-meta segment {:group (hash-segment group)}))
          (:decompressed event) groups)}))

(defmethod p-ext/apply-fn [:grouper nil]
  [event] (apply-fn-shim event))

(with-post-hook! #'apply-fn-shim
  (fn [{:keys [results]}]
    (info "[Grouper] Applied grouping fn to" (count results) "segments")))

