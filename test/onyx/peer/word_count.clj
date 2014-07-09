(ns onyx.peer.word-count
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.api]
            [taoensso.timbre :refer [info]]))

(def workflow {:in {:split-sentence {:group-by-word {:count-words :out}}}})

;;; Fn implementations

(defn split-sentence [sentence]
  (filter (partial not= "") (clojure.string/split sentence #"\s")))

(defn group-by-word [word]
  word)

(defn count-words [accretion word]
  (assoc accretion word (inc (get accretion word 0))))

;;; Fn interfaces

(defn split-sentence-interface [{:keys [sentence]}]
  (map (fn [x] {:word x}) (split-sentence sentence)))

(defn group-by-word-interface [{:keys [word] :as segment}]
  (group-by-word word))

(defn count-words-interface [state {:keys [word]}]
  (swap! state count-words word)
  [])

;;; Pipeline argument injection

(defmethod l-ext/inject-lifecycle-resources
  :onyx.peer.word-count/count-words
  [_ event]
  (let [words->n (atom {})]
    {:onyx.core/params [words->n]
     :words->n words->n}))

(defmethod l-ext/close-lifecycle-resources
  :onyx.peer.word-count/count-words
  [_ {:keys [words->n]}]
  (info @words->n))

;;; Execution

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hornetq-cluster-name "onyx-cluster")

(def hornetq-group-address "231.7.7.7")

(def hornetq-refresh-timeout 5000)

(def hornetq-discovery-timeout 5000)

(def hornetq-group-port 9876)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-util/create-queue! hq-config in-queue)
(hq-util/create-queue! hq-config out-queue)

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq    
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 1000}

   {:onyx/name :split-sentence
    :onyx/fn :onyx.peer.word-count/split-sentence-interface
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}

   {:onyx/name :group-by-word
    :onyx/fn :onyx.peer.word-count/group-by-word-interface
    :onyx/type :grouper
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}

   {:onyx/name :count-words
    :onyx/ident :onyx.peer.word-count/count-words
    :onyx/fn :onyx.peer.word-count/count-words-interface
    :onyx/type :aggregator
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}
   
   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 1000}])

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:hornetq/mode :udp
                 :hornetq.udp/cluster-name hornetq-cluster-name
                 :hornetq.udp/group-address hornetq-group-address
                 :hornetq.udp/group-port hornetq-group-port
                 :hornetq.udp/refresh-timeout hornetq-refresh-timeout
                 :hornetq.udp/discovery-timeout hornetq-discovery-timeout
                 :zookeeper/address "127.0.0.1:2181"
                 :onyx/id id
                 :onyx.coordinator/revoke-delay 5000})

(def peer-opts {:hornetq/mode :udp
                :hornetq.udp/cluster-name hornetq-cluster-name
                :hornetq.udp/group-address hornetq-group-address
                :hornetq.udp/group-port hornetq-group-port
                :hornetq.udp/refresh-timeout hornetq-refresh-timeout
                :hornetq.udp/discovery-timeout hornetq-discovery-timeout
                :zookeeper/address "127.0.0.1:2181"
                :onyx/id id})

(def data
  (map (fn [x] {:sentence x}) (clojure.string/split (slurp (clojure.java.io/resource "words.txt")) #"\n")))

(hq-util/write-and-cap! hq-config in-queue data 100)

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 5 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

#_(def results (hq-util/read! hq-config out-queue (inc (count data)) 1))

#_(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

#_(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

