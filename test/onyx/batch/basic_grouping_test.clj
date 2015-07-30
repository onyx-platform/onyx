(ns onyx.batch.basic-grouping-test
  (:require [midje.sweet :refer :all]
            [onyx.test-helper :refer [load-batch-config]]
            [onyx.plugin.local-file]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-batch-config))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def base-dir "target/")

(def input-file-name (str base-dir (java.util.UUID/randomUUID)))

(def output-file-name (str base-dir (java.util.UUID/randomUUID)))

(def names
  [{:name "Le"}
   {:name "Lauryn"}
   {:name "Reginia"}
   {:name "Marylin"}
   {:name "Dallas"}
   {:name "Arnetta"}
   {:name "Royce"}
   {:name "Vinita"}
   {:name "Sun"}
   {:name "Julieta"}
   {:name "Silva"}
   {:name "Allan"}
   {:name "Terica"}
   {:name "Adele"}
   {:name "Corine"}
   {:name "Alethia"}
   {:name "Kum"}
   {:name "Jospeh"}
   {:name "Kylie"}
   {:name "Mirian"}
   {:name "Vito"}
   {:name "Contessa"}
   {:name "Mai"}
   {:name "Sharyn"}
   {:name "Jaymie"}
   {:name "Velvet"}
   {:name "Jaye"}
   {:name "Ima"}
   {:name "Li"}
   {:name "Renata"}
   {:name "Beaulah"}
   {:name "Monty"}
   {:name "Stephaine"}
   {:name "Chu"}
   {:name "Buena"}
   {:name "Bee"}
   {:name "Caridad"}
   {:name "Philip"}
   {:name "Regenia"}
   {:name "Miyoko"}
   {:name "Jerica"}
   {:name "Monroe"}
   {:name "Kenny"}
   {:name "Allena"}
   {:name "Heath"}
   {:name "Luise"}
   {:name "Lorna"}
   {:name "Jannet"}
   {:name "Adolfo"}
   {:name "Bill"}
   {:name "Mauricio"}
   {:name "Shirley"}
   {:name "Jannet"}
   {:name "Cassy"}
   {:name "Carey"}
   {:name "Wanda"}
   {:name "Humberto"}
   {:name "Anderson"}
   {:name "Felisa"}
   {:name "Idalia"}
   {:name "Renna"}
   {:name "Adolfo"}
   {:name "Maribel"}
   {:name "Denny"}
   {:name "Frances"}
   {:name "Chase"}
   {:name "Ola"}
   {:name "Lulu"}
   {:name "Jacklyn"}
   {:name "Cami"}
   {:name "Zada"}
   {:name "Blair"}
   {:name "Lashanda"}
   {:name "Aurore"}
   {:name "Lynsey"}
   {:name "Joeann"}
   {:name "Filiberto"}
   {:name "Trista"}
   {:name "Belen"}
   {:name "Kelsey"}
   {:name "Selma"}
   {:name "Vicki"}
   {:name "Mckenzie"}
   {:name "Raquel"}
   {:name "Buford"}
   {:name "Erna"}
   {:name "Gudrun"}
   {:name "Hattie"}
   {:name "Marchelle"}
   {:name "Sibyl"}
   {:name "Elvina"}
   {:name "Cristina"}
   {:name "Katlyn"}
   {:name "Madelaine"}
   {:name "Caren"}
   {:name "Larissa"}
   {:name "Talitha"}
   {:name "Nickolas"}
   {:name "Giuseppe"}
   {:name "Irish"}])

(doseq [segment names]
  (spit (str input-file-name ".edn") segment :append true)
  (spit (str input-file-name ".edn") "\n" :append true))

(defn count-first-letters [grouping-key segments]
  {:letter grouping-key :n (count segments)})

(prn "Input file is:" input-file-name ".edn")
(prn "Output file is:" output-file-name ".edn")

(defn first-letter [segment]
  (first (:name segment)))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.local-file/input
    :onyx/type :input
    :onyx/medium :local-file
    :file/path input-file-name
    :file/extension ".edn"
    :onyx/doc "Reads segments a local file"}

   {:onyx/name :capitalize
    :onyx/fn :onyx.batch.basic-grouping-test/count-first-letters
    :onyx/group-by-fn :onyx.batch.basic-grouping-test/first-letter
    :onyx/type :reducing-function}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.local-file/output
    :onyx/type :output
    :onyx/medium :local-file
    :file/path output-file-name
    :file/extension ".edn"
    :onyx/doc "Writes segments to a local file"}])

(def workflow
  [[:in :capitalize]
   [:capitalize :out]])

(def v-peers (onyx.api/start-peers 1 peer-group))

(def job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    {:mode :batch
     :catalog catalog
     :workflow workflow
     :task-scheduler :onyx.task-scheduler/naive})))

(onyx.api/await-job-completion peer-config job-id)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)