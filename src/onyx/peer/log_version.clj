(ns onyx.peer.log-version)

(def version "0.13.6-SNAPSHOT")

(defn parse-semver
  [semantic-version]
  (when (some? semantic-version)
    (next (re-matches #"(\d+)\.(\d+)\.(\d+)\-?(.*)" semantic-version))))

(defn check-compatible-log-versions! [cluster-version]
  (let [[major minor patch] (parse-semver version)
        [cluster-major cluster-minor] (parse-semver cluster-version)]
    (when (not= [major minor] [cluster-major cluster-minor])
      (throw (ex-info "Incompatible versions of the Onyx cluster coordination log.
                      A new, distinct, :onyx/tenancy-id should be supplied when upgrading or downgrading Onyx." 
                      {:cluster-version cluster-version
                       :peer-version version})))))
