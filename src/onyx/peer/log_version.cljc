(ns onyx.peer.log-version)

(def version "0.11.1")

(defn check-compatible-log-versions! [cluster-version]
  (when-not (or (re-find #"-" version)
                 (= version cluster-version))
    (throw (ex-info "Incompatible versions of the Onyx cluster coordination log.
                     A new, distinct, :onyx/tenancy-id should be supplied when upgrading or downgrading Onyx." 
                    {:cluster-version cluster-version
                     :peer-version version}))))
