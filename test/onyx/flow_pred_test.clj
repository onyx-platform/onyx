(ns onyx.flow-pred-test
  (:require [midje.sweet :refer :all]
            [onyx.planning :refer [build-pred-fn]]))

(fact ((build-pred-fn (constantly true)) [1 2 3]) => true)

(fact ((build-pred-fn [:and (constantly true) (constantly false)]) [5 6 7]) => false)

(fact ((build-pred-fn [:and (constantly true) (constantly true)]) [5 6 7]) => true)

(fact ((build-pred-fn [:and (constantly false) [:and (constantly true) (constantly true)]]) [5]) => false)

(fact ((build-pred-fn [:and (constantly true) [:and (constantly true) (constantly false)]]) [5]) => false)

(fact ((build-pred-fn [:or (constantly true) (constantly false)]) [5]) => true)

(fact ((build-pred-fn [:or (constantly false) (constantly false)]) [5]) => nil)

(fact ((build-pred-fn [:or (constantly true) [:or (constantly true) (constantly false)]]) [5]) => true)

(fact ((build-pred-fn [:not (constantly true)]) [1 2 3]) => false)

(fact ((build-pred-fn [:not (constantly false)]) [5]) => true)

(fact ((build-pred-fn [:not [:not (constantly false)]]) [5]) => false)

(fact ((build-pred-fn [:not [:not (constantly true)]]) [5]) => true)

(fact ((build-pred-fn [:or (constantly true) [:and (constantly false) (constantly false)]]) [5]) => true)

(fact ((build-pred-fn [:or [:not (constantly true)] [:and (constantly false) (constantly false)]]) [5]) => nil)

(fact ((build-pred-fn [:and (constantly true) [:and (constantly true) (constantly true)]]) [1 2 3]) => true)

