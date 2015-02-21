(def flow-conditions-schema
  [{:flow/from :a
    :flow/to :all
    :flow/short-circuit? true
    :flow/exclude-keys [:my-side-effects-result-key]
    :flow/predicate :my/all-predicate-fn
    :flow/doc "Send segment to all branches if this predicate is true."}
   {:flow/from :a
    :flow/to :none
    :flow/short-circuit? true
    :flow/exclude-keys [:my-side-effects-result-key]
    :flow/predicate :my/none-predicate-fn}
   {:flow/from :a
    :flow/to [:c :d]
    :flow/short-circuit? true
    :flow/exclude-keys [:my-side-effects-result-key]
    :flow/predicate :my/predicate-fn}
   {:flow/from :a
    :flow/to [:b]
    :flow/exclude-keys [:my-side-effects-result-key]
    :flow/predicate [:or [:not :pred-c] [:and :pred-b :pred-a]]
    :adult/age 18
    :flow/params [:adult/age]
    :flow/doc "True if this segment represents an adult."}
   {:flow/from :a
    :flow/to [:c]
    :flow/exclude-keys [:my-side-effects-result-key]
    :flow/predicate [:or [:not :pred-c] [:and :pred-b :pred-a]]
    :adult/age 18
    :flow/params [:adult/age]
    :flow/doc "True if this segment represents an adult."}])