(ns genegraph.gene-validity.graphql.schema.control-cohort)

(def control-cohort
  {:name :ControlCohort
   :graphql-type :object
   :description "A control cohort in a case control study."
   :implements [:Resource :Cohort]
   :fields {}})

