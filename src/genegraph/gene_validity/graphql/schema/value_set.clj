(ns genegraph.gene-validity.graphql.schema.value-set)


(def value-set
  {:name :ValueSet
   :graphql-type :object
   :description "re-usable collections of terms that can be bound to attributes in a particular schema to constrain data entry."
   :implements [:Resource]
   :fields {:members {:type '(list :Resource)
                      :description "Concepts included in this value set."
                      :path [[:skos/is-in-scheme :<]]}}})
