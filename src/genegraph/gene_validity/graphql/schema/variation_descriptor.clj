(ns genegraph.gene-validity.graphql.schema.variation-descriptor)

(def variation-descriptor
  {:name :VariationDescriptor
   :graphql-type :object
   :description "A descriptor containing a reference to a GA4GH Variation object, commonly an allele."
   :implements [:Resource]
   :fields {:canonical_reference {:type '(list :Resource)
                                  :description "A list canonicalizations considered appropriate for the allele described therein."
                                  :path [:ga4gh/CanonicalReference]}}})

