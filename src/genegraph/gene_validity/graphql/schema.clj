(ns genegraph.gene-validity.graphql.schema
  (:require [genegraph.gene-validity.graphql.schema.resource :as model-resource]
            [genegraph.gene-validity.graphql.schema.statement :as model-statement]
            [genegraph.gene-validity.graphql.schema.evidence-line :as model-evidence-line]
            [genegraph.gene-validity.graphql.schema.agent :as model-agent]
            [genegraph.gene-validity.graphql.schema.contribution :as model-contribution]
            [genegraph.gene-validity.graphql.schema.find :as model-find]
            [genegraph.gene-validity.graphql.schema.proband-evidence :as model-proband]
            [genegraph.gene-validity.graphql.schema.variant-evidence :as model-variant-evidence]
            [genegraph.gene-validity.graphql.schema.family :as family]
            [genegraph.gene-validity.graphql.schema.value-set :as value-set]
            [genegraph.gene-validity.graphql.schema.bibliographic-resource :as model-bibliographic-resource]
            [genegraph.gene-validity.graphql.schema.segregation :as model-segregation]
            [genegraph.gene-validity.graphql.schema.case-control-evidence :as model-case-control]
            [genegraph.gene-validity.graphql.schema.cohort :as model-cohort]
            [genegraph.gene-validity.graphql.schema.case-cohort :as model-case-cohort]
            [genegraph.gene-validity.graphql.schema.control-cohort :as model-control-cohort]
            [genegraph.gene-validity.graphql.schema.variation-descriptor :as variation-descriptor]
            #_[genegraph.gene-validity.graphql.legacy-schema :as legacy-schema]
            [genegraph.gene-validity.graphql.common.schema-builder :as schema-builder]
            [com.walmartlabs.lacinia :as lacinia]
            [genegraph.framework.storage.rdf :refer [tx]]
            [com.walmartlabs.lacinia.schema :as lacinia-schema]))

(def rdf-to-graphql-type-mappings
  {:type-mappings
   [[:sepio/Assertion :Statement]
    [:sepio/Proposition :Statement]
    [:prov/Agent :Agent]
    [:sepio/EvidenceLine :Statement]
    [:dc/BibliographicResource :BibliographicResource]
    [:sepio/ProbandWithVariantEvidenceItem :ProbandEvidence]
    [:sepio/VariantEvidenceItem :VariantEvidence]
    [:ga4gh/VariationDescriptor :VariationDescriptor]
    [:sepio/FamilyCosegregation :Segregation]
    [:sepio/CaseControlEvidenceItem :CaseControlEvidence]
    [:stato/Cohort :Cohort]
    [:sepio/ValueSet :ValueSet]
    [:pco/Family :Family]]
   :default-type-mapping :GenericResource})

(def model
  [rdf-to-graphql-type-mappings
   model-resource/resource-interface
   model-resource/generic-resource
   model-resource/resource-query
   model-resource/record-metadata-query
   model-resource/record-metadata-query-result
   model-statement/statement
   model-evidence-line/evidence-line
   model-contribution/contribution
   model-proband/proband-evidence
   model-variant-evidence/variant-evidence
   model-agent/agent
   model-find/types-enum
   model-find/find-query
   model-find/query-result
   model-find/find-query
   variation-descriptor/variation-descriptor
   value-set/value-set
   family/family
   model-segregation/segregation
   model-case-control/case-control-evidence
   model-cohort/cohort
   model-case-cohort/case-cohort
   model-control-cohort/control-cohort
   model-bibliographic-resource/bibliographic-resource])


(defn schema []
  (schema-builder/schema model))

#_(defn merged-schema []
  (-> (legacy-schema/schema-for-merge)
      (medley/deep-merge (schema-builder/schema-description model))
      lacinia-schema/compile))

(defn schema-description []
  (schema-builder/schema-description model))

(defn query
  "Function not used except for evaluating queries in the REPL
  may consider moving into test namespace in future"
  ([query-str]
   (tx (lacinia/execute (schema) query-str nil nil)))
  ([query-str variables]
   (tx (lacinia/execute (schema) query-str variables nil))))

(comment
  (schema)

  )
