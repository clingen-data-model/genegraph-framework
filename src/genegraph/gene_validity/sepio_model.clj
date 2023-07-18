(ns genegraph.gene-validity.sepio-model
  (:require [clojure.edn :as edn]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.event :as event]
            [genegraph.gene-validity.names]))

(def construct-params
  {:gcibase "http://dataexchange.clinicalgenome.org/gci/"
   :legacy_report_base "http://dataexchange.clinicalgenome.org/gci/legacy-report_"
   :arbase "http://reg.genome.network/allele/"
   :cvbase "https://www.ncbi.nlm.nih.gov/clinvar/variation/"
   :pmbase "https://pubmed.ncbi.nlm.nih.gov/"
   :affbase "http://dataexchange.clinicalgenome.org/agent/"})

(rdf/declare-query construct-proposition
                   construct-evidence-level-assertion
                   construct-experimental-evidence-assertions
                   construct-genetic-evidence-assertion
                   construct-ad-variant-assertions
                   construct-ar-variant-assertions
                   construct-cc-and-seg-assertions
                   construct-proband-score
                   construct-model-systems-evidence
                   construct-functional-alteration-evidence
                   construct-functional-evidence
                   construct-rescue-evidence
                   construct-case-control-evidence
                   construct-proband-segregation-evidence
                   construct-family-segregation-evidence
                   construct-evidence-connections
                   construct-alleles
                   construct-articles
                   construct-earliest-articles
                   construct-secondary-contributions
                   construct-variant-score
                   construct-ar-variant-score
                   construct-unscoreable-evidence
                   unlink-variant-scores-when-proband-scores-exist
                   unlink-segregations-when-no-proband-and-lod-scores
                   add-legacy-website-id)

(def has-affiliation-query
  "Query that returns a curations full affiliation IRI as a Resource.
  Expects affiliations to have been preprocessed to IRIs from string form."
  (rdf/create-query "prefix gci: <http://dataexchange.clinicalgenome.org/gci/>
                   select ?affiliationIRI where {
                     ?proposition a gci:gdm .
                     OPTIONAL {
                      ?proposition gci:affiliation ?gdmAffiliationIRI .
                     }
                     OPTIONAL {
                      ?classification a gci:provisionalClassification .
                      ?classification gci:affiliation ?classificationAffiliationIRI .
                      ?classification gci:last_modified ?date .
                     }
                     BIND(COALESCE(?classificationAffiliationIRI, ?gdmAffiliationIRI) AS ?affiliationIRI) }
                     ORDER BY DESC(?date) LIMIT 1"))

(def is-publish-action-query
  (rdf/create-query "prefix gci: <http://dataexchange.clinicalgenome.org/gci/>
                      select ?classification where {
                      ?classification gci:publishClassification true }" ))

(def initial-construct-queries
  [construct-proposition
   construct-evidence-level-assertion
   construct-experimental-evidence-assertions
   construct-genetic-evidence-assertion
   construct-ad-variant-assertions
   construct-ar-variant-assertions
   construct-cc-and-seg-assertions
   construct-proband-score
   construct-model-systems-evidence
   construct-functional-alteration-evidence
   construct-functional-evidence
   construct-rescue-evidence
   construct-case-control-evidence
   construct-proband-segregation-evidence
   construct-family-segregation-evidence
   construct-evidence-connections
   construct-alleles
   construct-articles
   construct-earliest-articles
   construct-secondary-contributions
   construct-variant-score
   construct-ar-variant-score
   construct-unscoreable-evidence])

(defn publish-or-unpublish-role [event]
  (let [res
        (if (seq (is-publish-action-query (:gene-validity/gci-model event)))
          :cg/PublisherRole
          :cg/UnpublisherRole)]
    (println res)
    res))

(defn params-for-construct [event]
  (assoc construct-params
         :affiliation
         (first (has-affiliation-query (:gene-validity/gci-model event)))
         :publishRole
         (publish-or-unpublish-role event)))

(defn add-model [event]
  (let [gci-model (:gene-validity/gci-model event)
        params (params-for-construct event)
        unlinked-model (apply
                        rdf/union
                        (map #(% gci-model params)
                             initial-construct-queries))
        pruned-model (-> unlinked-model
                         unlink-variant-scores-when-proband-scores-exist
                         unlink-segregations-when-no-proband-and-lod-scores)]
    (assoc event
           ::event/model
           pruned-model)))









