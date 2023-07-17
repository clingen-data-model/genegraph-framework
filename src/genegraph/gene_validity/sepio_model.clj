(ns genegraph.gene-validity.sepio-model
  (:require [clojure.edn :as edn]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.event :as event]
            [genegraph.gene-validity.names]))


#_(def base "http://dataexchange.clinicalgenome.org/gci/")
#_(def legacy-report-base "http://dataexchange.clinicalgenome.org/gci/legacy-report_")
#_(def affbase "http://dataexchange.clinicalgenome.org/agent/")

(def construct-params
  {:gcibase "http://dataexchange.clinicalgenome.org/gci/"
   :legacy_report_base "http://dataexchange.clinicalgenome.org/gci/legacy-report_"
   #_#_:affiliation affiliation
   :arbase "http://reg.genome.network/allele/"
   :cvbase "https://www.ncbi.nlm.nih.gov/clinvar/variation/"
   :pmbase "https://pubmed.ncbi.nlm.nih.gov/"
   :affbase "http://dataexchange.clinicalgenome.org/agent/"
   #_#_:entrez_gene entrez-gene})

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

(defn add-model [event]
  (let [gci-model (:gene-validity/gci-model event)
        unlinked-model (apply
                        rdf/union
                        (map #(% gci-model construct-params)
                             initial-construct-queries))
        pruned-model (-> unlinked-model
                         unlink-variant-scores-when-proband-scores-exist
                         unlink-segregations-when-no-proband-and-lod-scores)]
    (assoc event
           ::event/model
           pruned-model)))









