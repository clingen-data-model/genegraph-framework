(ns genegraph.gene-validity
  (:require [genegraph.framework.storage.rdf :as rdf]))

(def base "http://dataexchange.clinicalgenome.org/gci/")
(def legacy-report-base "http://dataexchange.clinicalgenome.org/gci/legacy-report_")
(def affbase "http://dataexchange.clinicalgenome.org/agent/")

(comment
 (rdf/declare-query construct-proposition)

 (def the-curation
   (-> "/Users/tristan/data/genegraph/2023-01-17T1950/events/:gci-raw-missing-data/1ec53217-814e-44b3-a7b7-0f18311c20f3.json.edn"
       slurp
       edn/read-string
       (update-keys #(keyword (name %)))
       deserialize))

 (construct-proposition (:data the-curation)
                        {:gcibase base
                         :legacy_report_base legacy-report-base
                         #_#_:affiliation affiliation
                         :arbase "http://reg.genome.network/allele/"
                         :cvbase "https://www.ncbi.nlm.nih.gov/clinvar/variation/"
                         :pmbase "https://pubmed.ncbi.nlm.nih.gov/"
                         :affbase affbase
                         #_#_:entrez_gene entrez-gene})
 )
#_(-> "genegraph/gene_validity/construct_proposition.sparql"
    clojure.java.io/resource
    slurp
    genegraph.framework.storage.rdf.query/expand-query-str)







