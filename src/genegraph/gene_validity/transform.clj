(ns genegraph.gene-validity.transform
  (:require [clojure.edn :as edn]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.gene-validity.names]
            [genegraph.gene-validity.deserialize :refer [deserialize]]))


#_(def base "http://dataexchange.clinicalgenome.org/gci/")
#_(def legacy-report-base "http://dataexchange.clinicalgenome.org/gci/legacy-report_")
#_(def affbase "http://dataexchange.clinicalgenome.org/agent/")

(def params-for-construct-queries
  {:gcibase "http://dataexchange.clinicalgenome.org/gci/"
   :legacy_report_base "http://dataexchange.clinicalgenome.org/gci/legacy-report_"
   #_#_:affiliation affiliation
   :arbase "http://reg.genome.network/allele/"
   :cvbase "https://www.ncbi.nlm.nih.gov/clinvar/variation/"
   :pmbase "https://pubmed.ncbi.nlm.nih.gov/"
   :affbase "http://dataexchange.clinicalgenome.org/agent/"
   #_#_:entrez_gene entrez-gene})

(rdf/declare-query construct-proposition
                   construct-evidence-level-assertion)

(comment

 (def the-curation
   (-> "/Users/tristan/data/genegraph/2023-04-13T1609/events/:gci-raw-missing-data/1ec53217-814e-44b3-a7b7-0f18311c20f3.json.edn"
       slurp
       edn/read-string
       (update-keys #(keyword (name %)))
       deserialize))

 (rdf/pp-model
  (construct-proposition (:data the-curation)
                         params-for-construct-queries))
 
 )
#_(-> "genegraph/gene_validity/construct_proposition.sparql"
    clojure.java.io/resource
    slurp
    genegraph.framework.storage.rdf.query/expand-query-str)







