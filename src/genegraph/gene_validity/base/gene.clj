(ns genegraph.gene-validity.base.gene
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage :as storage]))

;; symbol -> skos:prefLabel ? rdf:label
;; name -> skos:altLabel 
;; everything else that needs to be searchable -> skos:hiddenLabel
;; uri -> munge of entrez id and https://www.ncbi.nlm.nih.gov/gene/

(def hgnc  "https://www.genenames.org")
(def ensembl  "https://www.ensembl.org")

(def locus-types {"immunoglobulin gene" "http://purl.obolibrary.org/obo/SO_0002122"
                  "T cell receptor gene" "http://purl.obolibrary.org/obo/SO_0002099"
                  "RNA, micro" "http://purl.obolibrary.org/obo/SO_0000276"
                  "gene with protein product" "http://purl.obolibrary.org/obo/SO_0001217"
                  "RNA, transfer" "http://purl.obolibrary.org/obo/SO_0000253"
                  "pseudogene" "http://purl.obolibrary.org/obo/SO_0000336"
                  "RNA, long non-coding" "http://purl.obolibrary.org/obo/SO_0001877"
                  "virus integration site" "http://purl.obolibrary.org/obo/SO_0000946?"
                  "RNA, vault" "http://purl.obolibrary.org/obo/SO_0000404"
                  "endogenous retrovirus" "http://purl.obolibrary.org/obo/SO_0000100"
                  "RNA, small nucleolar" "http://purl.obolibrary.org/obo/SO_0000275"
                  "T cell receptor pseudogene" "http://purl.obolibrary.org/obo/SO_0002099"
                  "immunoglobulin pseudogene" "http://purl.obolibrary.org/obo/SO_0002098"
                  "RNA, small nuclear" "http://purl.obolibrary.org/obo/SO_0000274"
                  "readthrough" "http://purl.obolibrary.org/obo/SO_0000883"
                  "RNA, ribosomal" "http://purl.obolibrary.org/obo/SO_0000252"
                  "RNA, misc" "http://purl.obolibrary.org/obo/SO_0000356"})

(defn gene-as-triple [gene]
  (let [uri (str "https://www.ncbi.nlm.nih.gov/gene/" (:entrez_id gene))
        hgnc-id (:hgnc_id gene)
        hgnc-iri (rdf/resource
                  (s/replace (:hgnc_id gene)
                             "HGNC"
                             "https://identifiers.org/hgnc"))
        ensembl-iri (rdf/resource
                     (str "http://rdf.ebi.ac.uk/resource/ensembl/"
                          (:ensembl_gene_id gene)))]
    (remove nil?
            (concat [[uri :skos/prefLabel (:symbol gene)]
                     [uri :skos/altLabel (:name gene)]
                     (when-let [loc (:location gene)] [uri :so/chromosome-band loc])
                     (when-let [locus-type (locus-types (:locus_type gene))]
                       [uri :rdf/type (rdf/resource locus-type)])
                     [uri :rdf/type :so/Gene]
                     [uri :owl/sameAs (rdf/resource hgnc-id)]
                     [hgnc-id :dc/source (rdf/resource hgnc)]
                     [uri :owl/sameAs ensembl-iri]
                     [uri :owl/sameAs hgnc-iri]
                     [ensembl-iri :dc/source (rdf/resource ensembl)]]
                    (map #(vector uri :skos/hiddenLabel %)
                         (:alias_symbol gene))
                    (map #(vector uri :skos/hiddenLabel %)
                         (:prev_name gene))
                    (map #(vector uri :skos/hiddenLabel %)
                         (:prev_symbol gene))))))

(defn genes-as-triple [genes-json]
  (let [genes (get-in genes-json [:response :docs])]
    (conj (mapcat gene-as-triple genes)
          ["https://www.genenames.org/" :rdf/type :void/Dataset])))

(defmethod rdf/as-model :genegraph.gene-validity.base/hgnc [{:keys [source]}]
  (with-open [r (io/reader (storage/->input-stream source))]
    (-> (json/read r :key-fn keyword)
        genes-as-triple
        rdf/statements->model)))

(comment
  (take 50
   (rdf/as-model {:format :genegraph.gene-validity.base/hgnc
                  :source (io/file "/users/tristan/data/genegraph-neo/hgnc.json")}))

  (rdf/resource :skos/prefLabel)
  )


;; (defmethod add-model :hgnc-genes [event]
;;   (let [model (-> event 
;;                   :genegraph.sink.event/value
;;                   (json/parse-string true)
;;                   genes-as-triple
;;                   db/statements-to-model)]
;;     (assoc event ::q/model model )))
