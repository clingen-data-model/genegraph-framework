(ns genegraph.gene-validity.base.affiliations
  (:require [clojure.data.csv :as csv]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage :as storage]))

(def affiliation-prefix "http://dataexchange.clinicalgenome.org/agent/")

(defn affiliation [[id label]]
  (if (and (> (count id) 0)
           (> (count label) 0))
    (let [iri (str affiliation-prefix id)]
      [[iri :skos/prefLabel (s/trim label)]
       [iri :rdfs/label (s/trim label)]
       [iri :rdf/type :cg/Affiliation]])
    []))

(defn affiliation-to-triples [affiliation-row]
  (let [[label id _ _ _ _ _ vcep-id vcep-label gcep-id gcep-label] affiliation-row
        affiliation-list [[id label] [vcep-id vcep-label] [gcep-id gcep-label]]]
    (mapcat affiliation affiliation-list)))

(defn affiliations-to-triples [affiliations-csv]
  (mapcat affiliation-to-triples (rest affiliations-csv)))

(defmethod rdf/as-model :genegraph.gene-validity.base/affiliations [{:keys [source]}]
  (-> source
      storage/->input-stream
      io/reader
      csv/read-csv
      affiliations-to-triples
      rdf/statements->model))
