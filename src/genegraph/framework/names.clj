(ns genegraph.framework.names
  "A module for translating keywords to IRIs used
  in RDF."
  (:require [genegraph.framework.protocol :as p]
            [clojure.edn :as edn]
            [clojure.set :as s]
            [clojure.java.io :as io]))

(defn prefix [names iri]
  (some ))

(defn iri->kw [names iri]
  (or (get (:keyword-mappings names) iri)
))

(defmethod p/init :names [names-def]
  (assoc names-def
         :iri-mappings
         (s/map-invert (:keyword-mappings names-def))))
(def test-names
  {:type :names
   :name :test-names
   :prefixes
   {"dc" "http://purl.org/dc/terms/"
    "owl" "http://www.w3.org/2002/07/owl#"
    "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
    "sepio" "http://purl.obolibrary.org/obo/SEPIO_"}
   :keyword-mappings
   {:sepio/has-evidence "http://purl.obolibrary.org/obo/SEPIO_0000189"
    :sepio/has-subject "http://purl.obolibrary.org/obo/SEPIO_0000388"
    :sepio/has-predicate "http://purl.obolibrary.org/obo/SEPIO_0000389"
    :sepio/has-object "http://purl.obolibrary.org/obo/SEPIO_0000390"
    :mondo/Disease "http://purl.obolibrary.org/obo/MONDO_0000001"
    :sepio/Assertion "http://purl.obolibrary.org/obo/SEPIO_0000001"}})

(-> test-names
    p/init)
