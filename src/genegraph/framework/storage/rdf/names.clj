(ns genegraph.framework.storage.rdf.names
  "A module for translating keywords to IRIs used in RDF.
   CURIES will conventionally be represented as strings with an upper case
  prefix, if a keyword representation is used, a lower case prefix is preferred. "
  (:require [clojure.set :as s]
            [clojure.string :as string])
  (:import [org.apache.jena.shared PrefixMapping PrefixMapping$Factory]))

(def global-aliases
  (atom {:prefixes {}
         :keyword-mappings {}
         :iri-mappings {}}))

(def prefix-mapping (PrefixMapping$Factory/create))

(defn add-keyword-mappings [aliases]
  (swap! global-aliases
         #(-> %
              (update :keyword-mappings merge aliases)
              (update :iri-mappings merge (s/map-invert aliases)))))

(defn add-prefixes [prefixes]
  (.setNsPrefixes prefix-mapping (update-keys prefixes string/upper-case))
  (swap! global-aliases update :prefixes merge (update-keys prefixes string/lower-case)))

(defn kw->iri [kw]
  (or (get (:keyword-mappings @global-aliases) kw)
      (str (get (:prefixes @global-aliases)
                (namespace kw)
                (namespace kw))
           (name kw))))

(defn- match-prefix [iri]
  (first (filter #(.startsWith iri (val %)) (:prefixes @global-aliases))))

(defn iri->kw [iri]
  (or (get (:iri-mappings @global-aliases) iri)
      (if-let [prefix (match-prefix iri)]
        (keyword (key prefix)
                 (subs iri (count (val prefix))))
        (keyword iri))))
(comment
 (add-prefixes
  {"dc" "http://purl.org/dc/terms/"
   "owl" "http://www.w3.org/2002/07/owl#"
   "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
   "sepio" "http://purl.obolibrary.org/obo/SEPIO_"})

 (add-keyword-mappings
  {:sepio/has-evidence "http://purl.obolibrary.org/obo/SEPIO_0000189"
   :sepio/has-subject "http://purl.obolibrary.org/obo/SEPIO_0000388"
   :sepio/has-predicate "http://purl.obolibrary.org/obo/SEPIO_0000389"
   :sepio/has-object "http://purl.obolibrary.org/obo/SEPIO_0000390"
   :mondo/Disease "http://purl.obolibrary.org/obo/MONDO_0000001"
   :sepio/Assertion "http://purl.obolibrary.org/obo/SEPIO_0000001"})

 (kw->iri :sepio/has-evidence)
 (kw->iri :dc/Title)
 (iri->kw "http://purl.org/dc/terms/Topic")
 (iri->kw "http://purl.org/dooblincork/terms/Topic")
 
 )
