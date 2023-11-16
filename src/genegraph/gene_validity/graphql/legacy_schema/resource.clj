(ns genegraph.gene-validity.graphql.resource
  (:require [genegraph.framework.storage.rdf :as rdf]))

(comment
  (genegraph.framework.storage.rdf.types/model
   (rdf/statements->model [[:cg/Tristan :rdfs/label "Tristan Nelson"]]))
  (rdf/resource
   :cg/Tristan
   (rdf/statements->model [[:cg/Tristan :rdfs/label "Tristan Nelson"]
                           [:cg/Tristan :skos/prefLabel "Tristan"]]))
  (rdf/ld-> (rdf/resource
             :cg/Tristan
             (rdf/statements->model [[:cg/Tristan :rdfs/label "Tristan Nelson"]
                                     [:cg/Tristan :skos/prefLabel "Tristan"]]))
            [:skos/prefLabel])

  (rdf/ld1->*  (rdf/resource
                       :cg/Tristan
                       (rdf/statements->model [[:cg/Tristan :rdfs/label "Tristan Nelson"]
                                               [:cg/Tristan :skos/prefLabel "Tristan"]]))
            [:skos/prefLabel :rdfs/label])

  (direct-superclasses
   nil
   nil
   (rdf/resource
    :foaf/Person
    (rdf/statements->model [[:cg/Tristan :rdfs/label "Tristan Nelson"]
                            [:cg/Tristan :skos/prefLabel "Tristan"]
                            [:cg/Tristan :rdf/type :foaf/Person]
                            [:cg/Tristan :skos/altLabel "Tris"]
                            [:foaf/Person :rdfs/subClassOf :foaf/Agent]])))

  )

(defn iri [context args value]
  (or (some-> value (rdf/ld1-> [:cg/website-legacy-id]) str)
      (str value)))

(defn curie [context args value]
  (rdf/curie (or (rdf/ld1-> value [:cg/website-legacy-id])
                 value)))

(defn label [context args value]
  (rdf/ld1->* value [:skos/prefLabel :rdfs/label :foaf/name]))

(defn website-display-label [context args value]
  (rdf/ld1->* value [:cg/website-display-label
                     :skos/prefLabel
                     :rdfs/label
                     :foaf/name]))

(defn type [context args value]
  (rdf/ld1-> value [:rdf/type]))

(defn alternative-label [context args value]
  (rdf/ld1-> value [:skos/altLabel]))

(defn description [context args value]
  (rdf/ld1-> value [:dc/description]))

(defn direct-superclasses [context args value]
  (rdf/ld-> value [:rdfs/subClassOf]))

(defn direct-subclasses [context args value]
  (rdf/ld-> value [[:rdfs/subClassOf :<]]))
