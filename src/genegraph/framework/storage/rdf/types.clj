(ns genegraph.framework.storage.rdf.types
  (:require [genegraph.framework.storage.rdf.names :as names]
            [clojure.core.protocols :as protocols :refer [Datafiable]]
            [clojure.datafy :as datafy :refer [datafy]]
            [clojure.string :as s]
            [taoensso.nippy :as nippy])
  (:import [org.apache.jena.rdf.model
            Literal
            RDFList
            Resource
            ResourceFactory
            AnonId
            Model
            ModelFactory]
           [java.io
            ByteArrayOutputStream
            ByteArrayInputStream]
           [org.apache.jena.query Dataset]
           [org.apache.jena.riot RDFDataMgr Lang]
           [org.apache.jena.datatypes.xsd.impl
            XSDBaseNumericType]))

(def first-property (ResourceFactory/createProperty "http://www.w3.org/1999/02/22-rdf-syntax-ns#first"))

(defprotocol Steppable
  (step [edge start]))

(defprotocol AsReference
  (to-ref [resource]))

(defprotocol AsClojureType
  (to-clj [x]))

(defprotocol AsRDFNode
  (to-rdf-node [x]))

(defprotocol ThreadableData
  "A data structure that can be accessed through the ld-> and ld1-> accessors
  similar to Clojure XML zippers"
  (ld-> [this ks])
  (ld1-> [this ks]))

(defprotocol RDFType
  (is-rdf-type? [this rdf-type]))

(defprotocol AsResource
  "Create an RDFResource given a reference"
  (resource [r] [r model]))

(defprotocol AsModel
  "Coerce argument into a model"
  (model [this]))

(extend-type Dataset

  AsModel
  (model [this] (.getUnionModel this)))

(extend-type Model
  
  AsModel
  (model [this] this))

(declare datafy-resource)

(declare navize)

(extend-type Resource
  ThreadableData
  (ld-> [this ks] (reduce (fn [nodes k]
                            (->> nodes
                                 (filter #(satisfies? ThreadableData %))
                                 (map #(step k %))
                                 (filter seq) flatten))
                          [this] 
                          ks))
  (ld1-> [this ks] (first (ld-> this ks))))

(defn- compose-object-for-datafy [o]
  (cond (instance? Literal o) (.toString o)
        (instance? Resource o) 
        (with-meta (-> o .toString symbol)
          {::datafy/obj o
           ::datafy/class (class o)
           `protocols/datafy #(-> % meta ::datafy/obj datafy-resource)})))

(extend-protocol AsResource
  
  java.lang.String
  (resource 
    ([r] (ResourceFactory/createResource r))
    ([r model] (resource (resource r) model)))
  
  clojure.lang.Keyword
  (resource
    ([r] (ResourceFactory/createResource (names/kw->iri r)))
    ([r model] (resource (resource r) model)))

  org.apache.jena.rdf.model.Resource
  (resource
    ([r] r)
    ([r model] (.inModel r (model model)))))



;; TODO reimplement freeze for Resource
#_(nippy/extend-freeze 
 RDFResource ::rdf-resource
 [x data-output]
 (let [resource-descriptor
       (if-let [id (-> x as-jena-resource .getURI)]
         {:iri id}
         {:bnode-id (-> x as-jena-resource .getId str)})]
   (nippy/freeze-to-out! data-output resource-descriptor)))

#_(nippy/extend-thaw 
 ::rdf-resource
 [data-input]
 (let [resource-descriptor (nippy/thaw-from-in! data-input)]
   (if (:iri resource-descriptor)
     (resource (:iri resource-descriptor))
     (resource (.createResource
                (get-all-graphs)
                (-> resource-descriptor :bnode-id AnonId/create))))))

;; Jena read methods expect to read to the end of the stream
;; when offered one as input. Since Nippy presents the
;; model serialized with other objects in the stream, need to
;; extract the serialized model from the stream before passing
;; to Jena. I suppose this costs us some unnecessary memcpy.
;; An alternative might be extending an input-stream class
;; that can send EOF after a fixed number of bytes are read.
;; Doesen't seem worth the work (right now)

(nippy/extend-freeze
 Model ::model
 [x data-output]
 (let [model-bytes (.toByteArray
                    (doto (ByteArrayOutputStream.)
                      (RDFDataMgr/write x Lang/RDFTHRIFT)))]
   (.writeLong data-output (alength model-bytes)) ; add byte count as prefix
   (.write data-output model-bytes 0 (alength model-bytes))))

(nippy/extend-thaw
 ::model
 [data-input]
 (let [model-byte-count (.readLong data-input)
       model-bytes (make-array Byte/TYPE model-byte-count)
       model (ModelFactory/createDefaultModel)]
   (.read data-input model-bytes 0 model-byte-count)
   (RDFDataMgr/read model (ByteArrayInputStream. model-bytes) Lang/RDFTHRIFT)
   model))

(defn- kw-to-property [kw]
  (ResourceFactory/createProperty (names/kw->iri kw)))

(extend-protocol Steppable

  ;; TODO 
  ;; Single keyword, treat as [:ns/prop :>] (outward pointing edge)
  clojure.lang.Keyword
  (step [edge start]
    (step [edge :>] start))
  
  ;; Expect edge to be a vector with form [:ns/prop <direction>], where direction is one
  ;; of :> :< :-
  ;; TODO fail more gracefully when starting point is a literal
  clojure.lang.IPersistentVector
  (step [edge start]
    (let [property (kw-to-property (first edge))
          out-fn (fn [n] (iterator-seq
                          (.listObjectsOfProperty (.getModel n) n property)))
          in-fn (fn [n] (iterator-seq
                         (.listResourcesWithProperty (.getModel n) property n)))
          both-fn #(concat (out-fn %) (in-fn %))
          step-fn (case (second edge)
                    :> out-fn
                    :< in-fn
                    :- both-fn)
          result (map to-clj (step-fn start))]
      (if (= 0 (count result)) nil result))))



(extend-protocol AsRDFNode

  java.lang.String
  (to-rdf-node [x] (ResourceFactory/createPlainLiteral x))

  ; Accept integers as node literals. RDF still stores it as a string, but just with a type metadata
  java.lang.Integer
  (to-rdf-node [x] (ResourceFactory/createTypedLiteral (str x) XSDBaseNumericType/XSDinteger))

  java.lang.Long
  (to-rdf-node [x] (ResourceFactory/createTypedLiteral (str x) XSDBaseNumericType/XSDlong))

  clojure.lang.Keyword
  (to-rdf-node [x] (ResourceFactory/createResource (names/kw->iri x)))
  
  Resource
  (to-rdf-node [x] x))

(defn- rdf-list-to-vector [rdf-list-node]
  (let [rdf-list (.as rdf-list-node RDFList)]
    (->> rdf-list .iterator iterator-seq (mapv #(to-clj %)))))

(extend-protocol AsClojureType

  Resource
  (to-clj [x] (if (.hasProperty x first-property)
                (rdf-list-to-vector x)
                x))
  
  Literal
  (to-clj [x] (.getValue x)))

