(ns genegraph.framework.storage.rdf
  "Storage processor for an RDF triplestore based on Apache Jena"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage.rdf.instance :as i]
            [genegraph.framework.storage.rdf.names :as names]
            [genegraph.framework.storage.rdf.algebra :as algebra]
            [genegraph.framework.storage.rdf.types :as types]
            [genegraph.framework.storage.rdf.query :as query]
            [genegraph.framework.storage :as s]
            [genegraph.framework.event :as event]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [org.apache.jena.rdf.model Model Resource ModelFactory
            ResourceFactory Statement Property]
           [org.apache.jena.tdb2 TDB2Factory]
           [org.apache.jena.query ReadWrite Query QueryFactory QueryExecutionFactory Dataset
            QuerySolutionMap]
           [org.apache.jena.sparql.algebra OpAsQuery]
           [org.apache.jena.riot RDFDataMgr Lang]))

(def instance-defaults
  {:queue-size 100})

(defrecord RDFStore [instance state queue-size text-assembly-path path]
  p/Lifecycle
  (start [this]
    (reset! instance (i/start-dataset (dissoc this :instance))))
  (stop [this]
    (.close @instance)
    (reset! state :stopped)))

(defmethod p/init :rdf [storage-def]
  (map->RDFStore
   (merge
    instance-defaults
    storage-def
    {:instance (atom nil)
     :state (atom :stopped)})))

(defmacro tx 
  "Open a read transaction on the persistent database. Most commands that read data from the databse will call this internally, since Jena TDB explicitly requires opening a transaction to read any data. If one wishes to issue multiple read commands within the scope of a single transaction, it is perfectly acceptable to wrap them all in a tx call, as this uses the var *in-tx* to ensure only a single transaction per thread is opened."
  [db & body]
  `(if (.isInTransaction ~db) 
     (do ~@body)
     (try
       (.begin ~db ReadWrite/READ)
       (do ~@body)
       (finally (.end ~db)))))

(def jena-rdf-format
  {:rdf-xml "RDF/XML"
   :json-ld "JSON-LD"
   :turtle "Turtle"
   ::rdf-xml "RDF/XML"
   ::json-ld "JSON-LD"
   ::turtle "Turtle"
   ::n-triples "N-TRIPLES"})

(derive ::rdf-xml ::rdf-serialization)
(derive ::json-ld ::rdf-serialization)
(derive ::turtle ::rdf-serialization)
(derive ::n-triples ::rdf-serialization)

(defn ^Model read-rdf
  "Accepts an InputStream, Reader, or String (resource path) to read into a Model"
  ([src] (read-rdf src ::rdf-xml))
  ([src format] (-> (ModelFactory/createDefaultModel)
                    (.read src nil (get jena-rdf-format format)))))

(defn resource [x]
  (types/resource x))

(resource :sepio/ApproverRole)

(defn ^Statement construct-statement
  "Takes a [s p o] triple, returns a single Statement."
  [[s p o]]
  (ResourceFactory/createStatement
   (resource s)
   (cond (instance? Property p) p
         (keyword? p) (ResourceFactory/createProperty (names/kw->iri p))
         :else (ResourceFactory/createProperty p))
   (if (or (string? o) (int? o) (float? o))
     (ResourceFactory/createTypedLiteral o)
     (resource o))))

(defn create-query
  ([query-source] (create-query query-source {}))
  ([query-source params] (query/create-query query-source params)))

(defmacro declare-query [& queries]
  (let [root# (-> *ns* str (string/replace #"\." "/") (string/replace #"-" "_") (str "/"))]
    `(do ~@(map #(let [filename# (str root# (string/replace % #"-" "_" ) ".sparql")]
                   `(def ~% (-> ~filename# io/resource slurp create-query)))
                queries))))

(defn union
  "Create a new model that is the union of models"
  [& models]
  (let [union-model (ModelFactory/createDefaultModel)]
    (doseq [model models] (.add union-model model))
    union-model))

(defn to-turtle [model]
  (let [os (ByteArrayOutputStream.)]
    (.write model os "TURTLE")
    (.toString os)))

(defn pp-model
  "Print a turtle-like string of model, with iri values
  substituted for local keywords when available."
  [model]
  (let [statements (iterator-seq (.listStatements model))
        predicate-iri-kw (map #(vector % (names/iri->kw %))
                              (set (map #(str (.getPredicate %)) statements)))
        object-iri-kw (map #(vector % (names/iri->kw %))
                           (set (map #(str (.getObject %)) statements)))]
    (println
     (reduce (fn [model-str [iri kw]]
               (string/replace model-str
                          (str "<" iri ">")
                          (str kw)))
             (to-turtle (.clearNsPrefixMap model))
             (filter second ; remove when no mapping exists
                     (concat predicate-iri-kw object-iri-kw))))))


(defn difference
  "Return the model representing the elements in MODEL-ONE not in MODEL-TWO"
  [^Model model-one ^Model model-two]
  (.difference model-one model-two))

(defn is-isomorphic?
  "Return true if MODEL-ONE is isomorphic relative to MODEL-TWO"
  [^Model model-one ^Model model-two]
  (.isIsomorphicWith model-one model-two))

;; Event serialization/deserialization methods

(defmethod event/deserialize ::rdf-serialization [event]
  (assoc event
         ::event/data
         (read-rdf (-> event
                       ::event/value
                       .getBytes
                       ByteArrayInputStream.)
                   (::event/format event))))

(defmethod event/serialize ::rdf-serialization [event]
  (let [os (ByteArrayOutputStream.)]
    (.write (::event/data event)
            os
            (get jena-rdf-format (::event/format event)))
    (assoc event ::event/value (.toString os))))

(comment
  (-> {::event/format ::turtle
       ::event/value (slurp "/users/tristan/data/genegraph-neo/base/dcterms.ttl")}

      event/deserialize
      (select-keys [::event/data])
      (assoc ::event/format ::n-triples)
      event/serialize
      ::event/value))


