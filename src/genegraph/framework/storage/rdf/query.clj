(ns genegraph.framework.storage.rdf.query
  (:require [genegraph.framework.storage.rdf.names :as names]
            [genegraph.framework.storage.rdf.types :as types]
            [genegraph.framework.storage.rdf.algebra :as algebra]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import [org.apache.jena.rdf.model Model Resource ModelFactory
            ResourceFactory Statement]
           [org.apache.jena.query ReadWrite Query QueryFactory
            QueryExecutionFactory Dataset QuerySolutionMap QuerySolution
            QueryExecutionDatasetBuilder QueryExecution]
           [org.apache.jena.sparql.algebra OpAsQuery]))

(defn- compose-select-result [qexec]
  (let [result (.execSelect qexec)]
    (mapv #(.getResource % (-> result .getResultVars first))
          (iterator-seq result))))

(defn- compose-select-table-result [qexec]
  (let [result (.execSelect qexec)]
    (mapv (fn [qs]
            (reduce (fn [m v] (assoc m (keyword v) (.getResource qs v)))
                    {}
                    (.getResultVars result)))
          (iterator-seq result))))

(defn- construct-query-solution-map [params]
  (let [query-params (filter #(-> % key namespace nil?) params)
        qs-map (QuerySolutionMap.)]
    (doseq [[k v] query-params]
      (.add qs-map (name k) (types/to-rdf-node v)))
    qs-map))

(def query-sort-order
  {:ASC Query/ORDER_ASCENDING
   :asc Query/ORDER_ASCENDING
   :DESC Query/ORDER_DESCENDING
   :desc Query/ORDER_DESCENDING})

;; TODO is cloning a query on each execution a possible
;; performance problem?
(defn construct-query-with-params [query query-params]
  (if-let [params (:genegraph.framework.storage.rdf/params query-params)]
    (let [modified-query (.clone query)]
      (when (:distinct params)
        (.setDistinct modified-query true))
      (when (:limit params)
        (.setLimit modified-query (:limit params)))
      (when (:offset params)
        (.setOffset modified-query (:offset params)))
      (when (:sort params)
        (let [{:keys [field direction]} (:sort params)]
          (.addResultVar modified-query (string/lower-case (name field)))
          (.addOrderBy modified-query
                       (string/lower-case (name field))
                       (query-sort-order direction))))
      modified-query)
    query))

(defn exec-select [qexec params]
  (case (get-in params [:genegraph.framework.storage.rdf/params :type])
    :count (-> qexec .execSelect iterator-seq count)
    :table (compose-select-table-result qexec)
    (compose-select-result qexec)))

(defn build-query-execution [query model params]
  (-> (QueryExecution/model (types/model model))
      (.query query)
      (.substitution (construct-query-solution-map params))
      .build))

(defn exec [query-def model params]
  (let [query (construct-query-with-params query-def params)]
    (with-open [qexec (build-query-execution query model params)]
      (cond
        (.isConstructType query) (.execConstruct qexec)
        (.isSelectType query) (exec-select qexec params)
        (.isAskType query) (.execAsk qexec)))))

(deftype StoredQuery [query]
  clojure.lang.IFn
  (invoke [this model] (this model {}))
  (invoke [this model params] (exec query model params))

  Object
  (toString [_] (str query)))

;; TODO fix so that non-whitespace terminated expressions are treated appropriately
(defn expand-query-str [query-str]
  (string/replace query-str
                  #":([a-zA-Z_0-9\-]+)/([a-zA-Z_0-9\-]+)"
                  (fn [[_ ns n]]
                    (str "<" (names/kw->iri (keyword ns n)) ">"))))

(defmacro declare-query [& queries]
  (let [root# (-> *ns* str (string/replace #"\." "/") (string/replace #"-" "_") (str "/"))]
    `(do ~@(map #(let [filename# (str root# (string/replace % #"-" "_" ) ".sparql")]
                   `(def ~% (-> ~filename# io/resource slurp create-query)))
                queries))))

(defn create-query
  "Return parsed query object. If query is not a string, assume object that can
use io/slurp"
  ([query-source] (create-query query-source {}))
  ([query-source params]
   (let [query (if  (coll? query-source)
                 (OpAsQuery/asQuery (algebra/op query-source))
                 (QueryFactory/create (expand-query-str
                                       (if (string? query-source)
                                         query-source
                                         (slurp query-source)))))]
     (case (::type params)
       :ask (.setQueryAskType query)
       (if  (::distinct params true)
         (.setDistinct query true)
         query))
     (->StoredQuery query))))
