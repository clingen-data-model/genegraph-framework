(ns genegraph.framework.storage.rdf
  "Storage processor for an RDF triplestore based on Apache Jena"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage.rdf.instance :as i]
            [genegraph.framework.storage :as s])
  (:import [org.apache.jena.rdf.model Model Resource ModelFactory
            ResourceFactory Statement]
           [org.apache.jena.tdb2 TDB2Factory]
           [org.apache.jena.query ReadWrite QueryFactory QueryExecutionFactory Dataset]))

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
   :turtle "Turtle"})

(defn ^Model read-rdf
  "Accepts an InputStream, Reader, or String (resource path) to read into a Model"
  ([src] (read-rdf src {}))
  ([src opts] (-> (ModelFactory/createDefaultModel)
                  (.read src nil (jena-rdf-format (:format opts :rdf-xml))))))


(comment
  (def m (read-rdf "file:///users/tristan/data/genegraph/2023-01-17T1950/base/dcterms.ttl"
                  {:format :turtle}))
 (def test-db
   (-> {:name :test-rdf-dataset
        :type :rdf
        :path "/Users/tristan/Desktop/test-jena"}
       (p/init)))
 (p/start test-db)
 (s/write @(:instance test-db) "http://example.db/" m (promise))
 (type @(:instance test-db))
 (tx @(:instance test-db) (println (s/read @(:instance test-db) "http://example.db/")))
 (p/stop db))
