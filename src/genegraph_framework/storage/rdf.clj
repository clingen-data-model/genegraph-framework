(ns genegraph-framework.storage.rdf
  "Storage processor for an RDF triplestore based on Apache Jena"
  (:require [genegraph-framework.protocol :as p]
            [genegraph-framework.storage.rdf.instance :as i]
            [genegraph-framework.storage :as s])
  (:import [org.apache.jena.rdf.model Model Resource ModelFactory]))

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

(comment
 (def db
   (-> {:name :test-rdf-dataset
        :type :rdf
        :path "/Users/tristan/Desktop/test-jena"}
       (p/init)))

 (p/start db)
 (s/write @(:instance db) "http://example.db/" (ModelFactory/createDefaultModel) (promise))
 (s/read @(:instance db) "http://example.db/")
 (p/stop db))
