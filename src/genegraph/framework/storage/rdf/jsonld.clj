(ns genegraph.framework.storage.rdf.jsonld
  (:require [clojure.java.io :as io]
            [charred.api :as json])
  (:import [org.apache.jena.riot.system JenaTitanium]
           [org.apache.jena.riot RDFDataMgr Lang]
           [org.apache.jena.rdf.model Model Statement ModelFactory]
           [org.apache.jena.query Dataset DatasetFactory]
           [org.apache.jena.sparql.core DatasetGraph]
           [com.apicatalog.jsonld JsonLd]
           [com.apicatalog.jsonld.serialization RdfToJsonld]
           [com.apicatalog.jsonld.document Document RdfDocument JsonDocument]
           [com.apicatalog.jsonld.lang Keywords]
           [com.apicatalog.rdf Rdf]
           [com.apicatalog.rdf.spi RdfProvider]
           [jakarta.json Json JsonReader JsonStructure JsonWriter]
           [java.net URI]
           [java.io StringWriter]))

(defn model->json-array
  [model]
  (-> (.asDatasetGraph
       (doto (DatasetFactory/create)
         (.addNamedModel "http://example/" model)))
      JenaTitanium/convert
      RdfDocument/of
      JsonLd/fromRdf
      .nativeTypes
      .get))

(defn append-context [array context]
  (-> (Json/createObjectBuilder)
      (.add Keywords/CONTEXT context)
      (.add Keywords/GRAPH array)
      (.build)))

(defn frame [doc frame]
  (-> (JsonLd/frame doc frame)
      .get))

(defn serialize-json-object [json-object]
  (with-open [sw (StringWriter.)
              jw (Json/createWriter sw)]
    (.writeObject jw json-object)
    (.toString sw)))

(defn json-file->json-object [path]
  (with-open [r (Json/createReader (io/reader path))]
    (.read r)))

(defn json-file->doc [path]
  (JsonDocument/of (json-file->json-object path)))

(defn model->json-ld [model frame-doc]
  (-> model
      model->json-array
      JsonDocument/of
      (frame frame-doc)
      serialize-json-object))

(comment
  
  (let [ttl-uri
        "file:///Users/tristan/data/genegraph-neo/jsonld/library.ttl"
        context-path
        "/Users/tristan/data/genegraph-neo/jsonld/context.json"
        frame-path
          "file:///Users/tristan/data/genegraph-neo/jsonld/library-frame.jsonld"]
      (-> (RDFDataMgr/loadModel ttl-uri Lang/TURTLE)
          (model->json-ld (json-file->doc frame-path))
          json/read-str))


  )
