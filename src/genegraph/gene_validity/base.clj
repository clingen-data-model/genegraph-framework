;; TODO delete this file; moving base functionality into a separate module

(ns genegraph.gene-validity.base
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.gcs :as gcs]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [hato.client :as hc]
            ;; may not need instance
            [genegraph.framework.storage.rdf.instance :as tdb-instance])
  (:import [java.io File InputStream OutputStream]
           [java.nio.channels Channels]))

;; formats
;; RDF: RDFXML, Turtle, JSON-LD
;; ucsc-cytoband
;; affiliations
;; loss-intolerance
;; hi-index
;; features
;; genes
;;

(defn init [options]
  (io/make-parents (:data-path options))
  (.mkdir (File. (:data-path options))))

;; TODO -- consider moving this to other file; may be a separate
;; operation from loading base files

(defn fetch-file [file-description options]
  (let [response (hc/get (:source file-description)
                         {:http-client (hc/build-http-client {:redirect-policy :always})
                          :as :stream})]
    (when (instance? InputStream (:body response))
      (with-open [wc (gcs/open-write-channel (:bucket options) (:target file-description))
                  os (Channels/newOutputStream wc)]
        (.transferTo (:body response) os)))))

(comment
  (fetch-file (first base-files)
              {:data-path "/Users/tristan/data/genegraph-neo/base/"
               :bucket "genegraph-framework-dev"})

  )

;; intention would be to trigger this via api call, partially
;; so that only one Genegraph instance gets tasked with doing this
(defn refresh-base [options]
  )



;; name -- graph name for file
;; source -- remote source for file
;; target -- local filename
;; format -- method used to parse and interpret file
;; reader-opts -- for RDF data, rdf language
;; refresh-interval -- frequency with which to check for updates for the given file

(defn load-base-file
  [event]
  (let [file-definition (:value event)]
    (with-open [is (io/input-stream
                    (str (::data-path event)
                         "/"
                         (:target file-definition)))]
      (assoc event
             ::rdf/model (rdf/read-rdf is (:reader-opts file-definition))
             ::rdf/iri (:name file-definition)))))

(comment
  (load-base-file {::data-path "/Users/tristan/data/genegraph/2023-04-13T1609/base"
                   :value {:name "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                           :source "http://www.w3.org/1999/02/22-rdf-syntax-ns.ttl",
                           :target "rdf.ttl",
                           :format :rdf
                           :reader-opts {:format :turtle}}})
  )

(comment

  (def options {:data-path "/Users/tristan/Desktop/base"
                :bucket "genegraph-framework-dev"
                :base-files base-files})
  
  (def base-events
    (mapv #(assoc %
                 :genegraph.framework.processor/options
                 options)
          base-files))

  (init options)
  
  (def ds
    (p/init
     {:name :gv-tdb
      :type :rdf
      :path "/Users/tristan/data/genegraph-neo/gv_tdb"}))

  (p/start ds)

  ds

  

  )
