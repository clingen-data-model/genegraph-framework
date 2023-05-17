(ns genegraph.gene-validity.base
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.gcs :as gcs]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [clojure.java.io :as io]
            [hato.client :as hc]
            ;; may not need instance
            [genegraph.framework.storage.rdf.instance :as tdb-instance])
  (:import [java.io File]))


(defn init [options]
  (io/make-parents (:data-path options))
  (.mkdir (File. (:data-path options))))

(comment
  (def options {:data-path "/Users/tristan/Desktop/base"})
  
  (def base-events
    (mapv #(assoc %
                 :genegraph.framework.processor/options
                 options)
     [{:name "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
       :source "http://www.w3.org/1999/02/22-rdf-syntax-ns.ttl",
       :target "rdf.ttl",
       :format :rdf
       :reader-opts {:format :turtle}}
      {:name "http://www.w3.org/2000/01/rdf-schema#"
       :source "http://www.w3.org/2000/01/rdf-schema.ttl",
       :target "rdf-schema.ttl",
       :format :rdf
       :reader-opts {:format :turtle}}]))

  (init options)

  (def ds
    (p/init
     {:name :gv-tdb
      :type :rdf
      :path "/Users/tristan/Desktop/gv_tdb"}))

  (p/start ds)

  

  )
