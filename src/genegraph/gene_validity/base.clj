(ns genegraph.gene-validity.base
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            ;; may not need instance
            [genegraph.framework.storage.rdf.instance :as tdb-instance]))







(comment
  (def base-events
    [{:name "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
      :source "http://www.w3.org/1999/02/22-rdf-syntax-ns.ttl",
      :target "rdf.ttl",
      :format :rdf
      :reader-opts {:format :turtle}}
     {:name "http://www.w3.org/2000/01/rdf-schema#"
      :source "http://www.w3.org/2000/01/rdf-schema.ttl",
      :target "rdf-schema.ttl",
      :format :rdf
      :reader-opts {:format :turtle}}])
  
  (def ds
    (p/init
     {:name :gv-tdb
      :type :rdf
      :path "/Users/tristan/Desktop/gv_tdb"}))

  (p/start ds)

  

  )
