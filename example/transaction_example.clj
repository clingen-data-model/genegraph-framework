(ns transaction-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage.rdf :as rdf])
  (:import [org.apache.jena.sparql.core Transactional]))


(def transaction-app
  {:type :genegraph-app
   :storage {:test-tdb
             {:type :rdf
              :name :test-tdb
              :path "/users/tristan/Desktop/test-tdb"}}})

(comment
  (def tapp (p/init transaction-app))
  (p/start tapp)
  (p/stop tapp)

  (time
   (let [db @(get-in tapp [:storage :test-tdb :instance])]
     (dotimes [n 100000]
       (.begin db org.apache.jena.query.ReadWrite/READ)
       (try (do (clojure.core/+ 1 1))
            (.commit db)
            (finally (.end db))))))

  (time
   (let [db @(get-in tapp [:storage :test-tdb :instance])]
     (dotimes [n 1000000]
       (rdf/tx db
         (+ 1 1)))))

  (macroexpand-1 `(rdf/tx db
                    (+ 1 1)))
  
  )
