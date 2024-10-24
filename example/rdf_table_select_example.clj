(ns rdf-table-select-example
  (:require [genegraph.framework.app]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rdf.names :as names]
            [portal.api :as portal]))

(comment
  (do (def p-window (portal/open))
      (add-tap #'portal/submit))
  )

(names/add-prefixes
 {"rdfs" "http://www.w3.org/2000/01/rdf-schema#"
  "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  "example" "http://example.com/"})

(def example-model
  (rdf/statements->model
   [[:example/test1 :rdf/type :example/type1]
    [:example/type1 :rdfs/subClassOf :example/type2]]))



(comment
  (rdf/pp-model example-model)
  
  (let [q (rdf/create-query "
select ?x ?type where {
 ?x a ?type .
 ?type :rdfs/subClassOf :example/type2 . }")]
    (q example-model {::rdf/params {:type :table}}))

  )
