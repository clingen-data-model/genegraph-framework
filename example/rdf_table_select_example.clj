(ns rdf-table-select-example
  (:require [genegraph.framework.app]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rdf.names :as names]
            [portal.api :as portal])
  (:import [org.apache.jena.query Dataset ARQ
            QueryExecutionFactory QueryFactory]
           [org.apache.jena.sparql.algebra Algebra]))

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
  
  (let [q (rdf/create-query
           [:project ['x 'type]
            [:filter
             #_[:exists
              [:bgp
               ['type :rdfs/subClassOf :example/type1]]]
             [:not-exists
              [:filter
               [:in 'types :rdf/type1 :rdf/type2]
               [:bgp
                ['type :rdf/subClassOf 'types]]]]
             [:bgp
              ['x :rdf/type 'type]
              ['type :rdfs/subClassOf :example/type2]]]])]
    (println (str q))
    (q example-model {::rdf/params {:type :table}}))

  (let [q (rdf/create-query
           [:project ['x]
            [:minus
             [:bgp
              ['x :rdf/type 'type]
              ['type :rdfs/subClassOf :example/type2]]
             [:bgp
              ['type :rdf/subClassOf :example/type3]]]])]
    (println (str q))
    (q example-model #_{::rdf/params {:type :table}}))

  )


;; experiments with filter ops in algebra

(comment
  (-> (QueryFactory/create "
prefix cg: <http://dataexchange.clinicalgenome.org/terms/>
select ?x where {
?x a cg:EvidenceLevelAssertion .
filter exists { 
    ?x cg:subject cg:NotThis .
  }

}")
      Algebra/compile
      str
      println)

  (-> (QueryFactory/create
"
PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
PREFIX :     <http://example.org/book/> 
PREFIX ns:   <http://example.org/ns#> 

SELECT ?book ?title ?price
{
   VALUES ?book { :book1 :book3 }
   ?book dc:title ?title ;
         ns:price ?price .
}
"
)
      Algebra/compile
      str
      println)

    (-> (QueryFactory/create
"
PREFIX dc:   <http://purl.org/dc/elements/1.1/> 

SELECT ?person ?name WHERE {
  ?person dc:title ?name .
  FILTER EXISTS {
  ?person a ?c .
  FILTER (?person IN (<http://example.org/person1>, <http://example.org/person2>))
  }
}
"
)
      Algebra/compile
      str
      println)

  
  )

"filter not exists {
    ?x cg:subject cg:ThisEither .
  }
filter not exists {
    ?x cg:subject cg:ForSureNotThis .
  }"

;; basic (one-item) filter
"(project (?x)
  (filter (notexists (bgp (triple ?x <http://dataexchange.clinicalgenome.org/terms/subject> <http://dataexchange.clinicalgenome.org/terms/NotThis>)))
    (bgp (triple ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dataexchange.clinicalgenome.org/terms/EvidenceLevelAssertion>))))"

;; multiple item filter, requires exprlist
"(project (?x)
  (filter (exprlist (notexists (bgp (triple ?x <http://dataexchange.clinicalgenome.org/terms/subject> <http://dataexchange.clinicalgenome.org/terms/NotThis>))) (notexists (bgp (triple ?x <http://dataexchange.clinicalgenome.org/terms/subject> <http://dataexchange.clinicalgenome.org/terms/ThisEither>))) (notexists (bgp (triple ?x <http://dataexchange.clinicalgenome.org/terms/subject> <http://dataexchange.clinicalgenome.org/terms/ForSureNotThis>))))
    (bgp (triple ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dataexchange.clinicalgenome.org/terms/EvidenceLevelAssertion>))))"

"filter not exists {
    ?x cg:subject cg:ThisEither .
  }
filter not exists {
    ?x cg:subject cg:ForSureNotThis .
  }"
