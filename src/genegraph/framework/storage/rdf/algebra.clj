(ns genegraph.framework.storage.rdf.algebra
  (:require [genegraph.framework.storage.rdf.names :as names])
  (:import [org.apache.jena.rdf.model Model Statement ResourceFactory Resource Literal RDFList SimpleSelector ModelFactory]
             [org.apache.jena.query Dataset QueryFactory Query QueryExecution
              QueryExecutionFactory QuerySolutionMap]
             [org.apache.jena.sparql.algebra AlgebraGenerator Algebra OpAsQuery Op]
             [org.apache.jena.graph Node NodeFactory Triple Node_Variable Node_Blank]
             [org.apache.jena.sparql.algebra.op OpDistinct OpProject OpFilter OpBGP OpConditional OpDatasetNames OpDiff OpDisjunction OpDistinctReduced OpExtend OpGraph OpGroup OpJoin OpLabel OpLeftJoin OpList OpMinus OpNull OpOrder OpQuad OpQuadBlock OpQuadPattern OpReduced OpSequence OpSlice OpTopN OpUnion OpTable ]
             [org.apache.jena.sparql.core BasicPattern Var VarExprList QuadPattern Quad]
             org.apache.jena.sparql.core.Prologue
             java.io.ByteArrayOutputStream))

(defn- var-seq [vars]
  (map #(Var/alloc (str %)) vars))

(defn triple
  "Construct triple for use in BGP. Part of query algebra."
  [stmt]
  (let [[s p o] stmt
        subject (cond
                  (instance? Node s) s
                  (= '_ s) Var/ANON
                  (symbol? s) (Var/alloc (str s))
                  (keyword? s) (.asNode (names/kw->iri s))
                  (string? s) (NodeFactory/createURI s)
                  :else (NodeFactory/createURI (str s)))
        predicate (cond
                    (instance? Node p) p
                    (keyword? p) (.asNode (names/kw->iri p))
                    :else (NodeFactory/createURI (str p)))
        object (cond 
                 (instance? Node o) o
                 (symbol? o) (Var/alloc (str o))
                 (keyword? o) (.asNode (names/kw->iri o))
                 (or (string? o)
                     (int? o)
                     (float? o)) (.asNode (ResourceFactory/createTypedLiteral o))
                 :else o)]
    (Triple. subject predicate object)))

(declare op)

(defn- op-union [a1 a2 & amore]
  (OpUnion. 
   (op a1)
   (if amore
     (apply op-union a2 amore)
     (op a2))))

(defn op
  "Convert a Clojure data structure to an Arq Op"
  [[op-name & [a1 a2 & amore :as args]]]
  (case op-name
    :distinct (OpDistinct/create (op a1))
    :project (OpProject. (op a2) (var-seq a1))
    ;; :filter (OpFilter/filterBy (ExprList. ^List (map expr (butlast args))) (op (last args)))
    :bgp (OpBGP. (BasicPattern/wrap (map triple args)))
    :conditional (OpConditional. (op a1) (op a2))
    :diff (OpDiff/create (op a1) (op a2))
    :disjunction (OpDisjunction/create (op a1) (op a2))
    ;; :extend (OpExtend/create (op a2) (var-expr-list a1))
    ;; :group (OpGroup/create (op (first amore))
    ;;                        (VarExprList. ^List (var-seq a1))
    ;;                        (var-aggr-list a2))
    :join (OpJoin/create (op a1) (op a2))
    :label (OpLabel/create a1 (op a2))
    ;; :left-join (OpLeftJoin/create (op a1) (op a2) (ExprList. ^List (map expr amore)))
    :list (OpList. (op a1))
    :minus (OpMinus/create (op a1) (op a2))
    :null (OpNull/create)
    ;; :order (OpOrder. (op a2) (sort-conditions a1))
    :reduced (OpReduced/create (op a1))
    :sequence (OpSequence/create (op a1) (op a2))
    :slice (OpSlice. (op a1) (long a1) (long (first amore)))
    ;; :top-n (OpTopN. (op (first amore)) (long a1) (sort-conditions a2))
    :union (apply op-union args)
    (throw (ex-info (str "Unknown operation " op-name) {:op-name op-name
                                                        :args args}))))
