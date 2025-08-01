(ns genegraph.framework.storage.rdf.algebra
  (:require [genegraph.framework.storage.rdf.names :as names]
            [io.pedestal.log :as log])
  (:import [org.apache.jena.rdf.model Model Statement ResourceFactory Resource Literal RDFList ModelFactory]
             [org.apache.jena.query Dataset QueryFactory Query QueryExecution
              QueryExecutionFactory QuerySolutionMap]
             [org.apache.jena.sparql.algebra AlgebraGenerator Algebra OpAsQuery Op]
             [org.apache.jena.graph Node NodeFactory Triple Node_Variable Node_Blank]
             [org.apache.jena.sparql.algebra.op OpDistinct OpProject OpFilter OpBGP OpConditional OpDatasetNames OpDisjunction OpDistinctReduced OpExtend OpGraph OpGroup OpJoin OpLabel OpLeftJoin OpList OpMinus OpNull OpOrder OpQuad OpQuadBlock OpQuadPattern OpReduced OpSequence OpSlice OpTopN OpUnion OpTable ]
             [org.apache.jena.sparql.core BasicPattern Var VarExprList QuadPattern Quad]
             [org.apache.jena.sparql.expr Expr NodeValue ExprVar ExprList
              E_NotExists E_Exists E_OneOf E_NotOneOf E_GreaterThan E_GreaterThanOrEqual E_Equals
              E_LessThan E_LessThanOrEqual]
             [java.util List]
             org.apache.jena.sparql.core.Prologue
             java.io.ByteArrayOutputStream))

(defn- var-seq [vars]
  (map #(Var/alloc (str %)) vars))

(defn ->expr
  "Attempts to coerce the given object into an expr for use in
  OneOf style expressions"
  [s]
  (cond
    (symbol? s) (-> s
                    str
                    Var/alloc
                    ExprVar.)
    (keyword? s) (-> s
                     names/kw->iri
                     NodeFactory/createURI
                     NodeValue/makeNode)
    (string? s) (NodeValue/makeString s)
    (integer? s) (NodeValue/makeInteger s)
    (double? s) (NodeValue/makeDouble s)
    (float? s) (NodeValue/makeFloat s)))

(defn triple
  "Construct triple for use in BGP. Part of query algebra."
  [stmt]
  (let [[s p o] stmt
        subject (cond
                  (instance? Node s) s
                  (= '_ s) Var/ANON
                  (symbol? s) (Var/alloc (str s))
                  (keyword? s) (NodeFactory/createURI (names/kw->iri s))
                  (string? s) (NodeFactory/createURI s)
                  :else (NodeFactory/createURI (str s)))
        predicate (cond
                    (instance? Node p) p
                    (keyword? p) (NodeFactory/createURI (names/kw->iri p))
                    :else (NodeFactory/createURI (str p)))
        object (cond 
                 (instance? Node o) o
                 (symbol? o) (Var/alloc (str o))
                 (keyword? o) (NodeFactory/createURI (names/kw->iri o))
                 (or (string? o)
                     (int? o)
                     (float? o)) (.asNode (ResourceFactory/createTypedLiteral o))
                 :else o)]
    (Triple/create subject predicate object)))

(declare op)
(declare expr)

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
    :filter (OpFilter/filterBy
             (ExprList. (mapv expr (butlast args)))
             (op (last args)))
    :bgp (OpBGP. (BasicPattern/wrap (map triple args)))
    :conditional (OpConditional. (op a1) (op a2))
    ;; :diff (OpDiff/create (op a1) (op a2))
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

(defn expr
  [[f & [a1 & amore :as args]]]
  (case f
    :not-exists (E_NotExists. (op a1))
    :exists (E_Exists. (op a1))
    :in (E_OneOf. (->expr a1) (ExprList. (mapv ->expr amore)))
    :not-in (E_NotOneOf. (->expr a1) (ExprList. (mapv ->expr amore)))
    :< (E_LessThan. (->expr a1) (->expr (first amore)))
    :<= (E_LessThanOrEqual. (->expr a1) (->expr (first amore)))
    := (E_Equals. (->expr a1) (->expr (first amore)))
    :> (E_GreaterThan. (->expr a1) (->expr (first amore)))
    :>= (E_GreaterThanOrEqual. (->expr a1) (->expr (first amore)))))

