(ns genegraph.gene-validity.graphql.schema.common
  (:require [genegraph.framework.storage.rdf :as rdf]
            [com.walmartlabs.lacinia.schema :refer [tag-with-type]]))

(def transitive-evidence
  (rdf/create-query
   "select ?evidence where {
    ?statement ( :sepio/has-evidence-line | :sepio/has-evidence-item | :sepio/has-evidence | ( ^ :sepio/has-subject )  ) + ?evidence .
    ?evidence ( a / :rdfs/sub-class-of * ) ?class }"))

(def direct-evidence
  (rdf/create-query
   "select ?evidence where {
    ?statement :sepio/has-evidence ?evidence .
    ?evidence ( a / :rdfs/sub-class-of * ) ?class }"))

;; Technical debt adopted to meet a specific use case for
;; the website and Phil. Would like to transition the site away from
;; using this, will remove this if and when that is possible.
(defn- has-proband-score-cap [resource]
  (some #(= (rdf/resource :sepio/ProbandScoreCapEvidenceLine) %)
        (rdf/ld-> resource [[:sepio/has-evidence :<] :rdf/type])))

(defn- hide-nested-variant-evidence [resources]
  (remove has-proband-score-cap resources))

(defn evidence-items [context args value]
  (let [result
        (cond
          (and (:transitive args) (:class args))
          (transitive-evidence value {:statement value
                                      :class (rdf/resource (:class args))})
          (:transitive args)
          (transitive-evidence value {:statement value})
          :else (rdf/ld-> value [:sepio/has-evidence]))]
    (if (:hide_nested_variant_evidence args)
      (hide-nested-variant-evidence result)
      result)))
