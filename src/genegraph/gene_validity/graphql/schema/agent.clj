(ns genegraph.gene-validity.graphql.schema.agent
  (:refer-clojure :exclude [agent]))

(def agent
  {:name :Agent
   :graphql-type :object
   :description "An agent, either an individual or an organization."
   :implements [:Resource]
   :fields {:contributions {:type '(list :Contribution)
                            :description "Contributions to entities made by this agent"
                            :path [[:sepio/has-agent :<]]}}})


