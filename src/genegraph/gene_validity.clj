(ns genegraph.gene-validity
  (:require [genegraph.gene-validity.base :as base]
            [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage :as storage]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(def gene-validity-initialization-events
  (-> (io/resource "genegraph/gene_validity/base.edn")
      slurp
      edn/read-string))

(defn store-model [event]
  (storage/store event
                 (get-in event [:storage :tdb])
                 (::rdf/iri event)
                 (::rdf/model event)))

(def gene-validity-genegraph-app
  {:kafka-clusters {:dx-ccloud
                    { :common {"ssl.endpoint.identification.algorithm" "https"
                               "sasl.mechanism" "PLAIN"
                               "request.timeout.ms" "20000"
                               "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                               "retry.backoff.ms" "500"
                               "security.protocol" "SASL_SSL"
                               "sasl.jaas.config" (System/getenv "DX_JAAS_CONFIG")}
                     :consumer {"key.deserializer"
                                "org.apache.kafka.common.serialization.StringDeserializer"
                                "value.deserializer"
                                "org.apache.kafka.common.serialization.StringDeserializer"}
                     :producer {"key.serializer"
                                "org.apache.kafka.common.serialization.StringSerializer"
                                "value.serializer"
                                "org.apache.kafka.common.serialization.StringSerializer"}}}
   :topics {:base {}}
   :storage {:tdb
             {:type :rdf
              :path "/Users/tristan/data/genegraph-neo/gv_tdb"}}
   :processors {:base-processor
                {:subscribe :base
                 :initial-events gene-validity-initialization-events
                 :interceptors [base/load-base-data-interceptor]}}})


(comment
  (def gv (app/create gene-validity-genegraph-app))
  (p/start gv)
  (p/stop gv)
  (keys gv)
  (p/offer (:base  @(:topics gv))
           {::base/data-path "/Users/tristan/data/genegraph-neo/base"
            :value (first gene-validity-initialization-events)})
  (:base @(:topics gv))
  (let [tdb @(:instance (:gene-validity-tdb  @(:storage gv)))]
    (rdf/tx tdb
     (count
      ((rdf/create-query "select ?x where {?x a ?type}")
       tdb
       {:type :rdfs/Class}))))
  )
