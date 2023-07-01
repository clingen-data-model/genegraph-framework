(ns genegraph.gene-validity
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

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
   :topics {:gene-validity-raw-dev {:initial-events []}
            :gene-valdity-raw-kafka {
                                     #_#_#_#_:kafka-cluster :dx-ccloud
                                     :kafka-topic "gene_validity_raw"}
            :gene-validity-rdf {}}
   :storage {:last-gene-validity-records
             {:type :rocksdb
              :path "/Users/tristan/data/genegraph-neo/last_gene_validity_records"}
             :gene-validity-raw-events
             {:type :rocksdb
              :path "/Users/tristan/data/genegraph-neo/gene_validity_raw_events"}}
   :processors {:gene-validity-processor
                {:subscribe :gene-validity-raw-dev
                 :interceptors `[base/load-base-data-interceptor]} }})


(comment
  (def gv (app/create gene-validity-genegraph-app))
  (p/start gv)
  (p/stop gv)
  (keys gv)
  (p/offer (:base  @(:topics gv))
           {:base/data-path "/Users/tristan/data/genegraph-neo/base"
            :value (first gene-validity-initialization-events)})
  (:base @(:topics gv))
  (let [tdb @(:instance (:gene-validity-tdb  @(:storage gv)))]
    (rdf/tx tdb
     (count
      ((rdf/create-query "select ?x where {?x a ?type}")
       tdb
       {:type :rdfs/Class}))))
  )
