(ns genegraph.gene-validity
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.processor :as processor]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.gene-validity.gci-model :as gci-model]
            [genegraph.gene-validity.sepio-model :as sepio-model]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))


(def prop-query
  (rdf/create-query "select ?x where {?x a ?type}"))

(defn add-action [event]
  (assoc event
         ::event/action
         (if (re-find #"\"publishClassification\": ?true"
                      (::event/value event))
           :publish
           :unpublish)))

(defn add-iri [event]
  (assoc event
         ::event/iri
         (-> (prop-query
              (::event/model event)
              {:type :sepio/GeneValidityProposition})
             first
             str)))

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
   :topics {:gene-validity-raw-dev {:initial-events {:type :file
                                                     :base "/users/tristan/data/genegraph-neo/"
                                                     :path "gene_validity_initial_events.edn.gz"}}
            :gene-valdity-raw-kafka {:kafka-cluster :dx-ccloud
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

(def gene-validity-transform
  (p/init
   {:type :processor
    :name :gene-validity-processor
    :interceptors `[gci-model/add-gci-model
                    sepio-model/add-model
                    add-action
                    add-iri]
    ::event/metadata {::event/format :json}}))

(comment
  (def gv-topic {:type :topic
                 :initial-events {:type :file
                                  :base "/users/tristan/data/genegraph-neo/"
                                  :path "gene_validity_initial_events.edn.gz"}
                 :kafka-topic "gene_validity_raw"
                 :start-kafka true
                 :kafka-cluster { :common {"ssl.endpoint.identification.algorithm" "https"
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
                                            "org.apache.kafka.common.serialization.StringSerializer"}}})

  (def gv-events "/users/tristan/data/genegraph-neo/gv_events.edn.gz")

  (genegraph.framework.topic/store-events-in-event-store gv-events gv-topic)

  (def e
    (event-store/with-event-reader [r gv-events]
      (into [] (take 10 (event-store/event-seq r)))))
  
  (def e1 (first e))

  (-> (processor/process-event p e1) ::event/model .size)
  
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
