(ns genegraph.framework.topic.kafka
  (:require [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common KafkaFuture]
           [java.util.concurrent TimeUnit]))
(comment
 (def a (Admin/create {"bootstrap.servers" "localhost:9092"}))

 (def res (.createTopics a [(NewTopic. "gene_validity_raw" 1 (short 1))]))

 (type res)

 (-> res (.topicId "test") (.getNow :incomplete))

 (NewTopic. "test" 1 (short 1))

 (-> a .listConsumerGroups .all (.get 100 TimeUnit/MILLISECONDS)) )


(ProducerRecord. "test" "key1" "value1")

(defn event->producer-record
  [{topic ::event/kafka-topic
    k ::event/key
    v ::event/value}]
  (if k
    (ProducerRecord. topic k v)
    (ProducerRecord. topic v)))

(event->producer-record {::event/kafka-topic "test"
                         ::event/key "k1"
                         ::event/value "v1"})

(defn publish [producer event]
  (.send producer (event->producer-record event)))

(comment

  (def gv-events
    (event-store/with-event-reader [r "/users/tristan/data/genegraph-neo/gv_events.edn.gz"]
      (into [] (event-store/event-seq r))))

  (count gv-events)

  (def producer-config
    {"bootstrap.servers" "localhost:9092"
     "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"                        "value.serializer"                "org.apache.kafka.common.serialization.StringSerializer"})
  
  (time
   (with-open [p (KafkaProducer. producer-config)]
     (run! #(publish p %)
           (map #(assoc % ::event/kafka-topic "test") gv-events))))

  )
