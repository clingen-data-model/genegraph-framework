(ns genegraph.framework.topic
  "Defines the logic for handling topics"
  (:require [clojure.spec.alpha :as spec]
            [genegraph.framework.protocol :as p])
  (:import  [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]
            [java.util List ArrayList Properties]
            [java.time Duration]
            [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecord ConsumerRecords]
            [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]
            [org.apache.kafka.common PartitionInfo TopicPartition]))

(spec/def ::topic
  (spec/keys :req-un [::name]
             :opt-un [::buffer-size ::timeout]))

(spec/def ::name keyword?)
(spec/def ::buffer-size int?)
(spec/def ::timeout int?)

(def topic-defaults
  {:timeout 1000
   :buffer-size 100})

(defn- start-kafka-consumer [topic]
  (let [topic-partitions [(TopicPartition. (:kafka-topic topic) 0)]
        consumer (doto (KafkaConsumer.
                        (merge (get-in topic [:kafka-cluster :common])
                               (get-in topic [:kafka-cluster :consumer])))
                   (.assign topic-partitions)
                   (.seekToBeginning topic-partitions))]
    (reset! (:state topic) :running)
    (reset! (:consumer topic) consumer)
    (.start 
     (Thread.
      (fn [] (while (= :running @(:state topic))
               (->> (.poll consumer (Duration/ofMillis 100))
                    .iterator
                    iterator-seq
                    (run! #(p/offer topic %)))))))))

(defrecord Topic [name
                  buffer-size
                  timeout
                  queue
                  consumer
                  kafka-cluster
                  state]
  
  p/Lifecycle
  (start [this]
    (when (and  (:kafka-cluster this) (:kafka-topic this))
      (start-kafka-consumer this)))
  (stop [this]
    (reset! state :stopped))

  p/Queue
  (poll [this]
    (.poll (:queue this)
           (:timeout this)
           TimeUnit/MILLISECONDS))
  (offer [this x]
    (.offer (:queue this)
            x
            (:timeout this)
            TimeUnit/MILLISECONDS)))

(defmethod p/init :topic  [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->Topic
     (assoc topic-def-with-defaults
            :queue (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))
            :consumer (atom nil)
            :state (atom :stopped)))))

(comment
  (def t (p/init {:name :test
                  :type :topic
                  :buffer-size 3}))
  (p/offer t {:hi :there})
  (p/poll t)

  (let [tps [(TopicPartition. "actionability" 0)]]
    (with-open [c (doto (KafkaConsumer.
                         {"ssl.endpoint.identification.algorithm" "https",
                          "sasl.mechanism" "PLAIN",
                          "request.timeout.ms" "20000",
                          "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092",
                          "retry.backoff.ms" "500",
                          "security.protocol" "SASL_SSL",
                          "sasl.jaas.config" (System/getenv "DX_JAAS_CONFIG"),
                          "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
                          "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})
                    (.assign tps)
                    (.seekToBeginning tps))]
      #_(.partitionsFor c "actionability")
      (->> (.poll c (Duration/ofMillis 100))
           .iterator
           iterator-seq
           count))))

#_(spec/explain ::topic {::aname :test})

