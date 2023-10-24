(ns genegraph.framework.kafka.admin
  "Methods for managing the lifecycle of topics."
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult OffsetSpec]
           [org.apache.kafka.common KafkaFuture]))

(defn create-admin-client [cluster-def]
  (Admin/create (:common-config cluster-def)))



(comment
  (def local-cluster
    {:common-config {"bootstrap.servers" "localhost:9092"}
     :producer-config {"key.serializer"
                       "org.apache.kafka.common.serialization.StringSerializer",
                       "value.serializer"
                       "org.apache.kafka.common.serialization.StringSerializer"}
     :consumer-config {"key.deserializer"
                       "org.apache.kafka.common.serialization.StringDeserializer"
                       "value.deserializer"
                       "org.apache.kafka.common.serialization.StringDeserializer"}})

  (with-open [admin (create-admin-client local-cluster)]
    (-> (.createTopics admin
                       [(doto (NewTopic. "gene_validity_complete" 1 (short 1))
                          (.configs {"retention.ms" "-1"}))])
        .all
        .get))

  )
