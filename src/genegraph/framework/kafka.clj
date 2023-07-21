(ns genegraph.framework.kafka
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common KafkaFuture]
           [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(defn producer-config [topic]
  (merge (get-in topic [:kafka-cluster :common-config])
         (get-in topic [:kafka-cluster :producer-config])))

(defrecord KafkaCluster [common-config
                         consumer-config
                         producer-config
                         producer]
  p/Lifecycle
  (start [this]
    (reset! producer (KafkaProducer. (merge producer-config common-config))))
  (stop [this]
    (.close @producer)
    (reset! producer nil)))

(defmethod p/init :kafka-cluster [def]
  (map->KafkaCluster (assoc def :producer (atom nil))))

(comment
  (def cluster (p/init
                {:name :local
                 :type :kafka-cluster
                 :common-config
                 {"bootstrap.servers" "localhost:9092"}
                 :producer-config
                 {"key.serializer"
                  "org.apache.kafka.common.serialization.StringSerializer",
                  "value.serializer"
                  "org.apache.kafka.common.serialization.StringSerializer"}}))

  (p/start cluster)
  (p/stop cluster)
  
  )

(defn event->producer-record
  [{topic ::event/kafka-topic
    k ::event/key
    v ::event/value}]
  (println topic k v)
  (if k
    (ProducerRecord. topic k v)
    (ProducerRecord. topic v)))

(def topic-defaults
  {:timeout 1000
   :buffer-size 100})

(defn publish [producer event]
  (.send producer (event->producer-record event)))

(defrecord KafkaConsumerGroupTopic
    [name
     buffer-size
     timeout
     queue
     consumer
     kafka-cluster
     kafka-topic
     state
     kafka-last-offset-at-start
     kafka-most-recent-offset]

  p/Lifecycle
  (start [this]
    )
  (stop [this]
    )

  p/Topic
  (publish [this event]
    (.send @(:producer cluster)
           (event->producer-record
            (assoc event
                   ::event/kafka-topic
                   kafka-topic))))
  (offset [this] "Local offset for topic")
   (last-offset [this] "Last available offset for topic")
  (set-offset! [this offset] "Set local offset for topic")
  (committed-offset [this] "Committed offset for topic.")
  (commit-offset! [this offset] "Commit offset to durable storage"))

(defmethod p/init :kafka-consumer-group-topic  [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->KafkaConsumerGroupTopic
     (assoc topic-def-with-defaults
            :queue (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))
            :consumer (atom nil)
            :producer (atom nil)
            :state (atom :stopped)
            ;; arbitrarily set > kafka-most-recent so we don't trigger up-to-date?
            :kafka-last-offset-at-start (atom 1) 
            :kafka-most-recent-offset (atom 0)
            :initial-events-complete (atom false)))))

(comment

  (def cluster (p/init
                {:name :local
                 :type :kafka-cluster
                 :common-config
                 {"bootstrap.servers" "localhost:9092"}
                 :producer-config
                 {"key.serializer"
                  "org.apache.kafka.common.serialization.StringSerializer",
                  "value.serializer"
                  "org.apache.kafka.common.serialization.StringSerializer"
                  "transactional.id" "testproducer"}}))

  (def t (p/init {:name :test-topic
                  :type :kafka-consumer-group-topic
                  :run-kafka-producer true
                  :kafka-cluster cluster
                  :kafka-topic "test"}))

  (p/start cluster)
  (p/stop cluster)

  (p/start t)
  (publish t {::event/key "akey"
              ::event/value "avalue"})
  (p/stop t)

  )



(comment
  (event->producer-record {::event/kafka-topic "test"
                           ::event/key "k1"
                           ::event/value "v1"})
  )



(comment
 (def a (Admin/create {"bootstrap.servers" "localhost:9092"}))

 (def res (.createTopics a [(NewTopic. "gene_validity_raw" 1 (short 1))]))

 (type res)

 (-> res (.topicId "test") (.getNow :incomplete))

 (NewTopic. "test" 1 (short 1))

 (-> a .listConsumerGroups .all (.get 100 TimeUnit/MILLISECONDS)) )

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
