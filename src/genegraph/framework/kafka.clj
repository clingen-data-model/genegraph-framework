(ns genegraph.framework.kafka
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerGroupMetadata
            OffsetAndMetadata]
           [org.apache.kafka.common KafkaFuture TopicPartition]
           [java.time Duration]
           [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(defn create-producer [cluster-def opts]
  (doto (KafkaProducer.
         (merge (:common-config cluster-def)
                (:producer-config cluster-def)
                opts))
    .initTransactions))

(defn commit-offset [event]
  (println "commit offset " event)
  (try
    (.sendOffsetsToTransaction
     (::event/producer event)
     {(TopicPartition. (::event/kafka-topic event) 0)
      (OffsetAndMetadata. (+ 1 (::event/offset event)))} ; use next offset, per kafka docs
     (ConsumerGroupMetadata. (::event/consumer-group event)))
    event
    (catch Exception e (assoc event ::event/error true))))

(defn create-admin [cluster-def]
  (Admin/create (:common-config cluster-def)))

(defn event->producer-record
  [{topic ::event/kafka-topic
    k ::event/key
    v ::event/value}]
  (if k
    (ProducerRecord. topic k v)
    (ProducerRecord. topic v)))

(defn consumer-record->event [record]
  {::event/key (.key record)
   ::event/value (.value record)
   ::event/timestamp (.timestamp record)
   ::event/kafka-topic (.topic record)
   ::event/source :kafka
   ::event/offset (.offset record)})

(def topic-test-app
  {:type :genegraph-app
   :kafka-clusters {:local
                    {:common-config {"bootstrap.servers" "localhost:9092"}
                     :producer-config {"key.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer",
                                       "value.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"}
                     :consumer-config {"key.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"
                                       "value.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"}}}
   :topics {:test
            {:name :test
             :type :kafka-consumer-group-topic
             :kafka-cluster :local
             :kafka-topic "test"
             :kafka-consumer-group "testcg"}}})

(comment
  (with-open [a (create-admin (get-in app-def-2 [:kafka-clusters :local]))]
    (-> a .listTopics .names .get))

  (with-open [a (create-admin (get-in app-def-2 [:kafka-clusters :local]))]
    (.deleteTopics a ["test" "gene_validity_raw"]))

  (with-open [a (create-admin (get-in app-def-2 [:kafka-clusters :local]))]
    (.createTopics a [(NewTopic. "test" 1 (short 1))]))

  (with-open [a (create-admin (get-in topic-test-app [:kafka-clusters :local]))]
    (.alterConsumerGroupOffsets
     a
     "testcg1"
     {(TopicPartition. "test" 0)
      (OffsetAndMetadata. 0)}))

)

(def topic-defaults
  {:timeout 1000
   :buffer-size 100
   :kafka-options {"enable.auto.commit" false
                   "auto.offset.reset" "earliest"}})


(defrecord KafkaConsumerGroupTopic
    [name
     buffer-size
     timeout
     ^BlockingQueue queue
     consumer
     consumer-thread
     kafka-cluster
     kafka-topic
     kafka-consumer-group
     kafka-options
     status
     kafka-last-offset-at-start
     kafka-most-recent-offset
     exception]

  p/Lifecycle
  (start [this]
    (reset! status :running)
    (reset!
     consumer-thread
     (Thread.
      (fn []
        (let [^KafkaConsumer new-consumer (KafkaConsumer.
                                           (apply
                                            merge
                                            (:common-config kafka-cluster)
                                            (:consumer-config kafka-cluster)
                                            kafka-options
                                            {"group.id" kafka-consumer-group}))]
          (reset! consumer new-consumer)
          (try
            (.subscribe new-consumer [kafka-topic])
            (while (= :running @status)
              (->> (.poll new-consumer (Duration/ofMillis 100))
                   .iterator
                   iterator-seq
                   (map consumer-record->event)
                   (map #(assoc %
                                ::event/topic-name name
                                ::event/consumer-group kafka-consumer-group))
                   ;; TODO implement circuit breaker;
                   ;; this is going to drop messages if the queue fills
                   ;; and does not empty quickly enough
                   (run! #(.offer queue % timeout TimeUnit/MILLISECONDS))))
            (reset! status :stopped)
            (catch Exception e
              (do (reset! status :exception)
                  (reset! exception e))))
          (try 
            (.unsubscribe new-consumer)
            (catch Exception e (reset! status :exception)))))))
    (.start @consumer-thread))
  
  (stop [this]
    (reset! status :stopping))

  p/Consumer
  (poll [this]
    (when-let [e (.poll (:queue this)
                        (:timeout this)
                        TimeUnit/MILLISECONDS)]
      (when (= :kafka (::event/source e))
        (reset! kafka-most-recent-offset (::event/offset e)))
      e))

  p/Publisher
  (publish [this event]
    (.send
     (::event/producer event)
     (event->producer-record (assoc event ::event/kafka-topic kafka-topic))))
  
  p/Offsets
  (offset [this] "Local offset for topic")
  (last-offset [this] "Last available offset for topic")
  (set-offset! [this offset] "Set local offset for topic")
  (committed-offset [this] "Committed offset for topic."))

(defmethod p/init :kafka-consumer-group-topic  [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->KafkaConsumerGroupTopic
     (assoc topic-def-with-defaults
            :queue (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))
            :consumer (atom nil)
            :consumer-thread (atom nil)
            :status (atom :stopped)
            ;; arbitrarily set > kafka-most-recent so we don't trigger up-to-date?
            :kafka-last-offset-at-start (atom 0) 
            :kafka-most-recent-offset (atom -1)
            :initial-events-complete (atom false)
            :exception (atom nil)))))

(comment
  (def t2 (p/init topic-test-app))
  (p/start t2)
  @(get-in t2 [:topics :test :status])
  (clojure.stacktrace/print-stack-trace @(get-in t2 [:topics :test :exception]))
  (p/stop t2)
  (with-open [p (create-producer (get-in t2 [:kafka-clusters :local])
                                 {"transactional.id" "test-producer"})]
    (.initTransactions p)
    (.beginTransaction p)
    (p/publish (get-in t2 [:topics :test])
               {::event/key "k2"
                ::event/value "v2"
                ::event/producer p})
    (.commitTransaction p))
  
  )

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
