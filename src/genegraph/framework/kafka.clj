(ns genegraph.framework.kafka
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult OffsetSpec]
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

(defn publish [topic event]
  (.send
     (::event/producer event)
     (event->producer-record
      (assoc event ::event/kafka-topic (:kafka-topic topic)))))

(defn poll [topic]
  (when-let [e (.poll (:queue topic)
                      (:timeout topic)
                      TimeUnit/MILLISECONDS)]
    (reset! (:kafka-most-recent-offset topic) (::event/offset e))
    e))

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
                                ::event/topic name
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
    (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
      (reset! kafka-most-recent-offset (::event/offset e))
      e))

  p/Publisher
  (publish [this event]
    (publish this event))
  
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

(defrecord KafkaProducerTopic
    [name
     kafka-cluster
     kafka-topic]

  p/Publisher
  (publish [this event]
    (publish this event)))

(defmethod p/init :kafka-producer-topic [topic-definition]
  (map->KafkaProducerTopic topic-definition))

(defrecord KafkaReaderTopic
    [name
     buffer-size
     timeout
     ^BlockingQueue queue
     consumer
     consumer-thread
     kafka-cluster
     kafka-topic
     kafka-options
     status
     kafka-last-offset-at-start
     kafka-most-recent-offset
     kafka-start-offset
     exception]

  p/Lifecycle
  (start [this]
    (reset! status :running)
    (reset!
     consumer-thread
     (Thread.
      (fn []
        (let [offset @kafka-start-offset
              topic-partition [(TopicPartition. kafka-topic 0)]
              ^KafkaConsumer new-consumer (KafkaConsumer.
                                           (apply
                                            merge
                                            (:common-config kafka-cluster)
                                            (:consumer-config kafka-cluster)
                                            kafka-options))]
          (reset! consumer new-consumer)
          (try
            (.assign new-consumer [topic-partition])
            (.seek new-consumer topic-partition offset)
            (while (= :running @status)
              (->> (.poll new-consumer (Duration/ofMillis 100))
                   .iterator
                   iterator-seq
                   (map consumer-record->event)
                   (map #(assoc % ::event/topic name))
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
    (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
      (reset! kafka-most-recent-offset (::event/offset e))
      e))

  p/Publisher
  (publish [this event]
    (publish this event))
  
  p/Offsets
  (offset [this] @kafka-most-recent-offset)
  (last-offset [this] @kafka-most-recent-offset)
  (set-offset! [this offset] (deliver kafka-start-offset offset))
  (committed-offset [this] "Committed offset for topic."))

(defmethod p/init :kafka-reader-topic  [topic-definition]
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








