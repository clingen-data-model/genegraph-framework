(ns genegraph.framework.kafka
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult OffsetSpec]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerGroupMetadata
            OffsetAndMetadata ConsumerRebalanceListener]
           [org.apache.kafka.common KafkaFuture TopicPartition]
           [java.time Duration]
           [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

;; TODO Start here
;; Cleanup code, there's a fair amount of duplicated code
;; 

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

;; TODO, rename or move to generic topic ns
(defn poll
  "Poll event queue for events (already pulled from Kafka)."
  [topic]
  (.poll (:queue topic)
         (:timeout topic)
         TimeUnit/MILLISECONDS))

(defn deliver-event
  "Offer event to local queue"
  [topic event]
  (.offer (:queue topic)
          event
          (:timeout topic)
          TimeUnit/MILLISECONDS))

(defn poll-kafka-consumer
  "default method to poll a Kafka consumer for new records.
  returns vector of events converted to Clojure data"
  [consumer]
  (->> (.poll consumer (Duration/ofMillis 100))
       .iterator
       iterator-seq
       (mapv consumer-record->event)))

(def topic-defaults
  {:timeout 1000
   :buffer-size 100
   :kafka-options {"enable.auto.commit" false
                   "auto.offset.reset" "earliest"}})

(defn create-kafka-consumer [topic]
  (let [config (apply
                merge
                (get-in topic [:kafka-cluster :common-config])
                (get-in topic [:kafka-cluster :consumer-config])
                (:kafka-options topic))]
    (if-let [cg (:kafka-consumer-group topic)]
      (KafkaConsumer. (assoc config "group.id" cg))
      (KafkaConsumer. config))))

(defn backing-store-lags-consumer-group? [topic]
  (let [state @(:state topic)
        local-offset (deref (:initial-local-offset state) 100 :timeout)
        cg-offset (deref (:initial-consumer-group-offset state) 100 :timeout)]
    (if (or (= :timeout local-offset) (= :timeout cg-offset))
      :timeout
      (< local-offset cg-offset))))


(defn produce-local-events
  "Provide events based on offset provided by local storage.
   If nil is provided for initial offset, return immediately.
   If the maximum available offset is nil, or not provided."
  ([topic] (produce-local-events topic Long/MAX_VALUE))
  ([topic last-offset]
   (try 
     (when-let [offset @(:initial-local-offset @(:state topic))]
       (let [topic-partition (TopicPartition. (:kafka-topic topic) 0)
             ^KafkaConsumer consumer (create-kafka-consumer topic)]
         (.assign consumer [topic-partition])
         (.seek consumer topic-partition offset)
         (while (and (= :running (:status @(:state topic))))
           (->> (poll-kafka-consumer consumer)
                (map #(assoc %
                             ::event/topic (:name topic)
                             ::event/skip-publish-effects true))
                (run! #(deliver-event topic %))))
         (.unsubscribe consumer)))
     (catch Exception e (p/exception topic e)))))

(defn update-local-storage!
  "Check to see if the most recently stored events in local storage are
  up to date. If not, push them onto the queue, avoiding publish effects.

  Return the first set of events pulled from the consumer-group backed
  consumer, since it's necessary to poll (possibly multiple times) for
  the subscription to register on the server and for the consumer offsets
  to be readable."
  [topic]
  (let [consumer (:kafka-consumer @(:state topic))]
    (loop [events (poll-kafka-consumer consumer)]
      (case (backing-store-lags-consumer-group? topic)
        true (do (produce-local-events topic) events)
        false events
        :timeout (recur (poll-kafka-consumer consumer))))))

(defn last-committed-offset [topic]
  (let [consumer (:kafka-consumer @(:state topic))]
    (some-> (.committed consumer
                        (.assignment consumer)
                        (Duration/ofMillis (:timeout topic)))
            vals
            first
            .offset)))

(defn consumer-rebalance-listener [topic]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [_ partitions]
      (println "partitions assigned")
      (deliver (:initial-consumer-group-offset @(:state topic))
               (last-committed-offset topic)))
    (onPartitionsLost [_ partitions] (println "partitons lost"))
    (onPartitionsRevoked [_ partitions] (println "partitions revoked"))))

(defrecord KafkaConsumerGroupTopic
    [name
     buffer-size
     timeout
     ^BlockingQueue queue
     kafka-cluster
     kafka-options
     kafka-consumer-group
     kafka-topic
     state]

    p/Lifecycle
    (start [this]
      (swap! state assoc :status :running)
      (.start
       (Thread.
        (fn []
          (let [^KafkaConsumer new-consumer (create-kafka-consumer this)]
            (try
              (swap! state assoc :kafka-consumer new-consumer)
              (.subscribe new-consumer
                          [kafka-topic]
                          (consumer-rebalance-listener this))
              (loop [events (update-local-storage! this)]
                (run! #(deliver-event
                        this
                        (assoc %
                               ::event/topic name
                               ::event/consumer-group kafka-consumer-group) )
                      events)
                (when (= :running (:status @state))
                  (recur (poll-kafka-consumer new-consumer))))
              (swap! state assoc :status :stopped)
              (.unsubscribe new-consumer)
              (catch Exception e (p/exception this e))))))))
  
    (stop [this]
      (swap! state assoc :status :stopping))

    p/Consumer
    (poll [this]
      (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
        (swap! state assoc :kafka-most-recent-offset (::event/offset e))
        e))

    p/Publisher
    (publish [this event]
      (publish this event))
  
    p/Offsets
    (offset [this] "Local offset for topic")
    (last-available-offset [this] "Last available offset for topic")
    (last-committed-offset [this] nil)
    (set-offset! [this offset] (deliver (:initial-local-offset @state) offset))
    (committed-offset [this] "Committed offset for topic."))

(defmethod p/init :kafka-consumer-group-topic  [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->KafkaConsumerGroupTopic
     (assoc topic-def-with-defaults
            :queue (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))
            :state (atom {:status :stopped
                          :initial-local-offset (promise)
                          :initial-consumer-group-offset (promise)})))))

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
  (last-available-offset [this] @kafka-most-recent-offset)
  (last-committed-offset [this] nil)
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








