(ns genegraph.framework.kafka
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [io.pedestal.log :as log])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult OffsetSpec]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerGroupMetadata
            OffsetAndMetadata ConsumerRebalanceListener]
           [org.apache.kafka.common KafkaFuture TopicPartition]
           [java.time Duration]
           [java.util.concurrent BlockingQueue ArrayBlockingQueue LinkedBlockingQueue TimeUnit]))

(defn create-producer [cluster-def opts]
  (doto (KafkaProducer.
         (merge (:common-config cluster-def)
                (:producer-config cluster-def)
                opts))
    .initTransactions))

(defn commit-offset [event]
  (.sendOffsetsToTransaction
   (::event/producer event)
   {(TopicPartition. (::event/kafka-topic event) 0)
    (OffsetAndMetadata. (+ 1 (::event/offset event)))} ; use next offset, per kafka docs
   (ConsumerGroupMetadata. (::event/consumer-group event)))
  event)

(defn commit-transaction [event]
  (.commitTransaction (::event/producer event)))

(defn event->producer-record
  [{topic ::event/kafka-topic
    k ::event/key
    v ::event/value
    partition ::event/partition
    ts ::event/timestamp
    :or {partition 0, ts (System/currentTimeMillis)}}]
  (ProducerRecord. topic (int partition) ts k v))

(defn consumer-record->event [record]
  {::event/key (.key record)
   ::event/value (.value record)
   ::event/timestamp (.timestamp record)
   ::event/kafka-topic (.topic record)
   ::event/partition (.partition record)
   ::event/source :kafka
   ::event/offset (.offset record)
   ::event/completion-promise (promise)})

(defn publish [topic event]
  (.send
   (::event/producer event)
   (-> event
       (assoc ::event/kafka-topic (:kafka-topic topic)
              ::event/format (:serialization topic))
       event/serialize
       event->producer-record)))

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

(defn end-offset [consumer]
  (-> (.endOffsets consumer (.assignment consumer)) first val))

(defn poll-kafka-consumer
  "default method to poll a Kafka consumer for new records.
  returns vector of events converted to Clojure data. Optionally
  accepts a map M of metadata to append to consumer records."
  ([consumer]
   (poll-kafka-consumer consumer {}))
  ([consumer m]
   (->> (.poll consumer (Duration/ofMillis 100))
        .iterator
        iterator-seq
        (mapv #(-> % consumer-record->event (merge m))))))

(defn consumer-topic-defaults [topic-def]
  (merge {:timeout 1000
          :queue (ArrayBlockingQueue. (:buffer-size topic-def 100))
          :event-status-queue (LinkedBlockingQueue.)
          :kafka-options {"enable.auto.commit" false
                          "auto.offset.reset" "earliest"
                          "isolation.level" "read_committed"}
          :initial-local-offset (promise)
          :end-offset-at-start (promise)
          :initial-consumer-group-offset (promise)
          :state (atom {:delivered-up-to-date-event? false
                        :status :stopped})}
         topic-def))

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
  (let [local-offset (deref (:initial-local-offset topic) 100 :timeout)
        cg-offset (deref (:initial-consumer-group-offset topic) 100 :timeout)]
    (log/info :fn ::backing-store-lags-consumer-group?
              :topic (:name topic)
              :local-offset local-offset
              :cg-offset cg-offset)
    (cond
      (or (= :timeout local-offset) (= :timeout cg-offset)) :timeout
      (and (number? local-offset) (number? cg-offset)) (< local-offset cg-offset)
      :else false)))


(defn kafka-position
  "Retrieve the position of the next record to be consumed from the
  Kafka consumer. Uses the active consumer for the topic if not otherwise
  provided. Throws exception if position cannot be read before topic
  timeout."
  ([topic]
   (kafka-position topic (:kafka-consumer @(:state topic))))
  ([topic consumer]
   (.position consumer
              (TopicPartition. (:kafka-topic topic) 0)
              (Duration/ofMillis (or (:timeout topic) 5000)))))

(defn create-local-kafka-consumer
  "Create a local Kafka consumer, initialzed to initial-offset"
  ([topic]
   (create-local-kafka-consumer topic @(:initial-local-offset topic)))
  ([topic initial-offset]
   (let [topic-partition (TopicPartition. (:kafka-topic topic) 0)]
     (doto (create-kafka-consumer topic)
       (.assign [topic-partition])
       (.seek topic-partition initial-offset)))))

(defn produce-local-events
  "Provide events based on offset provided by local storage.
   If nil is provided for initial offset, return immediately.
   If the maximum available offset is nil, or not provided."
  [topic]
  (let [last-offset @(:initial-consumer-group-offset topic)]
    (try 
      (when-let [offset @(:initial-local-offset topic)]
        (let [^KafkaConsumer consumer (create-local-kafka-consumer topic offset)]
          (while (and (= :running (:status @(:state topic)))
                      (< (kafka-position topic consumer) last-offset))
            (->> (poll-kafka-consumer
                  consumer
                  {::event/format (:serialization topic)})
                 (filter #(< (::event/offset %) last-offset))
                 (map #(assoc %
                              ::event/topic (:name topic)
                              ::event/skip-publish-effects true))
                 (run! #(deliver-event topic %))))
          (.unsubscribe consumer)))
      (catch Exception e (p/exception topic e)))))

(defn topic->event-file
  "Copy the events in a kafka-backed topic to an event seq file."
  [topic event-store-handle]
  (with-open [consumer (create-local-kafka-consumer (p/init topic) 0)]
    (event-store/with-event-writer [ew event-store-handle]
      (loop [events (poll-kafka-consumer
                     consumer
                     {::event/format (:serialization topic)})]
        (run! prn events)
        (when (< (kafka-position topic consumer) (end-offset consumer))
          (recur (poll-kafka-consumer
                  consumer
                  {::event/format (:serialization topic)})))))))

(comment
  (topic->event-file
   {:name :test-reader
    :type :kafka-reader-topic
    :kafka-cluster {:common-config {"bootstrap.servers" "localhost:9092"}
                    :producer-config {"key.serializer"
                                      "org.apache.kafka.common.serialization.StringSerializer",
                                      "value.serializer"
                                      "org.apache.kafka.common.serialization.StringSerializer"}
                    :consumer-config {"key.deserializer"
                                      "org.apache.kafka.common.serialization.StringDeserializer"
                                      "value.deserializer"
                                      "org.apache.kafka.common.serialization.StringDeserializer"}}
    :kafka-topic "test"}
   "/users/tristan/desktop/gv_events.edn.gz")
  (event-store/with-event-reader [r "/users/tristan/desktop/gv_events.edn.gz"]
    (count (event-store/event-seq r)))
  )

(defn update-local-storage!
  "Check to see if the most recently stored events in local storage are
  up to date. If not, push them onto the queue, avoiding publish effects.

  Return the first set of events pulled from the consumer-group backed
  consumer, since it's necessary to poll (possibly multiple times) for
  the subscription to register on the server and for the consumer offsets
  to be readable."
  [topic]
  (let [consumer (:kafka-consumer @(:state topic))]
    (loop [events (poll-kafka-consumer
                   consumer
                   {::event/format (:serialization topic)})]
      (case (backing-store-lags-consumer-group? topic)
        true (do (produce-local-events topic) events)
        false events
        :timeout (recur (poll-kafka-consumer
                         consumer
                         {::event/format (:serialization topic)}))))))

(defn last-committed-offset [topic]
  (let [consumer (:kafka-consumer @(:state topic))]
    (some-> (.committed consumer
                        (.assignment consumer)
                        (Duration/ofMillis (:timeout topic)))
            vals
            first
            .offset)))

;; TODO clean shutdown of Kafka
(defn consumer-rebalance-listener [topic]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [_ partitions]
      (println "consumer rebalance offsets " (last-committed-offset topic))
      (deliver (:end-offset-at-start topic)
               (end-offset (:kafka-consumer @(:state topic))))
      (deliver (:initial-consumer-group-offset topic)
               (last-committed-offset topic)))
    (onPartitionsLost [_ partitions] [])
    (onPartitionsRevoked [_ partitions] [])))


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
      (p/publish-system-update this
                               {:source name
                                :type :component-lifecycle
                                :entity-type (:type this)
                                :status :starting})
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
                  (recur (poll-kafka-consumer
                          new-consumer
                          {::event/format (:serialization this)}))))
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
    (set-offset! [this offset] (deliver (:initial-local-offset this) offset)))

(defn initial-consumer-state []
  {:initial-local-offset (promise)
   :end-offset-at-start (promise)
   :delivered-up-to-date-event? false
   :status :stopped})

(defmethod p/init :kafka-consumer-group-topic  [topic-definition]
  (map->KafkaConsumerGroupTopic (consumer-topic-defaults topic-definition)))

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
     kafka-cluster
     kafka-topic
     kafka-options
     state]

  p/Lifecycle
  (start [this]
    (swap! state assoc :status :running)
    (.start
     (Thread.
      (fn []
        (try
          (let [^KafkaConsumer consumer (create-local-kafka-consumer this)]
            (swap! state assoc :kafka-consumer consumer)
            (log/info :source :kafka-reader-thread :status :starting)
            (deliver (:end-offset-at-start this) (end-offset consumer))
            (while (= :running (:status @state))
              (->> (poll-kafka-consumer
                    consumer
                    {::event/format (:serialization this)})
                   (map #(assoc %
                                ::event/topic name
                                ::event/skip-publish-effects true))
                   (run! #(.offer queue % timeout TimeUnit/MILLISECONDS)))
              (Thread/sleep 100))
            (.unsubscribe consumer)
            (swap! state assoc :status :stopped))
          (catch Exception e (p/exception this e)))))))
  
  (stop [this]
    (swap! state assoc :status :stopping))

  p/Consumer
  (poll [this] (poll this))

  p/Publisher
  (publish [this event] (publish this event))
  
  p/Offsets
  (set-offset! [this offset]
    (println "set offset " offset)
    (deliver (:initial-local-offset this) offset)))

(defmethod p/init :kafka-reader-topic  [topic-definition]
  (map->KafkaReaderTopic (consumer-topic-defaults topic-definition)))

(comment
  (def t
    (p/init
     {:name :test-reader
      :type :kafka-reader-topic
      :kafka-cluster
      {:common-config {"bootstrap.servers" "localhost:9092"}
       :producer-config {"key.serializer"
                         "org.apache.kafka.common.serialization.StringSerializer",
                         "value.serializer"
                         "org.apache.kafka.common.serialization.StringSerializer"}
       :consumer-config {"key.deserializer"
                         "org.apache.kafka.common.serialization.StringDeserializer"
                         "value.deserializer"
                         "org.apache.kafka.common.serialization.StringDeserializer"}}
      :kafka-topic "test"}))
  (p/start t)
  (p/set-offset! t 0)
  (p/poll t)
  @(:state t)
  (p/stop t)
  )



