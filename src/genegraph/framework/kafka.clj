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

(defn poll
  "Poll event queue for events (already pulled from Kafka)."
  [topic]
  (when-let [event (.poll (:queue topic)
                          (:timeout topic)
                          TimeUnit/MILLISECONDS)]
    (swap! (:state topic)
           assoc
           :last-delivered-event-offset
           (::event/offset event))
    event))

(defn deliver-event
  "Offer event to local queue"
  [topic event]
  (.offer (:event-status-queue topic) event (:timeout topic) TimeUnit/MILLISECONDS)
  (.offer (:queue topic) event (:timeout topic) TimeUnit/MILLISECONDS))


(defn topic-up-to-date?
  "A TOPIC is considered up to date when processing has completed up
  to the last available offset when the application was started."
  [topic]
  (let [last-completed-offset (:last-completed-offset @(:state topic))
        local-offset @(:initial-local-offset @(:state topic))
        cg-offset @(:initial-consumer-group-offset @(:state topic))
        last-offset @(:end-offset-at-start @(:state topic))]
    (or  (and last-completed-offset (<= last-offset last-completed-offset))
         (and (if local-offset (<= last-offset local-offset) true)
              (if cg-offset (<= last-offset cg-offset) true)))))

(defn deliver-up-to-date-event-if-needed [topic]
  (when (and (not (:delivered-up-to-date-event? @(:state topic)))
             (topic-up-to-date? topic))
    (p/system-update topic {:state :up-to-date})
    (swap! (:state topic) assoc :delivered-up-to-date-event? true)))

(defn handle-event-status-updates [topic event]
  (let [status @(::event/completion-promise event)]
    (swap! (:state topic) assoc :last-completed-offset (::event/offset event))
    (deliver-up-to-date-event-if-needed topic)))

(defn start-status-queue-monitor [topic]
  (.start
   (Thread.
    (fn []
      (while (p/running? topic)
        (try
          (when-let [event (.poll (:event-status-queue topic)
                                  (:timeout topic)
                                  TimeUnit/MILLISECONDS)]
            (handle-event-status-updates topic event))
          (catch Exception e
            (clojure.stacktrace/print-stack-trace e))))))))

(defn end-offset [consumer]
  ;; Per Kafka docs, end offset reported by the consumer is
  ;; always the next offset *past* the last record in the
  ;; topic partition, we want the offset of the last record,
  ;; so decrement by two.
  ;; Unclear why Kafka offsets now increment by twos rather than ones,
  ;; but they do...
  (-> (.endOffsets consumer (.assignment consumer)) first val (- 2)))

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

(defn initial-state []
  {:delivered-up-to-date-event? false
   :status :stopped
   :initial-local-offset (promise)
   :end-offset-at-start (promise)
   :initial-consumer-group-offset (promise)})

(defn consumer-topic-defaults [topic-def]
  (merge {:timeout 1000
          :queue (ArrayBlockingQueue. (:buffer-size topic-def 100))
          :event-status-queue (LinkedBlockingQueue.)
          :kafka-options {"enable.auto.commit" false
                          "auto.offset.reset" "earliest"
                          "isolation.level" "read_committed"}

          ;; :initial-local-offset (promise)
          ;; :end-offset-at-start (promise)
          ;; :initial-consumer-group-offset (promise)
          :state (atom (initial-state))}
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
  (let [local-offset (deref (:initial-local-offset @(:state topic)) 100 :timeout)
        cg-offset (deref(:initial-consumer-group-offset @(:state topic)) 100 :timeout)]
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
   (create-local-kafka-consumer topic @(:initial-local-offset @(:state topic))))
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
  (let [last-offset @(:initial-consumer-group-offset @(:state topic))]
    (try 
      (when-let [offset @(:initial-local-offset @(:state topic))]
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
        (run! prn (map #(dissoc % ::event/completion-promise) events))
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
      (deliver (:end-offset-at-start @(:state topic))
               (end-offset (:kafka-consumer @(:state topic))))
      (deliver (:initial-consumer-group-offset @(:state topic))
               (last-committed-offset topic)))
    (onPartitionsLost [_ partitions] [])
    (onPartitionsRevoked [_ partitions] [])))

(defn perform-common-startup-actions! [topic]
  (reset! (:state topic) (initial-state))
  (swap! (:state topic) assoc :status :running)
  (Thread/startVirtualThread #(deliver-up-to-date-event-if-needed topic))
  (start-status-queue-monitor topic))

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
      (p/system-update this
                               {:source name
                                :type :component-lifecycle
                                :entity-type (:type this)
                                :status :starting})
      (perform-common-startup-actions! this)
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
      (poll this))

    p/Publisher
    (publish [this event]
      (publish this event))
  
    p/Offsets
    (set-offset! [this offset] (deliver (:initial-local-offset @state) offset)))

(derive KafkaConsumerGroupTopic :genegraph/topic)

(defmethod p/init :kafka-consumer-group-topic  [topic-definition]
  (map->KafkaConsumerGroupTopic (consumer-topic-defaults topic-definition)))

(defrecord KafkaProducerTopic
    [name
     kafka-cluster
     kafka-topic]

  p/Publisher
  (publish [this event]
    (publish this event)))

(derive KafkaProducerTopic :genegraph/topic)

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
    (perform-common-startup-actions! this)
    (deliver (:initial-consumer-group-offset @state) nil)
    (.start
     (Thread.
      (fn []
        (try
          (let [^KafkaConsumer consumer (create-local-kafka-consumer this)]
            (swap! state assoc :kafka-consumer consumer)
            (deliver (:end-offset-at-start @state) (end-offset consumer))
            (while (= :running (:status @state))
              (->> (poll-kafka-consumer
                    consumer
                    {::event/format (:serialization this)})
                   (map #(assoc %
                                ::event/topic name
                                ::event/skip-publish-effects true))
                   (run! #(deliver-event this %)))
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
    (deliver (:initial-local-offset @state) offset)))

(derive KafkaReaderTopic :genegraph/topic)

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



