(ns genegraph.framework.kafka
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [io.pedestal.log :as log]
            [clojure.string :as str])
  (:import [org.apache.kafka.clients.admin Admin NewTopic CreateTopicsResult OffsetSpec]
           [org.apache.kafka.clients.producer Producer KafkaProducer ProducerRecord
            Callback]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerGroupMetadata
            OffsetAndMetadata ConsumerRebalanceListener]
           [org.apache.kafka.common KafkaFuture TopicPartition]
           [org.apache.kafka.common.errors
            ApplicationRecoverableException
            RetriableException
            InvalidConfigurationException]
           [java.time Duration]
           [java.util.concurrent BlockingQueue ArrayBlockingQueue LinkedBlockingQueue
            LinkedBlockingDeque TimeUnit]))

(defn create-producer! [cluster-def opts]
  (let [merged-opts (merge (:common-config cluster-def)
                           (:producer-config cluster-def)
                           opts)
        p (KafkaProducer. merged-opts)]
    (when (get merged-opts "transactional.id")
      (.initTransactions p))
    p))

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
   ::event/completion-promise (promise)
   ::event/execution-thread (promise)})

(defn exception-outcome [e]
  (condp instance? e
    ApplicationRecoverableException {:outcome :restart-client
                                     :severity :warn}
    RetriableException {:outcome :retry-action
                        :severity :info}
    InvalidConfigurationException {:outcome :halt-application
                                   :severity :fatal}
    {:outcome :unknown
     :severity :error}))

(defn exception->error-map [e]
  (assoc (exception-outcome e)
         :message (.getMessage e)
         :exception (.getName (class e))))

(defn publish [topic event]
  (p/publish
   (or (::event/producer event) (:kafka-producer @(:state topic)))
   (assoc event
          ::event/kafka-topic (:kafka-topic topic)
          ::event/format (:serialization topic))))

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

;; <<loose end>> how to handle timeouts when the queue is full?
(defn deliver-event
  "Offer event to local queue"
  [topic event]
  (.offer (:event-status-queue topic) event)
  (.put (:queue topic) event))


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

;; TODO how to handle failed event effects
(defn handle-event-status-updates [topic event]
  (let [status (deref (::event/completion-promise event)
                      (::event/effect-timeout event (* 1000 60 60))
                      :timeout)]
    (when (= :timeout status)
      (let [thread-promise (::event/execution-thread event)]
        (if (and thread-promise (realized? thread-promise)) 
          (log/error :fn ::handle-event-status-updates
                     :msg "Timeout retrieving event status."
                     :stacktrace (.getStackTrace @thread-promise))
          (log/error :fn ::handle-event-status-updates
                     :msg "Timeout retrieving event status and no thread associated."))))
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
            (log/error :source ::start-status-queue-monitor))))))))

;; TODO crashes if called prior to assignments on consumer


(defn end-offset [consumer]
  ;; Per Kafka docs, end offset reported by the consumer is
  ;; always the next offset *past* the last record in the
  ;; topic partition, we want the offset of the last record,
  ;; so decrement by two.
  ;; Unclear why Kafka offsets now increment by twos rather than ones,
  ;; but they do...
  (some-> (.endOffsets consumer (.assignment consumer))
          first
          val
          (- 2)))


;; TODO UnifiedExceptionHandling

(defn poll-kafka-consumer
  "default method to poll a Kafka consumer for new records.
  returns vector of events converted to Clojure data. Optionally
  accepts a map M of metadata to append to consumer records."
  ([consumer]
   (poll-kafka-consumer consumer {}))
  ([consumer m]
   (->> (.poll consumer (Duration/ofMillis 500))
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
        (run! prn (map #(dissoc %
                                ::event/completion-promise
                                ::event/execution-thread)
                       events))
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
;; TODO UnifiedExceptionHandling
(defn consumer-rebalance-listener [topic]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [_ partitions]
      (log/info :fn :onPartitionsAssigned
                :topic (:name topic)
                :action :partitions-assigned)
      (when (seq partitions)
        (let [lco (last-committed-offset topic)
              eo (end-offset (:kafka-consumer @(:state topic)))]
          (log/info :fn :onPartitionsAssigned
                    :topic (:name topic)
                    :last-commited-offset lco
                    :end-offset eo
                    :partition-count (count partitions))
          (deliver (:end-offset-at-start @(:state topic)) eo)
          (deliver (:initial-consumer-group-offset @(:state topic)) lco))))
    (onPartitionsLost [_ partitions]
      (log/info :fn :onPartitionsLost :topic (:name topic))
      [])
    (onPartitionsRevoked [_ partitions]
      (log/info :fn :onParitionsRevoked :topic (:name topic))
      [])))

(defn init-producer! [opts]
  (when (:kafka-cluster opts)
    (let [producer (p/init (-> opts
                               (select-keys [:kafka-cluster
                                             :kafka-transactional-id
                                             :kafka-consumer-group])
                               (assoc :type :genegraph-kafka-producer)))]
      (p/start producer)
      (swap! (:state opts) assoc :kafka-producer producer))))

(defn perform-common-startup-actions! [{:keys [state] :as topic}]
  #_(reset! state (initial-state))
  (swap! state assoc :status :running)
  (when (:create-producer topic)
    (init-producer! topic))
  (Thread/startVirtualThread #(deliver-up-to-date-event-if-needed topic))
  (start-status-queue-monitor topic))

(defn stop-producer! [{:keys [state]}]
  (when-let [p (:kafka-producer @state)]
    (p/stop p)))

(defn perform-common-shutdown-actions! [{:keys [state] :as topic}]
  (swap! state assoc :status :stopping)
  (stop-producer! topic)
  (swap! state assoc :status :stopped))

;; Write last offset to state, check last offset before
;; pushing event to queue--should cover case were poll fails with
;; events in flight
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
      (perform-common-shutdown-actions! this))

    p/Resetable
    (reset [this]
      (when-let [opts (:reset-opts this)]
        (if (:clear-topic opts)
          (with-open [admin (kafka-admin/create-admin-client kafka-cluster)]
            (kafka-admin/reset-topic admin kafka-topic))
          (let [^KafkaConsumer new-consumer (create-kafka-consumer this)]
            (.subscribe new-consumer [kafka-topic])
            (while (not (seq (.assignment new-consumer)))
              (.poll new-consumer (Duration/ofMillis 100)))
            (.seekToBeginning new-consumer (.assignment new-consumer))
            (run! #(println "position "
                            (.position new-consumer %))
                  (.assignment new-consumer))
            (.commitSync new-consumer)
            (.unsubscribe new-consumer)
            (log/info :fn :reset :msg "Successfully reset" :topic name)))))

    p/Consumer
    (poll [this]
      (poll this))

    p/Publisher
    (publish [this event]
      (publish this event))

    p/Offsets
    (set-offset! [this offset] (deliver (:initial-local-offset @state) offset))

    p/Status
    (status [this]
      (let [s @state]
        {:name name
         :kafka-topic kafka-topic
         :kafka-consumer-group kafka-consumer-group
         :status (:status s)
         :queue-size (.size queue)
         :remaining-capacity (.remainingCapacity queue)
         :last-delivered-event-offset (:last-delivered-event-offset s)
         :last-completed-offset (:last-completed-offset s)
         :up-to-date (:delivered-up-to-date-event? s)})))

(derive KafkaConsumerGroupTopic :genegraph/topic)

(defmethod p/init :kafka-consumer-group-topic  [topic-definition]
  (map->KafkaConsumerGroupTopic (consumer-topic-defaults topic-definition)))

(defrecord KafkaProducerTopic
    [name
     kafka-cluster
     kafka-topic
     state]

  p/Lifecycle
  (start [this]
    (when (:create-producer this)
      (init-producer! this)))
  (stop [this]
    (perform-common-shutdown-actions! this))

  ;; Does nothing, but may consider using this to destroy the output of a
  ;; producer
  p/Resetable
  (reset [this]
    (when (get-in this [:reset-opts :clear-topic])
      (with-open [admin (kafka-admin/create-admin-client kafka-cluster)]
        (kafka-admin/reset-topic admin kafka-topic))))

  p/Publisher
  (publish [this event]
    (publish this event))

  p/Status
  (status [this]
    {:name name
     :kafka-topic kafka-topic
     :status (:status @state)}))

(derive KafkaProducerTopic :genegraph/topic)

(defmethod p/init :kafka-producer-topic [topic-definition]
  (map->KafkaProducerTopic
   (assoc topic-definition :state (atom {:status :stopped}))))

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

  ;; Currently does nothing. May want to create an option to reset
  ;; local state store (when not destroying it at the same time
  ;; during a reset
  p/Resetable
  (reset [this]
    )
  
  (stop [this]
    (perform-common-shutdown-actions! this))

  p/Consumer
  (poll [this] (poll this))

  p/Publisher
  (publish [this event] (publish this event))

  p/Offsets
  (set-offset! [this offset]
    (deliver (:initial-local-offset @state) offset))

  p/Status
  (status [this]
    (let [s @state]
      {:name name
       :kafka-topic kafka-topic
       :status (:status s)
       :queue-size (.size queue)
       :remaining-capacity (.remainingCapacity queue)
       :last-delivered-event-offset (:last-delivered-event-offset s)
       :last-completed-offset (:last-completed-offset s)
       :up-to-date (:delivered-up-to-date-event? s)})))

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

(defn transactional-producer? [producer]
  (:kafka-transactional-id producer))

(defn running? [{:keys [state]}]
  (not (get #{:stopped :failed :restarting} (:status @state))))

(defn report-producer-error [producer error]
  (when-let [t (:system-topic producer)]
    (p/publish t (assoc error :source (:name producer))))
  (log/error
    :fn :report-producer-error
    :error (:error error)
    :severity (:severity error)
    :message (:message error)))

(declare attempt-restart!)

(defn handle-producer-exception! [{:keys [state] :as producer} exception]
  (let [einfo (exception->error-map exception)]
    (report-producer-error producer einfo)
    (case (:outcome einfo)
      :halt-application (swap! state assoc :status :failed)
      :restart-client (attempt-restart! producer)
      "do nothing")))

(defn should-execute-commit? [{:keys [state queue] :as producer}]
  (and (transactional-producer? producer)
       (or (= 0 (.size queue))
           (< 100 (count (:uncommitted-records @state))))))

;; Start here, look at structure of
;; update offset to understand this
;; new-offsets is a map with the form
;; {[consumer-group kafka-topic] offset}
;; intended to hold the latest offset read for a given consumer-group, topic pair

(defn send-offsets! [{:keys [state] :as genegraph-kafka-producer}]
  (try
    (let [{:keys [producer new-offsets]} @state]
      (run!
       (fn [[[consumer-group kafka-topic] offset]]
         (.sendOffsetsToTransaction
          producer
          {(TopicPartition. kafka-topic 0)
           (OffsetAndMetadata. (+ 1 offset))} ; use next offset, per kafka docs
          (ConsumerGroupMetadata. consumer-group)))
       new-offsets))
    (catch Exception e
      (report-producer-error genegraph-kafka-producer
                             {:error :exception-sending-offsets
                              :severity :critical
                              :message (.getMessage e)}))))

(defn commit-transaction-if-needed!
  "Commit the transaction if there are no more events to publish,
  or if the maximum number of events to publish betwen transactions has been exceeded."
  [{:keys [state] :as producer}]
  (log/info :fn :commit-transaction-if-needed
            :action :enter)
  (when (should-execute-commit? producer)
    (log/info :fn :commit-transaction-if-needed
              :action :executing-commit)
    (try
      (send-offsets! producer)
      (.commitTransaction (:producer @state))
      (swap! state
             assoc
             :uncommitted-records []
             :transaction-status :committed-transaction)
      (catch Exception e
        (handle-producer-exception! producer e)))))

(defn deliver-commit-promise [event value]
  (when-let [p (:commit-promise event)]
    (deliver p value)))

;; Should pass more info along and leverage the system topic interface.
;; but this is a start.
(defn producer-callback [genegraph-producer event]
  (reify org.apache.kafka.clients.producer.Callback
    (onCompletion [this metadata exception]
      (if exception
        (do (report-producer-error genegraph-producer
                                   {:error :error-producing-record
                                    :severity :critical
                                    :message (.getMessage exception)})
            (deliver-commit-promise event false))
        (deliver-commit-promise event true)))))

(defn start-transaction-if-needed! [{:keys [state] :as producer}]
  (log/info :fn ::start-transaction-if-needed!
            :action :checking-if-transaction-needed)
  (when-not (= :in-transaction (:transaction-status @state))
      (log/info :fn ::start-transaction-if-needed!
                :action :starting-transaction)
    (try
      (.beginTransaction (:producer @state))
      (swap! state assoc :transaction-status :in-transaction)
      (catch Exception e
        (handle-producer-exception! producer e)))))

(defn send-record [{:keys [state] :as genegraph-producer} event]
  (when (transactional-producer? genegraph-producer)
    (swap! state update :uncommitted-records conj event)
    (start-transaction-if-needed! genegraph-producer))
  (.send
   (:producer @state)
   (-> event
       event/serialize
       event->producer-record)
   (producer-callback genegraph-producer
                      event)))

(defn update-offsets [{:keys [state] :as genegraph-producer}
                      {::event/keys [producer offset kafka-topic consumer-group]
                       :as event}]
  (log/info :fn :update-offsets
            :transactional-producer? (transactional-producer? genegraph-producer)
            :kafka-consumer-group consumer-group
            :offset offset
            :kafka-topic kafka-topic)
  (when (and (transactional-producer? genegraph-producer)
             consumer-group
             offset
             kafka-topic)
    (start-transaction-if-needed! genegraph-producer)
    (swap! state assoc-in [:new-offsets [consumer-group kafka-topic]] offset)))

(defn start-publish-thread! [{:keys [queue state] :as producer}]
  (let [t (Thread/startVirtualThread
    (fn []
      (log/info :fn ::start-publish-thread :msg "publish thread started")
      (while (running? producer)
        (when-let [e (.poll queue 500 TimeUnit/MILLISECONDS)]
          (log/info :fn :start-publish-thread
                    :msg "processing event"
                    :type (:type e :event))
          (try
            (case (:type e)
              :commit (commit-transaction-if-needed! producer)
              :offset (update-offsets producer (:event e))
              (send-record producer e))
            (catch Exception e
              (handle-producer-exception! producer e)))))
      (log/info :fn ::start-publish-thread :msg "publish thread stopped" )))]
    (swap! state assoc :publish-thread t)))

(defn resend-uncommitted-records!
  "In case the producers is restarted with transactional events in flight.
   Resend the events as needed."
  [{:keys [state] :as genegraph-producer}]
  (let [{:keys [uncommitted-records]} @state]
    (when (seq uncommitted-records)
      (try 
        (run! #(send-record genegraph-producer %) uncommitted-records)
        (commit-transaction-if-needed! genegraph-producer)
        (catch Exception e
          (handle-producer-exception! genegraph-producer e))))))

(defn start-producer!
  "Create a new Kafka producer and start the publish thread. Throws on failure."
  [{:keys [state kafka-cluster kafka-transactional-id name] :as genegraph-producer}]
  (let [p (create-producer!
           kafka-cluster
           (cond-> {"max.request.size" (int (* 1024 1024 10))}
             kafka-transactional-id (assoc "transactional.id" kafka-transactional-id)
             (keyword? name) (assoc "client.id" (clojure.core/name name))))]
    (swap! state
           assoc
           :producer p
           :status :running
           :transaction-status :not-in-transaction)
    (start-publish-thread! genegraph-producer)))

(defn attempt-restart!
  "Attempt to restart the producer in case of error. Uses an iterative loop
  to avoid stack overflow during repeated restart attempts. No limit on restarts
  is imposed; the expectation is that the Kafka cluster will eventually recover."
  [{:keys [state] :as genegraph-producer}]
  (loop []
    (swap! state #(-> %
                      (update :restarts inc)
                      (assoc :status :restarting)))
    (let [base-ms 500
          max-ms (* 1000 60 5)            ; 5 minutes
          {:keys [publish-thread restarts producer]} @state
          base-interval (min (* base-ms (Math/pow 2 restarts)) max-ms)
          jitter-interval (long (+ base-interval
                                   (* base-interval (Math/random) 0.2)))]
      (try (.close producer)
           (catch Exception _
             (log/info :fn ::attempt-restart
                       :msg "Could not close existing producer during restart.")))
      (try
        (.interrupt publish-thread)
        (catch Exception _
          (log/info :fn ::attempt-restart
                    :msg "Could not interrupt publish thread.")))
      (log/info :fn ::attempt-restart
                :name (:name genegraph-producer)
                :restarts restarts
                :msg "Restarting producer")
      (Thread/sleep jitter-interval)
      (let [outcome (try
                      (start-producer! genegraph-producer)
                      :success
                      (catch Exception e
                        (let [einfo (exception->error-map e)]
                          (report-producer-error genegraph-producer einfo)
                          (:outcome einfo))))]
        (case outcome
          :success nil
          :halt-application (swap! state assoc :status :failed)
          (recur))))))

(defrecord GenegraphKafkaProducer
    [state
     kafka-cluster
     kafka-transactional-id
     kafka-consumer-group
     producer-opts
     queue
     name]

    p/Publisher
    (publish [this event]
      (when-not (.offer queue event 10 TimeUnit/SECONDS)
        (report-producer-error this {:error :timeout-on-publish
                                     :severity :critical})))

    p/TransactionalPublisher

    (send-offset [this event]
      (when-not (.offer queue {:type :offset
                               :event event} 10 TimeUnit/SECONDS)
        (report-producer-error this {:error :timeout-on-offset-update
                                     :severity :critical})))
  
    (commit [this]
      (when-not (.offer queue {:type :commit} 10 TimeUnit/SECONDS)
        (report-producer-error this {:error :timeout-on-commit
                                     :severity :critical})))

    p/Lifecycle
    (start [this]
      (try
        (start-producer! this)
        (catch Exception e
          (handle-producer-exception! this e))))
    
    (stop [this]
      (.close (:producer @state))
      (swap! state assoc :status :stopped)))

(defmethod p/init :genegraph-kafka-producer [opts]
  (-> opts
      (assoc :state (atom {:status :initialized
                           :records-sent 0
                           :uncommitted-records []
                           :new-offsets {}
                           :restarts 0}))
      (assoc :queue (ArrayBlockingQueue. 100))
      map->GenegraphKafkaProducer))





