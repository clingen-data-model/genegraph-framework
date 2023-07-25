(ns genegraph.framework.processor
  "Logic to handle processing over topics"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as s]
            [genegraph.framework.event :as event]
            [genegraph.framework.kafka :as kafka]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as interceptor-chain]))

(defn metadata-interceptor
  "Decorate the event with appropriate metadata by merging metadata from
   event on top of metadata from processor"
  [processor]
  (interceptor/interceptor
   {:name ::metadata-interceptor
    :enter (fn [event]
             (merge event
                    (::event/metadata processor)
                    (::event/metadata event)))}))

(defn storage-interceptor
  "Returns an interceptor for managing storage related to events.
  On enter, passes in references to the handles for storage objects
  the processor will require.
  On exit, manages any side effects related to the declared storage ."
  [processor]
  (interceptor/interceptor
   {:name ::storage-interceptor
    :enter (fn [event]
             (if (:storage processor)
               (assoc event
                      ::s/storage
                      (update-vals (:storage processor)
                                   (fn [s] @(:instance s))))
               event))
    :leave (fn [event]
             (try
               (run! (fn [{:keys [store command args commit-promise]}]
                       (apply command args))
                     (:effects event))
               event
               (catch Exception e (assoc event :error {:fn ::storage-interceptor
                                                       :exception e}))))}))

;; This one doesn't require a reference to the containing processor
;; so can exist as a simple def
(def deserialize-interceptor
  (interceptor/interceptor
   {:name ::deserialize-interceptor
    :enter (fn [e] (event/deserialize e))}))

(defn any-kafka-topics?
  "True if any events are sourced from or destined to
   topics backed by Kafka."
  [events topics]
  (let [kafka-topics (->> topics (filter :kafka-cluster) (map :name) set)]
    (some kafka-topics (map ::event/topic events))))

(comment
  (let [t [{:name :is-kafka-topic
            :kafka-cluster {}}
           {:name :is-not-kafka-topic}]]
    [(any-kafka-topics?
      [{::event/topic :is-kafka-topic}
       {::event/topic :is-not-kafka-topic}]
      t)
     (any-kafka-topics?
      [{::event/topic :is-not-kafka-topic}]
      t)])
  )

(defn publish-interceptor [processor]
  (interceptor/interceptor
   {:name ::publish-interceptor
    :leave (fn [event]
             (let [use-transaction (any-kafka-topics? (::event/publish event)
                                                      (:topics processor))
                   producer @(:producer processor)]
               (when use-transaction (.beginTransaction producer))
               (run! #(p/publish (get (:topics processor) (::event/topic %))
                                 (assoc % ::kafka/producer producer))
                     (::event/publish event))
               (when use-transaction (.commitTransaction producer)))
             event)}))

(defn get-subscribed-topic
  [processor]
  (get (:topics processor) (:subscribe processor)))

(defn ->interceptor 
  "Transform input to Pedestal interceptor. The primary use case for
  this is to render symbols as an interceptor, allowing for dynamic resolution
  in a running system."
  [v]
  (if (symbol? v)
    (interceptor/interceptor
     {:enter (var-get (resolve v))})
    (interceptor/interceptor v)))

(defn add-interceptors [event processor]
  (interceptor-chain/enqueue
   event
   (concat
    [(metadata-interceptor processor)
     (publish-interceptor processor)
     (storage-interceptor processor)
     deserialize-interceptor]
    (mapv ->interceptor (:interceptors processor)))))

(defn process-event [processor event]
  (-> event
      (add-interceptors processor)
      (interceptor-chain/terminate-when :error)
      interceptor-chain/execute))

;; name -- name of processor
;; subscribe -- topic to source events from
;; storage -- storage entities to pass forward to event handling
;; topics -- topics to publish processed events to
;; state -- :running or :stopped
;; interceptors -- interceptor chain
;; options -- additional configuration parameters to pass into events and init fn
;; init-fn -- optional init fn, runs prior to processor start

(defrecord Processor [name
                      subscribe
                      storage
                      topics
                      state
                      interceptors
                      options
                      producer
                      producer-cluster
                      init-fn]

  p/Lifecycle
  (start [this]
    (reset! (:state this) :running)
    (when subscribe
      (.start
       (Thread.
        #(while (= :running @(:state this))
           (try 
             (when-let [event (p/poll (get-subscribed-topic this))]
               (process-event this event))
             (catch Exception e
               (clojure.stacktrace/print-stack-trace e)))))))
    (when producer-cluster
      (reset! producer
              (kafka/create-producer
               producer-cluster
               {"transactional.id" (str name)}))))
  (stop [this]
    (reset! (:state this) :stopped)
    (when-let [p @producer]
      (.close p)
      (reset! producer nil))))

(defmethod p/init :processor [processor-def]
  (-> processor-def
      (assoc :state (atom :stopped))
      (assoc :producer (atom nil))
      map->Processor))


(comment
  (def p
    (p/init
     {:name :test-processor
      :type :processor
      :interceptors `[identity]
      :producer-cluster {:name :local
                         :type :kafka-cluster
                         :common-config
                         {"bootstrap.servers" "localhost:9092"}
                         :producer-config
                         {"key.serializer"
                          "org.apache.kafka.common.serialization.StringSerializer",
                          "value.serializer"
                          "org.apache.kafka.common.serialization.StringSerializer"}}}))
  
  (p/start p)
  (p/stop p)


  )
