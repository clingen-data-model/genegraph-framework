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
               (when-not (::event/skip-local-effects event)
                   (run! (fn [{:keys [store command args commit-promise]}]
                           (apply command args))
                         (::event/effects event)))
               event
               (catch Exception e (assoc event :error {:fn ::storage-interceptor
                                                       :exception e}))))}))

(def deserialize-interceptor
  (interceptor/interceptor
   {:name ::deserialize-interceptor
    :enter (fn [e] (event/deserialize e))}))

(defn commit-kafka-transaction-interceptor [processor]
  (interceptor/interceptor
   {:name ::commit-kafka-transaction
    :leave (fn [event]
             (if (:kafka-cluster processor)
               (.commitTransaction @(:producer processor)))
             event)}))

(defn commit-kafka-offset-interceptor [processor]
  (interceptor/interceptor
   {:name ::commit-kafka-offset
    :leave (fn [event]
             (if (and (:kafka-cluster processor)
                      (::event/consumer-group event))
               (when (and (::event/consumer-group event)
                          (::event/offset event)
                          (::event/kafka-topic event))
                 (kafka/commit-offset (assoc
                                       event
                                       ::event/producer
                                       @(:producer processor)))))
             event)}))

(defn backing-store-instance
  "Return instance of backing store for PROCESSOR"
  [processor]
  (if-let [backing-store (:backing-store processor)]
    @(get-in processor [:storage backing-store :instance])))

(defn add-backing-store-interceptor [processor]
  (interceptor/interceptor
   {:name ::add-backing-store
    :enter (fn [event]
             (if (:backing-store processor)
               (assoc event
                      ::event/backing-store
                      @(get-in processor
                               [:storage
                                (:backing-store processor)
                                :instance]))
               event))}))

(def commit-local-offset-interceptor
  (interceptor/interceptor
   {:name ::commit-local-offset
    :enter (fn [event]
             (if (and (::event/backing-store event)
                      (::event/offset event)
                      (::event/topic event))
               (event/record-offset event)
               event))}))

(defn open-kafka-transaction-interceptor [processor]
  (interceptor/interceptor
   {:name ::open-kafka-transaction
    :leave (fn [event]
             (if (:kafka-cluster processor)
               (.beginTransaction @(:producer processor)))
             event)}))

;; TODO, handle error case if topic does not exist
(defn publish-interceptor [processor]
  (interceptor/interceptor
   {:name ::publish-interceptor
    :leave (fn [event]
             (when-not (::event/skip-publish-effects event)
               (let [producer @(:producer processor)]
                 (run! #(if-let [topic (get (:topics processor) (::event/topic %))]
                          (p/publish topic (assoc % ::event/producer producer)))
                       (::event/publish event))))
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

(defn interceptors-for-processor [processor]
  (vec
   (concat
    [(metadata-interceptor processor)
     (storage-interceptor processor)
     (add-backing-store-interceptor processor)
     commit-local-offset-interceptor
     (commit-kafka-transaction-interceptor processor) ;; rename to sendoffsetstotransaction
     (commit-kafka-offset-interceptor processor)
     (publish-interceptor processor)
     (open-kafka-transaction-interceptor processor)
     deserialize-interceptor]
    (mapv ->interceptor (:interceptors processor)))))

(defn add-interceptors [event processor]
  (interceptor-chain/enqueue event (interceptors-for-processor processor)))

(defn process-event [processor event]
  (-> event
      (add-interceptors processor)
      (interceptor-chain/terminate-when :error)
      interceptor-chain/execute))

(defn initial-offset
  "Return starting offset for subscribed topic from backing store.
  Note that initial offset will be one slot past the offset for
  the last read record."
  [processor]
  (if-let [backing-store (backing-store-instance processor)]
    (let [o (s/retrieve-offset backing-store (:subscribe processor))]
      (case o
        nil 0
        ::s/miss 0
        (+ 1 o)))
    nil))




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
                      kafka-cluster
                      backing-store
                      init-fn]

  p/EventProcessor
  (process [this event] (process-event this event))
  (interceptors [this] (interceptors-for-processor this))

  p/Lifecycle
  (start [this]
    (reset! (:state this) :running)
    ;; start producer first, else race condition
    (if kafka-cluster
      (deliver producer
               (kafka/create-producer
                kafka-cluster
                {"transactional.id" (str name)}))
      (deliver producer nil))
    (when-let [subscribed-topic (get-subscribed-topic this)]
      (when (satisfies? p/Offsets subscribed-topic)
        (p/set-offset! subscribed-topic (initial-offset this)))
      (.start
       (Thread.
        #(while (= :running @(:state this))
           (try 
             (when-let [event (p/poll subscribed-topic)]
               (process-event this event))
             (catch Exception e
               (clojure.stacktrace/print-stack-trace e))))))))
  
  (stop [this]
    (reset! (:state this) :stopped)
    (when kafka-cluster
      (.close @producer))))

(defmethod p/init :processor [processor-def]
  (-> processor-def
      (assoc :state (atom :stopped))
      (assoc :producer (promise))
      map->Processor))



