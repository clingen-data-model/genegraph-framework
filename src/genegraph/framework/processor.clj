(ns genegraph.framework.processor
  "Logic to handle processing over topics"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as s]
            [genegraph.framework.event :as event]
            [genegraph.framework.kafka :as kafka]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as interceptor-chain]
            [io.pedestal.log :as log])
  (:import [org.apache.kafka.clients.producer Producer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer ConsumerGroupMetadata OffsetAndMetadata]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue TimeUnit Semaphore]))

;; Kafka specific stuff
;; This probably gets refactored as soon as another message broker is supported

;; TODO UnifiedExceptionHandling

(defn commit-kafka-transaction! [event]
  (when-let [^Producer p (::event/producer event)]
    (try 
      (.commitTransaction p)
      (catch Exception e
        (log/warn :fn :commit-kafka-transaction!
                  :exception (str (type e))
                  :exception-class :kafka))))
  event)


;; TODO UnifiedExceptionHandling

(defn add-offset-to-tx! [{::event/keys [producer consumer-group offset kafka-topic]
                          :as event}]
  (when (and producer consumer-group offset kafka-topic)
    (try
      (.sendOffsetsToTransaction
       producer
       {(TopicPartition. kafka-topic 0)
        (OffsetAndMetadata. (+ 1 offset))} ; use next offset, per kafka docs
       (ConsumerGroupMetadata. consumer-group))
      (catch Exception e
        (log/warn :fn ::add-offset-to-tx!
                  :exception-class :kafka)
        (throw e))))
  event)

(defn open-kafka-transaction! [event]
  (when-let [^Producer p (::event/producer event)]
    (.beginTransaction p))
  event)

;; /Kafka stuff

(defn publish-events! [{::event/keys [skip-publish-effects topics producer] :as event}]
  (when-not skip-publish-effects
    (run! #(if-let [topic (get topics (::event/topic %))]
             (p/publish topic (assoc % ::event/producer producer)))
          (::event/publish event)))
  event)

(defn perform-publish-effects! [event]
  (-> event
      open-kafka-transaction!
      publish-events!
      add-offset-to-tx!
      commit-kafka-transaction!))

(def publish-effects-interceptor
  (interceptor/interceptor
   {:name ::publish-effects-interceptor
    :leave #(perform-publish-effects! %)}))

(defn update-local-storage! [{::event/keys [skip-local-effects effects] :as event}]
  (when-not skip-local-effects
    (run! (fn [{:keys [store command args commit-promise]}]
            (try
              (apply command args)
              (catch Exception e
                (when-not (realized? commit-promise)
                  (deliver commit-promise
                           {:store store
                            :exception e})))))
          effects))
  event)

(defn record-local-offset! [{::event/keys [backing-store offset topic] :as event}]
  (if (and backing-store offset topic)
    (event/record-offset event)
    event))

(defn perform-local-effects! [event]
  (-> event
      record-local-offset!
      update-local-storage!))

(def local-effects-interceptor
  (interceptor/interceptor
   {:name ::local-effects-interceptor
    :leave #(perform-local-effects! %)}))

(defn effect-error [event]
  (some #(not= true (deref (:commit-promise %)
                           (::event/effect-timeout event (* 1000 60 60))
                           :timeout))
        (filter
         :commit-promise
         (::event/effects event))))

(defn deliver-completion-promise [event]
  (when-let [p (::event/completion-promise event)]
    (Thread/startVirtualThread
     (fn []
       (if-let [error (effect-error event)]
         (deliver p false)
         (deliver p true)))))
  event)

(def deliver-completion-promise-interceptor
  (interceptor/interceptor
   {:name ::deliver-completion-promise-interceptor
    :leave #(deliver-completion-promise %)}))

(defn add-processor-defined-metadata [event processor]
  (merge event (::event/metadata processor)))

(defn add-storage-refs [event processor]
  (assoc event
         ::s/storage
         (update-vals (:storage processor) s/instance)))

(defn backing-store-instance
  "Return instance of backing store for PROCESSOR"
  [processor]
  (if-let [backing-store (:backing-store processor)]
    (s/instance (get-in processor [:storage backing-store]))))

(defn add-backing-store [event processor]
  (assoc event ::event/backing-store (backing-store-instance processor)))

(defn add-producer [event processor]
  (if-let [producer (:producer @(:state processor))]
    (assoc event ::event/producer producer)
    event))

(defn add-topics [event processor]
  (assoc event ::event/topics (:topics processor)))

(defn add-processor [event processor]
  (assoc event ::event/processor processor))

(defn add-app-state [event processor]
  (-> (add-processor-defined-metadata event processor)
      (add-storage-refs processor)
      (add-backing-store processor)
      (add-producer processor)
      (add-topics processor)
      (add-processor processor)))

(defn app-state-interceptor [processor]
  (interceptor/interceptor
   {:name ::app-state-interceptor
    :enter #(add-app-state % processor)}))

(defn get-subscribed-topic
  [processor]
  (get (:topics processor) (:subscribe processor)))

(defn ->interceptor 
  "Transform input to Pedestal interceptor. The primary use case for
  this is to render symbols as an interceptor, allowing for dynamic resolution
  in a running system."
  [v]
  (cond (symbol? v) (interceptor/interceptor
                     {:enter (var-get (resolve v))})
        (interceptor/interceptor? v) v
        :else (interceptor/interceptor v)))

(defn process-event-effects! [event]
  (-> event
      perform-publish-effects!
      perform-local-effects!
      deliver-completion-promise))

(defn deliver-execution-thread [event]
  (when (::event/execution-thread event)
    (deliver (::event/execution-thread event) (Thread/currentThread)))
  event)

(defn process-event [processor event]
  (-> event
      deliver-execution-thread
      (add-app-state processor)
      event/deserialize
      (interceptor-chain/enqueue (:interceptors processor))
      interceptor-chain/execute
      process-event-effects!))

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

(defn init-kafka-producer! [{:keys [kafka-cluster state] :as p}]
  (when kafka-cluster
    (let [producer (kafka/create-producer
                    kafka-cluster
                    {"transactional.id" (or (:kafka-transactional-id p)
                                            (name (:name p)))
                     "max.request.size" (int (* 1024 1024 10))})]
      (swap! state assoc :producer producer))))

(defn set-topic-offset! [processor subscribed-topic]
  (when (satisfies? p/Offsets subscribed-topic)
    (p/set-offset! subscribed-topic (initial-offset processor))))

(defn processor-init
  "Configure the map to be passed into the processor constructor of choice
  (parallel, normal, whatever)."
  [processor-def]
  (let [init-fn (:init-fn processor-def identity)]
    (-> processor-def
        (assoc :state (atom {:status :stopped})
               :interceptors (map ->interceptor (:interceptors processor-def)))
        init-fn)))

;; name -- name of processor
;; subscribe -- topic to source events from
;; storage -- storage entities to pass forward to event handling
;; topics -- topics to publish processed events to
;; state -- :running or :stopped
;; interceptors -- interceptor chain
;; init-fn -- optional init fn, runs prior to processor start

(defrecord Processor [name
                      subscribe
                      storage
                      topics
                      state
                      interceptors
                      kafka-cluster
                      backing-store
                      init-fn]

  p/EventProcessor
  (process [this event] (process-event this event))

  p/StatefulInterceptors
  (as-interceptors [this]
    (into []
          (concat [(app-state-interceptor this)
                   local-effects-interceptor
                   publish-effects-interceptor
                   deliver-completion-promise-interceptor]
                  (map ->interceptor interceptors))))

  p/Lifecycle
  (start [this]
    (when-not (= :running (:status @state))
      (p/system-update this {:state :starting})
      (swap! state assoc :status :running)
      ;; start producer first, else race condition
      (init-kafka-producer! this)
      (when-let [subscribed-topic (get-subscribed-topic this)]
        (set-topic-offset! this subscribed-topic)
        (.start
         (Thread.
          #(while (= :running (:status @state))
             (when-let [event (p/poll subscribed-topic)]
               (try
                 (process-event this event)
                 (catch Exception e
                   (log/error :source ::start
                              :record ::Processor
                              :name name
                              :offset (::event/offset event)
                              :exception e)
                   #_(reset! state :error)
                   #_(p/system-update this {:state :error}))))))))
      (p/system-update this {:state :started})))
  
  (stop [this]
    (swap! state assoc :status :stopped)
    (when kafka-cluster
      (.close (:producer @state)))))

(defmethod p/init :processor [processor-def]
  (map->Processor (processor-init processor-def)))

(derive Processor :genegraph/processor)

(defn process-parallel-event [processor event]
  (.put (:effect-queue processor)
        (future
          (-> event
              (interceptor-chain/enqueue (:interceptors processor))
              interceptor-chain/execute))))

(defn get-or-create-gate [state event k]
  (if-let [gate (get (:gates @state) k)]
    gate
    (let [new-gate (Semaphore. 1 true)]
      (swap! state assoc-in :gates k new-gate)
      new-gate)))

(defn await-prior-event [{:keys [gate-fn state parallel-gate-timeout] :as processor} event]
  #_(tap> processor)
  (let [k (gate-fn (::event/data event))
        gate (get (:gates @state) k)
        timeout (or parallel-gate-timeout 30)]
    (when (= :timeout (and gate (deref gate (* timeout 1000) :timeout)))
      (log/error :fn :process-gated-parallel-event
                 :error :timeout-exceeded
                 :key k
                 :timeout timeout)
      (throw (ex-info "Timeout waiting for event gate"
                      {:key k
                       :cause :timeout-exceeded})))))

(defn process-gated-parallel-event [{:keys [gate-fn state parallel-gate-timeout] :as processor} event]
  (let [k (gate-fn (::event/data event))
        gate (get (:gates @state) k)
        timeout (or parallel-gate-timeout 30)]
    (await-prior-event processor event)
    (swap! state assoc-in [:gates k] (::event/completion-promise event))
    (process-parallel-event processor event)))

(defn deserialize-parallel-event [processor event]
  (.put (:deserialized-event-queue processor)
        (future
          (-> (add-app-state event processor)
              event/deserialize
              (update ::event/completion-promise #(or % (promise)))))))

(defrecord ParallelProcessor [name
                              subscribe
                              storage
                              topics
                              state
                              interceptors
                              kafka-cluster
                              backing-store
                              deserialized-event-queue
                              effect-queue
                              gate-fn
                              init-fn]

  p/EventProcessor
  (process [this event] (process-event this event))

  p/Lifecycle
  (start [this]
    (swap! state assoc :status :running)
    ;; start producer first, else race condition
    (init-kafka-producer! this)
    (when-let [subscribed-topic (get-subscribed-topic this)]
      (set-topic-offset! this subscribed-topic)
      (.start
       (Thread.
        #(while (p/running? this)
           (try 
             (when-let [event (p/poll subscribed-topic)]
               (deserialize-parallel-event this event))
             (catch Exception e
               (log/error :source ::start
                          :fn ::deserialize-parallel-event
                          :record ::ParallelProcessor))))))
      (.start
       (Thread.
        #(while (p/running? this)
           (try 
             (when-let [event-future (.poll deserialized-event-queue
                                            500
                                            TimeUnit/MILLISECONDS)]
               (if gate-fn
                 (process-gated-parallel-event this @event-future)
                 (process-parallel-event this @event-future)))
             (catch Exception e
               (log/error :source ::start
                          :record ::ParallelProcessor))))))
      (.start
       (Thread.
        #(while (p/running? this)
           (try 
             (when-let [event-future (.poll effect-queue
                                             500
                                             TimeUnit/MILLISECONDS)]
               (process-event-effects! @event-future))
             (catch Exception e
               (log/error :source ::start :record ::ParallelProcessor))))))))
  
  (stop [this]
    (swap! state assoc :status :stopped)
    (when kafka-cluster
      (.close (:producer @state)))))

(derive ParallelProcessor :genegraph/processor)

(defmethod p/init :parallel-processor [processor-def]
  (-> processor-def
      processor-init
      (assoc :effect-queue (ArrayBlockingQueue. 30))
      (assoc :deserialized-event-queue (ArrayBlockingQueue. 30))
      map->ParallelProcessor))
