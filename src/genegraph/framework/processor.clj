(ns genegraph.framework.processor
  "Logic to handle processing over topics"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as s]
            [genegraph.framework.event :as event]
            [genegraph.framework.kafka :as kafka]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as interceptor-chain])
  (:import [org.apache.kafka.clients.producer Producer]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue TimeUnit]))

(defn commit-kafka-transaction! [{^Producer producer ::event/producer :as event}]
  (when producer (.commitTransaction producer))
  event)

(defn add-offset! [{::event/keys [producer consumer-group offset kafka-topic]
                             :as event}]
  (when (and producer consumer-group offset kafka-topic)
    (kafka/commit-offset event))
  event)

(defn open-kafka-transaction! [{^Producer producer ::event/producer :as event}]
  (when producer (.beginTransaction producer))
  event)

(defn publish-events! [{::event/keys [producer skip-publish-effects topics] :as event}]
  (when-not skip-publish-effects
    (run! #(if-let [topic (get topics (::event/topic %))]
             (p/publish topic (assoc % ::event/producer producer)))
          (::event/publish event)))
  event)

(defn perform-publish-effects! [event]
  (-> event
      open-kafka-transaction!
      publish-events!
      add-offset!
      commit-kafka-transaction!))

(def publish-effects-interceptor
  (interceptor/interceptor
   {:name ::publish-effects-interceptor
    :leave #(perform-publish-effects! %)}))

(defn update-local-storage! [{::event/keys [skip-local-effects effects] :as event}]
  (when-not skip-local-effects
    (run! (fn [{:keys [store command args commit-promise]}]
            (apply command args))
          effects))
  event)

(defn commit-local-offset! [{::event/keys [backing-store offset topic] :as event}]
  (when (and backing-store offset topic)
    (event/record-offset event))
  event)

(defn perform-local-effects! [event]
  (-> event
      update-local-storage!
      commit-local-offset!))

(def local-effects-interceptor
  (interceptor/interceptor
   {:name ::local-effects-interceptor
    :leave #(perform-local-effects! %)}))

(defn add-processor-defined-metadata [event processor]
  (merge event (::event/metadata processor)))

(defn add-storage-refs [event processor]
  (assoc event
         ::s/storage
         (update-vals (:storage processor)
                      (fn [s] @(:instance s)))))

(defn add-backing-store [event processor]
  (if (:backing-store processor)
    (assoc event
           ::event/backing-store
           @(get-in processor
                    [:storage
                     (:backing-store processor)
                     :instance]))
    event))

(defn add-producer [event processor]
  (if-let [producer (:producer processor)]
    (assoc event ::event/producer (deref producer))
    event))

(defn add-topics [event processor]
  (assoc event ::event/topics (:topics processor)))

(defn add-app-state [event processor]
  (-> (add-processor-defined-metadata event processor)
      (add-storage-refs processor)
      (add-backing-store processor)
      (add-producer processor)
      (add-topics processor)))

(defn app-state-interceptor [processor]
  (interceptor/interceptor
   {:name ::app-state-interceptor
    :enter #(add-app-state % processor)}))

(defn backing-store-instance
  "Return instance of backing store for PROCESSOR"
  [processor]
  (if-let [backing-store (:backing-store processor)]
    @(get-in processor [:storage backing-store :instance])))

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

(defn process-event [processor event]
  (-> event
      (add-app-state processor)
      event/deserialize
      (interceptor-chain/enqueue (:interceptors processor))
      interceptor-chain/execute
      perform-publish-effects!
      perform-local-effects!))

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


(defn init-kafka-producer! [{:keys [producer kafka-cluster name]}]
  (if kafka-cluster
    (deliver producer
             (kafka/create-producer
              kafka-cluster
              {"transactional.id" (str name "-" (random-uuid))
               "max.request.size" (int (* 1024 1024 10))}))
    (deliver producer nil)))

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
               :producer (promise)
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
                      producer
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
                   publish-effects-interceptor]
                  (map ->interceptor interceptors))))

  p/Lifecycle
  (start [this]
    (swap! state assoc :status :running)
    ;; start producer first, else race condition
    (init-kafka-producer! this)
    (when-let [subscribed-topic (get-subscribed-topic this)]
      (set-topic-offset! this subscribed-topic)
      (.start
       (Thread.
        #(while (= :running (:status @state))
           (try 
             (when-let [event (p/poll subscribed-topic)]
               (process-event this event))
             (catch Exception e
               (clojure.stacktrace/print-stack-trace e))))))))
  
  (stop [this]
    (swap! state assoc :status :stopped)
    (when kafka-cluster
      (.close @producer))))

(defmethod p/init :processor [processor-def]
  (map->Processor (processor-init processor-def)))

(defn process-parallel-event [processor event]
  (.put (:effect-queue processor)
        (future
          (-> (add-app-state event processor)
              event/deserialize
              (interceptor-chain/enqueue (:interceptors processor))
              interceptor-chain/execute))))

(defn process-event-effects! [event]
  perform-publish-effects!
  perform-local-effects!)

(defrecord ParallelProcessor [name
                              subscribe
                              storage
                              topics
                              state
                              interceptors
                              producer
                              kafka-cluster
                              backing-store
                              effect-queue
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
               (process-parallel-event this event))
             (catch Exception e
               (clojure.stacktrace/print-stack-trace e))))))
      (.start
       (Thread.
        #(while (p/running? this)
           (try 
             (when-let [event @(.take effect-queue)]
               (process-event-effects! event))
             (catch Exception e
               (clojure.stacktrace/print-stack-trace e))))))))
  
  (stop [this]
    (swap! state assoc :status :stopped)
    (when kafka-cluster
      (.close @producer))))

(defmethod p/init :parallel-processor [processor-def]
  (-> processor-def
      processor-init
      (assoc :effect-queue (ArrayBlockingQueue. 100))
      map->ParallelProcessor))
