(ns genegraph.framework.processor
  "Logic to handle processing over topics"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as s]
            [genegraph.framework.event :as event]
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
                      (update-vals @(:storage processor)
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

(defn publish-interceptor [processor]
  (interceptor/interceptor
   {:name ::publish-interceptor
    :enter (fn [event]
             (if (:topics processor)
               (assoc event
                      ::event/topics
                      @(:topics processor))
               event))
    :leave (fn [event]

             event)}))

(defn get-subscribed-topic
  [processor]
  (get @(:topics processor) (:subscribe processor)))

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
               (clojure.stacktrace/print-stack-trace e))))))))
  (stop [this]
    (reset! (:state this) :stopped)))



(defmethod p/init :processor [processor-def]
  (-> processor-def
      (assoc :state (atom :stopped))
      #_ (update :interceptors
              #(mapv ->interceptor %))
      map->Processor))
