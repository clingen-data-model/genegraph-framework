(ns genegraph.framework.processor
  "Logic to handle processing over topics"
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as s]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as interceptor-chain]
            [clojure.spec.alpha :as spec]))

(spec/def ::processor
  (spec/keys :req-un [::name ::subscribe]))

(spec/def ::status #(:running :stopped))

(defn storage-interceptor
  "Returns an interceptor for managing storage related to events.
  On enter, passes in references to the handles for storage objects
  the processor will require.
  On exit, manages any side effects related to the declared storage ."
  [processor]
  (interceptor/interceptor
   {:name ::storage-interceptor
    :enter (fn [event]
             (assoc event
                    ::s/storage
                    (update-vals @(:storage processor)
                                 (fn [s] @(:instance s)))))
    :leave (fn [event]
             (run! (fn [{:keys [store command args commit-promise]}]
                     (apply command args))
                   (:effects event))
             event)}))

(defn get-subscribed-topic
  [processor]
  (get @(:topics processor) (:subscribe processor)))

(comment
  (interceptor-chain/execute
   (interceptor-chain/enqueue
    {:type :event :key :k :value :v}
    [(storage-interceptor (atom {:test-storage {:instance (atom :s)}}) [:test-storage])])))

(defn ->interceptor 
  "Transform input to Pedestal interceptor. The primary use case for
  this is to render symbols as an interceptor, allowing for dynamic resolution
  in a running system."
  [v]
  (if (symbol? v)
    (interceptor/interceptor
     {:enter (var-get (resolve v))})
    (interceptor/interceptor v)))

(defn process-event [processor event]
  (interceptor-chain/execute
   (interceptor-chain/enqueue
    (assoc event ::options (:options processor))
    (cons
     (storage-interceptor processor)
     (mapv ->interceptor (:interceptors processor))))))

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
    (.start
     (Thread.
      #(while (= :running @(:state this))
         (try 
           (when-let [event (p/poll (get-subscribed-topic this))]
             (process-event this event))
           (catch Exception e
             (clojure.stacktrace/print-stack-trace e)))))))
  (stop [this]
    (reset! (:state this) :stopped)))



(defmethod p/init :processor [processor-def]
  (-> processor-def
      (assoc :state (atom :stopped))
      #_ (update :interceptors
              #(mapv ->interceptor %))
      map->Processor))
