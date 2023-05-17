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
             (run! (fn [{:keys [scope store command args commit-promise]}]
                     (println scope command store args)
                     (apply command
                            (get (::s/storage event) store)
                            args))
                   (:effects event)))}))

(defn get-subscribed-topic
  [processor]
  (get @(:topics processor) (:subscribe processor)))

(comment
  (interceptor-chain/execute
   (interceptor-chain/enqueue
    {:type :event :key :k :value :v}
    [(storage-interceptor (atom {:test-storage {:instance (atom :s)}}) [:test-storage])])))


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
    (let [add-storage (storage-interceptor this)]
      (reset! (:state this) :running)
      (.start
       (Thread.
        #(while (= :running @(:state this))
           (try 
             (when-let [event (p/poll (get-subscribed-topic this))]
               (interceptor-chain/execute
                (interceptor-chain/enqueue
                 (assoc event ::options options)
                 (cons
                  add-storage
                  (:interceptors this)))))
             (catch Exception e
               (clojure.stacktrace/print-stack-trace e))))))))
  (stop [this]
    (reset! (:state this) :stopped)))

(defmethod p/init :processor [processor-def]
  (-> processor-def
      (assoc :state (atom :stopped))
      (update :interceptors
              #(mapv interceptor/interceptor %))
      map->Processor))
