(ns genegraph.framework.protocol
  "Defines protocols, multimethods and commonly used functions
  for entities in Genegraph.")

(defmulti init :type)

(defmulti log-event #(get-in % [:genegraph.framework.event/data :type]))

(defprotocol Lifecycle
  (start [this])
  (stop [this]))

(defprotocol Resetable
  "Reset the given resource to its initial state. Behavior can be modified with passed options.

  For topics, will reset offsets, but not destroy data stored in Kafka by default. Will reset offsets for consumer-group backed topics. Setting {:recreate-kafka-topic true} in opts will delete and recreate the Kafka topic, the cluster will preserve the permissions on the existing topic.

  For storage interfaces, will destroy all locally stored data. Setting {:destroy-snapshot true} in opts will also attempt to delete the upstream snapshot backing the data.

  Assumes the entity has not been started, but has been initialized."
  (reset [this]))

(defprotocol Consumer
  "For topics allowing consumption of records"
  (poll [this] "Returns a value, or nil if poll request times out."))

(defprotocol Publisher
  "For topics allowing publication of records"
  (publish [this event] "Publish event to topic"))

(defprotocol Offsets
  "For topics supporting offsets"
  (set-offset! [this offset] "Set local offset for topic"))

(defprotocol EventProcessor
  "Protocol for processor objects"
  (process [this event] "Process event, including side effects"))

(defprotocol StatefulInterceptors
  "Protocol for processors that are capable of representing their
  operation as a sequence of interceptors."
  (as-interceptors [this] "Return a sequence of interceptors to perform the event handling for an event"))

(defn running?
  "True if component is running. Depends on component having a :state
  atom with a :status field reporting its current state."
  [this]
  (= :running (-> this :state deref :status)))

(defn system-update
  "Called when a component reaches a lifecycle milestone (exceptional or unexceptional).
  Reports the event to the system topic associated with the component."
  [this detail]
  (when-let [t (:system-topic this)]
    (publish t {:genegraph.framework.event/data
                (assoc detail
                       :source (:name this)
                       :entity-type (type this))})))

(defn exception
  "Registers exception e occurred in entity this"
  [this e]
  (swap! (:state this)
         assoc
         :status :exception
         :exception e)
  (if (:system-topic this)
    (system-update
     this
     {:source this
      :type :exception
      :exception e})))
