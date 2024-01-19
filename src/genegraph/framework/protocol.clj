(ns genegraph.framework.protocol
  "Defines protocols, multimethods and commonly used functions
  for entities in Genegraph.")

(defmulti init :type)

(defmulti log-event :type)

(defprotocol Lifecycle
  (start [this])
  (stop [this]))

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

(defn publish-system-update
  "Called when a component reaches a lifecycle milestone (exceptional or unexceptional).
  Reports the event to the system topic associated with the component."
  [this event]
  (when-let [t (:system-topic this)]
    (publish t event)))

(defn exception
  "Registers exception e occurred in entity this"
  [this e]
  (swap! (:state this)
         assoc
         :status :exception
         :exception e)
  (if (:system-topic this)
    (publish-system-update
     this
     {:source this
      :type :exception
      :exception e})))
