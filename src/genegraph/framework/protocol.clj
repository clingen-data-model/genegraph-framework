(ns genegraph.framework.protocol
  "Defines protocols, multimethods, and commonly used functions
  for entities in Genegraph.")

(defmulti init :type)

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

(defn running? [this]
  (= :running (-> this :state deref :status)))

(defn exception
  "Registers exception e occurred in entity this"
  [this e]
  (println "exception " (type e) " in " (:name this) )
  (swap! (:state this)
         assoc
         :status :exception
         :exception e))
