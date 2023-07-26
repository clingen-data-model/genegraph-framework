(ns genegraph.framework.protocol
  "Defines protocols and multimethods for entities in Genegraph")

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
  (offset [this] "Local offset for topic")
  (last-offset [this] "Last available offset for topic")
  (set-offset! [this offset] "Set local offset for topic")
  (committed-offset [this] "Committed offset for topic."))
