(ns genegraph.framework.protocol
  "Defines protocols and multimethods for entities in Genegraph")

(defmulti init :type)

(defprotocol Lifecycle
  (start [this])
  (stop [this]))

(defprotocol Queue
  "Protocol for a stateful queue."
  (poll [this] "Returns a value, or nil if poll request times out.")
  (offer [this x] "Adds x to queue, returns nil on timeout."))

(defprotocol Topic
  "A topic, i.e. backed by Kafka or another mechanism."
  (publish [this event] "Publish event to topic")
  (offset [this] "Local offset for topic")
  (last-offset [this] "Last available offset for topic")
  (set-offset! [this offset] "Set local offset for topic")
  (committed-offset [this] "Committed offset for topic.")
  (commit-offset! [this offset] "Commit offset to durable storage"))
