(ns genegraph.framework.event
  "Protocols, multimethods, and a common namespace for operations
  on events"
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [genegraph.framework.storage :as storage]))

(defmulti add-model ::format)

(defmulti deserialize ::format)

(defmulti serialize ::format)

;; just return event if serialization not defined
(defmethod deserialize :default [event]
  event)

(defmethod deserialize :json [event]
  (assoc event ::data (json/read-str (::value event) :key-fn keyword)))

(defmethod deserialize :edn [event]
  (assoc event ::data (edn/read-string (::value event))))

(defmethod serialize :default [event]
  event)

(defmethod serialize :json [event]
  (assoc event ::value (json/write-str (::data event))))

(defmethod serialize :edn [event]
  (assoc event ::value (pr-str (::data event))))

(defn store
  "Add deferred write effect to event"
  [event instance k v]
  (let [commit-promise (promise)]
    (update event
            :effects
            conj
            {:command storage/write
             :args [(get-in event [::storage/storage instance]) k v commit-promise]
             :commit-promise commit-promise})))


(defn publish [event topic publish-event]
  "add deferred publish effect to event"
  (update event
          :publish-events
          conj
          {:topic topic
           :event publish-event}))
