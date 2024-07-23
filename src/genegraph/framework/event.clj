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
  (assoc event ::value (::data event)))

(defmethod serialize :json [event]
  (assoc event ::value (json/write-str (::data event))))

(defmethod serialize :edn [event]
  (assoc event ::value (pr-str (::data event))))

(defn record-offset [event]
  (update event
          ::effects
          conj
          {:command storage/store-offset
           :args [(::backing-store event)
                  (::topic event)
                  (::offset event)]}))

(defn store
  "Add deferred write effect to event"
  [event instance k v]
  (let [commit-promise (promise)]
    (update event
            ::effects
            conj
            {:store instance
             :command storage/write
             :args [(get-in event [::storage/storage instance]) k v commit-promise]
             :commit-promise commit-promise})))

(defn delete
  "Add deferred delete effect to event"
  [event instance k]
  (let [commit-promise (promise)]
    (update event
            ::effects
            conj
            {:command storage/delete
             :args [(get-in event [::storage/storage instance]) k commit-promise]
             :commit-promise commit-promise})))

(defn publish
  "add deferred publish effect to event"
  [event publish-event]
  (update event
          ::publish
          conj
          publish-event))
