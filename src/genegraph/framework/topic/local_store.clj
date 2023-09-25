(ns genegraph.framework.topic.local-store
  "Local store-backed topic. Default (and perhaps only) implementation to be rocks db backed.
  Topic does not "
  (:require 
   [genegraph.framework.protocol :as p]
   [genegraph.framework.event :as event]
   [genegraph.framework.storage :as storage])
  (:import
   [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(defn- init-offsets! [{:keys [current-offset max-offset]} offset]
  (reset! current-offset offset)
  (reset! max-offset offset))

(defrecord LocalStoreTopic
    [name
     buffer-size
     timeout
     ^BlockingQueue queue
     status
     exception
     current-offset
     max-offset
     event-store
     event-store-instance
     event-store-def]
  
  p/Lifecycle
  (start [this]
    (p/start event-store)
    (reset! event-store-instance @(:instance event-store))
    (let [offset (storage/read @event-store-instance :max-offset)]
      (if (= ::storage/miss offset)
        (init-offsets! this 0)
        (init-offsets! this offset))))
  
  (stop [this]
    (storage/write @event-store-instance :max-offset @max-offset)
    (p/stop event-store))

  p/Publisher
  (publish [this event]
    )
  )



(defmethod p/init :local-store-topic [topic-definition]
  (-> topic-definition
      (assoc :event-store (p/init (:event-store-def topic-definition)))
      (assoc :event-store-instance (atom nil))
      (assoc :current-offset (atom nil))
      (assoc :max-offset (atom nil))
      map->LocalStoreTopic))

(comment
  (def t (p/init
          {:type :local-store-topic
           :name :test-local-store
           :event-store-def {:type :rocksdb
                             :name :test-local-store-event-store
                             :path "/users/tristan/data/genegraph-neo/rocks-event-store"}}))

  (-> t :event-store type)
  (-> t :event-store-instance deref type)
  (p/start t)
  (p/stop t)

  (name :a/this)
  )
