(ns genegraph.framework.topic.local-store
  " WIP -- needs more design effort.

  Local store-backed topic. Default (and perhaps only) implementation to be rocks db backed.
  Topic does not "
  (:require 
   [genegraph.framework.protocol :as p]
   [genegraph.framework.event :as event]
   [genegraph.framework.storage :as storage])
  (:import
   [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(def topic-defaults
  {:timeout 1000
   :buffer-size 10})

(defn- init-offsets! [{:keys [state]} offset]
  (swap! state
         assoc
         :current-offset offset
         :max-offset offset))

(defn handle-published-event [topic]
  (.poll publisher-queue timeout TimeUnit/MILLISECONDS))

(defn handle-stored-event [topic]
  (storage/read @(:instance event-store) current-offset))

(defrecord LocalStoreTopic
    [name
     buffer-size
     timeout
     ^BlockingQueue consumer-queue
     ^BlockingQueue publisher-queue
     state
     event-store]
  
  p/Lifecycle
  (start [this]
    (p/start event-store)
    (let [offset (-> event-store :instance deref (storage/read :max-offset))]
      (if (= ::storage/miss offset)
        (init-offsets! this 0)
        (init-offsets! this offset)))
    (swap! state assoc :status :running)
    (.start
     (Thread.
      (fn []
        (while (= :running (:status @state))
          (let [{:keys [current-offset max-offset]} @state]
            (if (= current-offset max-offset)
              
              (handle-stored-event this))))))))
  
  (stop [this]
    (swap! state assoc :status :stopped)
    (-> event-store :instance deref (storage/write :max-offset (:max-offset @state)))
    (p/stop event-store))

  p/Offsets
  (offset [this] "Local offset for topic")
  (last-offset [this] "Last available offset for topic")
  (set-offset! [this offset] "Set local offset for topic")
  (committed-offset [this] "Committed offset for topic.")

  p/Publisher
  (publish [this event]
    (storage/write @(:instance event-store) (:max-offset @state) event)
    (swap! state update :max-offset inc))

  p/Consumer
  (poll [this]
    (.poll consumer-queue timeout TimeUnit/MILLISECONDS)))


(defmethod p/init :local-store-topic [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults topic-definition)]
    (-> topic-def-with-defaults
        (assoc :event-store (p/init (:event-store-def topic-def-with-defaults)))
        (assoc :state (atom {}))
        (assoc :consumer-queue (ArrayBlockingQueue. 1))
        (assoc :publisher-queue (ArrayBlockingQueue.
                                (:buffer-size topic-def-with-defaults)))
        map->LocalStoreTopic)))

(comment
  (def t (p/init
          {:type :local-store-topic
           :name :test-local-store
           :event-store-def {:type :rocksdb
                             :name :test-local-store-event-store
                             :path "/users/tristan/data/genegraph-neo/rocks-event-store"}}))

  (-> t :event-store type)
  (-> t :event-store :instance deref type)
  (p/start t)
  (p/stop t)

  )
