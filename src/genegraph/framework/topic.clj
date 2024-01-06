(ns genegraph.framework.topic
  "Basic functions for topic handling"
  (:require
   [genegraph.framework.protocol :as p]
   [genegraph.framework.event :as event])
  (:import
   [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(def topic-defaults
  {:timeout 1000
   :buffer-size 10})

(defrecord SimpleQueueTopic [name ^BlockingQueue queue timeout]
  p/Consumer
  (poll [this]
    (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
      (assoc e ::event/topic name)))

  p/Publisher
  (publish [this event]
    (.offer queue event timeout TimeUnit/MILLISECONDS)))

(derive SimpleQueueTopic :genegraph/topic)

(defmethod p/init :simple-queue-topic [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->SimpleQueueTopic
     (assoc topic-def-with-defaults
            :queue
            (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))))))

(comment
  (def q (p/init {:name :test-topic
                  :type :simple-queue-topic}))
  (p/publish q {:key "k" :value "v"})
  (p/poll q)

  )
