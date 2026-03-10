(ns genegraph.framework.topic
  "Basic functions for topic handling"
  (:require
   [genegraph.framework.protocol :as p]
   [genegraph.framework.event :as event])
  (:import
   [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]
   [java.time Instant]))

(def topic-defaults
  {:timeout 1000
   :buffer-size 10})

(defrecord SimpleQueueTopic [name ^BlockingQueue queue timeout]
  p/Consumer
  (poll [this]
    (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
      (assoc e
             ::event/topic name
             ::event/completion-promise (::event/completion-promise e (promise)))))

  p/Publisher
  (publish [this event]
    (when-let [p (:commit-promise event)]
      (deliver p true))
    (.offer queue event timeout TimeUnit/MILLISECONDS))

  p/Status
  (status [this]
    {:name name
     :queue-size (.size queue)
     :remaining-capacity (.remainingCapacity queue)}))

(derive SimpleQueueTopic :genegraph/topic)

(defmethod p/init :simple-queue-topic [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->SimpleQueueTopic
     (assoc topic-def-with-defaults
            :queue
            (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))))))

;;; TimerTopic
;;
;; Produces a timer-tick event at a fixed interval. The queue has capacity 1;
;; if the consumer is slow a tick is dropped rather than accumulated.
;; `:interval` is in milliseconds. `:timeout` controls how long `poll` blocks.

(defrecord TimerTopic [name interval timeout ^ArrayBlockingQueue queue state system-topic]
  p/Consumer
  (poll [this]
    (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
      (assoc e ::event/topic name)))

  p/Lifecycle
  (start [this]
    (when-not (= :running (:status @state))
      (p/system-update this {:type :starting})
      (swap! state assoc :status :running)
      (let [thread (Thread/startVirtualThread
                    (fn []
                      (try
                        (while (= :running (:status @state))
                          (Thread/sleep (long interval))
                          (when (= :running (:status @state))
                            (.offer queue
                                    {::event/data {:type :timer-tick
                                                   :timestamp (Instant/now)}
                                     ::event/completion-promise (promise)})))
                        (catch InterruptedException _
                          nil))))]
        (swap! state assoc :thread thread))
      (p/system-update this {:type :started})))

  (stop [this]
    (swap! state assoc :status :stopped)
    (when-let [t (:thread @state)]
      (.interrupt t)))

  p/Status
  (status [this]
    {:name name
     :interval interval
     :queue-size (.size queue)
     :status (:status @state)}))

(derive TimerTopic :genegraph/topic)

(defmethod p/init :timer-topic [topic-def]
  (map->TimerTopic
   (assoc topic-def
          :timeout (get topic-def :timeout 1000)
          :queue (ArrayBlockingQueue. 1)
          :state (atom {:status :stopped}))))

(comment
  (def q (p/init {:name :test-topic
                  :type :simple-queue-topic}))
  (p/publish q {:key "k" :value "v"})
  (p/poll q)
  (p/status q)

  )
